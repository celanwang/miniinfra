package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/query"
	"github.com/celanwang/miniinfra/minidb/internal/wire"
)

const maxSessions = 128

func Serve(ctx context.Context, listener net.Listener, db *engine.DB) error {
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var clients sync.WaitGroup
	defer clients.Wait()
	slots := make(chan struct{}, maxSessions)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
			clients.Go(func() {
				defer func() { <-slots }()
				serveConnection(ctx, conn, db)
			})
		default:
			conn.Close()
		}
	}
}

func serveConnection(ctx context.Context, conn net.Conn, db *engine.DB) {
	ctx, disconnect := context.WithCancel(ctx)
	var receiving sync.WaitGroup
	defer func() {
		disconnect()
		conn.Close()
		receiving.Wait()
	}()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	session := query.NewSession(db)
	defer session.Close()

	if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
		return
	}
	// The receiver keeps reading while execution waits for a lock. Only the
	// executor touches the session, including rollback on disconnect.
	requests := make(chan wire.Request)
	var readErr error
	receiving.Go(func() {
		defer close(requests)
		readErr = readRequests(ctx, conn, requests)
		if errors.Is(readErr, io.EOF) {
			disconnect()
		}
	})
	for req := range requests {
		if ctx.Err() != nil {
			return
		}
		statement, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := session.Execute(statement, req.SQL, req.Args)
		cancel()
		response := wire.Response{Result: result}
		if err != nil {
			response = wire.Failure(err)
		}
		inTx := session.InTransaction()
		response.InTransaction = &inTx
		if !send(conn, response) {
			return
		}
		if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
			return
		}
	}
	if readErr != nil && ctx.Err() == nil {
		send(conn, wire.Failure(fmt.Errorf("read request: %w", readErr)))
	}
}

func readRequests(ctx context.Context, conn net.Conn, requests chan<- wire.Request) error {
	reader := wire.Reader(conn)
	for {
		var req wire.Request
		if err := wire.Read(reader, &req); err != nil {
			// Scanner errors other than frame size indicate the end of the
			// transport. Malformed frames still get a protocol error.
			if reader.Err() != nil && !errors.Is(err, bufio.ErrTooLong) {
				return io.EOF
			}
			return err
		}
		select {
		case requests <- req:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func send(conn net.Conn, response wire.Response) bool {
	data, err := wire.Encode(response)
	if err != nil {
		// Oversized successful SELECT results may leave an active transaction.
		// Close the session after reporting the error so its locks are released.
		data, _ = wire.Encode(wire.Failure(err))
	}
	if deadlineErr := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		return false
	}
	_, writeErr := conn.Write(data)
	return err == nil && writeErr == nil
}
