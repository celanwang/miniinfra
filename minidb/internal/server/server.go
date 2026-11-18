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
		readErr = readRequests(ctx, conn, requests, disconnect)
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
			response = failureResponse(err)
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
		send(conn, failureResponse(fmt.Errorf("read request: %w", readErr)))
	}
}

func readRequests(ctx context.Context, conn net.Conn, requests chan<- wire.Request, disconnect context.CancelFunc) error {
	reader := wire.Reader(conn)
	for {
		var req wire.Request
		if err := wire.Read(reader, &req); err != nil {
			// FIN may be a half-close. Finish processing accepted requests
			// and send their replies. A transport error cancels execution.
			if errors.Is(err, io.EOF) {
				return nil
			}
			if reader.Err() != nil && !errors.Is(err, bufio.ErrTooLong) {
				disconnect()
				return nil
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

func failureResponse(err error) wire.Response {
	code := "execution"
	switch {
	case errors.Is(err, engine.ErrUnique):
		code = "unique"
	case errors.Is(err, engine.ErrDeadlock):
		code = "deadlock"
	case errors.Is(err, engine.ErrLockWait):
		code = "lock_wait"
	case errors.Is(err, engine.ErrCommitUnknown):
		code = "commit_unknown"
	case errors.Is(err, engine.ErrConstraint):
		code = "constraint"
	case errors.Is(err, engine.ErrUnavailable):
		code = "unavailable"
	}
	return wire.Response{Code: code, Message: err.Error()}
}

func send(conn net.Conn, response wire.Response) bool {
	data, err := wire.Encode(response)
	if err != nil {
		// Oversized successful SELECT results may leave an active transaction.
		// Close the session after reporting the error so its locks are released.
		data, _ = wire.Encode(failureResponse(err))
	}
	if deadlineErr := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
		return false
	}
	_, writeErr := conn.Write(data)
	return err == nil && writeErr == nil
}
