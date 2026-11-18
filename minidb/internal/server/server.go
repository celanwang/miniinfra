package server

import (
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
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	session := query.NewSession(db)
	defer session.Close()
	reader := wire.Reader(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
			return
		}
		var req wire.Request
		if err := wire.Read(reader, &req); err != nil {
			if !errors.Is(err, io.EOF) {
				send(conn, wire.Failure(fmt.Errorf("read request: %w", err)))
			}
			return
		}
		statement, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := session.Execute(statement, req.SQL, req.Args)
		cancel()
		response := wire.Response{Result: result}
		if err != nil {
			response = wire.Failure(err)
		}
		if !send(conn, response) {
			return
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
