package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/server"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:7432", "TCP listen address")
	dir := flag.String("data", ".", "existing data directory")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *addr, *dir); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr, dir string) (err error) {
	db, err := engine.Open(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("MiniDB listening on %s", listener.Addr())
	return server.Serve(ctx, listener, db)
}
