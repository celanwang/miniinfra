package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/celanwang/miniinfra/minimq"
	"github.com/celanwang/miniinfra/minimq/internal/maintenance"
)

func main() {
	var cfg minimq.Config
	dir := flag.String("dir", "./minimq-data", "partition data directory")
	listen := flag.String("listen", "127.0.0.1:7400", "HTTP listen address")
	uncertainty := flag.Duration("clock-error", 0, "claimed local clock error bound")
	flag.Int64Var(&cfg.CapacityBytes, "capacity", 0, "storage budget in bytes (0 uses the default)")
	flag.DurationVar(&cfg.HistoryRetention, "history-retention", 0, "history retention (0 uses the default)")
	flag.DurationVar(&cfg.DeadRetention, "dead-retention", 0, "dead-letter retention (0 uses the default)")
	flag.Parse()
	if *uncertainty < 0 {
		log.Fatal("clock error bound must be nonnegative")
	}
	cfg.Clock = func() (minimq.TimeBounds, error) {
		now := time.Now().UTC()
		return minimq.TimeBounds{Lower: now.Add(-*uncertainty), Upper: now.Add(*uncertainty)}, nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *dir, *listen, cfg); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dir, address string, cfg minimq.Config) (err error) {
	p, err := minimq.Open(dir, cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, p.Close()) }()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: routes(p), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute,
	}
	work, cancel := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		maintain(work, p)
	}()
	defer func() {
		cancel()
		<-maintenanceDone
	}()
	log.Printf("MiniMQ listening on %s", listener.Addr())
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	select {
	case err := <-serving:
		return errors.Join(err, server.Close())
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Shutdown(shutdown); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}
}

func maintain(ctx context.Context, p *minimq.Partition) {
	limit := minimq.Limits{Messages: minimq.MaxBatchMessages, Bytes: minimq.MaxBatchBytes}
	// Below this count the byte budget can still fit any valid message.
	minFullBatch := min(limit.Messages, limit.Bytes/minimq.MaxMessageBytes)
	maintenance.Run(ctx, func() bool {
		count, err := p.AdvanceScheduled(limit)
		if err != nil {
			log.Printf("scheduled delivery maintenance: %v", err)
		}
		return count >= minFullBatch
	}, func() {
		if _, err := p.AdvanceRetries(limit); err != nil {
			log.Printf("retry maintenance: %v", err)
		}
	}, func() {
		if err := p.Compact(); err != nil {
			log.Printf("checkpoint: %v", err)
		}
	})
}
