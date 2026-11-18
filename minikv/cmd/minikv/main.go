package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/celanwang/miniinfra/minikv/internal/server"
	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("minikv", flag.ContinueOnError)
	flags.SetOutput(output)
	cfg := server.DefaultConfig()
	listen := flags.String("listen", "127.0.0.1:7321", "TCP listen address")
	shards := flags.Int("shards", 16, "number of cache shards")
	cacheBytes := flags.Int64("cache-bytes", 64<<20, "total cache accounting budget in bytes")
	entries := flags.Int("entries", 65536, "total cache slot limit")
	samples := flags.Int("eviction-samples", 16, "maximum slots probed during a write")
	flags.IntVar(&cfg.MaxConnections, "max-connections", cfg.MaxConnections, "maximum active connections")
	flags.IntVar(&cfg.MaxKeyBytes, "max-key-bytes", cfg.MaxKeyBytes, "maximum key length in bytes")
	flags.IntVar(&cfg.MaxValueBytes, "max-value-bytes", cfg.MaxValueBytes, "maximum value length in bytes")
	flags.Int64Var(&cfg.InboundBytes, "inbound-bytes", cfg.InboundBytes, "total in-flight request byte limit")
	flags.Int64Var(&cfg.OutboundBytes, "outbound-bytes", cfg.OutboundBytes, "total in-flight value byte limit")
	flags.Int64Var(&cfg.InboundPerConn, "inbound-per-conn", cfg.InboundPerConn, "in-flight request byte limit per connection")
	flags.Int64Var(&cfg.OutboundPerConn, "outbound-per-conn", cfg.OutboundPerConn, "in-flight value byte limit per connection")
	flags.DurationVar(&cfg.ReadTimeout, "read-timeout", cfg.ReadTimeout, "deadline for reading a complete request")
	flags.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "deadline for writing a complete response")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *shards < 1 {
		return errors.New("provide a positive shard count and use flags for all options")
	}

	cacheConfig := store.Config{
		Shards:          *shards,
		BytesPerShard:   *cacheBytes / int64(*shards),
		EntriesPerShard: *entries / *shards,
		Samples:         *samples,
	}
	if err := cacheConfig.Validate(); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()

	cache, err := store.New(cacheConfig)
	if err != nil {
		return err
	}
	s, err := server.New(cache, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(output, "MiniKV listening on", listener.Addr())
	return s.Serve(ctx, listener)
}
