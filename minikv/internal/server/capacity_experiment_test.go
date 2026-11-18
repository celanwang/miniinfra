package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func TestCapacityPeer(t *testing.T) {
	if os.Getenv("MINIKV_CAPACITY_PEER") != "1" {
		t.Skip("requires the capacity experiment driver")
	}
	cache, err := store.New(store.Config{
		Shards: 16, BytesPerShard: 4 << 20, EntriesPerShard: 4096, Samples: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cache, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:7321")
	if err != nil {
		t.Fatal(err)
	}

	var peak atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		shards := make([]store.ShardStats, cache.ShardCount())
		for i := range shards {
			shards[i] = cache.Stats(i)
		}
		_ = json.NewEncoder(w).Encode(struct {
			Shards       []store.ShardStats
			OutboundPeak int64
		}{shards, peak.Load()})
	})
	admin := &http.Server{Addr: "127.0.0.1:9122", Handler: mux, ReadHeaderTimeout: time.Second}
	defer admin.Close()
	go func() {
		_ = admin.ListenAndServe()
	}()
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-tick.C:
				used := s.outbound.used.Load()
				if used > peak.Load() {
					peak.Store(used)
				}
			}
		}
	}()
	if err := s.Serve(context.Background(), listener); err != nil {
		t.Fatal(err)
	}
}
