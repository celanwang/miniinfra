package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"runtime/metrics"
	"strconv"
	"strings"
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
	mux.HandleFunc("GET /runtime", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(pressureRuntime(s))
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

func pressureRuntime(s *Server) map[string]float64 {
	values := map[string]float64{
		"InboundBytes": float64(s.inbound.used.Load()), "OutboundBytes": float64(s.outbound.used.Load()),
	}
	for file, fields := range map[string]map[string]string{
		"/proc/self/status":            {"VmRSS:": "RSSKiB", "VmHWM:": "RSSPeakKiB"},
		"/sys/fs/cgroup/cpu.stat":      {"usage_usec": "CPUUsec", "throttled_usec": "ThrottledUsec"},
		"/sys/fs/cgroup/memory.events": {"oom": "OOM", "oom_kill": "OOMKill"},
	} {
		raw, _ := os.ReadFile(file)
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				if name, ok := fields[parts[0]]; ok {
					values[name], _ = strconv.ParseFloat(parts[1], 64)
				}
			}
		}
	}
	for file, name := range map[string]string{"memory.current": "CgroupBytes", "memory.peak": "CgroupPeakBytes"} {
		raw, err := os.ReadFile("/sys/fs/cgroup/" + file)
		if err == nil {
			values[name], _ = strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
		}
	}
	samples := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
	}
	metrics.Read(samples)
	for i, name := range []string{"GCCycles", "LiveHeapBytes", "AllocBytes", "GCCPUSeconds", "GCAssistSeconds"} {
		value := samples[i].Value
		switch value.Kind() {
		case metrics.KindUint64:
			values[name] = float64(value.Uint64())
		case metrics.KindFloat64:
			values[name] = value.Float64()
		}
	}
	return values
}
