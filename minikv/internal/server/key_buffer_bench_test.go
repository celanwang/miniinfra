package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

// Signal after serveConn reaches its first read, without sending a frame.
type keyProbeListener struct {
	net.Listener
	ready chan struct{}
}

func (l keyProbeListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &keyProbeConn{Conn: c, ready: l.ready}, nil
}

type keyProbeConn struct {
	net.Conn
	ready chan struct{}
}

func (c *keyProbeConn) Read(p []byte) (int, error) {
	if c.ready != nil {
		c.ready <- struct{}{}
		c.ready = nil
	}
	return c.Conn.Read(p)
}

func benchmarkKeyBufferTCP(b *testing.B, maxKeyBytes, connections int) (string, chan struct{}, []byte, []byte) {
	b.Helper()
	cache, err := store.New(store.Config{Shards: 1, BytesPerShard: 1 << 20, EntriesPerShard: 8, Samples: 4})
	if err != nil {
		b.Fatal(err)
	}
	long := strings.Repeat("l", maxKeyBytes)
	if cache.Set([]byte("k"), []byte("old"), 0, false) != store.Stored ||
		cache.Set([]byte(long), []byte("old"), 0, false) != store.Stored {
		b.Fatal("could not prefill key buffer benchmark")
	}
	cfg := DefaultConfig()
	cfg.MaxKeyBytes = maxKeyBytes
	s, err := New(cache, cfg)
	if err != nil {
		b.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ready := make(chan struct{}, connections)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, keyProbeListener{listener, ready}) }()
	b.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			b.Error(err)
		}
	})
	return listener.Addr().String(), ready, []byte("GET 1\nk"), fmt.Appendf(nil, "GET %d\n%s", len(long), long)
}

func dialKeyProbe(b *testing.B, address string, ready <-chan struct{}) *client {
	b.Helper()
	c, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	select {
	case <-ready:
	case <-time.After(time.Second):
		b.Fatal("server did not reach its first read")
	}
	return &client{c, bufio.NewReaderSize(c, headerBytes)}
}

func keyProbeGET(b *testing.B, c *client, frame []byte, value *[3]byte) {
	b.Helper()
	if _, err := c.Write(frame); err != nil {
		b.Fatal(err)
	}
	line, err := c.reader.ReadSlice('\n')
	if err != nil || string(line) != "VALUE 3\n" {
		b.Fatalf("GET header = %q (%v)", line, err)
	}
	if _, err := io.ReadFull(c.reader, value[:]); err != nil || string(value[:]) != "old" {
		b.Fatalf("GET value = %q (%v)", value[:], err)
	}
}

func BenchmarkTCPKeyBufferPool(b *testing.B) {
	for _, maxKeyBytes := range []int{1024, 64 << 10} {
		b.Run(fmt.Sprint(maxKeyBytes), func(b *testing.B) {
			const connections = 32
			address, ready, short, long := benchmarkKeyBufferTCP(b, maxKeyBytes, connections)
			var value [3]byte
			heap := func() int64 {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				return int64(m.HeapAlloc)
			}
			for b.Loop() {
				base := heap()
				var pool [connections]*client
				for i := range pool {
					pool[i] = dialKeyProbe(b, address, ready)
				}
				b.ReportMetric(float64(heap()-base)/connections, "idle-B/conn")
				for _, c := range pool {
					keyProbeGET(b, c, short, &value)
				}
				b.ReportMetric(float64(heap()-base)/connections, "short-B/conn")
				for i, c := range pool {
					if i < 4 {
						keyProbeGET(b, c, long, &value)
					}
					keyProbeGET(b, c, short, &value)
				}
				b.ReportMetric(float64(heap()-base)/connections, "occasional-B/conn")
				for range 8 {
					for _, c := range pool {
						keyProbeGET(b, c, long, &value)
					}
				}
				b.ReportMetric(float64(heap()-base)/connections, "long-B/conn")
				for _, c := range pool {
					_ = c.Close()
				}
			}
		})
	}
}

func BenchmarkTCPKeyBufferGET(b *testing.B) {
	for _, maxKeyBytes := range []int{1024, 64 << 10} {
		for _, name := range []string{"short", "long"} {
			b.Run(fmt.Sprintf("%d/%s", maxKeyBytes, name), func(b *testing.B) {
				address, ready, short, long := benchmarkKeyBufferTCP(b, maxKeyBytes, 1)
				c := dialKeyProbe(b, address, ready)
				frame := short
				if name == "long" {
					frame = long
				}
				var value [3]byte
				keyProbeGET(b, c, frame, &value)
				b.ReportAllocs()
				for b.Loop() {
					keyProbeGET(b, c, frame, &value)
				}
			})
		}
	}
}
