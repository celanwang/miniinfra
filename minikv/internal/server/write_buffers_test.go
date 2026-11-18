package server

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func TestWriteBuffersUseExactCapacityAndStayBounded(t *testing.T) {
	const size = minReusableWriteBytes
	p := writeBuffers{limit: 2 * size}
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() { p.put(p.take(size)) })
	}
	workers.Wait()

	p.mu.Lock()
	retained := p.bytes
	p.mu.Unlock()
	if retained == 0 || retained > p.limit {
		t.Fatalf("retained %d bytes, limit %d", retained, p.limit)
	}
	if got := p.take(size + 1); len(got) != size+1 || cap(got) != size+1 {
		t.Fatalf("unexpected buffer dimensions: %d/%d", len(got), cap(got))
	}
	before := p.bytes
	small := p.take(size - 1)
	p.put(small)
	if p.bytes != before {
		t.Fatal("small receive buffer was retained")
	}
}

func TestLargeWritesKeepBufferOwnershipAndFrameBoundary(t *testing.T) {
	cache, err := store.New(store.Config{
		Shards: 1, BytesPerShard: 256 << 10, EntriesPerShard: 4, Samples: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cache, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	c := s.pipe(t)
	payload := strings.Repeat("a", 128<<10)
	replace := strings.Repeat("b", len(payload))
	wrong := strings.Repeat("x", len(payload))
	set := func(value, option string) string {
		return fmt.Sprintf("SET 1 %d NONE%s\nk%s", len(value), option, value)
	}

	c.exchange(t, set(payload, " NX"), "STORED\n")
	c.exchange(t, set(wrong, " NX"), "EXISTS\n")
	c.send(t, "GET 1\nk")
	c.value(t, payload)
	c.exchange(t, set(replace, ""), "STORED\n")
	c.exchange(t, set(wrong, " NX"), "EXISTS\n")
	before := s.cache.Stats(0).UsedBytes
	adjacent := strings.Repeat("d", len(replace)-1)
	c.exchange(t, set(adjacent, ""), "STORED\n")
	if bytes := s.cache.Stats(0).UsedBytes; bytes != before-1 {
		t.Fatalf("adjacent write charged %d bytes, want %d", bytes, before-1)
	}

	// A rejected frame followed by a GET on the same connection must leave
	// both the existing value and the next request frame intact.
	oversized := strings.Repeat("c", 300<<10)
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(c, set(oversized, "")+"GET 1\nk")
		done <- err
	}()
	c.line(t, "NO_SPACE\n")
	c.value(t, adjacent)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	c.send(t, fmt.Sprintf("SET 1 %d NONE\nkn", len(replace)))
	waitBudget(t, &s.inbound, int64(len(replace)+1))
	_ = c.Close()
	waitBudget(t, &s.inbound, 0)
	other := s.pipe(t)
	other.send(t, "GET 1\nk")
	other.value(t, adjacent)
}

func TestRoundedReceiveCapacityUsesAndReturnsInboundBudget(t *testing.T) {
	const bucket = 128 << 10
	for _, test := range []struct {
		name     string
		perConn  int64
		reserved int64
	}{
		{"rounds", bucket + 1, bucket + 1},
		{"falls back to exact capacity", bucket, bucket},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := testServer(t, 2, func(c *Config) { c.InboundPerConn = test.perConn })
			client := s.pipe(t)
			client.send(t, fmt.Sprintf("SET 1 %d NONE\nkx", bucket-1))
			waitBudget(t, &s.inbound, test.reserved)
			_ = client.Close()
			waitBudget(t, &s.inbound, 0)
			other := s.pipe(t)
			other.exchange(t, "GET 1\nk", "MISS\n")
		})
	}
}

func TestWriteBuffersAdmitsNewSizeAfterFullOldPool(t *testing.T) {
	for _, sizes := range [][2]int{{1 << 20, 512 << 10}, {512 << 10, 1 << 20}} {
		t.Run(fmt.Sprint(sizes[0]), func(t *testing.T) {
			p := writeBuffers{limit: maxIdleWriteBytes}
			for range int(p.limit) / sizes[0] {
				p.put(make([]byte, sizes[0]))
			}
			if p.bytes != p.limit {
				t.Fatalf("first size retained %d bytes, want %d", p.bytes, p.limit)
			}
			next := p.take(sizes[1])
			p.put(next)
			oldReleased := (sizes[1] + sizes[0] - 1) / sizes[0] * sizes[0]
			wantBytes := p.limit - int64(oldReleased) + int64(sizes[1])
			var capacity int64
			for _, idle := range p.idle {
				capacity += int64(cap(idle))
			}
			if p.bytes != wantBytes || p.bytes != capacity || p.bytes > p.limit {
				t.Fatalf("idle accounting %d, actual %d, want %d with limit %d", p.bytes, capacity, wantBytes, p.limit)
			}
			reused := p.take(sizes[1])
			if &next[0] != &reused[0] {
				t.Fatalf("full pool retained the old size and discarded the new %d-byte buffer", sizes[1])
			}
		})
	}
}
