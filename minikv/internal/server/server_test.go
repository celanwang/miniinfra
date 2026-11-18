package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func testServer(t *testing.T, entries int, configure func(*Config)) *Server {
	t.Helper()
	cache, err := store.New(store.Config{
		Shards: 1, BytesPerShard: 1024, EntriesPerShard: entries, Samples: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	if configure != nil {
		configure(&cfg)
	}
	s, err := New(cache, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type client struct {
	net.Conn
	reader *bufio.Reader
}

func (s *Server) pipe(t *testing.T) *client {
	t.Helper()
	server, peer := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.serveConn(server)
	}()
	t.Cleanup(func() {
		_ = peer.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not close the connection")
		}
	})
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	return &client{peer, bufio.NewReader(peer)}
}

func (c *client) send(t *testing.T, frame string) {
	t.Helper()
	if _, err := io.WriteString(c, frame); err != nil {
		t.Fatal(err)
	}
}

func (c *client) line(t *testing.T, expected string) {
	t.Helper()
	line, err := c.reader.ReadString('\n')
	if err != nil || line != expected {
		t.Fatalf("reply = %q (%v), want %q", line, err, expected)
	}
}

func (c *client) exchange(t *testing.T, frame, expected string) {
	t.Helper()
	c.send(t, frame)
	c.line(t, expected)
}

func (c *client) value(t *testing.T, expected string) {
	t.Helper()
	c.line(t, fmt.Sprintf("VALUE %d\n", len(expected)))
	var payload = make([]byte, len(expected))
	if _, err := io.ReadFull(c.reader, payload); err != nil || string(payload) != expected {
		t.Fatalf("value = %q (%v), want %q", payload, err, expected)
	}
}

func (c *client) end(t *testing.T) {
	t.Helper()
	if _, err := c.reader.ReadByte(); err != io.EOF {
		t.Fatalf("end of connection: %v", err)
	}
}

func waitBudget(t *testing.T, b *budget, want int64) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for b.used.Load() != want {
		if time.Now().After(end) {
			t.Fatalf("budget = %d, want %d", b.used.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPartialRequestReturnsInboundBudget(t *testing.T) {
	for _, ending := range []string{"deadline", "disconnect"} {
		t.Run(ending, func(t *testing.T) {
			s := testServer(t, 2, func(c *Config) {
				c.InboundBytes = 16
				c.ReadTimeout = 300 * time.Millisecond
			})
			c := s.pipe(t)
			c.exchange(t, "SET 1 3 NONE\nkold", "STORED\n")
			c.send(t, "SET 1 15 1000\nkpartial")
			waitBudget(t, &s.inbound, 16)
			other := s.pipe(t)
			other.exchange(t, "GET 1\nk", "BUSY\n")
			other.end(t)
			if ending == "deadline" {
				c.end(t)
			} else {
				_ = c.Close()
			}
			waitBudget(t, &s.inbound, 0)
			other = s.pipe(t)
			other.send(t, "GET 1\nk")
			other.value(t, "old")
		})
	}
}

func TestEvictedValueOutlivesSlotWhileReplyIsBlocked(t *testing.T) {
	for _, ending := range []string{"success", "deadline", "disconnect"} {
		t.Run(ending, func(t *testing.T) {
			s := testServer(t, 1, func(c *Config) {
				c.OutboundBytes = 3
				c.WriteTimeout = 350 * time.Millisecond
			})
			slow := s.pipe(t)
			other := s.pipe(t)
			other.exchange(t, "SET 1 3 NONE\nkold", "STORED\n")
			slow.exchange(t, "GET 1\nk", "VALUE 3\n")
			waitBudget(t, &s.outbound, 3)

			other.exchange(t, "GET 1\nk", "BUSY\n")
			other.exchange(t, "SET 1 3 NONE NX\nnnew", "STORED\n")
			other.exchange(t, "GET 1\nk", "MISS\n")
			other.exchange(t, "GET 1\nn", "BUSY\n")
			waitBudget(t, &s.outbound, 3)
			switch ending {
			case "success":
				payload := make([]byte, 3)
				if _, err := io.ReadFull(slow.reader, payload); err != nil || string(payload) != "old" {
					t.Fatalf("old response = %q (%v)", payload, err)
				}
			case "deadline":
				waitBudget(t, &s.outbound, 0)
				slow.end(t)
			case "disconnect":
				_ = slow.Close()
			}
			waitBudget(t, &s.outbound, 0)
			other.send(t, "GET 1\nn")
			other.value(t, "new")
		})
	}
}

func TestTCPCommandsAndShutdown(t *testing.T) {
	s := testServer(t, 3, func(c *Config) {
		c.MaxValueBytes = 1000
		c.SweepInterval = time.Millisecond
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	c := &client{conn, bufio.NewReader(conn)}
	c.exchange(t, "GET 2\nk\n", "MISS\n")
	c.exchange(t, "SET 2 3 700 NX\nk\na\x00b", "STORED\n")
	c.exchange(t, "SET 2 3 NONE NX\nk\nnew", "EXISTS\n")
	c.exchange(t, "SET 2 1000 NONE\nk\n"+strings.Repeat("x", 1000), "NO_SPACE\n")
	c.send(t, "GET 2\nk\n")
	c.value(t, "a\x00b")
	end := time.Now().Add(3 * time.Second)
	for s.cache.Stats(0).Entries > 0 {
		if time.Now().After(end) {
			t.Fatal("original TTL was not reclaimed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.exchange(t, "GET 2\nk\n", "MISS\n")
	c.exchange(t, "SET 2 0 NONE NX\nk\n", "STORED\n")
	c.send(t, "GET 2\nk\n")
	c.value(t, "")
	cancel()
	c.end(t)
}

func TestUnconsumedRequestClosesConnection(t *testing.T) {
	s := testServer(t, 1, func(c *Config) {
		c.MaxValueBytes = 16
	})
	for _, test := range []struct {
		header string
		status string
	}{
		{"SET 1 17 1000\n", "TOO_LARGE\n"},
		{"SET 1 3 0\n", "BAD_REQUEST\n"},
		{"SET 1 3 9223372036855\n", "BAD_REQUEST\n"},
		{strings.Repeat("X", headerBytes), "BAD_REQUEST\n"},
	} {
		c := s.pipe(t)
		c.exchange(t, test.header, test.status)
		c.end(t)
		waitBudget(t, &s.inbound, 0)
	}
}
