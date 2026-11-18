package server

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTCPRetryNXAfterLostStoredReply(t *testing.T) {
	s := testServer(t, 1, func(c *Config) {
		c.InboundBytes = 4
		c.SweepInterval = time.Hour
	})
	address, _ := s.listenTestTCP(t)
	proxy, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	caller := dialTestTCP(t, proxy.Addr().String())
	downstream, err := proxy.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer downstream.Close()
	if err := downstream.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	upstream := dialTestTCP(t, address)
	const ttl = 600 * time.Millisecond
	set := fmt.Sprintf("SET 1 3 %d NX\nkold", ttl.Milliseconds())
	caller.send(t, set)
	forward := make([]byte, len(set))
	if _, err := io.ReadFull(downstream, forward); err != nil {
		t.Fatal(err)
	}
	upstream.exchange(t, string(forward), "STORED\n")
	witnessed := time.Now()
	if got := s.inbound.used.Load(); got != 0 {
		t.Fatalf("STORED still holds %d inbound bytes", got)
	}
	// Drop the complete reply at the proxy and close the caller's connection.
	_ = downstream.Close()
	_ = upstream.Close()
	caller.end(t)

	fresh := dialTestTCP(t, address)
	const get = "GET 1\nk"
	fresh.send(t, get)
	fresh.value(t, "old")
	time.Sleep(time.Until(witnessed.Add(ttl / 2)))
	fresh.exchange(t, set, "EXISTS\n")
	fresh.exchange(t, "SET 1 3 NONE NX\nkbad", "EXISTS\n")
	fresh.send(t, get)
	fresh.value(t, "old")

	// The first write has expired, while a renewed TTL would still be live.
	time.Sleep(time.Until(witnessed.Add(ttl + 10*time.Millisecond)))
	fresh.exchange(t, get, "MISS\n")
	fresh.exchange(t, set, "STORED\n")
	fresh.send(t, get)
	fresh.value(t, "old")
	waitBudget(t, &s.inbound, 0)
	waitBudget(t, &s.outbound, 0)
}

func expiredTCPHotKey(t *testing.T) (*Server, string, *client) {
	t.Helper()
	s := testServer(t, 1, func(c *Config) { c.SweepInterval = time.Hour })
	address, _ := s.listenTestTCP(t)
	reader := dialTestTCP(t, address)
	reader.exchange(t, "SET 3 3 100\nhotold", "STORED\n")
	reader.send(t, "GET 3\nhot")
	reader.value(t, "old")
	until := time.Now().Add(2 * time.Second)
	for {
		value, err := poolGet(reader, "hot")
		if err == nil && value == "" {
			break
		}
		if err != nil || value != "old" || time.Now().After(until) {
			t.Fatalf("waiting for old version to expire: GET = %q (%v)", value, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s.cache.Stats(0).Entries != 1 {
		t.Fatal("expected the expired entry to still occupy its slot")
	}
	return s, address, reader
}

func TestTCPExpiredHotKeyNXRefill(t *testing.T) {
	t.Run("concurrent candidates", func(t *testing.T) {
		s, address, reader := expiredTCPHotKey(t)
		const writers = 8
		var clients [writers]*client
		var values [writers]string
		for i := range clients {
			clients[i] = dialTestTCP(t, address)
			clients[i].exchange(t, "GET 3\nhot", "MISS\n")
			values[i] = strings.Repeat(string(rune('a'+i)), 3)
			clients[i].send(t, "SET 3 3 2000 NX\nhot"+values[i][:2])
		}
		waitBudget(t, &s.inbound, writers*6)
		reader.exchange(t, "GET 3\nhot", "MISS\n")

		type reply struct {
			value string
			line  string
			err   error
		}
		start := make(chan struct{})
		replies := make(chan reply, writers)
		for i, c := range clients {
			go func() {
				<-start
				if _, err := io.WriteString(c, values[i][2:]); err != nil {
					replies <- reply{value: values[i], err: err}
					return
				}
				line, err := c.reader.ReadString('\n')
				replies <- reply{values[i], line, err}
			}()
		}
		close(start)
		winner := ""
		seen := make(map[string]bool)
		for received := 0; received < writers; {
			value, err := poolGet(reader, "hot")
			if err != nil {
				t.Fatal(err)
			}
			seen[value] = true
			select {
			case result := <-replies:
				received++
				if result.err != nil {
					t.Fatal(result.err)
				}
				switch result.line {
				case "STORED\n":
					if winner != "" {
						t.Fatalf("two candidates stored: %q and %q", winner, result.value)
					}
					winner = result.value
				case "EXISTS\n":
				default:
					t.Fatalf("candidate %q returned %q", result.value, result.line)
				}
			default:
			}
		}
		if winner == "" {
			t.Fatal("no candidate stored")
		}
		for value := range seen {
			if value != "" && value != winner {
				t.Fatalf("GET returned %q during refill, winner was %q", value, winner)
			}
		}
		reader.send(t, "GET 3\nhot")
		reader.value(t, winner)
		waitBudget(t, &s.inbound, 0)
	})

	t.Run("later frame completes first", func(t *testing.T) {
		s, address, reader := expiredTCPHotKey(t)
		early := dialTestTCP(t, address)
		late := dialTestTCP(t, address)
		early.send(t, "SET 3 3 30000 NX\nhotsl")
		waitBudget(t, &s.inbound, 6)
		const ttl = 250 * time.Millisecond
		late.send(t, fmt.Sprintf("SET 3 3 %d NX\nhotne", ttl.Milliseconds()))
		waitBudget(t, &s.inbound, 12)
		// The later frame waits incomplete longer than its own TTL.
		time.Sleep(ttl + 10*time.Millisecond)
		reader.exchange(t, "GET 3\nhot", "MISS\n")

		beforePublish := time.Now()
		late.exchange(t, "w", "STORED\n")
		afterPublish := time.Now()
		early.exchange(t, "o", "EXISTS\n")
		time.Sleep(time.Until(beforePublish.Add(ttl / 2)))
		reader.send(t, "GET 3\nhot")
		reader.value(t, "new")
		time.Sleep(time.Until(afterPublish.Add(ttl + 10*time.Millisecond)))
		reader.exchange(t, "GET 3\nhot", "MISS\n")
		waitBudget(t, &s.inbound, 0)
	})
}
