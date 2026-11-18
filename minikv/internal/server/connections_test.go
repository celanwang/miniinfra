package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

func poolCloseKind(err error) string {
	switch {
	case errors.Is(err, io.EOF):
		return "EOF"
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, syscall.EPIPE):
		return "broken pipe"
	default:
		return ""
	}
}

func poolGet(c *client, key string) (string, error) {
	if _, err := fmt.Fprintf(c, "GET %d\n%s", len(key), key); err != nil {
		return "", err
	}
	line, err := c.reader.ReadString('\n')
	if line == "" && err != nil {
		return "", err
	}
	switch line {
	case "VALUE 3\n":
		var value [3]byte
		if _, err := io.ReadFull(c.reader, value[:]); err != nil {
			return "", fmt.Errorf("incomplete GET value: %v", err)
		}
		return string(value[:]), nil
	case "MISS\n":
		return "", nil
	default:
		return "", fmt.Errorf("unexpected GET reply %q (%v)", line, err)
	}
}

type poolAttempt struct {
	client *client
	value  string
	err    error
}

func tryPoolConnections(t *testing.T, address string, count int) []poolAttempt {
	t.Helper()
	attempts := make([]poolAttempt, count)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Go(func() {
			conn, err := net.DialTimeout("tcp", address, time.Second)
			if err != nil {
				attempts[i].err = err
				return
			}
			c := &client{conn, bufio.NewReader(conn)}
			attempts[i].client = c
			if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
				attempts[i].err = err
				return
			}
			attempts[i].value, attempts[i].err = poolGet(c, "k")
		})
	}
	wg.Wait()
	for _, attempt := range attempts {
		if c := attempt.client; c != nil {
			t.Cleanup(func() { _ = c.Close() })
		}
	}
	return attempts
}

func TestTCPIdlePoolReclaimsSlots(t *testing.T) {
	const parkedCount = 4
	const readTimeout = 800 * time.Millisecond
	s := testServer(t, 4, func(c *Config) {
		c.MaxConnections = parkedCount + 1
		c.ReadTimeout = readTimeout
	})
	address, _ := s.listenTestTCP(t)
	hot := dialTestTCP(t, address)
	hot.exchange(t, "SET 1 3 NONE\nkold", "STORED\n")
	hotGETs := 0
	checkHot := func() {
		t.Helper()
		if v, err := poolGet(hot, "k"); err != nil || v != "old" {
			t.Fatalf("hot connection GET = %q (%v)", v, err)
		}
		hotGETs++
	}

	var parked [parkedCount]*client
	var idleSince [parkedCount]time.Time
	for i := range parked {
		parked[i] = dialTestTCP(t, address)
		parked[i].send(t, "GET 1\nk")
		parked[i].value(t, "old")
		idleSince[i] = time.Now()
	}
	fullCloses := make(map[string]int)
	for _, attempt := range tryPoolConnections(t, address, parkedCount) {
		kind := poolCloseKind(attempt.err)
		if kind == "" {
			t.Fatalf("full slots should close new TCP clients without a reply: %v", attempt.err)
		}
		fullCloses[kind]++
		if attempt.client != nil {
			_ = attempt.client.Close()
		}
	}
	checkHot()

	type closure struct {
		index int
		at    time.Time
		line  string
		err   error
	}
	closed := make(chan closure, parkedCount)
	for i, c := range parked {
		go func() {
			line, err := c.reader.ReadString('\n')
			closed <- closure{i, time.Now(), line, err}
		}()
	}
	var partialStarted, allClosed time.Time
	var partialWait time.Duration
	var observed [parkedCount]time.Duration
	idleCloses := make(map[string]int)
	partial := time.After(time.Until(idleSince[0].Add(readTimeout / 2)))
	deadline := time.After(2 * readTimeout)
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	for remaining := parkedCount; remaining != 0; {
		select {
		case <-partial:
			partialStarted = time.Now()
			parked[0].send(t, "SET 1 6 NONE\nkxx")
			waitBudget(t, &s.inbound, 7)
		case <-tick.C:
			checkHot()
		case event := <-closed:
			kind := poolCloseKind(event.err)
			if event.line != "" || kind == "" {
				t.Fatalf("idle connection %d closed with %q (%v)", event.index, event.line, event.err)
			}
			idleCloses[kind]++
			observed[event.index] = event.at.Sub(idleSince[event.index])
			if event.index == 0 {
				partialWait = event.at.Sub(partialStarted)
				if partialStarted.IsZero() || partialWait >= readTimeout {
					t.Fatal("a late partial request extended the read deadline")
				}
			}
			if event.at.After(allClosed) {
				allClosed = event.at
			}
			remaining--
		case <-deadline:
			t.Fatal("idle pool did not close within two read periods")
		}
	}
	waitBudget(t, &s.inbound, 0)

	// An old socket cannot resume work, even if the local write succeeds.
	if _, err := io.WriteString(parked[1], "SET 1 3 NONE\nzbad"); err != nil && poolCloseKind(err) == "" {
		t.Fatal(err)
	}
	if line, err := parked[1].reader.ReadString('\n'); line != "" || poolCloseKind(err) == "" {
		t.Fatalf("stale socket replied %q (%v)", line, err)
	}

	fresh := make([]*client, 0, parkedCount)
	rejected := 0
	for len(fresh) < parkedCount {
		checkHot()
		if time.Since(allClosed) >= readTimeout/2 {
			t.Fatalf("only %d idle slots could be reacquired", len(fresh))
		}
		for _, attempt := range tryPoolConnections(t, address, parkedCount-len(fresh)) {
			if poolCloseKind(attempt.err) != "" {
				if attempt.client != nil {
					_ = attempt.client.Close()
				}
				rejected++
				continue
			}
			if attempt.err != nil || attempt.value != "old" {
				t.Fatalf("reconnected GET = %q (%v)", attempt.value, attempt.err)
			}
			fresh = append(fresh, attempt.client)
		}
		if len(fresh) < parkedCount {
			time.Sleep(time.Millisecond)
		}
	}
	recovery := time.Since(allClosed)
	if recovery >= readTimeout/2 {
		t.Fatalf("reacquiring all idle slots took %v", recovery)
	}
	t.Logf("full pool 0/%d %v; deadlines %v %v, partial waited %v; recovered %d/%d in %v (%d retry closes); hot %d/%d",
		parkedCount, fullCloses, observed, idleCloses, partialWait, len(fresh), parkedCount, recovery, rejected, hotGETs, hotGETs)
	fresh[1].exchange(t, "GET 1\nz", "MISS\n")

	// The writer never consumes its reply. A second TCP client can still see the write.
	fresh[0].send(t, "SET 1 3 NONE\nznew")
	until := time.Now().Add(readTimeout / 2)
	for {
		v, err := poolGet(fresh[1], "z")
		if v == "new" && err == nil {
			break
		}
		if err != nil || v != "" || time.Now().After(until) {
			t.Fatalf("unconfirmed write never became visible: %q (%v)", v, err)
		}
		time.Sleep(time.Millisecond)
	}
	_ = fresh[0].Close()
	waitBudget(t, &s.inbound, 0)
	waitBudget(t, &s.outbound, 0)
}
