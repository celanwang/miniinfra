package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

// The real TCP routes share the partition with explicit, closely spaced
// Compact calls so repeated idle rounds can be compared under the same load.
func TestIdleCheckpointsOverTCP(t *testing.T) {
	dir := os.Getenv("MINIMQ_IDLE_CHECKPOINT_TCP_DIR")
	if dir == "" {
		t.Skip("set MINIMQ_IDLE_CHECKPOINT_TCP_DIR to a fresh dead-letter fixture copy")
	}
	p, err := minimq.Open(dir, dueExperimentClock(time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	original, err := p.Status()
	if err != nil || original.DeadLetters == 0 {
		t.Fatalf("fixture has no retained dead letters: %+v, %v", original, err)
	}
	server := httptest.NewServer(routes(p))
	t.Cleanup(server.Close)
	client := server.Client()
	requireDuePost[struct{}](t, client, server.URL, "/v1/subscriptions/register", registerRequest{Subscription: "idle-control"})
	requireDuePost[minimq.PublishResult](t, client, server.URL, "/v1/publish", minimq.PublishRequest{ID: "idle-control", Body: []byte("work")})
	pull := pullRequest{Subscription: "idle-control", Worker: "worker", Limit: minimq.Limits{Messages: 1, Bytes: 64}}
	ds := requireDuePost[[]minimq.Delivery](t, client, server.URL, "/v1/deliveries/pull", pull)
	if len(ds) != 1 || ds[0].Message.ID != "idle-control" {
		t.Fatalf("first ordinary Pull = %d results", len(ds))
	}
	ack := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: ds[0].Instance, Token: ds[0].Token}
	if got := requireDuePost[minimq.AckResult](t, client, server.URL, "/v1/deliveries/ack", ack); got != minimq.AckAccepted {
		t.Fatalf("first ordinary ACK = %q", got)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	headPath := filepath.Join(dir, "HEAD")
	head, err := os.ReadFile(headPath)
	if err != nil || len(head) != 24 || binary.LittleEndian.Uint64(head[12:20]) != 0 {
		t.Fatalf("the delivery fixture must have an empty WAL: %v", err)
	}
	deadPage := listDeadRequest{Subscription: "billing", Limit: 16}
	old := requireDuePost[[]minimq.DeadLetter](t, client, server.URL, "/v1/dead-letters/list", deadPage)
	if len(old) != 16 {
		t.Fatalf("original dead-letter page = %d entries", len(old))
	}
	for _, phase := range []string{"quiet", "compact"} {
		start := time.Now().Add(15 * time.Millisecond)
		pulls, acks := make(chan idleTCPResult, 1), make(chan idleTCPResult, 1)
		go func() {
			pulls <- idleTCPRequests(start, func() error {
				ds, err := duePost[[]minimq.Delivery](client, server.URL, "/v1/deliveries/pull", pull)
				if err == nil && len(ds) != 0 {
					err = fmt.Errorf("idle Pull returned %d deliveries", len(ds))
				}
				return err
			})
		}()
		go func() {
			acks <- idleTCPRequests(start.Add(time.Millisecond), func() error {
				got, err := duePost[minimq.AckResult](client, server.URL, "/v1/deliveries/ack", ack)
				if err == nil && got != minimq.AckPreviouslyAccepted {
					err = fmt.Errorf("repeated ACK = %q", got)
				}
				return err
			})
		}()
		if phase == "compact" {
			var elapsed []time.Duration
			for i := range 32 {
				time.Sleep(time.Until(start.Add(time.Duration(i)*10*time.Millisecond + 1500*time.Microsecond)))
				at := time.Now()
				if err := p.Compact(); err != nil {
					t.Fatal(err)
				}
				elapsed = append(elapsed, time.Since(at))
			}
			logDeadPageTimes(t, phase, "idle_compact", elapsed)
		}
		pullResult, ackResult := <-pulls, <-acks
		if pullResult.err != nil || ackResult.err != nil {
			t.Fatalf("TCP %s: Pull = %v, ACK = %v", phase, pullResult.err, ackResult.err)
		}
		logDeadPageTimes(t, phase, "empty_pull", pullResult.samples)
		logDeadPageTimes(t, phase, "repeated_ack", ackResult.samples)
	}
	current, err := os.ReadFile(headPath)
	if err != nil || !bytes.Equal(head, current) {
		t.Fatalf("idle requests and checkpoints replaced HEAD: %v", err)
	}
	server.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	broker := startDueBroker(t, dir, time.Now())
	client = &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	reopened := requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", deadPage)
	if len(reopened) != len(old) {
		t.Fatalf("reopened dead-letter page = %d entries", len(reopened))
	}
	for i, letter := range reopened {
		if letter.ID != old[i].ID || !bytes.Equal(letter.Message.Body, old[i].Message.Body) {
			t.Fatalf("reopened page differs at %d", i)
		}
	}
	if got := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", ack); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("reopened ACK = %q", got)
	}
	if ds := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", pull); len(ds) != 0 {
		t.Fatalf("reopened Pull = %d deliveries", len(ds))
	}
}

type idleTCPResult struct {
	samples []time.Duration
	err     error
}

func idleTCPRequests(start time.Time, request func() error) (result idleTCPResult) {
	for i := range 160 {
		time.Sleep(time.Until(start.Add(time.Duration(i) * 2 * time.Millisecond)))
		at := time.Now()
		if err := request(); err != nil {
			result.err = err
			return
		}
		result.samples = append(result.samples, time.Since(at))
	}
	return
}
