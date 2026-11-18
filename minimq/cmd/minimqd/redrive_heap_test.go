package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

const redriveHeapSampleEnv = "MINIMQ_REDRIVE_HEAP_SAMPLE_FILE"

type redriveHeapSample struct {
	HeapAlloc, TotalAlloc uint64
}

// The parent samples only the broker process, without serving a diagnostics route.
func sampleRedriveBrokerHeap(ctx context.Context, path string) func() {
	requests := make(chan os.Signal, 1)
	signal.Notify(requests, syscall.SIGUSR1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-requests:
				runtime.GC()
				runtime.GC()
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				data, err := json.Marshal(redriveHeapSample{stats.HeapAlloc, stats.TotalAlloc})
				if err == nil {
					err = os.WriteFile(path+".next", data, 0o600)
				}
				if err == nil {
					err = os.Rename(path+".next", path)
				}
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
				}
			}
		}
	}()
	return func() { signal.Stop(requests) }
}

func readRedriveBrokerHeap(t *testing.T, broker *dueBroker, path string) redriveHeapSample {
	t.Helper()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := broker.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var sample redriveHeapSample
			if err := json.Unmarshal(data, &sample); err != nil {
				t.Fatal(err)
			}
			return sample
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("broker heap sample timed out")
	return redriveHeapSample{}
}

func TestRedriveHeapOverTCP(t *testing.T) {
	mode := os.Getenv("MINIMQ_REDRIVE_HEAP")
	if mode == "" {
		t.Skip("set MINIMQ_REDRIVE_HEAP to distinct or same")
	}
	if mode != "same" && mode != "distinct" {
		t.Fatal("MINIMQ_REDRIVE_HEAP must be distinct or same")
	}
	const operations = 12
	const bodyBytes = 128 << 10
	sources := operations
	if mode == "same" {
		sources = 1
	}
	root := t.TempDir()
	dir, clock, samplePath := filepath.Join(root, "data"), filepath.Join(root, "clock"), filepath.Join(root, "heap")
	t.Setenv(redriveHeapSampleEnv, samplePath)
	setDueClockFile(t, clock, dueExperimentEpoch)
	broker := startDueBrokerWithClock(t, dir, time.Time{}, clock)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	requireDuePost[struct{}](t, client, broker.url, "/v1/subscriptions/register", registerRequest{
		Subscription: "notify", Config: minimq.SubscriptionConfig{MaxAttempts: 1},
	})
	body := func(i int) []byte { return bytes.Repeat([]byte{byte(i + 1)}, bodyBytes) }
	for i := range sources {
		req := minimq.PublishRequest{ID: fmt.Sprintf("source-%02d", i), Body: body(i)}
		requireDuePost[minimq.PublishResult](t, client, broker.url, "/v1/publish", req)
	}
	pull := pullRequest{Subscription: "notify", Worker: "worker", Limit: minimq.Limits{Messages: sources, Bytes: sources * bodyBytes}}
	ds := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", pull)
	if len(ds) != sources {
		t.Fatalf("source Pull = %d deliveries, want %d", len(ds), sources)
	}
	deadIDs := make([]string, 0, sources)
	for i, d := range ds {
		if !bytes.Equal(d.Message.Body, body(i)) {
			t.Fatalf("wrong source body at %d", i)
		}
		req := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: d.Instance, Token: d.Token, Reason: "operator-fixture"}
		result := requireDuePost[minimq.FailureResult](t, client, broker.url, "/v1/deliveries/fail", req)
		if result.DeadLetterID == "" {
			t.Fatalf("source %d did not become a dead letter", i)
		}
		deadIDs = append(deadIDs, result.DeadLetterID)
	}
	restart := func() {
		t.Helper()
		broker.stop(t)
		client.CloseIdleConnections()
		broker = startDueBrokerWithClock(t, dir, time.Time{}, clock)
	}
	restart()
	initial := redriveTCPStatus(t, client, broker.url)
	before := readRedriveBrokerHeap(t, broker, samplePath)
	const redrivePath = "/v1/dead-letters/redrive"
	results := make([]minimq.RedriveResult, operations)
	request := func(i int) redriveRequest {
		return redriveRequest{Subscription: pull.Subscription, DeadLetterID: deadIDs[i%sources], OperationID: fmt.Sprintf("operator-%02d", i)}
	}
	seen := make(map[string]bool, operations)
	for i := range operations {
		req := request(i)
		r := requireDuePost[minimq.RedriveResult](t, client, broker.url, redrivePath, req)
		if r.Instance == "" || seen[r.Instance] || r.SourceDeadLetterID != req.DeadLetterID {
			t.Fatalf("distinct operation %d returned %+v", i, r)
		}
		seen[r.Instance] = true
		results[i] = r
	}
	admitted := readRedriveBrokerHeap(t, broker, samplePath)
	after := redriveTCPStatus(t, client, broker.url)
	if after.PendingRedrives != operations {
		t.Fatalf("pending after admission = %d", after.PendingRedrives)
	}
	for range operations {
		if got := requireDuePost[minimq.RedriveResult](t, client, broker.url, redrivePath, request(0)); got != results[0] {
			t.Fatalf("repeated operation returned %+v", got)
		}
	}
	repeated := readRedriveBrokerHeap(t, broker, samplePath)
	if status := redriveTCPStatus(t, client, broker.url); status != after {
		t.Fatal("repeated operation changed pending work, storage or reservation")
	}
	restart()
	if status := redriveTCPStatus(t, client, broker.url); status != after {
		t.Fatal("reopened broker changed admitted work")
	}
	reopened := readRedriveBrokerHeap(t, broker, samplePath)
	t.Logf("mode=%s source_count=%d operations=%d body_bytes=%d admitted_heap_delta=%d admitted_alloc=%d repeated_heap_delta=%d repeated_alloc=%d reopened_heap_delta=%d reserved_delta=%d stored_delta=%d",
		mode, sources, operations, bodyBytes, int64(admitted.HeapAlloc)-int64(before.HeapAlloc), admitted.TotalAlloc-before.TotalAlloc,
		int64(repeated.HeapAlloc)-int64(admitted.HeapAlloc), repeated.TotalAlloc-admitted.TotalAlloc,
		int64(reopened.HeapAlloc)-int64(before.HeapAlloc), after.ReservedBytes-initial.ReservedBytes, after.StoredBytes-initial.StoredBytes)

	broker.stop(t)
	client.CloseIdleConnections()
	drainAt := dueExperimentEpoch.Add(8 * 24 * time.Hour)
	setDueClockFile(t, clock, drainAt)
	p, err := minimq.Open(dir, minimq.Config{Clock: func() (minimq.TimeBounds, error) {
		return minimq.TimeBounds{Lower: drainAt, Upper: drainAt}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	broker = startDueBrokerWithClock(t, dir, time.Time{}, clock)
	if status := redriveTCPStatus(t, client, broker.url); status.DeadLetters != 0 ||
		status.FirstOffset != uint64(sources) || status.PendingRedrives != operations {
		t.Fatalf("reopened checkpoint did not isolate tasks from their expired sources: %+v", status)
	}

	pull.Limit = minimq.Limits{Messages: 1, Bytes: bodyBytes}
	for i, r := range results {
		setDueClockFile(t, clock, drainAt.Add(time.Duration(i)*time.Second))
		ds := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", pull)
		if len(ds) != 1 || ds[0].Instance != r.Instance || ds[0].Attempt != 1 || !bytes.Equal(ds[0].Message.Body, body(i%sources)) {
			t.Fatalf("recovered operation %d returned %d deliveries or wrong body/instance", i, len(ds))
		}
		d := ds[0]
		req := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: d.Instance, Token: d.Token}
		if ack := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", req); ack != minimq.AckAccepted {
			t.Fatalf("ACK for redrive %d = %q", i, ack)
		}
	}
	if status := redriveTCPStatus(t, client, broker.url); status.PendingRedrives != 0 || status.NextOffset != uint64(sources) {
		t.Fatalf("status after settling redrives = %+v", status)
	}
}

func redriveTCPStatus(t *testing.T, client *http.Client, url string) minimq.PartitionStatus {
	t.Helper()
	res, err := client.Get(url + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var status minimq.PartitionStatus
	if err := json.NewDecoder(res.Body).Decode(&status); err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("status = HTTP %d, %v", res.StatusCode, err)
	}
	return status
}
