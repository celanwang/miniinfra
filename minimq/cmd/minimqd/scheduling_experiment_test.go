package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

var dueExperimentEpoch = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

const dueExperimentLead = 630 * time.Millisecond
const dueExperimentChild = "MINIMQ_DUE_CHILD_DIR"
const dueExperimentClockFile = "MINIMQ_DUE_CHILD_CLOCK_FILE"

// The child runs the same listener, HTTP routes and maintenance loop as minimqd.
func TestDueExperimentBroker(t *testing.T) {
	dir := os.Getenv(dueExperimentChild)
	if dir == "" {
		t.Skip("broker subprocess for TestDueExperiment")
	}
	origin, err := time.Parse(time.RFC3339Nano, os.Getenv("MINIMQ_DUE_CHILD_ORIGIN"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg := dueExperimentClock(origin)
	if path := os.Getenv(dueExperimentClockFile); path != "" {
		cfg.Clock = func() (minimq.TimeBounds, error) {
			data, err := os.ReadFile(path)
			if err != nil {
				return minimq.TimeBounds{}, err
			}
			now, err := time.Parse(time.RFC3339Nano, string(data))
			return minimq.TimeBounds{Lower: now, Upper: now}, err
		}
	}
	if err := run(ctx, dir, "127.0.0.1:0", cfg); err != nil {
		t.Fatal(err)
	}
}

func dueExperimentClock(origin time.Time) minimq.Config {
	return minimq.Config{Clock: func() (minimq.TimeBounds, error) {
		now := dueExperimentEpoch
		if !origin.IsZero() {
			now = now.Add(time.Since(origin))
		}
		return minimq.TimeBounds{Lower: now, Upper: now}, nil
	}}
}

type dueBroker struct {
	url  string
	cmd  *exec.Cmd
	done chan error
}

func startDueBroker(t *testing.T, dir string, origin time.Time) *dueBroker {
	t.Helper()
	return startDueBrokerWithClock(t, dir, origin, "")
}

func startDueBrokerWithClock(t *testing.T, dir string, origin time.Time, clockFile string) *dueBroker {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDueExperimentBroker$", "-test.v")
	cmd.Env = append(os.Environ(), dueExperimentChild+"="+dir, "MINIMQ_DUE_CHILD_ORIGIN="+origin.Format(time.RFC3339Nano),
		dueExperimentClockFile+"="+clockFile)
	cmd.Stdout = os.Stdout
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	broker := &dueBroker{cmd: cmd, done: make(chan error, 1)}
	t.Cleanup(func() { broker.stop(t) })
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			if _, address, ok := strings.Cut(scanner.Text(), "MiniMQ listening on "); ok {
				ready <- address
			} else {
				fmt.Fprintln(os.Stderr, scanner.Text())
			}
		}
		close(ready)
		broker.done <- cmd.Wait()
	}()
	select {
	case address := <-ready:
		if address == "" {
			t.Fatal("broker exited without listening")
		}
		broker.url = "http://" + address
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not start within 10s")
	}
	return broker
}

func (b *dueBroker) stop(t *testing.T) {
	t.Helper()
	if b.cmd == nil {
		return
	}
	if err := b.cmd.Process.Kill(); err != nil {
		t.Error(err)
	}
	if err := <-b.done; err == nil {
		t.Error("broker exited before the crash test")
	}
	b.cmd = nil
}

func duePost[Response any](client *http.Client, url, path string, request any) (Response, error) {
	var response Response
	body, err := json.Marshal(request)
	if err != nil {
		return response, err
	}
	res, err := client.Post(url+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return response, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		var failure errorResponse
		_ = json.NewDecoder(res.Body).Decode(&failure)
		return response, fmt.Errorf("%s: HTTP %d: %s", path, res.StatusCode, failure.Error)
	}
	return response, json.NewDecoder(res.Body).Decode(&response)
}

func requireDuePost[Response any](t *testing.T, client *http.Client, url, path string, request any) Response {
	t.Helper()
	response, err := duePost[Response](client, url, path, request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

type dueFixture struct {
	requests []minimq.PublishRequest
	byID     map[string]minimq.PublishRequest
	retry    minimq.Delivery
}

func seedDueExperiment(t *testing.T, dir, mode string, count, bodyBytes int) dueFixture {
	t.Helper()
	p, err := minimq.Open(dir, dueExperimentClock(time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	fixture := dueFixture{byID: make(map[string]minimq.PublishRequest, count)}
	if err := p.Register("early", minimq.SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		deliverAt := dueExperimentEpoch.Add(dueExperimentLead)
		if mode == "spread" {
			deliverAt = deliverAt.Add(time.Duration(4-i%5) * 150 * time.Millisecond)
		}
		req := minimq.PublishRequest{ID: fmt.Sprintf("scheduled-%04d", i), Body: bytes.Repeat([]byte{byte(i)}, bodyBytes), DeliverAt: deliverAt}
		if result, err := p.Publish(req); err != nil || !result.Scheduled || result.ID != req.ID {
			t.Fatalf("admission for %s = %+v, %v", req.ID, result, err)
		}
		fixture.requests = append(fixture.requests, req)
		fixture.byID[req.ID] = req
	}
	slices.SortStableFunc(fixture.requests, func(a, b minimq.PublishRequest) int { return a.DeliverAt.Compare(b.DeliverAt) })
	lease := dueExperimentLead + 70*time.Millisecond
	fixture.retry = seedTimeout(t, p, "retry", minimq.SubscriptionConfig{LeaseDuration: lease, MaxAttempts: 2, RetryDelay: 15 * time.Millisecond})
	seedTimeout(t, p, "expiry", minimq.SubscriptionConfig{LeaseDuration: lease, MaxAttempts: 1})
	if err := p.Register("late", minimq.SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func seedTimeout(t *testing.T, p *minimq.Partition, subscription string, cfg minimq.SubscriptionConfig) minimq.Delivery {
	t.Helper()
	if err := p.Register(subscription, cfg); err != nil {
		t.Fatal(err)
	}
	id := subscription + "-sentinel"
	if _, err := p.Publish(minimq.PublishRequest{ID: id}); err != nil {
		t.Fatal(err)
	}
	ds, err := p.Pull(subscription, "probe", minimq.Limits{Messages: 1, Bytes: 64})
	if err != nil || len(ds) != 1 || ds[0].Message.ID != id {
		t.Fatalf("lease fixture for %s = %+v, %v", subscription, ds, err)
	}
	return ds[0]
}

func TestDueExperiment(t *testing.T) {
	mode := os.Getenv("MINIMQ_DUE_EXPERIMENT")
	if mode == "" {
		t.Skip("set MINIMQ_DUE_EXPERIMENT to burst, spread, restart, or partial")
	}
	if mode != "burst" && mode != "spread" && mode != "restart" && mode != "partial" {
		t.Fatal("MINIMQ_DUE_EXPERIMENT must be burst, spread, restart, or partial")
	}
	count, bodyBytes := 640, 1024
	if os.Getenv("MINIMQ_DUE_SIZE") == "large" {
		count, bodyBytes = 384, 64<<10
	}
	dir := t.TempDir()
	fixture := seedDueExperiment(t, dir, mode, count, bodyBytes)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	origin := time.Now()
	firstDue := origin.Add(dueExperimentLead)
	broker := startDueBroker(t, dir, origin)
	probe := fixture.requests[0]
	if got := requireDuePost[minimq.PublishResult](t, client, broker.url, "/v1/publish/status", idRequest{ID: probe.ID}); !got.Scheduled {
		t.Fatalf("early status = %+v", got)
	}
	early := pullRequest{Subscription: "early", Worker: "observer", Limit: minimq.Limits{Messages: minimq.MaxBatchMessages, Bytes: minimq.MaxBatchBytes}}
	if before := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", early); len(before) != 2 ||
		before[0].Message.ID != "retry-sentinel" || before[1].Message.ID != "expiry-sentinel" {
		t.Fatalf("pre-deadline deliveries = %d, want the two immediate sentinels", len(before))
	}
	if page := requireDuePost[minimq.HistoryPage](t, client, broker.url, "/v1/history/read", historyRequest{Limit: early.Limit}); page.Next != 2 {
		t.Fatalf("pre-deadline history ends at %d, want 2", page.Next)
	}
	var restartDelay time.Duration
	var beforeCrash minimq.PublishResult
	switch mode {
	case "restart":
		broker.stop(t)
		time.Sleep(time.Until(firstDue.Add(125 * time.Millisecond)))
		broker = startDueBroker(t, dir, origin)
		restartDelay = time.Since(firstDue)
	case "partial":
		beforeCrash = waitForPartialRelease(t, client, broker.url, fixture.requests, firstDue)
		broker.stop(t)
		broker = startDueBroker(t, dir, origin)
		restartDelay = time.Since(firstDue)
	default:
		time.Sleep(time.Until(firstDue.Add(-140 * time.Millisecond)))
	}
	// Keep ordinary load fixed through the same observation horizon for both policies.
	stopAt := firstDue.Add(1600 * time.Millisecond)
	ordinaryDone := make(chan dueOrdinary, 1)
	go func(url string) { ordinaryDone <- runDueOrdinary(client, url, stopAt) }(broker.url)
	retryDone := make(chan dueRetry, 1)
	go func(url string) { retryDone <- runDueRetry(client, url, origin, fixture.retry, stopAt) }(broker.url)
	seen := make(map[string]uint64, count)
	var latencies []time.Duration
	var drained time.Time
	var lastLease minimq.Delivery
	var releaseBatches []int
	for len(seen) < count && time.Now().Before(stopAt) {
		deliveries := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", early)
		arrived := time.Now()
		for _, d := range deliveries {
			req, scheduled := fixture.byID[d.Message.ID]
			if !scheduled {
				continue
			}
			if len(seen) == count {
				t.Fatalf("extra scheduled delivery for %s", req.ID)
			}
			if _, duplicate := seen[req.ID]; duplicate || fixture.requests[len(seen)].ID != req.ID {
				t.Fatalf("scheduled delivery %d = %s, want %s", len(seen), req.ID, fixture.requests[len(seen)].ID)
			}
			deadline := origin.Add(req.DeliverAt.Sub(dueExperimentEpoch))
			if arrived.Before(deadline) || d.Message.PublishedAt.Before(req.DeliverAt) || d.Attempt != 1 || !bytes.Equal(d.Message.Body, req.Body) {
				t.Fatalf("invalid release of %s at %s, scheduled at %s, attempt %d", req.ID, d.Message.PublishedAt, req.DeliverAt, d.Attempt)
			}
			if len(seen) == 0 || !d.Message.PublishedAt.Equal(lastLease.Message.PublishedAt) {
				releaseBatches = append(releaseBatches, 0)
			}
			releaseBatches[len(releaseBatches)-1]++
			seen[req.ID] = d.Message.Offset
			latencies = append(latencies, arrived.Sub(deadline))
			lastLease = d
		}
		drained = arrived
		if len(seen) < count {
			time.Sleep(12 * time.Millisecond)
		}
	}
	ordinary, retry := <-ordinaryDone, <-retryDone
	if len(seen) != count || ordinary.err != nil || retry.err != nil {
		t.Fatalf("released %d/%d; ordinary = %v; retry = %v", len(seen), count, ordinary.err, retry.err)
	}
	if mode == "partial" && seen[beforeCrash.ID] != beforeCrash.Offset {
		t.Fatalf("release of %s moved after recovery from %d to %d", beforeCrash.ID, beforeCrash.Offset, seen[beforeCrash.ID])
	}
	n := 0
	for _, completed := range ordinary.completed {
		if !completed.Before(firstDue) && !completed.After(drained) {
			n++
		}
	}
	if n == 0 {
		t.Fatal("ordinary publish, pull and ACK made no progress during release")
	}
	t.Logf("mode=%s count=%d body=%d p50=%s p99=%s max_lateness=%s drain=%s restart_ready=%s ordinary=%d during=%d publish_p99=%s pull_p99=%s ack_p99=%s ordinary_gap=%s retry=%s expiry=%s batches=%v",
		mode, count, bodyBytes, duePercentile(latencies, 50), duePercentile(latencies, 99), duePercentile(latencies, 100), drained.Sub(firstDue), restartDelay,
		len(ordinary.completed), n, duePercentile(ordinary.publish, 99), duePercentile(ordinary.pull, 99), duePercentile(ordinary.ack, 99), ordinary.maxGap, retry.redelivered, retry.expired, releaseBatches)

	broker.stop(t)
	broker = startDueBroker(t, dir, origin)
	checkDueRecovery(t, client, broker.url, fixture, seen, lastLease, ordinary.lastAck, retry.ack)
}

func waitForPartialRelease(t *testing.T, client *http.Client, url string, requests []minimq.PublishRequest, due time.Time) minimq.PublishResult {
	t.Helper()
	for time.Now().Before(due.Add(time.Second)) {
		first := requireDuePost[minimq.PublishResult](t, client, url, "/v1/publish/status", idRequest{ID: requests[0].ID})
		if !first.Scheduled {
			last := requireDuePost[minimq.PublishResult](t, client, url, "/v1/publish/status", idRequest{ID: requests[len(requests)-1].ID})
			if !last.Scheduled {
				t.Fatal("all scheduled work was visible before the partial-release crash")
			}
			return first
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no durable release before the partial-release crash")
	return minimq.PublishResult{}
}

type dueOrdinary struct {
	completed          []time.Time
	publish, pull, ack []time.Duration
	maxGap             time.Duration
	lastAck            deliveryRequest
	err                error
}

func runDueOrdinary(client *http.Client, url string, stopAt time.Time) (result dueOrdinary) {
	previous := time.Now()
	for i := 0; time.Now().Before(stopAt); i++ {
		start := time.Now()
		id := "ordinary-" + strconv.Itoa(i)
		_, err := duePost[minimq.PublishResult](client, url, "/v1/publish", minimq.PublishRequest{ID: id, Body: []byte(id)})
		published := time.Now()
		if err != nil {
			result.err = err
			return
		}
		req := pullRequest{Subscription: "late", Worker: "online", Limit: minimq.Limits{Messages: 2, Bytes: minimq.MaxBatchBytes}}
		ds, err := duePost[[]minimq.Delivery](client, url, "/v1/deliveries/pull", req)
		pulled := time.Now()
		if err != nil || len(ds) != 1 || ds[0].Message.ID != id {
			result.err = fmt.Errorf("late subscription pull for %s returned %d: %v", id, len(ds), err)
			return
		}
		result.lastAck = deliveryRequest{Subscription: req.Subscription, Worker: req.Worker, Instance: ds[0].Instance, Token: ds[0].Token}
		ack, err := duePost[minimq.AckResult](client, url, "/v1/deliveries/ack", result.lastAck)
		end := time.Now()
		if err != nil || ack != minimq.AckAccepted {
			result.err = fmt.Errorf("ordinary ACK = %q, %v", ack, err)
			return
		}
		result.completed = append(result.completed, end)
		result.publish = append(result.publish, published.Sub(start))
		result.pull = append(result.pull, pulled.Sub(published))
		result.ack = append(result.ack, end.Sub(pulled))
		result.maxGap = max(result.maxGap, end.Sub(previous))
		previous = end
		time.Sleep(time.Until(start.Add(45 * time.Millisecond)))
	}
	return
}

type dueRetry struct {
	redelivered, expired time.Duration
	ack                  deliveryRequest
	err                  error
}

func runDueRetry(client *http.Client, url string, origin time.Time, first minimq.Delivery, stopAt time.Time) (result dueRetry) {
	deadline := origin.Add(first.Deadline.Sub(dueExperimentEpoch))
	time.Sleep(time.Until(deadline.Add(15 * time.Millisecond)))
	for (result.ack.Token == "" || result.expired == 0) && time.Now().Before(stopAt) {
		if result.ack.Token == "" {
			req := pullRequest{Subscription: "retry", Worker: "probe", Limit: minimq.Limits{Messages: 1, Bytes: 64}}
			ds, err := duePost[[]minimq.Delivery](client, url, "/v1/deliveries/pull", req)
			if err != nil {
				result.err = err
				return
			}
			if len(ds) > 0 {
				if ds[0].Message.ID != first.Message.ID || ds[0].Attempt != 2 || ds[0].Token == first.Token {
					result.err = fmt.Errorf("timeout retry returned %s attempt %d", ds[0].Message.ID, ds[0].Attempt)
					return
				}
				result.redelivered = time.Since(deadline)
				result.ack = deliveryRequest{Subscription: req.Subscription, Worker: req.Worker, Instance: ds[0].Instance, Token: ds[0].Token}
				ack, err := duePost[minimq.AckResult](client, url, "/v1/deliveries/ack", result.ack)
				if err != nil || ack != minimq.AckAccepted {
					result.err = fmt.Errorf("retry ACK = %q, %v", ack, err)
					return
				}
			}
		}
		if result.expired == 0 {
			letters, err := duePost[[]minimq.DeadLetter](client, url, "/v1/dead-letters/list", listDeadRequest{Subscription: "expiry", Limit: 1})
			if err != nil {
				result.err = err
				return
			}
			if len(letters) != 0 {
				if letters[0].Message.ID != "expiry-sentinel" || letters[0].DeadAt.Before(first.Deadline) {
					result.err = fmt.Errorf("unexpected expiration: %+v", letters[0])
					return
				}
				result.expired = time.Since(deadline)
			}
		}
		time.Sleep(12 * time.Millisecond)
	}
	if result.ack.Token == "" || result.expired == 0 {
		result.err = fmt.Errorf("retry or background expiry missed the observation window")
	}
	return
}

func duePercentile(samples []time.Duration, percent int) time.Duration {
	slices.Sort(samples)
	return samples[(len(samples)*percent+99)/100-1]
}

func checkDueRecovery(t *testing.T, client *http.Client, url string, fixture dueFixture, seen map[string]uint64, lastLease minimq.Delivery, ordinary, retry deliveryRequest) {
	t.Helper()
	var offset uint64
	n := 0
	limit := minimq.Limits{Messages: minimq.MaxBatchMessages, Bytes: minimq.MaxBatchBytes}
	for {
		page := requireDuePost[minimq.HistoryPage](t, client, url, "/v1/history/read", historyRequest{Offset: offset, Limit: limit})
		if len(page.Messages) == 0 {
			break
		}
		for _, msg := range page.Messages {
			if req, ok := fixture.byID[msg.ID]; ok {
				if n >= len(fixture.requests) || fixture.requests[n].ID != msg.ID || seen[msg.ID] != msg.Offset || !bytes.Equal(msg.Body, req.Body) || msg.PublishedAt.Before(req.DeliverAt) {
					t.Fatalf("recovered history record %d: %s at offset %d", n, msg.ID, msg.Offset)
				}
				n++
			}
		}
		offset = page.Next
	}
	if n != len(fixture.requests) {
		t.Fatalf("recovered %d scheduled records, want %d", n, len(fixture.requests))
	}
	for _, req := range fixture.requests {
		status := requireDuePost[minimq.PublishResult](t, client, url, "/v1/publish/status", idRequest{ID: req.ID})
		if status.Scheduled || status.Offset != seen[req.ID] {
			t.Fatalf("recovered receipt for %s = %+v", req.ID, status)
		}
	}
	if duplicate := requireDuePost[minimq.PublishResult](t, client, url, "/v1/publish", fixture.requests[0]); !duplicate.Scheduled {
		t.Fatalf("duplicate publish lost its original receipt: %+v", duplicate)
	}
	if page := requireDuePost[minimq.HistoryPage](t, client, url, "/v1/history/read", historyRequest{Offset: offset, Limit: limit}); len(page.Messages) != 0 {
		t.Fatal("duplicate publish appended a history record")
	}
	for _, req := range []deliveryRequest{ordinary, retry} {
		if got := requireDuePost[minimq.AckResult](t, client, url, "/v1/deliveries/ack", req); got != minimq.AckPreviouslyAccepted {
			t.Fatalf("recovered ACK = %q", got)
		}
	}
	ack := deliveryRequest{Subscription: "early", Worker: "observer", Instance: lastLease.Instance, Token: lastLease.Token}
	if got := requireDuePost[minimq.AckResult](t, client, url, "/v1/deliveries/ack", ack); got != minimq.AckAccepted {
		t.Fatalf("recovered active lease ACK = %q", got)
	}
}
