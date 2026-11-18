package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

const (
	durablePathDepth        = 32
	durablePathRounds       = 4
	durablePathBodyBytes    = 1024
	durablePathSubscription = "ordinary"
	durablePublishPath      = "/v1/publish"
	durablePullPath         = "/v1/deliveries/pull"
	durableAckPath          = "/v1/deliveries/ack"
)

// Run with MINIMQ_DURABLE_PATH=1 GOWORK=off go test -run '^TestDurablePathOverTCP$' -count=4 -v ./cmd/minimqd.
// Each round fills the unsealed tail to the same queue depth, then drains it
// with individual ACKs. Publish-to-ACK latency includes both phases. A fixed
// clock keeps leases live, and checkpoint overlap invalidates the trial.
// Recovery checks an abrupt process restart using the same data directory.
func TestDurablePathOverTCP(t *testing.T) {
	if os.Getenv("MINIMQ_DURABLE_PATH") != "1" {
		t.Skip("set MINIMQ_DURABLE_PATH=1 to run the durable TCP workload")
	}
	for _, workers := range []int{1, 4} {
		for _, batch := range []int{1, 8} {
			t.Run(fmt.Sprintf("clients-%d/pull-%d", workers, batch), func(t *testing.T) {
				runDurablePath(t, workers, batch)
			})
		}
	}
}

type durablePublication struct {
	request minimq.PublishRequest
	receipt minimq.PublishResult
	started time.Time
	elapsed time.Duration
}

type durableCompletion struct {
	offset  uint64
	ack     deliveryRequest
	elapsed time.Duration
	end     time.Time
}

func runDurablePath(t *testing.T, workers, batch int) {
	t.Helper()
	const count = durablePathDepth * durablePathRounds
	dir := t.TempDir()
	broker := startDueBroker(t, dir, time.Time{})
	transport := &http.Transport{MaxConnsPerHost: workers, MaxIdleConnsPerHost: workers}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	requireDuePost[struct{}](t, client, broker.url, "/v1/subscriptions/register", registerRequest{Subscription: durablePathSubscription})
	body := bytes.Repeat([]byte{'m'}, durablePathBodyBytes)
	byOffset := make([]durablePublication, count)
	acks := make([]deliveryRequest, count)
	var publishTimes, pullTimes, ackTimes, endToEnd []time.Duration
	started := time.Now()
	// Drain each fixed-depth round before admitting the next one.
	for round := range durablePathRounds {
		var published [durablePathDepth]durablePublication
		if err := runDurablePathWorkers(workers, func(worker int) error {
			for i := worker; i < len(published); i += workers {
				req := minimq.PublishRequest{ID: fmt.Sprintf("message-%03d", round*durablePathDepth+i), Body: body}
				at := time.Now()
				got, err := duePost[minimq.PublishResult](client, broker.url, durablePublishPath, req)
				if err != nil || got.ID != req.ID || got.Scheduled {
					return fmt.Errorf("publish %s returned %+v, %v", req.ID, got, err)
				}
				published[i] = durablePublication{request: req, receipt: got, started: at, elapsed: time.Since(at)}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, pub := range published {
			offset := pub.receipt.Offset
			if offset < uint64(round*durablePathDepth) || offset >= uint64((round+1)*durablePathDepth) || byOffset[offset].request.ID != "" {
				t.Fatalf("unexpected publication offset %d for %s", offset, pub.request.ID)
			}
			byOffset[offset] = pub
			publishTimes = append(publishTimes, pub.elapsed)
		}

		completed := make([][]durableCompletion, workers)
		pullSamples := make([][]time.Duration, workers)
		if err := runDurablePathWorkers(workers, func(worker int) error {
			request := pullRequest{Subscription: durablePathSubscription, Worker: fmt.Sprintf("worker-%d", worker),
				Limit: minimq.Limits{Messages: batch, Bytes: batch * durablePathBodyBytes}}
			for range durablePathDepth / workers / batch {
				at := time.Now()
				deliveries, err := duePost[[]minimq.Delivery](client, broker.url, durablePullPath, request)
				if err != nil || len(deliveries) != batch {
					return fmt.Errorf("pull for %s delivered %d of %d, %v", request.Worker, len(deliveries), batch, err)
				}
				pullSamples[worker] = append(pullSamples[worker], time.Since(at))
				for _, delivery := range deliveries {
					msg := delivery.Message
					if msg.Offset >= count || byOffset[msg.Offset].request.ID != msg.ID || !bytes.Equal(msg.Body, body) || delivery.Attempt != 1 {
						return fmt.Errorf("unexpected delivery %s at %d, attempt %d", msg.ID, msg.Offset, delivery.Attempt)
					}
					if delivery.Instance != fmt.Sprintf("history:%d", msg.Offset) || delivery.Token == "" {
						return fmt.Errorf("unexpected delivery identity at offset %d", msg.Offset)
					}
					ack := deliveryRequest{Subscription: request.Subscription, Worker: request.Worker, Instance: delivery.Instance, Token: delivery.Token}
					at := time.Now()
					got, err := duePost[minimq.AckResult](client, broker.url, durableAckPath, ack)
					end := time.Now()
					if err != nil || got != minimq.AckAccepted {
						return fmt.Errorf("ack %s returned %q, %v", msg.ID, got, err)
					}
					completed[worker] = append(completed[worker], durableCompletion{offset: msg.Offset, ack: ack, elapsed: end.Sub(at), end: end})
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for worker, samples := range completed {
			pullTimes = append(pullTimes, pullSamples[worker]...)
			for _, sample := range samples {
				if acks[sample.offset].Token != "" {
					t.Fatalf("offset %d completed twice", sample.offset)
				}
				acks[sample.offset] = sample.ack
				ackTimes = append(ackTimes, sample.elapsed)
				endToEnd = append(endToEnd, sample.end.Sub(byOffset[sample.offset].started))
			}
		}
	}
	elapsed := time.Since(started)
	if len(ackTimes) != count {
		t.Fatalf("confirmed %d unique messages, want %d", len(ackTimes), count)
	}
	before := redriveTCPStatus(t, client, broker.url)
	if before.FirstOffset != 0 || before.NextOffset != count || before.ReservedBytes != 0 ||
		before.Scheduled != 0 || before.PendingRedrives != 0 || before.DeadLetters != 0 {
		t.Fatalf("unexpected state before restart: %+v", before)
	}
	if files, err := filepath.Glob(filepath.Join(dir, "checkpoint-*")); err != nil || len(files) != 0 {
		t.Fatalf("checkpoint overlapped the durable path trial: %v, %v", files, err)
	}
	broker.stop(t)
	client.CloseIdleConnections()
	broker = startDueBroker(t, dir, time.Time{})
	if after := redriveTCPStatus(t, client, broker.url); after != before {
		t.Fatalf("partition state after restart = %+v, want %+v", after, before)
	}
	verifyDurablePathReceipts(t, client, broker.url, byOffset, acks)

	t.Logf("durable_path clients=%d batch=%d depth=%d body_bytes=%d confirmed=%d pull_requests=%d elapsed=%s unique_acks_per_s=%.2f",
		workers, batch, durablePathDepth, len(body), len(ackTimes), len(pullTimes), elapsed, float64(len(ackTimes))/elapsed.Seconds())
	for _, group := range []struct {
		name  string
		times []time.Duration
	}{{"publish", publishTimes}, {"pull", pullTimes}, {"ack", ackTimes}, {"publish_to_ack", endToEnd}} {
		t.Logf("durable_latency %s n=%d p50=%s p99=%s", group.name, len(group.times), duePercentile(group.times, 50), duePercentile(group.times, 99))
	}
}

func verifyDurablePathReceipts(t *testing.T, client *http.Client, url string, byOffset []durablePublication, acks []deliveryRequest) {
	t.Helper()
	for offset, pub := range byOffset {
		if got := requireDuePost[minimq.PublishResult](t, client, url, durablePublishPath+"/status", idRequest{ID: pub.request.ID}); got != pub.receipt {
			t.Fatalf("recovered publish status = %+v, want %+v", got, pub.receipt)
		}
		if got := requireDuePost[minimq.PublishResult](t, client, url, durablePublishPath, pub.request); got != pub.receipt {
			t.Fatalf("repeated publish = %+v, want %+v", got, pub.receipt)
		}
		if got := requireDuePost[minimq.AckResult](t, client, url, durableAckPath, acks[offset]); got != minimq.AckPreviouslyAccepted {
			t.Fatalf("repeated ACK for %s = %q", pub.request.ID, got)
		}
	}
	limit := minimq.Limits{Messages: len(byOffset), Bytes: len(byOffset) * durablePathBodyBytes}
	page := requireDuePost[minimq.HistoryPage](t, client, url, "/v1/history/read", historyRequest{Limit: limit})
	if len(page.Messages) != len(byOffset) || page.Next != uint64(len(byOffset)) {
		t.Fatalf("recovered history has %d records and next offset %d", len(page.Messages), page.Next)
	}
	for i, msg := range page.Messages {
		want := byOffset[i].request
		if msg.Offset != uint64(i) || msg.ID != want.ID || !bytes.Equal(msg.Body, want.Body) {
			t.Fatalf("recovered record at %d = %s offset %d, want %s", i, msg.ID, msg.Offset, want.ID)
		}
	}
	request := pullRequest{Subscription: durablePathSubscription, Worker: "recovery", Limit: limit}
	if ds := requireDuePost[[]minimq.Delivery](t, client, url, durablePullPath, request); len(ds) != 0 {
		t.Fatalf("recovered subscription redelivered %d acknowledged messages", len(ds))
	}
}

func runDurablePathWorkers(count int, work func(int) error) error {
	var workers sync.WaitGroup
	results := make([]error, count)
	for worker := range count {
		workers.Go(func() { results[worker] = work(worker) })
	}
	workers.Wait()
	for _, err := range results {
		if err != nil {
			return err
		}
	}
	return nil
}
