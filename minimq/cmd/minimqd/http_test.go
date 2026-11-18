package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

func call[Response any](t *testing.T, handler http.Handler, path string, request any, code int) Response {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
	if w.Code != code {
		t.Fatalf("%s status = %d, want %d. Body: %s", path, w.Code, code, w.Body.String())
	}
	var result Response
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHTTPPublishReadPullAndAckAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	handler := routes(p)
	limit := minimq.Limits{Messages: 10, Bytes: 4096}
	call[struct{}](t, handler, "/v1/subscriptions/register", registerRequest{Subscription: "notify"}, http.StatusOK)
	req := minimq.PublishRequest{ID: "order-shipped", Body: []byte("order 731")}
	pub := call[minimq.PublishResult](t, handler, "/v1/publish", req, http.StatusOK)
	page := call[minimq.HistoryPage](t, handler, "/v1/history/read", historyRequest{Limit: limit}, http.StatusOK)
	if page.Next != pub.Offset+1 || len(page.Messages) != 1 || !bytes.Equal(page.Messages[0].Body, req.Body) {
		t.Fatalf("unexpected history: %+v", page)
	}
	pull := pullRequest{Subscription: "notify", Worker: "sender", Limit: limit}
	deliveries := call[[]minimq.Delivery](t, handler, "/v1/deliveries/pull", pull, http.StatusOK)
	if len(deliveries) != 1 || deliveries[0].Attempt != 1 {
		t.Fatalf("history read changed delivery state: %+v", deliveries)
	}
	ack := deliveryRequest{Subscription: "notify", Worker: "sender", Instance: deliveries[0].Instance, Token: deliveries[0].Token}
	if got := call[minimq.AckResult](t, handler, "/v1/deliveries/ack", ack, http.StatusOK); got != minimq.AckAccepted {
		t.Fatalf("ACK = %q", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	handler = routes(reopened)
	if got := call[minimq.AckResult](t, handler, "/v1/deliveries/ack", ack, http.StatusOK); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("recovered ACK = %q", got)
	}
	if got := call[[]minimq.Delivery](t, handler, "/v1/deliveries/pull", pull, http.StatusOK); len(got) != 0 {
		t.Fatalf("ACKed message was redelivered: %+v", got)
	}
}

func TestFullDeadLetterBatchOverTCP(t *testing.T) {
	if os.Getenv("MINIMQ_WAL_TCP_TEST") != "1" {
		t.Skip("set MINIMQ_WAL_TCP_TEST=1 to exercise a full dead-letter batch over TCP")
	}
	const bodySize = 64 << 10
	const count = minimq.MaxBatchBytes / bodySize
	dir := t.TempDir()
	cfg := dueExperimentClock(time.Time{})
	cfg.SegmentMessages = count
	p, err := minimq.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if err := p.Register("final", minimq.SubscriptionConfig{MaxAttempts: 1, LeaseDuration: 500 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		req := minimq.PublishRequest{ID: fmt.Sprintf("final-%d", i), Body: bytes.Repeat([]byte{byte(i)}, bodySize)}
		if _, err := p.Publish(req); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	limit := minimq.Limits{Messages: count, Bytes: minimq.MaxBatchBytes}
	if ds, err := p.Pull("final", "held", limit); err != nil || len(ds) != count {
		t.Fatalf("grant %d leases = %d, %v", count, len(ds), err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	origin := time.Now().Add(-time.Second)
	broker := startDueBroker(t, dir, origin)
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	requireDuePost[struct{}](t, client, broker.url, "/v1/subscriptions/register", registerRequest{Subscription: "ordinary"})
	ordinary := minimq.PublishRequest{ID: "ordinary", Body: []byte("ack")}
	requireDuePost[minimq.PublishResult](t, client, broker.url, "/v1/publish", ordinary)
	pull := pullRequest{Subscription: "ordinary", Worker: "online", Limit: minimq.Limits{Messages: 1, Bytes: 64}}
	ds := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", pull)
	if len(ds) != 1 || ds[0].Message.ID != ordinary.ID {
		t.Fatalf("ordinary TCP Pull returned %d deliveries", len(ds))
	}
	ack := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: ds[0].Instance, Token: ds[0].Token}
	if got := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", ack); got != minimq.AckAccepted {
		t.Fatalf("ordinary TCP ACK = %q", got)
	}

	list := listDeadRequest{Subscription: "final", Limit: count}
	deadline := time.Now().Add(5 * time.Second)
	var letters []minimq.DeadLetter
	for len(letters) == 0 && time.Now().Before(deadline) {
		letters = requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", list)
		if len(letters) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	check := func(letters []minimq.DeadLetter) {
		t.Helper()
		if len(letters) != count {
			t.Fatalf("visible dead-letter batch = %d, want %d", len(letters), count)
		}
		seen := make(map[uint64]bool, count)
		for _, letter := range letters {
			offset := letter.Message.Offset
			if offset >= count || seen[offset] || letter.Attempts != 1 || letter.Reason != "lease expired" ||
				letter.Message.ID != fmt.Sprintf("final-%d", offset) ||
				!bytes.Equal(letter.Message.Body, bytes.Repeat([]byte{byte(offset)}, bodySize)) {
				t.Fatalf("invalid dead letter for offset %d", offset)
			}
			seen[offset] = true
		}
	}
	check(letters)
	broker.stop(t)
	broker = startDueBroker(t, dir, origin)
	recovered := requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", list)
	check(recovered)
	for i, letter := range recovered {
		if letter.ID != letters[i].ID {
			t.Fatalf("dead-letter ID changed at list position %d", i)
		}
	}
	list.After = recovered[len(recovered)-1].ID
	if extra := requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", list); len(extra) != 0 {
		t.Fatalf("reopened TCP list contains %d extra letters", len(extra))
	}
	if got := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", ack); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("recovered TCP ACK = %q", got)
	}
	t.Logf("verified %d dead letters over TCP before and after process exit", count)
}
