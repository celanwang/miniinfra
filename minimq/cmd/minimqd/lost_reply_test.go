package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

func TestCommittedDeliveryRepliesLostOverTCP(t *testing.T) {
	root := t.TempDir()
	dir, clock := filepath.Join(root, "data"), filepath.Join(root, "clock")
	setTime := func(now time.Time) {
		t.Helper()
		next := clock + ".next"
		if err := os.WriteFile(next, []byte(now.Format(time.RFC3339Nano)), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, clock); err != nil {
			t.Fatal(err)
		}
	}
	setTime(dueExperimentEpoch)
	start := func() *dueBroker {
		t.Helper()
		return startDueBrokerWithClock(t, dir, time.Time{}, clock)
	}
	broker := start()
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	const lease = time.Minute
	const backoff = 30 * time.Second
	requireDuePost[struct{}](t, client, broker.url, "/v1/subscriptions/register", registerRequest{
		Subscription: "notify",
		Config:       minimq.SubscriptionConfig{LeaseDuration: lease, RetryDelay: backoff, MaxAttempts: 3},
	})
	message := minimq.PublishRequest{ID: "one-message", Body: []byte("send a notice")}
	published := requireDuePost[minimq.PublishResult](t, client, broker.url, "/v1/publish", message)
	pull := pullRequest{Subscription: "notify", Worker: "worker", Limit: minimq.Limits{Messages: 1, Bytes: 64}}
	const pullPath = "/v1/deliveries/pull"
	const ackPath = "/v1/deliveries/ack"

	// Only the test observer gets these values. The client sees an EOF.
	first := dropSuccessfulReply[[]minimq.Delivery](t, client, broker.url, pullPath, pull)
	if len(first) != 1 || first[0].Attempt != 1 || first[0].Instance == "" || first[0].Token == "" ||
		first[0].Message.ID != message.ID || first[0].Message.Offset != published.Offset ||
		!bytes.Equal(first[0].Message.Body, message.Body) ||
		!first[0].Deadline.Equal(dueExperimentEpoch.Add(lease)) {
		t.Fatalf("first Pull at the origin = %+v", first)
	}
	old := first[0]
	oldACK := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: old.Instance, Token: old.Token}
	broker.stop(t)
	client.CloseIdleConnections()
	broker = start()
	requireEmpty := func(phase string) {
		t.Helper()
		probe := pull
		probe.Worker = "another-worker"
		if got := requireDuePost[[]minimq.Delivery](t, client, broker.url, pullPath, probe); len(got) != 0 {
			t.Fatalf("Pull %s returned %d deliveries", phase, len(got))
		}
	}
	requireEmpty("after recovery with a valid lease")
	setTime(old.Deadline.Add(-time.Nanosecond))
	requireEmpty("just before expiry")
	setTime(old.Deadline)
	requireEmpty("at expiry with backoff")
	ready := old.Deadline.Add(backoff)
	setTime(ready.Add(-time.Nanosecond))
	requireEmpty("just before retry readiness")

	setTime(ready)
	retry := requireDuePost[[]minimq.Delivery](t, client, broker.url, pullPath, pull)
	if len(retry) != 1 || retry[0].Instance != old.Instance || retry[0].Attempt != old.Attempt+1 ||
		retry[0].Token == "" || retry[0].Token == old.Token || !retry[0].Deadline.Equal(ready.Add(lease)) ||
		retry[0].Message.ID != message.ID || retry[0].Message.Offset != published.Offset ||
		!bytes.Equal(retry[0].Message.Body, message.Body) {
		t.Fatalf("Pull at retry readiness = %+v", retry)
	}
	fresh := retry[0]
	stale, err := postLostReplyRequest(t, client, broker.url+ackPath, oldACK)
	if err != nil {
		t.Fatal(err)
	}
	var failure errorResponse
	err = json.NewDecoder(stale.Body).Decode(&failure)
	stale.Body.Close()
	if err != nil || stale.StatusCode != http.StatusConflict || failure.Code != "lease_expired" {
		t.Fatalf("old ACK = HTTP %d, code %q, %v", stale.StatusCode, failure.Code, err)
	}

	newACK := deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: fresh.Instance, Token: fresh.Token}
	if got := dropSuccessfulReply[minimq.AckResult](t, client, broker.url, ackPath, newACK); got != minimq.AckAccepted {
		t.Fatalf("new ACK at the origin = %q", got)
	}
	broker.stop(t)
	client.CloseIdleConnections()
	setTime(fresh.Deadline.Add(backoff))
	broker = start()
	requireEmpty("after losing the ACK reply and passing retry readiness")
	if got := requireDuePost[minimq.AckResult](t, client, broker.url, ackPath, newACK); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("repeated ACK after recovery = %q", got)
	}
	page := requireDuePost[minimq.HistoryPage](t, client, broker.url, "/v1/history/read", historyRequest{
		Limit: minimq.Limits{Messages: 2, Bytes: 128},
	})
	if page.Next != published.Offset+1 || len(page.Messages) != 1 || page.Messages[0].ID != message.ID ||
		page.Messages[0].Offset != published.Offset || !bytes.Equal(page.Messages[0].Body, message.Body) {
		t.Fatalf("history after both recoveries = %+v", page)
	}
}

// The proxy reads the complete upstream HTTP response, then closes the
// downstream TCP connection before writing any status line or response body.
func dropSuccessfulReply[Response any](t *testing.T, client *http.Client, origin, path string, request any) Response {
	t.Helper()
	type observation struct {
		reply Response
		err   error
	}
	observed := make(chan observation, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sample observation
		sample.reply, sample.err = readOriginReply[Response](client, origin, r)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			err = conn.Close()
		}
		sample.err = errors.Join(sample.err, err)
		observed <- sample
	}))
	defer proxy.Close()
	res, err := postLostReplyRequest(t, client, proxy.URL+path, request)
	if res != nil {
		_ = res.Body.Close()
	}
	if res != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("%s client saw response %t and error %v, want EOF before an HTTP response", path, res != nil, err)
	}
	select {
	case sample := <-observed:
		if sample.err != nil {
			t.Fatalf("%s origin observation failed: %v", path, sample.err)
		}
		return sample.reply
	case <-time.After(5 * time.Second):
		t.Fatalf("%s proxy did not record the origin reply", path)
		var zero Response
		return zero
	}
}

func readOriginReply[Response any](client *http.Client, origin string, incoming *http.Request) (result Response, err error) {
	req, err := http.NewRequestWithContext(incoming.Context(), incoming.Method, origin+incoming.URL.RequestURI(), incoming.Body)
	if err != nil {
		return result, err
	}
	req.ContentLength = incoming.ContentLength
	req.Header.Set("Content-Type", incoming.Header.Get("Content-Type"))
	res, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return result, fmt.Errorf("origin HTTP status %d", res.StatusCode)
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(data, &result)
	return result, err
}

func postLostReplyRequest(t *testing.T, client *http.Client, url string, request any) (*http.Response, error) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return client.Post(url, "application/json", bytes.NewReader(body))
}
