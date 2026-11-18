package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

type deadPageQuery struct {
	name string
	req  listDeadRequest
	ids  []string
	size int
}

type deadPageResult struct {
	pages map[string][]time.Duration
	err   error
}

// This uses the same subprocess, routes, and TCP client as the due experiment.
// Pass a fresh copy of the seeded fixture because ordinary requests write to it.
func TestDeadLetterPagesOverTCP(t *testing.T) {
	dir := os.Getenv("MINIMQ_DEAD_PAGE_TCP_DIR")
	if dir == "" {
		t.Skip("set MINIMQ_DEAD_PAGE_TCP_DIR to a copy of the seeded fixture")
	}
	data, err := os.ReadFile(filepath.Join(dir, "dead-page-fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	var groups map[string][]string
	if err := json.Unmarshal(data, &groups); err != nil {
		t.Fatal(err)
	}
	query := func(label, name string, first, bodySize int) deadPageQuery {
		ids := groups[name]
		if first < 0 || first+16 > len(ids) {
			t.Fatalf("missing rows for fixture page %s", label)
		}
		after := ""
		if first > 0 {
			after = ids[first-1]
		}
		return deadPageQuery{label, listDeadRequest{Subscription: name, After: after, Limit: 16}, ids[first : first+16], bodySize}
	}
	n := len(groups["billing"])
	queries := []deadPageQuery{
		query("first", "billing", 0, 256),
		query("middle", "billing", n/2, 256),
		query("tail", "billing", n-32, 256),
		query("other", "email", 0, 256),
	}
	bulk := query("bulk", "bulk", 0, 64<<10)

	origin := time.Now()
	broker := startDueBroker(t, dir, origin)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 4
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	for _, q := range append(slices.Clone(queries), bulk) {
		repeats := 96
		if q.name == "bulk" {
			repeats = 24
		}
		quiet := make([]time.Duration, 0, repeats)
		var page []minimq.DeadLetter
		for range repeats {
			at := time.Now()
			page = requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", q.req)
			quiet = append(quiet, time.Since(at))
			if err := checkDeadPage(q, page); err != nil {
				t.Fatal(err)
			}
		}
		logDeadPageTimes(t, "quiet", q.name, quiet)
		if q.name == "first" || q.name == "bulk" {
			writer := deadPageDiscard{header: make(http.Header)}
			elapsed := make([]time.Duration, 0, repeats)
			for range repeats {
				at := time.Now()
				respond(writer, page, nil)
				elapsed = append(elapsed, time.Since(at))
			}
			logDeadPageTimes(t, "encode", q.name, elapsed)
		}
	}

	const ordinary = 32
	requireDuePost[struct{}](t, client, broker.url, "/v1/subscriptions/register", registerRequest{Subscription: "online"})
	for i := range ordinary {
		req := minimq.PublishRequest{ID: fmt.Sprintf("ordinary-%02d", i), Body: []byte("work")}
		requireDuePost[minimq.PublishResult](t, client, broker.url, "/v1/publish", req)
	}

	start := time.Now().Add(20 * time.Millisecond)
	browsers := make(chan deadPageResult, 2)
	for id := range cap(browsers) {
		go func(url string) {
			browsers <- browseDeadPages(client, url, queries, start.Add(time.Duration(id)*7*time.Millisecond))
		}(broker.url)
	}
	pull := pullRequest{Subscription: "online", Worker: "worker", Limit: minimq.Limits{Messages: 1, Bytes: 64}}
	var claims, acks []time.Duration
	var receipt deliveryRequest
	for i := range ordinary {
		time.Sleep(time.Until(start.Add(time.Duration(i) * 44 * time.Millisecond)))
		at := time.Now()
		deliveries := requireDuePost[[]minimq.Delivery](t, client, broker.url, "/v1/deliveries/pull", pull)
		claims = append(claims, time.Since(at))
		if len(deliveries) != 1 || deliveries[0].Message.ID != fmt.Sprintf("ordinary-%02d", i) {
			t.Fatalf("ordinary pull %d returned %d deliveries", i, len(deliveries))
		}
		d := deliveries[0]
		receipt = deliveryRequest{Subscription: pull.Subscription, Worker: pull.Worker, Instance: d.Instance, Token: d.Token}
		at = time.Now()
		ack := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", receipt)
		acks = append(acks, time.Since(at))
		if ack != minimq.AckAccepted {
			t.Fatalf("ordinary ack %d = %q", i, ack)
		}
	}
	pages := make(map[string][]time.Duration)
	for range cap(browsers) {
		result := <-browsers
		if result.err != nil {
			t.Fatal(result.err)
		}
		for name, values := range result.pages {
			pages[name] = append(pages[name], values...)
		}
	}
	for _, q := range queries {
		logDeadPageTimes(t, "mixed", q.name, pages[q.name])
	}
	logDeadPageTimes(t, "mixed", "pull", claims)
	logDeadPageTimes(t, "mixed", "ack", acks)

	broker.stop(t)
	broker = startDueBroker(t, dir, origin)
	for _, q := range append(slices.Clone(queries), bulk) {
		page := requireDuePost[[]minimq.DeadLetter](t, client, broker.url, "/v1/dead-letters/list", q.req)
		if err := checkDeadPage(q, page); err != nil {
			t.Fatal(err)
		}
	}
	if got := requireDuePost[minimq.AckResult](t, client, broker.url, "/v1/deliveries/ack", receipt); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("recovered ordinary ACK = %q", got)
	}
}

func browseDeadPages(client *http.Client, url string, queries []deadPageQuery, start time.Time) deadPageResult {
	result := deadPageResult{pages: make(map[string][]time.Duration)}
	for i := range 64 {
		time.Sleep(time.Until(start.Add(time.Duration(i) * 22 * time.Millisecond)))
		q := queries[i%len(queries)]
		at := time.Now()
		page, err := duePost[[]minimq.DeadLetter](client, url, "/v1/dead-letters/list", q.req)
		result.pages[q.name] = append(result.pages[q.name], time.Since(at))
		if err == nil {
			err = checkDeadPage(q, page)
		}
		if err != nil {
			result.err = err
			break
		}
	}
	return result
}

func checkDeadPage(q deadPageQuery, page []minimq.DeadLetter) error {
	if len(page) != len(q.ids) {
		return fmt.Errorf("%s page length = %d, want %d", q.name, len(page), len(q.ids))
	}
	for i, d := range page {
		body := d.Message.Body
		if d.ID != q.ids[i] || d.Subscription != q.req.Subscription || len(body) != q.size || body[0] != 'm' || body[len(body)-1] != 'm' {
			return fmt.Errorf("%s page mismatch at position %d", q.name, i)
		}
	}
	return nil
}

func logDeadPageTimes(t *testing.T, phase, name string, samples []time.Duration) {
	t.Helper()
	slices.Sort(samples)
	t.Logf("phase=%s page=%s count=%d p50=%s p99=%s", phase, name, len(samples), samples[(len(samples)-1)/2], samples[(99*len(samples)+99)/100-1])
}

type deadPageDiscard struct{ header http.Header }

func (w deadPageDiscard) Header() http.Header       { return w.header }
func (deadPageDiscard) WriteHeader(int)             {}
func (deadPageDiscard) Write(p []byte) (int, error) { return len(p), nil }
