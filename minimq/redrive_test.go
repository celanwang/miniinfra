package minimq

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRedriveBodySurvivesExternalMutationAndSourceRemoval(t *testing.T) {
	dir := t.TempDir()
	base := walFixtureTime
	now := base
	cfg := Config{
		Clock: testClock(&now), SegmentMessages: 1,
		HistoryRetention: time.Minute, DeadRetention: 2 * time.Minute,
	}
	p := openTest(t, dir, cfg)
	const delay = time.Second
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 2, RetryDelay: delay})
	wantBody := bytes.Repeat([]byte{'m'}, 128<<10)
	input := bytes.Clone(wantBody)
	if _, err := p.Publish(PublishRequest{ID: "original", Body: input}); err != nil {
		t.Fatal(err)
	}
	input[0] = 'x'
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	limit := Limits{Messages: 1, Bytes: len(wantBody)}
	pull := func() Delivery {
		t.Helper()
		got, err := p.Pull("notify", "worker", limit)
		if err != nil || len(got) != 1 || !bytes.Equal(got[0].Message.Body, wantBody) {
			t.Fatalf("Pull returned %d deliveries, %v, or changed body", len(got), err)
		}
		return got[0]
	}
	fail := func(d Delivery) FailureResult {
		t.Helper()
		got, err := p.Fail("notify", "worker", d.Instance, d.Token, "fixture")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	source := pull()
	source.Message.Body[0] = 'y'
	now = fail(source).RetryAt
	deadID := fail(pull()).DeadLetterID
	if deadID == "" {
		t.Fatal("source did not become a dead letter")
	}
	expires := now.Add(cfg.DeadRetention)
	now = base.Add(cfg.HistoryRetention)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	var gap *OffsetGap
	if _, err := p.Read(0, limit); !errors.As(err, &gap) || gap.First != 1 {
		t.Fatalf("source history was not reclaimed: %v", err)
	}

	before, err := p.Status()
	if err != nil {
		t.Fatal(err)
	}
	oldCapacity, oldEnd := p.config.CapacityBytes, p.journal.End()
	p.config.CapacityBytes = 4096
	_, err = p.Redrive("notify", deadID, "operator-1")
	p.config.CapacityBytes = oldCapacity
	if status, statusErr := p.Status(); !errors.Is(err, ErrCapacity) || statusErr != nil ||
		status != before || len(p.redriveReceipts) != 0 || p.journal.End() != oldEnd ||
		p.subscriptions["notify"].redriveCount != 0 {
		t.Fatalf("rejected redrive changed status, receipt or WAL: %v, %v", err, statusErr)
	}
	admit := func(op string) RedriveResult {
		t.Helper()
		result, err := p.Redrive("notify", deadID, op)
		if err != nil || result.Instance == "" || result.SourceDeadLetterID != deadID {
			t.Fatalf("operation %s returned %+v, %v", op, result, err)
		}
		return result
	}
	first, second, third := admit("operator-1"), admit("operator-2"), admit("operator-3")
	if first.Instance == second.Instance || first.Instance == third.Instance || second.Instance == third.Instance ||
		admit("operator-1") != first {
		t.Fatal("wrong instance identity for an independent or repeated operation")
	}
	reserve := p.subscriptions["notify"].Config.taskReserve(len(wantBody))
	if status, err := p.Status(); err != nil || status.PendingRedrives != 3 || status.ReservedBytes != before.ReservedBytes+3*reserve {
		t.Fatalf("redrive reservation = %+v, %v", status, err)
	}

	letter, err := p.GetDeadLetter("notify", deadID)
	if err != nil || !bytes.Equal(letter.Message.Body, wantBody) {
		t.Fatalf("GetDeadLetter body = %v", err)
	}
	letter.Message.Body[0] = 'a'
	list, err := p.ListDeadLetters("notify", "", 1)
	if err != nil || len(list) != 1 || !bytes.Equal(list[0].Message.Body, wantBody) {
		t.Fatalf("ListDeadLetters body = %v", err)
	}
	list[0].Message.Body[len(wantBody)-1] = 'b'
	pullRedrive := func(result RedriveResult, attempt int) Delivery {
		t.Helper()
		d := pull()
		if d.Instance != result.Instance || d.Attempt != attempt {
			t.Fatalf("got instance %s attempt %d, want %s attempt %d", d.Instance, d.Attempt, result.Instance, attempt)
		}
		return d
	}
	d := pullRedrive(first, 1)
	d.Message.Body[0] = 'c'
	now = fail(d).RetryAt
	ackTest(t, p, "notify", "worker", pullRedrive(first, 2), AckAccepted)
	if letter, err := p.GetDeadLetter("notify", deadID); err != nil || !bytes.Equal(letter.Message.Body, wantBody) {
		t.Fatalf("retry changed the original dead letter: %v", err)
	}
	now = expires
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.dead) != 0 {
		t.Fatal("checkpoint retained the expired source letter")
	}
	d = pullRedrive(second, 1)
	d.Message.Body[0] = 'd'
	ackTest(t, p, "notify", "worker", d, AckAccepted)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	ackTest(t, p, "notify", "worker", d, AckPreviouslyAccepted)
	if admit("operator-3") != third {
		t.Fatal("recovery lost the redrive operation receipt")
	}
	now = now.Add(redriveInterval)
	ackTest(t, p, "notify", "worker", pullRedrive(third, 1), AckAccepted)
	if status, err := p.Status(); err != nil || status.PendingRedrives != 0 ||
		status.ReservedBytes != before.ReservedBytes || status.NextOffset != before.NextOffset {
		t.Fatalf("status after settling redrives = %+v, %v", status, err)
	}
}

func TestRedriveAfterOriginalSegmentAndDeadLetterAreRemoved(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{
		Clock: testClock(&now), HistoryRetention: time.Minute, DeadRetention: time.Hour, SegmentMessages: 1,
	}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 1})
	registerTest(t, p, "analytics", SubscriptionConfig{})
	publishTest(t, p, "notice")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	d := pullTest(t, p, "analytics", "reader", 1)[0]
	ackTest(t, p, "analytics", "reader", d, AckAccepted)
	d = pullTest(t, p, "notify", "sender", 1)[0]
	failure, err := p.Fail("notify", "sender", d.Instance, d.Token, "unavailable")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "history-0000000000000000")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source segment was not deleted: %v", err)
	}
	first, err := p.Redrive("notify", failure.DeadLetterID, "operator-1")
	if err != nil || first.Instance == d.Instance {
		t.Fatalf("redrive = %+v, %v", first, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	// The retry is still within its receipt period, after the original dead letter expires.
	now = now.Add(time.Hour)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetDeadLetter("notify", failure.DeadLetterID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dead letter after retention: %v", err)
	}
	if repeated, err := p.Redrive("notify", failure.DeadLetterID, "operator-1"); err != nil || repeated != first {
		t.Fatalf("redrive retry = %+v, %v, want %+v", repeated, err, first)
	}
	if _, err := p.Redrive("notify", "another-dead-letter", "operator-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("operator ID reused for a different source: %v", err)
	}
	pullTest(t, p, "analytics", "reader", 0)
	publishTest(t, p, "new-notice")
	deliveries := pullTest(t, p, "notify", "sender", 2)
	replay := deliveries[0]
	if replay.Instance != first.Instance || replay.Attempt != 1 || replay.Token == d.Token ||
		replay.Message.ID != d.Message.ID || string(replay.Message.Body) != "notice" {
		t.Fatalf("unexpected redrive delivery: %+v", replay)
	}
	if deliveries[1].Message.ID != "new-notice" {
		t.Fatalf("live task did not share the batch: %+v", deliveries[1])
	}
	ackTest(t, p, "notify", "sender", replay, AckAccepted)
	ackTest(t, p, "notify", "sender", deliveries[1], AckAccepted)
	now = now.Add(time.Second)
	pullTest(t, p, "notify", "sender", 0)
	page, err := p.Read(1, pageLimit)
	if err != nil || page.Next != 2 || len(page.Messages) != 1 || page.Messages[0].ID != "new-notice" {
		t.Fatalf("redrive changed history: %+v, %v", page, err)
	}
}

func TestRedriveExpiryScopesSurviveCheckpointAndWAL(t *testing.T) {
	now := walFixtureTime
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	for _, name := range []string{"alpha", "beta"} {
		registerTest(t, p, name, SubscriptionConfig{MaxAttempts: 1})
	}
	publishTest(t, p, "source")
	registerTest(t, p, "ordinary", SubscriptionConfig{})
	admit := func(name, deadID, op string) RedriveResult {
		t.Helper()
		got, err := p.Redrive(name, deadID, op)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	sourceIDs := make(map[string]string)
	var deadline time.Time
	for _, name := range []string{"alpha", "beta"} {
		d := pullTest(t, p, name, "worker", 1)[0]
		failure, err := p.Fail(name, "worker", d.Instance, d.Token, "fixture")
		if err != nil || failure.DeadLetterID == "" {
			t.Fatalf("source failure for %s = %+v, %v", name, failure, err)
		}
		sourceIDs[name] = failure.DeadLetterID
		pending := admit(name, failure.DeadLetterID, "checkpoint")
		d = pullTest(t, p, name, "worker", 1)[0]
		if d.Instance != pending.Instance {
			t.Fatalf("first redrive for %s = %+v", name, d)
		}
		deadline = d.Deadline
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	fromWAL := admit("beta", sourceIDs["beta"], "wal")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	checkCounts := func(alpha, beta int) {
		t.Helper()
		if a, b := p.subscriptions["alpha"].redriveCount, p.subscriptions["beta"].redriveCount; a != alpha || b != beta ||
			len(p.redrives) != alpha+beta || p.subscriptions["ordinary"].redriveCount != 0 {
			t.Fatalf("redrives for alpha/beta = %d/%d with %d total, want %d/%d", a, b, len(p.redrives), alpha, beta)
		}
	}
	checkCounts(1, 2)
	now = deadline
	pullTest(t, p, "ordinary", "worker", 0)
	checkCounts(1, 2)
	pullTest(t, p, "alpha", "worker", 0)
	checkCounts(0, 2)
	if n, err := p.AdvanceRetries(pageLimit); err != nil || n != 1 {
		t.Fatalf("global expiry = %d, %v", n, err)
	}
	checkCounts(0, 1)
	d := pullTest(t, p, "beta", "worker", 1)[0]
	if d.Instance != fromWAL.Instance || d.Attempt != 1 {
		t.Fatalf("remaining WAL redrive = %+v", d)
	}
	failure, err := p.Fail("beta", "worker", d.Instance, d.Token, "fixture")
	if err != nil || failure.DeadLetterID == "" {
		t.Fatalf("redrive failure = %+v, %v", failure, err)
	}
	checkCounts(0, 0)
}
