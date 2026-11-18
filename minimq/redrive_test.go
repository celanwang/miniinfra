package minimq

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
