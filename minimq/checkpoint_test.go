package minimq

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCheckpointPreservesProtectedHistoryAndReclaimsSealedPrefix(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now), HistoryRetention: time.Minute, SegmentMessages: 1}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 1})
	publishTest(t, p, "notice")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "history-0000000000000000")
	if _, err := os.Stat(source); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("protected segment disappeared: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	d := pullTest(t, p, "notify", "worker", 1)[0]
	failure, err := p.Fail("notify", "worker", d.Instance, d.Token, "permanent failure")
	if err != nil || failure.DeadLetterID == "" {
		t.Fatalf("dead-letter conversion = %+v, %v", failure, err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("obsolete segment still exists: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	var gap *OffsetGap
	if _, err := p.Read(0, pageLimit); !errors.As(err, &gap) || gap.First != 1 {
		t.Fatalf("expected durable offset gap, got %v", err)
	}
	if letter, err := p.GetDeadLetter("notify", failure.DeadLetterID); err != nil || string(letter.Message.Body) != "notice" {
		t.Fatalf("dead letter after source segment removal = %+v, %v", letter, err)
	}
	if got := publishTest(t, p, "later"); got.Offset != 1 {
		t.Fatalf("offset reused after compaction: %+v", got)
	}
}

func TestCapacityRejectionLeavesSpaceForTaskCompletion(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now), CapacityBytes: 220 << 10, SegmentMessages: 1, HistoryRetention: time.Minute}
	p := openTest(t, t.TempDir(), cfg)
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 1})
	for _, id := range []string{"one", "two", "three"} {
		publishTest(t, p, id)
	}
	if _, err := p.Publish(PublishRequest{ID: "four"}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("fourth publish: %v", err)
	}
	deliveries := pullTest(t, p, "notify", "worker", 3)
	ackTest(t, p, "notify", "worker", deliveries[0], AckAccepted)
	for _, d := range deliveries[1:] {
		if _, err := p.Fail("notify", "worker", d.Instance, d.Token, "reject"); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Hour)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := publishTest(t, p, "four"); got.Offset != 3 {
		t.Fatalf("refused publish took an offset: %+v", got)
	}
}
