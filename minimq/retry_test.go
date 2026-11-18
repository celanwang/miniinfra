package minimq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExpiryKeepsOrderedPrefixAcrossBatchLimitsAndReopen(t *testing.T) {
	dir := t.TempDir()
	lower := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	upper := lower
	cfg := Config{SegmentMessages: 4, Clock: func() (TimeBounds, error) {
		return TimeBounds{Lower: lower, Upper: upper}, nil
	}}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "alpha", SubscriptionConfig{MaxAttempts: 2})
	registerTest(t, p, "beta", SubscriptionConfig{MaxAttempts: 1})
	payloads := []string{"aaa", "bbbb", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	for offset, body := range payloads {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("message-%d", offset), Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	alpha := pullTest(t, p, "alpha", "old", len(payloads))
	pullTest(t, p, "beta", "old", len(payloads))
	deadline := alpha[0].Deadline
	lower, upper = deadline.Add(-time.Nanosecond), deadline
	if n, err := p.AdvanceRetries(pageLimit); err != nil || n != 0 {
		t.Fatalf("expiry before trusted lower bound reached deadline: %d, %v", n, err)
	}

	lower = deadline
	if n, err := p.AdvanceRetries(Limits{Messages: 3, Bytes: 5}); err != nil || n != 3 {
		t.Fatalf("count-limited expiry = %d, %v", n, err)
	}
	for i := range payloads {
		// For equal deadlines, history:10 sorts before history:2.
		selected := i == 0 || i == 1 || i == 10
		settled := p.subscriptions["alpha"].Tasks[uint64(i)].Lease.Token == ""
		if settled != selected {
			t.Fatalf("alpha offset %d settled = %t, want %t", i, settled, selected)
		}
		if task := p.subscriptions["beta"].Tasks[uint64(i)]; task.Lease.Token == "" {
			t.Fatalf("beta offset %d preceded a tied alpha lease", i)
		}
	}

	// Local expiration in Pull leaves the other subscription's tied leases alone.
	ds, err := p.Pull("alpha", "new", Limits{Messages: 1, Bytes: 16})
	if err != nil || len(ds) != 1 || ds[0].Instance != alpha[0].Instance || ds[0].Attempt != 2 || ds[0].Token == alpha[0].Token {
		t.Fatalf("fresh alpha claim = %+v, %v", ds, err)
	}
	if n, err := p.AdvanceRetries(Limits{Messages: 3, Bytes: 2}); !errors.Is(err, ErrInvalid) || n != 0 {
		t.Fatalf("oversized head of ordered prefix = %d, %v", n, err)
	}
	if n, err := p.AdvanceRetries(Limits{Messages: 3, Bytes: 5}); err != nil || n != 1 {
		t.Fatalf("byte-limited expiry with a smaller later entry = %d, %v", n, err)
	}
	assertLetters := func(want ...uint64) {
		t.Helper()
		letters, err := p.ListDeadLetters("beta", "", MaxBatchMessages)
		if err != nil || len(letters) != len(want) {
			t.Fatalf("dead letter count = %d, %v; want %d", len(letters), err, len(want))
		}
		expected := make(map[uint64]string, len(want))
		for _, offset := range want {
			expected[offset] = payloads[offset]
		}
		for _, letter := range letters {
			body, found := expected[letter.Message.Offset]
			if !found || string(letter.Message.Body) != body || letter.Attempts != 1 {
				t.Fatalf("unexpected or duplicate letter at offset %d", letter.Message.Offset)
			}
			delete(expected, letter.Message.Offset)
		}
	}
	assertLetters(0)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	assertLetters(0)
	if _, err := p.Ack("alpha", "old", alpha[0].Instance, alpha[0].Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("superseded ACK after reopen = %v", err)
	}
	ackTest(t, p, "alpha", "new", ds[0], AckAccepted)
	if n, err := p.AdvanceRetries(Limits{Messages: 3, Bytes: 5}); err != nil || n != 2 {
		t.Fatalf("recovered byte-limited expiry = %d, %v", n, err)
	}
	assertLetters(0, 1, 10)
	if n, err := p.AdvanceRetries(pageLimit); err != nil || n != len(payloads)-3 {
		t.Fatalf("drain remaining beta leases = %d, %v", n, err)
	}
	if n, err := p.AdvanceRetries(pageLimit); err != nil || n != 0 {
		t.Fatalf("second pass over drained leases = %d, %v", n, err)
	}
	offsets := make([]uint64, len(payloads))
	for i := range offsets {
		offsets[i] = uint64(i)
	}
	assertLetters(offsets...)
}

func TestExpiryOrdersTiedHistoryBeforeRedrive(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	p := openTest(t, t.TempDir(), Config{Clock: testClock(&now)})
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 1})
	publishTest(t, p, "original")
	first := pullTest(t, p, "notify", "worker", 1)[0]
	failed, err := p.Fail("notify", "worker", first.Instance, first.Token, "test")
	if err != nil {
		t.Fatal(err)
	}
	redrive, err := p.Redrive("notify", failed.DeadLetterID, "redo")
	if err != nil {
		t.Fatal(err)
	}
	publishTest(t, p, "independent")
	ds := pullTest(t, p, "notify", "worker", 2)
	if ds[0].Instance != redrive.Instance || !ds[0].Deadline.Equal(ds[1].Deadline) {
		t.Fatalf("expected tied history and redrive leases: %+v", ds)
	}
	now = ds[0].Deadline
	limit := Limits{Messages: 1, Bytes: 64}
	if n, err := p.AdvanceRetries(limit); err != nil || n != 1 {
		t.Fatalf("expire one tied lease = %d, %v", n, err)
	}
	letters, err := p.ListDeadLetters("notify", "", 3)
	if err != nil || len(letters) != 2 {
		t.Fatalf("after first expiry: %+v, %v", letters, err)
	}
	for _, letter := range letters {
		if letter.ID != failed.DeadLetterID && letter.Instance != ds[1].Instance {
			t.Fatalf("redrive expired before history: %+v", letter)
		}
	}
	if n, err := p.AdvanceRetries(limit); err != nil || n != 1 {
		t.Fatalf("expire redrive = %d, %v", n, err)
	}
	letters, err = p.ListDeadLetters("notify", "", 3)
	if err != nil || len(letters) != 3 {
		t.Fatalf("after redrive expiry: %+v, %v", letters, err)
	}
	for _, letter := range letters {
		if letter.Instance == redrive.Instance && letter.SourceDeadLetterID == failed.DeadLetterID {
			return
		}
	}
	t.Fatal("expired redrive lost its source")
}

func TestRetryBackoffAndInterruptedDeadLetterConversion(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: time.Minute, RetryDelay: time.Hour, MaxAttempts: 2})
	publishTest(t, p, "notice")
	first := pullTest(t, p, "notify", "a", 1)[0]
	now = first.Deadline
	if count, err := p.AdvanceRetries(pageLimit); err != nil || count != 1 {
		t.Fatalf("expire first lease = %d, %v", count, err)
	}
	pullTest(t, p, "notify", "b", 0)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	now = now.Add(time.Hour)
	second := pullTest(t, p, "notify", "b", 1)[0]
	if second.Attempt != 2 {
		t.Fatalf("attempt = %d", second.Attempt)
	}
	headPath := filepath.Join(dir, "HEAD")
	before, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatal(err)
	}
	now = second.Deadline
	if count, err := p.AdvanceRetries(pageLimit); err != nil || count != 1 {
		t.Fatalf("expire final lease = %d, %v", count, err)
	}
	letters, err := p.ListDeadLetters("notify", "", 10)
	if err != nil || len(letters) != 1 || string(letters[0].Message.Body) != "notice" {
		t.Fatalf("dead letters: %+v, %v", letters, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Emulate death after the conversion frame reached disk but before HEAD was installed.
	if err := os.WriteFile(headPath, before, 0o600); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	letters, err = p.ListDeadLetters("notify", "", 10)
	if err != nil || len(letters) != 0 {
		t.Fatalf("uncommitted dead letter became visible: %+v, %v", letters, err)
	}
	history, err := p.Read(0, pageLimit)
	if err != nil || len(history.Messages) != 1 || string(history.Messages[0].Body) != "notice" {
		t.Fatalf("source payload was lost: %+v, %v", history, err)
	}
	if count, err := p.AdvanceRetries(pageLimit); err != nil || count != 1 {
		t.Fatalf("retry conversion = %d, %v", count, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if count, err := p.AdvanceRetries(pageLimit); err != nil || count != 0 {
		t.Fatalf("repeat conversion = %d, %v", count, err)
	}
	letters, err = p.ListDeadLetters("notify", "", 10)
	if err != nil || len(letters) != 1 || letters[0].Attempts != 2 {
		t.Fatalf("recovered dead letters: %+v, %v", letters, err)
	}
	pullTest(t, p, "notify", "c", 0)
}
