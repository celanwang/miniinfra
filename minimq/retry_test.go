package minimq

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
