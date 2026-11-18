package minimq

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSealedHistoryBatchAcrossSegmentsAndTail(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SegmentMessages: 2}
	p := openTest(t, dir, cfg)
	bodies := [][]byte{[]byte("a"), []byte("bb"), nil, []byte("dddd"), []byte("eeeee"), []byte("ffffff")}
	for i, body := range bodies[:5] {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprint(i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if p.tailOffset != 4 || len(p.tail) != 1 || len(p.segments) != 2 {
		t.Fatalf("unexpected resident history after seal: tail offset %d, tail length %d, segments %d",
			p.tailOffset, len(p.tail), len(p.segments))
	}
	if _, err := p.Publish(PublishRequest{ID: "5", Body: bodies[5]}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if p.tailOffset != 4 || len(p.tail) != 2 || len(p.segments) != 2 {
		t.Fatalf("unexpected resident history after replay: tail offset %d, tail length %d, segments %d",
			p.tailOffset, len(p.tail), len(p.segments))
	}
	page, err := p.Read(1, Limits{Messages: 6, Bytes: 11})
	if err != nil || page.Next != 5 || len(page.Messages) != 4 {
		t.Fatalf("page = %+v, %v", page, err)
	}
	for i, msg := range page.Messages {
		if msg.Offset != uint64(i+1) || !bytes.Equal(msg.Body, bodies[i+1]) {
			t.Fatalf("wrong record at index %d: %+v", i, msg)
		}
	}
	page.Messages[0].Body[0] = 'x'
	page, err = p.Read(1, Limits{Messages: 1, Bytes: 2})
	if err != nil || len(page.Messages) != 1 || string(page.Messages[0].Body) != "bb" {
		t.Fatalf("second read aliasing result: %+v, %v", page, err)
	}
	if _, err := p.Read(1, Limits{Messages: 4, Bytes: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("byte limit below first record: %v", err)
	}
	page, err = p.Read(5, Limits{Messages: 1, Bytes: 6})
	if err != nil || page.Next != 6 || len(page.Messages) != 1 || !bytes.Equal(page.Messages[0].Body, bodies[5]) {
		t.Fatalf("journal tail after recovery: %+v, %v", page, err)
	}
}

func TestCorruptSealedHistoryStopsReadsAndRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{SegmentMessages: 1}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "worker", SubscriptionConfig{})
	publishTest(t, p, "payload")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "history-0000000000000000")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var data [1]byte
	if _, err := f.ReadAt(data[:], 9); err != nil {
		t.Fatal(err)
	}
	data[0] ^= 0x80
	if _, err := f.WriteAt(data[:], 9); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(0, pageLimit); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("online corruption: %v", err)
	}
	if _, err := p.Pull("worker", "consumer", pageLimit); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("partition continued serving after damaged segment: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("recovery skipped committed corruption: %v", err)
	}
}

func TestExpiredSealedLeaseCopiesDeadLetterBeforeReclaim(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now), SegmentMessages: 1, HistoryRetention: time.Second}
	dir := t.TempDir()
	p := openTest(t, dir, cfg)
	registerTest(t, p, "worker", SubscriptionConfig{LeaseDuration: time.Minute, MaxAttempts: 1})
	publishTest(t, p, "body")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	d := pullTest(t, p, "worker", "consumer", 1)[0]
	now = d.Deadline
	if _, err := p.AdvanceRetries(Limits{Messages: 1, Bytes: 3}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("dead letter too large for expiry batch: %v", err)
	}
	if n, err := p.AdvanceRetries(pageLimit); err != nil || n != 1 {
		t.Fatalf("expiry after bounded batch = %d, %v", n, err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "history-0000000000000000")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source history not reclaimed: %v", err)
	}
	letters, err := p.ListDeadLetters("worker", "", 10)
	if err != nil || len(letters) != 1 || string(letters[0].Message.Body) != "body" {
		t.Fatalf("independent dead letter = %+v, %v", letters, err)
	}
	result, err := p.Redrive("worker", letters[0].ID, "restore-once")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if retry, err := p.Redrive("worker", letters[0].ID, "restore-once"); err != nil || retry != result {
		t.Fatalf("redrive after replay = %+v, %v", retry, err)
	}
	redriven := pullTest(t, p, "worker", "consumer", 1)[0]
	if redriven.Instance != result.Instance || string(redriven.Message.Body) != "body" {
		t.Fatalf("redriven record = %+v", redriven)
	}
}
