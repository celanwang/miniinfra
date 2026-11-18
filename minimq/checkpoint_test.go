package minimq

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func publishLargeCheckpointTail(t *testing.T, p *Partition) []byte {
	t.Helper()
	body := bytes.Repeat([]byte{'x'}, MaxMessageBytes)
	for i := range MaxBatchBytes / MaxMessageBytes {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("tail-%d", i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestCheckpointSealsRepeatedPrefixAndRetainsSparseSuffix(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now), HistoryRetention: time.Hour}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: 2 * time.Hour})
	body := publishLargeCheckpointTail(t, p)
	prefix := uint64(MaxBatchBytes / MaxMessageBytes)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 0 || uint64(len(p.tail)) != prefix {
		t.Fatal("idle checkpoint sealed the tail")
	}
	now = now.Add(30 * time.Minute)
	leases, err := p.Pull("notify", "worker", Limits{Messages: 2, Bytes: 2 * len(body)})
	if err != nil || len(leases) != 2 {
		t.Fatalf("pull before sealing: %d, %v", len(leases), err)
	}
	ackTest(t, p, "notify", "worker", leases[0], AckAccepted)
	if got := publishTest(t, p, "sparse"); got.Offset != prefix {
		t.Fatalf("sparse offset = %d, want %d", got.Offset, prefix)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 || uint64(p.segments[0].Count) != prefix ||
		len(p.tail) != 1 || p.tail[0].Offset != prefix {
		t.Fatalf("segments = %d, tail = %d; want one sealed prefix and one inline suffix", len(p.segments), len(p.tail))
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	page, err := p.Read(prefix-1, Limits{Messages: 2, Bytes: 2 * len(body)})
	if err != nil || len(page.Messages) != 2 || !bytes.Equal(page.Messages[0].Body, body) || string(page.Messages[1].Body) != "sparse" {
		t.Fatalf("read across checkpoint prefix and suffix: next=%d, count=%d, %v", page.Next, len(page.Messages), err)
	}
	ackTest(t, p, "notify", "worker", leases[0], AckPreviouslyAccepted)
	ackTest(t, p, "notify", "worker", leases[1], AckAccepted)
	now = now.Add(30 * time.Minute)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if status, _ := p.Status(); status.FirstOffset != 0 {
		t.Fatalf("unsettled records lost retention: %+v", status)
	}
	remaining, err := p.Pull("notify", "worker", Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes})
	if err != nil || len(remaining) != int(prefix)-1 {
		t.Fatalf("remaining deliveries = %d, %v", len(remaining), err)
	}
	for _, d := range remaining {
		ackTest(t, p, "notify", "worker", d, AckAccepted)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	var gap *OffsetGap
	if _, err := p.Read(prefix-1, pageLimit); !errors.As(err, &gap) || gap.First != prefix {
		t.Fatalf("expired settled history: %v", err)
	}
	page, err = p.Read(prefix, pageLimit)
	if err != nil || len(page.Messages) != 1 || string(page.Messages[0].Body) != "sparse" {
		t.Fatalf("sparse history after collection: %+v, %v", page, err)
	}
}

func TestCheckpointKeepsInlineTailWhenEarlySealCannotFit(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := openTest(t, t.TempDir(), Config{Clock: testClock(&now)})
	publishLargeCheckpointTail(t, p)
	registerTest(t, p, "late", SubscriptionConfig{})
	p.observe(func(string, operationTiming) {})
	clock, err := p.clock()
	if err != nil {
		t.Fatal(err)
	}
	// Obtain the exact inline staging requirement without writing a file.
	p.config.CapacityBytes = 1
	if err := p.compact(clock, 0); !errors.Is(err, ErrCapacity) {
		t.Fatalf("inline capacity preflight = %v", err)
	}
	p.config.CapacityBytes = p.storageBytes() + int64(p.timings.checkpoint.imageBytes+sha256.Size+8192)
	if prefix := p.checkpointedTailPrefix(); prefix == 0 {
		t.Fatal("tail did not qualify for sealing")
	} else if err := p.compact(clock, prefix); !errors.Is(err, ErrCapacity) {
		t.Fatalf("early segment capacity preflight = %v", err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 0 || len(p.tail) != MaxBatchBytes/MaxMessageBytes || p.journal.End() != 0 {
		t.Fatal("capacity fallback did not install the inline checkpoint")
	}
}

func TestCheckpointProcessExitAtSyncBoundaries(t *testing.T) {
	const crashCode = 74
	cfg := Config{SegmentMessages: 1}
	if stop := os.Getenv("MINIMQ_TEST_CHECKPOINT_SYNC"); stop != "" {
		boundary, err := strconv.Atoi(stop)
		if err != nil {
			t.Fatal(err)
		}
		p := openTest(t, os.Getenv("MINIMQ_TEST_CHECKPOINT_DIR"), cfg)
		var calls int
		p.journal.ObserveSync = func(time.Duration) {
			calls++
			if calls == boundary {
				os.Exit(crashCode)
			}
		}
		if err := p.Compact(); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("completed without reaching sync %d", boundary)
	}

	for _, cut := range []struct {
		name       string
		sync       int
		generation uint64
	}{
		{"first-segment-data", 1, 2},
		{"staged-entries-durable", 5, 2},
		{"head-data-before-replace", 6, 2},
		{"head-durable-before-collection", 7, 3},
	} {
		t.Run(cut.name, func(t *testing.T) {
			dir := t.TempDir()
			p := openTest(t, dir, cfg)
			registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: 2 * time.Minute})
			publishTest(t, p, "first")
			if err := p.Compact(); err != nil {
				t.Fatal(err)
			}
			publishTest(t, p, "second")
			publishTest(t, p, "third")
			leases := pullTest(t, p, "notify", "worker", 3)
			ackTest(t, p, "notify", "worker", leases[0], AckAccepted)
			ackTest(t, p, "notify", "worker", leases[1], AckAccepted)
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointProcessExitAtSyncBoundaries$")
			cmd.Env = append(os.Environ(), "MINIMQ_TEST_CHECKPOINT_DIR="+dir,
				"MINIMQ_TEST_CHECKPOINT_SYNC="+strconv.Itoa(cut.sync))
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != crashCode {
				t.Fatalf("child did not exit at the requested boundary: %v\n%s", err, output)
			}
			head, err := os.ReadFile(filepath.Join(dir, "HEAD"))
			if err != nil || len(head) != 24 || binary.LittleEndian.Uint64(head[4:]) != cut.generation {
				t.Fatalf("expected HEAD generation %d, got %x, %v", cut.generation, head, err)
			}

			recovered := openTest(t, dir, cfg)
			page, err := recovered.Read(0, pageLimit)
			if err != nil || page.Next != 3 || len(page.Messages) != 3 {
				t.Fatalf("recovered history = %+v, %v", page, err)
			}
			for i, want := range []string{"first", "second", "third"} {
				if string(page.Messages[i].Body) != want {
					t.Fatalf("body %d = %q, want %q", i, page.Messages[i].Body, want)
				}
			}
			ackTest(t, recovered, "notify", "worker", leases[0], AckPreviouslyAccepted)
			ackTest(t, recovered, "notify", "worker", leases[1], AckPreviouslyAccepted)
			pullTest(t, recovered, "notify", "other-worker", 0)
			ackTest(t, recovered, "notify", "worker", leases[2], AckAccepted)
		})
	}
}

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

func TestCompactReclaimsExpiredPartialSegment(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now), SegmentMessages: 16, HistoryRetention: time.Minute}
	p := openTest(t, dir, cfg)
	publishTest(t, p, "only-message")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if history, err := p.Read(0, pageLimit); err != nil || len(history.Messages) != 1 {
		t.Fatalf("unexpired partial segment: %+v, %v", history, err)
	}
	now = now.Add(time.Minute)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(0, pageLimit); err == nil {
		t.Fatal("expired partial segment was not reclaimed")
	}
	if got := publishTest(t, p, "later"); got.Offset != 1 {
		t.Fatalf("new publication reused offset: %+v", got)
	}
}
