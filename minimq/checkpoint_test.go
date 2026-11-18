package minimq

import (
	"bytes"
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

func TestCheckpointKeepsRecentTailInlineWithPendingAndSettledTasks(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	count := MaxBatchBytes / MaxMessageBytes
	cfg := Config{Clock: testClock(&now), SegmentMessages: count + 1, HistoryRetention: time.Hour}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{})
	body := bytes.Repeat([]byte{'x'}, MaxMessageBytes)
	for i := range count {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("recent-%d", i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	leases, err := p.Pull("notify", "worker", Limits{Messages: 2, Bytes: 2 * len(body)})
	if err != nil || len(leases) != 2 {
		t.Fatalf("initial recent leases = %d, %v", len(leases), err)
	}
	ackTest(t, p, "notify", "worker", leases[0], AckAccepted)
	compactInline := func() {
		t.Helper()
		if err := p.Compact(); err != nil {
			t.Fatal(err)
		}
		if len(p.segments) != 0 || len(p.tail) != count {
			t.Fatalf("recent tail layout: %d segments, %d inline records", len(p.segments), len(p.tail))
		}
		page, err := p.Read(uint64(count-1), Limits{Messages: 1, Bytes: len(body)})
		if err != nil || len(page.Messages) != 1 || !bytes.Equal(page.Messages[0].Body, body) {
			t.Fatalf("recent read = %d messages, %v", len(page.Messages), err)
		}
	}
	compactInline()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	ackTest(t, p, "notify", "worker", leases[0], AckPreviouslyAccepted)
	ackTest(t, p, "notify", "worker", leases[1], AckAccepted)
	remaining, err := p.Pull("notify", "worker", Limits{Messages: count, Bytes: MaxBatchBytes})
	if err != nil || len(remaining) != count-2 {
		t.Fatalf("remaining recent deliveries = %d, %v", len(remaining), err)
	}
	for _, d := range remaining {
		if !bytes.Equal(d.Message.Body, body) {
			t.Fatalf("wrong body at offset %d", d.Message.Offset)
		}
		ackTest(t, p, "notify", "worker", d, AckAccepted)
	}
	compactInline()

	if got := publishTest(t, p, "suffix"); got.Offset != uint64(count) {
		t.Fatalf("suffix assigned offset %d", got.Offset)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 || p.segments[0].Count != count+1 || len(p.tail) != 0 {
		t.Fatal("full segment did not seal")
	}
	d := pullTest(t, p, "notify", "worker", 1)[0]
	ackTest(t, p, "notify", "worker", d, AckAccepted)
	now = now.Add(time.Hour)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	var gap *OffsetGap
	if _, err := p.Read(0, pageLimit); !errors.As(err, &gap) || gap.First != uint64(count+1) {
		t.Fatalf("expired settled history = %v", err)
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
	if p.journal.End() != 0 {
		t.Fatal("expected an empty WAL before age only reclamation")
	}
	if history, err := p.Read(0, pageLimit); err != nil || len(history.Messages) != 1 {
		t.Fatalf("unexpired partial segment: %+v, %v", history, err)
	}
	head := checkpointHEAD(t, dir)
	now = now.Add(time.Minute - time.Nanosecond)
	if err := p.Compact(); err != nil || !bytes.Equal(head, checkpointHEAD(t, dir)) {
		t.Fatalf("checkpoint before age boundary changed HEAD: %v", err)
	}
	now = now.Add(time.Nanosecond)
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

func TestCheckpointReclaimsTailPrefixAndRetainsSuffixAfterFailures(t *testing.T) {
	now := walFixtureTime
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now), SegmentMessages: 2, HistoryRetention: time.Minute}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{})
	publish := func(i int) {
		t.Helper()
		if got := publishTest(t, p, fmt.Sprintf("message-%d", i)); got.Offset != uint64(i) {
			t.Fatalf("publish %d took offset %d", i, got.Offset)
		}
	}
	publish(0)
	publish(1)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	publish(2)
	publish(3)
	for _, d := range pullTest(t, p, "notify", "worker", 4) {
		ackTest(t, p, "notify", "worker", d, AckAccepted)
	}
	now = now.Add(time.Minute)
	publish(4)
	publish(5)
	publish(6)
	head := checkpointHEAD(t, dir)

	checkUnchanged := func() {
		t.Helper()
		if !bytes.Equal(head, checkpointHEAD(t, dir)) || p.firstOffset != 0 || p.tailOffset != 2 ||
			p.nextOffset() != 7 || len(p.segments) != 1 || string(p.tail[0].Body) != "message-2" {
			t.Fatal("failed checkpoint changed HEAD or retired uncommitted history")
		}
	}
	capacity := p.config.CapacityBytes
	p.config.CapacityBytes = 4096
	if err := p.Compact(); !errors.Is(err, ErrCapacity) {
		t.Fatalf("bounded checkpoint = %v", err)
	}
	checkUnchanged()
	p.config.CapacityBytes = capacity
	// A directory at the suffix's new segment path makes staging fail.
	blocked := filepath.Join(dir, (&segment{Base: 4}).name())
	if err := os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := p.Compact(); err == nil || p.failed == nil {
		t.Fatal("staging failure did not stop the partition")
	}
	checkUnchanged()
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	checkUnchanged()
	checkPage := func(first, count int) {
		t.Helper()
		page, err := p.Read(uint64(first), pageLimit)
		if err != nil || len(page.Messages) != count || page.Next != 7 {
			t.Fatalf("history from %d = %+v, %v", first, page, err)
		}
		for i, msg := range page.Messages {
			offset := i + first
			if msg.Offset != uint64(offset) || string(msg.Body) != fmt.Sprintf("message-%d", offset) {
				t.Fatalf("body at offset %d after checkpoint = %q", offset, msg.Body)
			}
		}
	}
	checkPage(0, 7)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	checkSuffix := func() {
		t.Helper()
		status, err := p.Status()
		if err != nil || status.FirstOffset != 4 || status.NextOffset != 7 ||
			len(p.segments) != 1 || p.segments[0].Base != 4 || len(p.tail) != 1 {
			t.Fatalf("retained suffix = %+v, %v", status, err)
		}
		var gap *OffsetGap
		if _, err := p.Read(0, pageLimit); !errors.As(err, &gap) || gap.First != 4 {
			t.Fatalf("read before the suffix = %v", err)
		}
		checkPage(4, 3)
	}
	checkSuffix()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	checkSuffix()
	publish(7)
}

func TestCheckpointKeepsExpiredTailBehindUnexpiredSegment(t *testing.T) {
	lower, upper := walFixtureTime, walFixtureTime.Add(10*time.Minute)
	cfg := Config{SegmentMessages: 2, HistoryRetention: time.Minute, Clock: func() (TimeBounds, error) {
		return TimeBounds{Lower: lower, Upper: upper}, nil
	}}
	dir := t.TempDir()
	p := openTest(t, dir, cfg)
	publishTest(t, p, "older-one")
	publishTest(t, p, "older-two")
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	// A tighter clock bound can give later slots earlier publication times.
	lower = lower.Add(time.Second)
	upper = lower
	publishTest(t, p, "later-one")
	publishTest(t, p, "later-two")
	lower = lower.Add(time.Minute)
	upper = lower
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if p.firstOffset != 0 || len(p.segments) != 2 || p.segments[1].Base != 2 {
		t.Fatal("later expired segment jumped over the older retained segment")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	page, err := p.Read(0, pageLimit)
	if err != nil || len(page.Messages) != 4 || page.Next != 4 || string(page.Messages[3].Body) != "later-two" {
		t.Fatalf("history behind a retained older segment = %+v, %v", page, err)
	}
}

func TestCheckpointExpiresMetadataWithAnEmptyWAL(t *testing.T) {
	now := walFixtureTime
	expiry := now.Add(time.Minute)
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now), DedupRetention: time.Minute, DeadRetention: time.Minute}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{MaxAttempts: 1})
	publishTest(t, p, "ordinary")
	publishTest(t, p, "failure")
	ds := pullTest(t, p, "notify", "worker", 2)
	ackTest(t, p, "notify", "worker", ds[0], AckAccepted)
	failed, err := p.Fail("notify", "worker", ds[1].Instance, ds[1].Token, "failure")
	if err != nil {
		t.Fatal(err)
	}
	redrive, err := p.Redrive("notify", failed.DeadLetterID, "retain-task")
	if err != nil {
		t.Fatal(err)
	}
	scheduled := PublishRequest{ID: "delayed", Body: []byte("later"), DeliverAt: expiry}
	original, err := p.Publish(scheduled)
	if err != nil || !original.Scheduled {
		t.Fatalf("schedule = %+v, %v", original, err)
	}
	before := checkpointHEAD(t, dir)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	head := checkpointHEAD(t, dir)
	if bytes.Equal(before, head) || p.journal.End() != 0 {
		t.Fatal("the initial checkpoint did not publish the new WAL state")
	}
	now = expiry.Add(-time.Nanosecond)
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(head, checkpointHEAD(t, dir)) {
		t.Fatal("checkpoint before the expiry boundary changed HEAD")
	}
	now = expiry
	capacity := p.config.CapacityBytes
	p.config.CapacityBytes = 4096
	err = p.Compact()
	p.config.CapacityBytes = capacity
	if !errors.Is(err, ErrCapacity) || !bytes.Equal(head, checkpointHEAD(t, dir)) ||
		len(p.publishes) != 3 || len(p.dead) != 1 || len(p.acknowledgments) != 1 || len(p.redriveReceipts) != 1 {
		t.Fatalf("failed checkpoint changed live metadata or HEAD: %v", err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(head, checkpointHEAD(t, dir)) || p.journal.End() != 0 {
		t.Fatal("expiry at the boundary did not publish a new generation")
	}
	check := func() {
		t.Helper()
		if len(p.publishes) != 1 || len(p.dead) != 0 || len(p.acknowledgments) != 0 || len(p.redriveReceipts) != 0 ||
			len(p.redrives) != 1 || len(p.scheduled) != 1 {
			t.Fatal("checkpoint pruned the wrong metadata")
		}
		if got, err := p.Publish(scheduled); err != nil || got != original {
			t.Fatalf("pending scheduled receipt = %+v, %v", got, err)
		}
		if got, err := p.PublishStatus("delayed"); err != nil || !got.Scheduled {
			t.Fatalf("pending status = %+v, %v", got, err)
		}
	}
	check()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	check()
	recovered := pullTest(t, p, "notify", "worker", 1)[0]
	if recovered.Instance != redrive.Instance || string(recovered.Message.Body) != "failure" {
		t.Fatalf("redrive after receipt and dead-letter expiry: %+v", recovered)
	}
	if n, err := p.AdvanceScheduled(pageLimit); err != nil || n != 1 {
		t.Fatalf("publish due item after reopening = %d, %v", n, err)
	}
}

func TestCheckpointSealsFullTailWithAnEmptyWAL(t *testing.T) {
	now := walFixtureTime
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now), SegmentMessages: 16}
	p := openTest(t, dir, cfg)
	for _, id := range []string{"one", "two", "three"} {
		publishTest(t, p, id)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.SegmentMessages = 2
	p = openTest(t, dir, cfg)
	if p.journal.End() != 0 || len(p.tail) != 3 || len(p.segments) != 0 {
		t.Fatal("recovered fixture must have an inline tail and an empty WAL")
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 || p.segments[0].Count != 2 || len(p.tail) != 1 {
		t.Fatal("checkpoint with an empty WAL did not seal the newly full segment")
	}
	if page, err := p.Read(0, pageLimit); err != nil || len(page.Messages) != 3 || page.Messages[2].ID != "three" {
		t.Fatalf("history across sealed and inline records = %+v, %v", page, err)
	}
}
