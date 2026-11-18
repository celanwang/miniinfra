package minimq

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func checkpointHEAD(t testing.TB, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "HEAD"))
	if err != nil || len(data) != 24 {
		t.Fatalf("HEAD has %d bytes: %v", len(data), err)
	}
	return data
}

func BenchmarkIdleCheckpoint(b *testing.B) {
	dir := os.Getenv("MINIMQ_IDLE_CHECKPOINT_FIXTURE")
	if dir == "" {
		b.Skip("set MINIMQ_IDLE_CHECKPOINT_FIXTURE to a seeded dead-letter fixture")
	}
	now := walFixtureTime
	p, err := Open(dir, Config{Clock: testClock(&now)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Close() })
	before := checkpointHEAD(b, dir)
	if p.journal.End() != 0 || len(p.dead) == 0 {
		b.Fatal("expected a checkpointed fixture with retained dead letters and an empty WAL")
	}
	var held time.Duration
	p.observe(func(_ string, s operationTiming) {
		held += s.finished.Sub(s.locked)
		if s.syncCalls != 0 {
			b.Fatal("an idle checkpoint synced the filesystem")
		}
	})
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Compact(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(held.Nanoseconds())/float64(b.N), "held-ns/op")
	b.ReportMetric(float64(len(p.dead)), "letters")
	if after := checkpointHEAD(b, dir); !bytes.Equal(before, after) {
		b.Fatal("an idle checkpoint replaced HEAD")
	}
}

func TestIdleCheckpointActiveControl(t *testing.T) {
	dir := os.Getenv("MINIMQ_IDLE_CHECKPOINT_ACTIVE_DIR")
	if dir == "" {
		t.Skip("set MINIMQ_IDLE_CHECKPOINT_ACTIVE_DIR to a fresh fixture copy")
	}
	now := walFixtureTime
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	if p.journal.End() != 0 || len(p.dead) == 0 {
		t.Fatal("expected a checkpointed fixture with retained dead letters and an empty WAL")
	}
	original := len(p.dead)
	var sample operationTiming
	p.observe(func(_ string, s operationTiming) { sample = s })
	run := func(label string, wantDead int) {
		t.Helper()
		before := checkpointHEAD(t, dir)
		if err := p.Compact(); err != nil {
			t.Fatal(err)
		}
		after := checkpointHEAD(t, dir)
		generation := binary.LittleEndian.Uint64(before[4:12])
		if binary.LittleEndian.Uint64(after[4:12]) != generation+1 || p.journal.End() != 0 ||
			len(p.dead) != wantDead || sample.syncCalls == 0 {
			t.Fatalf("%s did not publish the expected generation: dead=%d, syncs=%d", label, len(p.dead), sample.syncCalls)
		}
		t.Logf("phase=%s held=%s prepare=%s encode=%s sync=%s sync_calls=%d image_bytes=%d dead=%d",
			label, sample.finished.Sub(sample.locked), sample.checkpoint.prepare, sample.checkpoint.encode,
			sample.syncTime, sample.syncCalls, sample.checkpoint.imageBytes, len(p.dead))
	}
	registerTest(t, p, "checkpoint-control", SubscriptionConfig{})
	run("wal", original)
	now = now.Add(7 * 24 * time.Hour)
	run("expiry", 0)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	verifyIdleCheckpointControl(t, dir)
}

func TestVerifyIdleCheckpointActiveControl(t *testing.T) {
	dir := os.Getenv("MINIMQ_IDLE_CHECKPOINT_VERIFY_DIR")
	if dir == "" {
		t.Skip("set MINIMQ_IDLE_CHECKPOINT_VERIFY_DIR to the completed active control")
	}
	verifyIdleCheckpointControl(t, dir)
}

func verifyIdleCheckpointControl(t *testing.T, dir string) {
	t.Helper()
	now := walFixtureTime.Add(7 * 24 * time.Hour)
	p := openTest(t, dir, Config{Clock: testClock(&now)})
	if p.journal.End() != 0 || len(p.dead) != 0 || p.subscriptions["checkpoint-control"] == nil {
		t.Fatalf("reopened active control: WAL=%d, dead=%d, registration=%t",
			p.journal.End(), len(p.dead), p.subscriptions["checkpoint-control"] != nil)
	}
}
