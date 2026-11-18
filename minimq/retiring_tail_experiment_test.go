package minimq

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

const retiringTailRows = 32
const retiringTailRetention = time.Minute

func TestSeedRetiringTailCheckpoint(t *testing.T) {
	dir := os.Getenv("MINIMQ_TAIL_RETIRE_SEED")
	if dir == "" {
		t.Skip("set MINIMQ_TAIL_RETIRE_SEED to an empty directory")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("fixture directory must be empty: %v", err)
	}
	now := walFixtureTime
	cfg := Config{Clock: testClock(&now), HistoryRetention: retiringTailRetention}
	p := openTest(t, dir, cfg)
	if os.Getenv("MINIMQ_TAIL_RETIRE_PROTECT") == "1" {
		registerTest(t, p, "hold", SubscriptionConfig{})
	}
	body := bytes.Repeat([]byte{'r'}, checkpointBodyBytes)
	for i := range retiringTailRows {
		req := PublishRequest{ID: fmt.Sprintf("retire-%02d", i), Body: body}
		if _, err := p.Publish(req); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if p.journal.End() != 0 || len(p.tail) != retiringTailRows || len(p.segments) != 0 {
		t.Fatal("fixture must contain an inline checkpoint tail and an empty WAL")
	}
}

func BenchmarkRetiringTailCheckpoint(b *testing.B) {
	fixture := os.Getenv("MINIMQ_TAIL_RETIRE_FIXTURE")
	protected := os.Getenv("MINIMQ_TAIL_RETIRE_PROTECTED_FIXTURE")
	if fixture == "" || protected == "" {
		b.Skip("seed both the reclaimable and protected fixtures")
	}
	for _, trial := range []struct {
		name, dir string
		width     int
		age       time.Duration
		first     uint64
	}{
		{"expired_partial", fixture, MaxBatchMessages, retiringTailRetention, retiringTailRows},
		{"expired_full", fixture, retiringTailRows, retiringTailRetention, retiringTailRows},
		{"recent_full", fixture, retiringTailRows, 0, 0},
		{"protected_partial", protected, MaxBatchMessages, retiringTailRetention, 0},
	} {
		b.Run(trial.name, func(b *testing.B) {
			now := walFixtureTime.Add(trial.age)
			cfg := Config{Clock: testClock(&now), HistoryRetention: retiringTailRetention, SegmentMessages: trial.width}
			var held, encode, frame, sync time.Duration
			var imageBytes, staged int
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				dir := b.TempDir()
				copyWALFixture(b, trial.dir, dir)
				p, err := Open(dir, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = p.Close() })
				if p.journal.End() != 0 || len(p.tail) != retiringTailRows {
					b.Fatal("expected an inline tail with an empty WAL")
				}
				p.observe(func(_ string, s operationTiming) {
					held += s.finished.Sub(s.locked)
					encode += s.checkpoint.encode
					frame += s.checkpoint.frame
					sync += s.syncTime
					imageBytes = s.checkpoint.imageBytes
					staged = s.checkpoint.stagedSegments
				})
				b.StartTimer()
				err = p.Compact()
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				wantSegments := 1
				if trial.first != 0 {
					wantSegments = 0
				}
				status, err := p.Status()
				if err != nil || status.FirstOffset != trial.first || status.NextOffset != retiringTailRows ||
					len(p.segments) != wantSegments || len(p.tail) != 0 || staged != wantSegments {
					b.Fatalf("compacted fixture: status=%+v segments=%d, staged=%d, %v", status, len(p.segments), staged, err)
				}
				if err := p.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			for name, value := range map[string]time.Duration{
				"held-ns/op": held, "encode-ns/op": encode, "frame-ns/op": frame, "sync-ns/op": sync,
			} {
				b.ReportMetric(float64(value.Nanoseconds())/float64(b.N), name)
			}
			b.ReportMetric(float64(imageBytes), "image-B")
			b.ReportMetric(float64(staged), "staged")
		})
	}
}
