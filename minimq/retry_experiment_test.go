package minimq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq/internal/maintenance"
)

const (
	retryExperimentFatal  = 144
	retryExperimentPulses = 24
	retryExperimentRounds = 6
)

func TestRetryExperiment(t *testing.T) {
	mode := os.Getenv("MINIMQ_RETRY_EXPERIMENT")
	if mode == "" {
		t.Skip("set MINIMQ_RETRY_EXPERIMENT to live or expired")
	}
	if mode != "live" && mode != "expired" {
		t.Fatal("MINIMQ_RETRY_EXPERIMENT must be live or expired")
	}
	rows := experimentInt(t, "MINIMQ_RETRY_ROWS", 8192, 256, 32768)
	if rows%MaxBatchMessages != 0 {
		t.Fatal("MINIMQ_RETRY_ROWS must be a multiple of the batch size")
	}
	step := time.Duration(0)
	switch os.Getenv("MINIMQ_RETRY_DEADLINES") {
	case "", "tied":
	case "staggered":
		step = time.Millisecond
	default:
		t.Fatal("MINIMQ_RETRY_DEADLINES must be tied or staggered")
	}

	dir := t.TempDir()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	cfg := Config{Clock: testClock(&now), CapacityBytes: 16 << 30, SegmentMessages: 64}
	body := bytes.Repeat([]byte{'s'}, 256)
	deadBody := bytes.Repeat([]byte{'d'}, checkpointBodyBytes)
	if err := seedCheckpointBacklog(dir, cfg, body, rows, 0); err != nil {
		t.Fatal(err)
	}
	p := openTest(t, dir, cfg)
	first := seedRetryExperiment(t, p, rows, body, deadBody, func() { now = now.Add(step) })
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if mode == "expired" {
		now = base.Add(5*time.Minute + time.Second)
	} else {
		now = base.Add(time.Minute)
	}
	p = openTest(t, dir, cfg)
	samples := make(map[string][]operationTiming)
	p.observe(func(op string, sample operationTiming) {
		samples[op] = append(samples[op], sample)
	})
	last, err := runRetryExperiment(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples["retry"]) != retryExperimentRounds || len(samples["ack"]) != 2*retryExperimentPulses ||
		len(samples["pull"]) != retryExperimentPulses {
		t.Fatalf("observed retry/pull/ack = %d/%d/%d", len(samples["retry"]), len(samples["pull"]), len(samples["ack"]))
	}
	t.Logf("mode=%s rows=%d deadline_step=%s", mode, rows, step)
	reportRetryExperiment(t, samples)
	if mode == "live" {
		// The same busy subscription, with the body limit below every available record.
		for range 5 {
			ds, err := p.Pull("slow", "idle", Limits{Messages: 1, Bytes: 1})
			if err != nil || len(ds) != 0 {
				t.Fatalf("live busy Pull = %d, %v", len(ds), err)
			}
		}
		reportRetryOperations(t, "live_busy_pull", samples["pull"][retryExperimentPulses:], nil)
		for _, s := range samples["retry"] {
			if s.retry.settled != 0 {
				t.Fatal("still-valid leases were settled")
			}
		}
		ackTest(t, p, "slow", "held", first, AckAccepted)
	} else {
		if s := samples["retry"][0].retry; s.settled != 128 || s.deadBytes != MaxBatchBytes {
			t.Fatalf("first retry batch = %+v", s)
		}
		for {
			n, err := p.AdvanceRetries(Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes})
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
		}
		if _, err := p.Ack("slow", "held", first.Instance, first.Token); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("expired ACK = %v", err)
		}
		ds, err := p.Pull("slow", "retry-worker", Limits{Messages: 1, Bytes: MaxBatchBytes})
		if err != nil || len(ds) != 1 {
			t.Fatalf("retry lease = %d, %v", len(ds), err)
		}
		second := ds[0]
		if second.Instance != first.Instance || second.Token == first.Token || second.Attempt != 2 {
			t.Fatalf("retry of %s = %+v", first.Instance, second)
		}
		ackTest(t, p, "slow", "retry-worker", second, AckAccepted)
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	ackTest(t, p, "online", "ordinary", last, AckPreviouslyAccepted)
	pullTest(t, p, "online", "ordinary", 0)
	wantDead := 0
	if mode == "expired" {
		wantDead = retryExperimentFatal
	}
	verifyRetryExperimentDead(t, p, wantDead, deadBody)
}

func seedRetryExperiment(t *testing.T, p *Partition, rows int, body, deadBody []byte, step func()) Delivery {
	t.Helper()
	registerTest(t, p, "final", SubscriptionConfig{MaxAttempts: 1, LeaseDuration: 5 * time.Minute})
	for i := range 3 * p.config.SegmentMessages {
		payload := body
		if i < retryExperimentFatal {
			payload = deadBody
		}
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("final-%d", i), Body: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{128, retryExperimentFatal - 128} {
		ds, err := p.Pull("final", "held", Limits{Messages: n, Bytes: n * len(deadBody)})
		if err != nil || len(ds) != n {
			t.Fatalf("grant final cohort: %d, %v", len(ds), err)
		}
	}
	var first Delivery
	for i := 0; i < rows; i += MaxBatchMessages {
		// A single Pull grants one deadline to up to 256 real deliveries.
		step()
		ds, err := p.Pull("slow", "held", Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes})
		if err != nil || len(ds) != MaxBatchMessages || ds[0].Message.Offset != uint64(i) {
			t.Fatalf("grant held cohort at %d: %d, %v", i, len(ds), err)
		}
		if i == 0 {
			first = ds[0]
		}
	}
	registerTest(t, p, "online", SubscriptionConfig{LeaseDuration: 30 * time.Minute})
	for i := range 2 * retryExperimentPulses {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("online-%d", i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	return first
}

func runRetryExperiment(p *Partition) (Delivery, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	limit := Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes}
	var err error
	go func() {
		defer close(done)
		rounds := 0
		maintenance.Run(ctx, func() bool { return false }, func() {
			if ctx.Err() != nil {
				return
			}
			_, err = p.AdvanceRetries(limit)
			rounds++
			if err != nil || rounds == retryExperimentRounds {
				cancel()
			}
		}, func() {})
	}()
	var last Delivery
	foreground := func() error {
		start := time.Now()
		for i := range retryExperimentPulses {
			time.Sleep(time.Until(start.Add(time.Duration(i) * 67 * time.Millisecond)))
			ds, err := p.Pull("online", "ordinary", Limits{Messages: 2, Bytes: MaxBatchBytes})
			if err != nil || len(ds) != 2 {
				return fmt.Errorf("ordinary Pull at pulse %d: %d, %v", i, len(ds), err)
			}
			for _, d := range ds {
				if ack, err := p.Ack("online", "ordinary", d.Instance, d.Token); err != nil || ack != AckAccepted {
					return fmt.Errorf("ordinary ACK at %d: %s, %v", d.Message.Offset, ack, err)
				}
				last = d
			}
		}
		return nil
	}()
	if foreground != nil {
		cancel()
	}
	<-done
	return last, errors.Join(err, foreground)
}

func reportRetryExperiment(t *testing.T, samples map[string][]operationTiming) {
	t.Helper()
	for _, op := range []string{"pull", "ack"} {
		reportRetryOperations(t, op, samples[op], samples["retry"])
	}
	for i, s := range samples["retry"] {
		r := s.retry
		t.Logf("round=%d visited=%d eligible=%d settled=%d dead=%d dead_bytes=%d lock=%s scan=%s order=%s read=%s prepare=%s commit=%s sync=%s",
			i+1, r.visited, r.candidates, r.settled, r.dead, r.deadBytes, s.finished.Sub(s.locked), r.scan,
			r.order, r.read, r.prepare-r.read, r.commit, s.syncTime)
	}
}

func reportRetryOperations(t *testing.T, op string, samples, retries []operationTiming) {
	t.Helper()
	var elapsed, waits, held, overlaps []time.Duration
	for _, s := range samples {
		elapsed = append(elapsed, s.finished.Sub(s.arrived))
		waits = append(waits, s.locked.Sub(s.arrived))
		held = append(held, s.finished.Sub(s.locked))
		for _, r := range retries {
			from := s.arrived
			if !from.Before(r.locked) && from.Before(r.finished) {
				overlaps = append(overlaps, r.finished.Sub(from))
				break
			}
		}
	}
	var overlapP99 time.Duration
	if len(overlaps) != 0 {
		overlapP99 = historyP99(overlaps)
	}
	t.Logf("op=%s count=%d p99=%s wait_p99=%s lock_p99=%s overlap_retry=%d overlap_p99=%s",
		op, len(samples), historyP99(elapsed), historyP99(waits), historyP99(held), len(overlaps), overlapP99)
}

func verifyRetryExperimentDead(t *testing.T, p *Partition, want int, body []byte) {
	t.Helper()
	seen := make(map[uint64]bool)
	after := ""
	for {
		letters, err := p.ListDeadLetters("final", after, MaxBatchMessages)
		if err != nil {
			t.Fatal(err)
		}
		if len(letters) == 0 {
			break
		}
		for _, d := range letters {
			if seen[d.Message.Offset] || d.Attempts != 1 || d.Reason != "lease expired" || !bytes.Equal(d.Message.Body, body) {
				t.Fatalf("invalid recovered dead letter %s at %d", d.ID, d.Message.Offset)
			}
			seen[d.Message.Offset] = true
			after = d.ID
		}
	}
	if len(seen) != want {
		t.Fatalf("recovered %d dead letters, want %d", len(seen), want)
	}
}
