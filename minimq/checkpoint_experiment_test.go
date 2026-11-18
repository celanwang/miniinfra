package minimq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq/internal/maintenance"
)

const checkpointBodyBytes = 64 << 10
const queuedFastHistory = 32
const checkpointPulseSize = 4
const checkpointProbeWorker = "checkpoint-probe"

func TestCheckpointExperiment(t *testing.T) {
	mode := os.Getenv("MINIMQ_CHECKPOINT_EXPERIMENT")
	if mode == "" {
		t.Skip("set MINIMQ_CHECKPOINT_EXPERIMENT to default or forced")
	}
	if mode != "default" && mode != "forced" {
		t.Fatal("MINIMQ_CHECKPOINT_EXPERIMENT must be default or forced")
	}
	cache := os.Getenv("MINIMQ_CHECKPOINT_CACHE")
	if cache == "" {
		cache = "warm"
	}
	if cache != "warm" && cache != "cold" {
		t.Fatal("MINIMQ_CHECKPOINT_CACHE must be warm or cold")
	}
	backlog := experimentInt(t, "MINIMQ_CHECKPOINT_BACKLOG", 2048, 64, 16384)
	rate := experimentInt(t, "MINIMQ_CHECKPOINT_RATE", 16, 4, 128)
	pulses := 40
	cycles := 10
	pulseInterval := time.Duration(checkpointPulseSize) * time.Second / time.Duration(rate)
	if mode == "default" {
		pulses = int((3*maintenance.CheckpointInterval + 3*time.Second) / pulseInterval)
		cycles = 3
	}
	probesPerCycle := min(8, (backlog-queuedFastHistory-checkpointPulseSize-1)/cycles)
	probeCount := probesPerCycle * cycles
	body := bytes.Repeat([]byte("minimq\r!"), checkpointBodyBytes/8)
	cfg := Config{CapacityBytes: 16 << 30}
	dir := filepath.Join(t.TempDir(), "data")
	if err := seedCheckpointBacklog(dir, cfg, body, backlog, queuedFastHistory+probeCount); err != nil {
		t.Fatal(err)
	}
	heapBefore := liveHeap()
	p, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	heapDelta := liveHeap() - heapBefore
	probes, err := p.Pull("fast", checkpointProbeWorker, Limits{Messages: probeCount, Bytes: probeCount * len(body)})
	if err != nil || len(probes) != probeCount {
		t.Fatalf("prepare %d checkpoint leases: got %d, %v", probeCount, len(probes), err)
	}
	resident, pages, err := checkCheckpointCache(dir, cache == "cold")
	if err != nil {
		t.Fatal(err)
	}
	if cache == "cold" && (pages == 0 || resident*100 > pages) {
		t.Fatalf("could not evict at least 99%% of archived pages: %d/%d resident", resident, pages)
	}
	t.Logf("mode=%s os=%s cache=%s resident_pages=%d/%d backlog=%d body_bytes=%d reopen_heap_delta=%d target_messages_per_second=%d",
		mode, runtime.GOOS, cache, resident, pages, backlog, len(body), heapDelta, rate)

	acks := make([]operationTiming, 0, checkpointPulseSize*pulses+queuedFastHistory+probeCount)
	var checkpoints []operationTiming
	p.observe(func(op string, timing operationTiming) {
		if op == "ack" {
			acks = append(acks, timing)
		} else {
			checkpoints = append(checkpoints, timing)
		}
	})
	gates := make([]chan struct{}, cycles)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	cycle := 0
	p.timings.checkpointLocked = func() {
		if cycle < len(gates) {
			close(gates[cycle])
			cycle++
		}
	}
	heap, last, err := runCheckpointLoad(p, mode, pulseInterval, pulses, backlog, body, probes, gates)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "default" && len(checkpoints) < 3 {
		t.Fatalf("observed %d default checkpoints, expected at least 3", len(checkpoints))
	}
	if len(acks) != checkpointPulseSize*pulses+queuedFastHistory+probeCount {
		t.Fatalf("recorded %d ACKs", len(acks))
	}
	t.Logf("ack_count=%d probe_acks=%d replay_pages=%d", len(acks), probeCount, pulses)
	reportCheckpointTimings(t, acks, checkpoints, heap)
	verifyExperimentReopen(t, p, dir, cfg, last, uint64(backlog+checkpointPulseSize*pulses))
}

func experimentInt(t *testing.T, name string, fallback, lower, upper int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < lower || n > upper {
		t.Fatalf("%s must be between %d and %d", name, lower, upper)
	}
	return n
}

func seedCheckpointBacklog(dir string, cfg Config, body []byte, count, fastPending int) error {
	p, err := Open(dir, cfg)
	if err != nil {
		return err
	}
	defer p.Close()
	for _, name := range []string{"slow", "fast"} {
		if err := p.Register(name, SubscriptionConfig{LeaseDuration: 5 * time.Minute, Window: 65536}); err != nil {
			return err
		}
	}
	// Materialize a durable starting image before the timed workload. The
	// first part of the backlog has already settled for the fast subscription.
	p.subscriptions["fast"].Floor = uint64(count - fastPending)
	digest := sha256.Sum256(body)
	sealed := count / 256 * 256
	for i := range sealed {
		id := fmt.Sprintf("seed-%d", i)
		reserve := p.subscriptions["slow"].Config.taskReserve(len(body))
		if uint64(i) >= p.subscriptions["fast"].Floor {
			reserve *= 2
		}
		tx := transaction{
			Sequence: p.sequence + 1, Lower: p.timeLower, ReserveDelta: reserve,
			Publish: &publication{
				Message: Message{ID: id, Offset: uint64(i), Body: body, PublishedAt: p.timeLower},
				Receipt: publishReceipt{
					Digest: digest, Result: PublishResult{ID: id, Offset: uint64(i)},
					Expires: p.timeLower.Add(24 * time.Hour), VisibleOffset: uint64(i),
				},
			},
		}
		if err := p.apply(tx); err != nil {
			return err
		}
		if (i+1)%256 == 0 {
			if err := p.Compact(); err != nil {
				return err
			}
		}
	}
	// Install any short tail through the regular journal before reopening.
	for i := sealed; i < count; i++ {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("seed-%d", i), Body: body}); err != nil {
			return err
		}
	}
	return p.Compact()
}

type heapTiming struct {
	at    time.Time
	bytes uint64
}

func runCheckpointLoad(p *Partition, mode string, pace time.Duration, pulses, backlog int, body []byte, probes []Delivery, gates []chan struct{}) ([]heapTiming, Delivery, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pulses)*pace+2*time.Minute)
	defer cancel()
	var tasks, maintenanceTasks sync.WaitGroup
	var once sync.Once
	var failure error
	fail := func(err error) {
		once.Do(func() {
			failure = err
			cancel()
		})
	}
	run := func(task func() error) {
		tasks.Go(func() {
			if err := task(); err != nil {
				fail(err)
			}
		})
	}
	runMaintenance := func() {
		limit := Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes}
		maintenance.Run(ctx, func() bool {
			if _, err := p.AdvanceScheduled(limit); err != nil {
				fail(err)
			}
			return false // This workload has no delayed publications.
		}, func() {
			if _, err := p.AdvanceRetries(limit); err != nil {
				fail(err)
			}
		}, func() {
			if err := p.Compact(); err != nil {
				fail(err)
			}
		})
	}
	if mode == "default" {
		maintenanceTasks.Go(runMaintenance)
	}
	var heap []heapTiming
	maintenanceTasks.Go(func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		for {
			metrics.Read(sample)
			heap = append(heap, heapTiming{time.Now(), sample[0].Value.Uint64()})
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	})

	replay := make(chan uint64, pulses)
	leases := make(chan Delivery, 8)
	total := checkpointPulseSize*pulses + queuedFastHistory
	limit := Limits{Messages: checkpointPulseSize, Bytes: checkpointPulseSize * len(body)}
	perCycle := len(probes) / len(gates)
	for i, delivery := range probes {
		gate := gates[i/perCycle]
		run(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-gate:
			}
			got, err := p.Ack("fast", checkpointProbeWorker, delivery.Instance, delivery.Token)
			if err != nil || got != AckAccepted {
				return fmt.Errorf("probe ACK offset %d: %q, %v", delivery.Message.Offset, got, err)
			}
			return nil
		})
	}
	run(func() error {
		defer close(replay)
		start := time.Now()
		for step := range pulses {
			if mode == "default" {
				timer := time.NewTimer(time.Until(start.Add(time.Duration(step+1) * pace)))
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
			for i := range checkpointPulseSize {
				if err := ctx.Err(); err != nil {
					return err
				}
				id := fmt.Sprintf("live-%d", checkpointPulseSize*step+i)
				if _, err := p.Publish(PublishRequest{ID: id, Body: body}); err != nil {
					return err
				}
			}
			replay <- uint64((step * 97) % (backlog - queuedFastHistory - len(probes) - limit.Messages))
			if mode == "forced" && (step+1)%4 == 0 {
				if err := p.Compact(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	run(func() error {
		for offset := range replay {
			page, err := p.Read(offset, limit)
			if err != nil || page.Next != offset+uint64(limit.Messages) || len(page.Messages) != limit.Messages {
				return fmt.Errorf("incomplete replay at %d: %v", offset, err)
			}
			for _, msg := range page.Messages {
				if !bytes.Equal(msg.Body, body) {
					return fmt.Errorf("history payload mismatch at %d", msg.Offset)
				}
			}
		}
		return nil
	})
	run(func() error {
		defer close(leases)
		for sent := 0; sent < total; {
			if err := ctx.Err(); err != nil {
				return err
			}
			deliveries, err := p.Pull("fast", "worker", limit)
			if err != nil {
				return err
			}
			if len(deliveries) == 0 {
				time.Sleep(2 * time.Millisecond)
			}
			for _, d := range deliveries {
				select {
				case leases <- d:
				case <-ctx.Done():
					return ctx.Err()
				}
				sent++
			}
		}
		return nil
	})
	var last Delivery
	run(func() error {
		expected := uint64(backlog - queuedFastHistory)
		for d := range leases {
			if d.Message.Offset != expected || d.Attempt != 1 || !bytes.Equal(d.Message.Body, body) {
				return fmt.Errorf("unexpected delivery at %d, wanted offset %d", d.Message.Offset, expected)
			}
			got, err := p.Ack("fast", "worker", d.Instance, d.Token)
			if err != nil || got != AckAccepted {
				return fmt.Errorf("ack offset %d: %q, %v", expected, got, err)
			}
			expected++
			last = d
		}
		return nil
	})
	tasks.Wait()
	cancel()
	maintenanceTasks.Wait()
	return heap, last, failure
}

func checkpointPhase(arrived, locked time.Time, checkpoints []operationTiming) string {
	phase := "far"
	nearest := time.Second + 1
	for _, cp := range checkpoints {
		if arrived.Before(cp.finished) && locked.After(cp.locked) {
			return "during"
		}
		if before := cp.locked.Sub(arrived); before >= 0 && before < nearest {
			phase, nearest = "before", before
		}
		if after := arrived.Sub(cp.finished); after >= 0 && after < nearest {
			phase, nearest = "after", after
		}
	}
	return phase
}

func reportCheckpointTimings(t *testing.T, acks, checkpoints []operationTiming, heap []heapTiming) {
	t.Helper()
	byPhase := make(map[string][]operationTiming)
	for _, ack := range acks {
		phase := checkpointPhase(ack.arrived, ack.locked, checkpoints)
		if ack.worker == checkpointProbeWorker {
			byPhase["probe_"+phase] = append(byPhase["probe_"+phase], ack)
			continue
		}
		byPhase["all"] = append(byPhase["all"], ack)
		byPhase[phase] = append(byPhase[phase], ack)
	}
	peaks, heapSamples := make(map[string]uint64), make(map[string]int)
	for _, sample := range heap {
		phase := checkpointPhase(sample.at, sample.at, checkpoints)
		peaks["all"] = max(peaks["all"], sample.bytes)
		peaks[phase] = max(peaks[phase], sample.bytes)
		heapSamples["all"]++
		heapSamples[phase]++
	}
	for _, phase := range []string{"all", "before", "during", "after", "far", "probe_during", "probe_after", "probe_far"} {
		samples := byPhase[phase]
		if len(samples) == 0 {
			t.Logf("phase=%s n=0 heap_peak_bytes=%d heap_samples=%d", phase, peaks[phase], heapSamples[phase])
			continue
		}
		var total, wait, held, flush []time.Duration
		for _, s := range samples {
			total = append(total, s.finished.Sub(s.arrived))
			wait = append(wait, s.locked.Sub(s.arrived))
			held = append(held, s.finished.Sub(s.locked))
			flush = append(flush, s.syncTime)
		}
		p99 := historyP99(total)
		t.Logf("phase=%s n=%d ack_p50=%s ack_p99=%s wait_p99=%s held_p99=%s sync_p99=%s heap_peak_bytes=%d heap_samples=%d",
			phase, len(samples), total[(len(total)-1)/2], p99, historyP99(wait), historyP99(held), historyP99(flush),
			peaks[phase], heapSamples[phase])
	}
	for i, cp := range checkpoints {
		stage := cp.checkpoint
		held := cp.finished.Sub(cp.locked)
		cleanup := stage.cleanup - stage.cleanupSync
		rest := held - stage.prepare - stage.encode - stage.frame - stage.write - cleanup - cp.syncTime
		t.Logf("checkpoint=%d wait=%s held=%s prepare=%s encode=%s frame=%s write=%s sync_time=%s cleanup=%s rest=%s sync_calls=%d",
			i+1, cp.locked.Sub(cp.arrived), held, stage.prepare, stage.encode, stage.frame,
			stage.write, cp.syncTime, cleanup, rest, cp.syncCalls)
	}
}

func verifyExperimentReopen(t *testing.T, p *Partition, dir string, cfg Config, last Delivery, end uint64) {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, dir, cfg)
	status, err := reopened.Status()
	if err != nil || status.FirstOffset != 0 || status.NextOffset != end {
		t.Fatalf("reopened partition = %+v, %v", status, err)
	}
	ackTest(t, reopened, "fast", "worker", last, AckPreviouslyAccepted)
	pullTest(t, reopened, "fast", "worker", 0)
	for _, offset := range []uint64{0, end - 1} {
		page, err := reopened.Read(offset, Limits{Messages: 1, Bytes: checkpointBodyBytes})
		if err != nil || len(page.Messages) != 1 || page.Messages[0].Offset != offset {
			t.Fatalf("replayed offset %d: %v", offset, err)
		}
	}
}

func BenchmarkEncodeCheckpointSegment(b *testing.B) {
	body := bytes.Repeat([]byte("minimq\r!"), checkpointBodyBytes/8)
	messages := make([]storedMessage, 256)
	at := time.Now().UTC()
	for i := range messages {
		messages[i] = storedMessage{
			Message:          Message{ID: fmt.Sprintf("event-%d", i), Offset: uint64(i), PublishedAt: at, Body: body},
			AcceptedSequence: uint64(i + 1),
		}
	}
	b.SetBytes(int64(len(body) * len(messages)))
	b.ReportAllocs()
	for b.Loop() {
		var s segment
		if _, err := encodeSegment(&s, messages, nil); err != nil {
			b.Fatal(err)
		}
	}
}
