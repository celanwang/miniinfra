package minimq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq/internal/maintenance"
)

const shortTailHistory = 2048

func TestShortTailCheckpointExperiment(t *testing.T) {
	mode := os.Getenv("MINIMQ_SHORT_TAIL_EXPERIMENT")
	if mode == "" {
		t.Skip("set MINIMQ_SHORT_TAIL_EXPERIMENT to default, quiet, or sparse")
	}
	if mode != "default" && mode != "quiet" && mode != "sparse" {
		t.Fatal("MINIMQ_SHORT_TAIL_EXPERIMENT must be default, quiet, or sparse")
	}
	rows := experimentInt(t, "MINIMQ_SHORT_TAIL_ROWS", 128, 16, 224)
	cycles := 6
	if mode == "default" {
		cycles = 3
	}
	var recent *recentTailWork
	pullBatches := 0
	if os.Getenv("MINIMQ_SHORT_TAIL_RECENT") == "1" {
		pullBatches = min(4, (rows-1)/(4*cycles))
		if pullBatches == 0 {
			t.Fatal("starting tail is too small for the recent Pull and ACK cohorts")
		}
	}
	perCycle := min(8, (rows-1-2*pullBatches*cycles)/(2*cycles))
	cfg := Config{CapacityBytes: 4 << 30}
	body := bytes.Repeat([]byte("minimq\r!"), checkpointBodyBytes/8)
	dir := filepath.Join(t.TempDir(), "data")
	if err := seedCheckpointBacklog(dir, cfg, body, shortTailHistory, 0); err != nil {
		t.Fatal(err)
	}
	p, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p != nil {
			_ = p.Close()
		}
	})
	for i := range rows {
		if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("tail-%d", i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	lease := func(worker string, count int) []Delivery {
		t.Helper()
		ds, err := p.Pull("fast", worker, Limits{Messages: count, Bytes: count * len(body)})
		if err != nil || len(ds) != count {
			t.Fatalf("pregrant %s = %d, %v", worker, len(ds), err)
		}
		for i := range ds {
			ds[i].Message.Body = nil
		}
		return ds
	}
	ordinary := lease("tail-worker", perCycle*cycles)
	probes := lease(checkpointProbeWorker, perCycle*cycles)
	outstanding := lease("pending", 1)[0]
	if pullBatches != 0 {
		recent = &recentTailWork{
			rows: rows, pullBatches: pullBatches,
			next: uint64(shortTailHistory + 2*perCycle*cycles + 1),
		}
	}

	var initial operationTiming
	p.observe(func(_ string, timing operationTiming) { initial = timing })
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = nil
	heapBefore := liveHeap()
	p, err = Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	heapStart := liveHeap() - heapBefore
	t.Logf("mode=%s backlog=%d initial_tail=%d body=%d cycles=%d initial_encode=%s initial_image_bytes=%d reopen_heap_delta=%d",
		mode, shortTailHistory, rows, len(body), cycles, initial.checkpoint.encode, initial.checkpoint.imageBytes, heapStart)
	gates := make([]chan struct{}, cycles)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	var acks, checkpoints []operationTiming
	p.observe(func(op string, timing operationTiming) {
		if op == "ack" {
			acks = append(acks, timing)
		} else {
			checkpoints = append(checkpoints, timing)
		}
	})
	cycle := 0
	p.timings.checkpointLocked = func() {
		if cycle < len(gates) {
			close(gates[cycle])
			cycle++
		}
	}
	cpuStart := shortTailCPU(t)
	publishes, err := shortTailLoad(p, mode, body, ordinary, probes, gates, recent)
	if err != nil {
		t.Fatal(err)
	}
	cpu := shortTailCPU(t) - cpuStart
	if len(checkpoints) != cycles || len(acks) != len(ordinary)+len(probes)+2*pullBatches*cycles {
		t.Fatalf("recorded %d checkpoints and %d ACKs", len(checkpoints), len(acks))
	}
	var repeatedBytes, repeatedEncodes, retainedTail int
	seenEnd := uint64(shortTailHistory + rows)
	for i, cp := range checkpoints {
		stage := cp.checkpoint
		end := stage.tailFirst + uint64(stage.tailRecords)
		repeated := 0
		if stage.tailRecords != 0 {
			if earlier := min(seenEnd, end); earlier > stage.tailFirst {
				repeated = int(earlier-stage.tailFirst) * len(body)
				repeatedBytes += repeated
				repeatedEncodes++
			}
			seenEnd = max(seenEnd, end)
		}
		t.Logf("tail_checkpoint=%d records=%d raw_bytes=%d repeated_bytes=%d image_bytes=%d new_segments=%d",
			i+1, stage.tailRecords, stage.tailBytes, repeated, stage.imageBytes, stage.stagedSegments)
	}
	for _, msg := range p.tail {
		retainedTail += len(msg.Body)
	}
	heapEnd := liveHeap() - heapBefore
	status, err := p.Status()
	if err != nil {
		t.Fatal(err)
	}
	var publishP99 time.Duration
	if len(publishes) != 0 {
		publishP99 = historyP99(publishes)
	}
	t.Logf("repeated_encodes=%d repeated_bytes=%d tail_retained_bytes=%d heap_delta_after_gc=%d stored_bytes=%d cpu=%s publish_count=%d publish_p99=%s history_reads=%d",
		repeatedEncodes, repeatedBytes, retainedTail, heapEnd, status.StoredBytes, cpu, len(publishes), publishP99, len(ordinary))
	reportCheckpointTimings(t, acks, checkpoints, nil)
	if recent != nil {
		pullP99 := historyP99(recent.pulls)
		t.Logf("recent_read_groups=%v pages_per_group=%d bytes_per_group=%d recent_pull_batches=%d pull_p50=%s pull_p99=%s",
			recent.readGroups, recentTailReadPages, recentTailReadPages*4*len(body), len(recent.pulls),
			recent.pulls[(len(recent.pulls)-1)/2], pullP99)
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	expected := uint64(shortTailHistory + rows + len(publishes))
	if status, err := p.Status(); err != nil || status.FirstOffset != 0 || status.NextOffset != expected {
		t.Fatalf("reopened = %+v, %v", status, err)
	}
	ackTest(t, p, "fast", "tail-worker", ordinary[len(ordinary)-1], AckPreviouslyAccepted)
	ackTest(t, p, "fast", checkpointProbeWorker, probes[len(probes)-1], AckPreviouslyAccepted)
	ackTest(t, p, "fast", "pending", outstanding, AckAccepted)
	if recent != nil {
		ackTest(t, p, "fast", recentTailWorker, recent.last, AckPreviouslyAccepted)
	}
	for _, offset := range []uint64{shortTailHistory - 1, shortTailHistory, expected - 1} {
		page, err := p.Read(offset, Limits{Messages: 1, Bytes: len(body)})
		if err != nil || len(page.Messages) != 1 || page.Messages[0].Offset != offset || !bytes.Equal(page.Messages[0].Body, body) {
			t.Fatalf("reopened history at %d: %v", offset, err)
		}
	}
}

func shortTailLoad(p *Partition, mode string, body []byte, ordinary, probes []Delivery, gates []chan struct{}, recent *recentTailWork) ([]time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var maintenanceDone, probesDone sync.WaitGroup
	errs := make(chan error, 1)
	if mode == "default" {
		maintenanceDone.Go(func() {
			fail := func(err error) {
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					cancel()
				}
			}
			limit := Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes}
			maintenance.Run(ctx, func() {
				_, scheduled := p.AdvanceScheduled(limit)
				_, retries := p.AdvanceRetries(limit)
				fail(errors.Join(scheduled, retries))
			}, func() { fail(p.Compact()) })
		})
	}
	defer func() {
		cancel()
		probesDone.Wait()
		maintenanceDone.Wait()
	}()
	var publishes []time.Duration
	count := len(probes) / len(gates)
	start := time.Now()
	for cycle, gate := range gates {
		first := cycle * count
		results := make(chan error, count)
		for _, d := range probes[first : first+count] {
			probesDone.Go(func() {
				select {
				case <-gate:
					results <- shortTailAck(p, checkpointProbeWorker, d)
				case <-ctx.Done():
					results <- ctx.Err()
				}
			})
		}
		for i, d := range ordinary[first : first+count] {
			if mode == "default" {
				at := start.Add(time.Duration(cycle)*maintenance.CheckpointInterval +
					time.Duration(i+1)*(maintenance.CheckpointInterval-2*time.Second)/time.Duration(count))
				timer := time.NewTimer(time.Until(at))
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return nil, shortTailFailure(ctx, errs)
				}
			}
			if err := shortTailAck(p, "tail-worker", d); err != nil {
				return nil, err
			}
			offset := uint64(shortTailHistory - 2 + (i % 2))
			page, err := p.Read(offset, Limits{Messages: 4, Bytes: 4 * len(body)})
			if err != nil || page.Next != offset+4 || len(page.Messages) != 4 {
				return nil, fmt.Errorf("history across the short-tail boundary: %v", err)
			}
		}
		if mode == "sparse" {
			at := time.Now()
			if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("sparse-%d", cycle), Body: body}); err != nil {
				return nil, err
			}
			publishes = append(publishes, time.Since(at))
		}
		if mode != "default" {
			if err := p.Compact(); err != nil {
				return nil, err
			}
		}
		for range count {
			select {
			case err := <-results:
				if err != nil {
					return nil, err
				}
			case <-ctx.Done():
				return nil, shortTailFailure(ctx, errs)
			}
		}
		if recent != nil {
			if err := recent.run(p, body); err != nil {
				return nil, err
			}
		}
	}
	return publishes, nil
}

const recentTailReadPages = 96
const recentTailWorker = "recent-tail-worker"

type recentTailWork struct {
	rows, pullBatches int
	next              uint64
	readGroups, pulls []time.Duration
	last              Delivery
}

func (r *recentTailWork) run(p *Partition, body []byte) error {
	begin := time.Now()
	for page := range recentTailReadPages {
		offset := uint64(shortTailHistory + 4*(page%(r.rows/4)))
		got, err := p.Read(offset, Limits{Messages: 4, Bytes: 4 * len(body)})
		if err != nil || got.Next != offset+4 || len(got.Messages) != 4 {
			return fmt.Errorf("recent read at offset %d: %v", offset, err)
		}
		for i, msg := range got.Messages {
			if msg.Offset != offset+uint64(i) || !bytes.Equal(msg.Body, body) {
				return fmt.Errorf("unexpected recent message at offset %d", msg.Offset)
			}
		}
	}
	r.readGroups = append(r.readGroups, time.Since(begin))
	for range r.pullBatches {
		begin := time.Now()
		ds, err := p.Pull("fast", recentTailWorker, Limits{Messages: 2, Bytes: 2 * len(body)})
		r.pulls = append(r.pulls, time.Since(begin))
		if err != nil || len(ds) != 2 {
			return fmt.Errorf("recent pull at offset %d: %d, %v", r.next, len(ds), err)
		}
		for _, d := range ds {
			if d.Message.Offset != r.next || d.Attempt != 1 || !bytes.Equal(d.Message.Body, body) {
				return fmt.Errorf("unexpected recent delivery at offset %d", d.Message.Offset)
			}
			if err := shortTailAck(p, recentTailWorker, d); err != nil {
				return err
			}
			r.next++
			d.Message.Body = nil
			r.last = d
		}
	}
	return nil
}

func shortTailFailure(ctx context.Context, errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return ctx.Err()
	}
}

func shortTailAck(p *Partition, worker string, delivery Delivery) error {
	got, err := p.Ack("fast", worker, delivery.Instance, delivery.Token)
	if err != nil || got != AckAccepted {
		return fmt.Errorf("ack %s = %q, %v", delivery.Instance, got, err)
	}
	return nil
}

func shortTailCPU(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
