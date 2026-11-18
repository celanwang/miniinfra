package minimq

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"
)

// TestHistoryExperiment measures live heap after reopening and ACK latency while
// one subscription pins history and another drains it. Keep it opt-in because
// explicit GC and filesystem timing are part of the measurement.
func TestHistoryExperiment(t *testing.T) {
	if os.Getenv("MINIMQ_HISTORY_EXPERIMENT") != "1" {
		t.Skip("set MINIMQ_HISTORY_EXPERIMENT=1 to measure mixed history load")
	}
	dir := t.TempDir()
	cfg := Config{SegmentMessages: 16}
	p, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p != nil {
			_ = p.Close()
		}
	})
	registerTest(t, p, "slow", SubscriptionConfig{})
	registerTest(t, p, "fast", SubscriptionConfig{})
	body := bytes.Repeat([]byte("minimq\n!"), 8<<10)
	first := 0
	var allAcks []time.Duration
	for _, end := range []int{32, 128} {
		acks, reads, err := runHistoryLoad(p, body, first, end)
		if err != nil {
			t.Fatal(err)
		}
		allAcks = append(allAcks, acks...)
		if err := p.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		p = nil
		before := liveHeap()
		start := time.Now()
		p, err = Open(dir, cfg)
		reopen := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		heap := liveHeap() - before
		status, err := p.Status()
		if err != nil || status.FirstOffset != 0 || status.NextOffset != uint64(end) {
			t.Fatalf("slow subscription lost history: %+v, %v", status, err)
		}
		t.Logf("backlog=%d bytes_per_body=%d live_heap_delta=%d reopen=%s history_pages=%d ack_p99=%s n=%d",
			end, len(body), heap, reopen, reads, historyP99(acks), len(acks))
		first = end
	}
	t.Logf("online_ack_p99=%s n=%d", historyP99(allAcks), len(allAcks))
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}

func historyP99(values []time.Duration) time.Duration {
	slices.Sort(values)
	return values[(99*len(values)+99)/100-1]
}

func runHistoryLoad(p *Partition, body []byte, first, end int) ([]time.Duration, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	failures := make(chan error, 3)
	readOffsets := make(chan uint64, end-first)
	var acks []time.Duration
	var reads int
	go func() {
		err := consumeHistoryLoad(ctx, p, body, end-first, &acks)
		if err != nil {
			cancel()
		}
		failures <- err
	}()
	go func() {
		limit := Limits{Messages: 4, Bytes: 4 * len(body)}
		for {
			select {
			case <-ctx.Done():
				failures <- ctx.Err()
				return
			case offset, ok := <-readOffsets:
				if !ok {
					failures <- nil
					return
				}
				page, err := p.Read(offset, limit)
				if err != nil {
					failures <- err
					cancel()
					return
				}
				if len(page.Messages) != limit.Messages {
					failures <- fmt.Errorf("short history page at %d: %d records", offset, len(page.Messages))
					cancel()
					return
				}
				for _, msg := range page.Messages {
					if !bytes.Equal(msg.Body, body) {
						failures <- fmt.Errorf("wrong history payload at %d", msg.Offset)
						cancel()
						return
					}
				}
				reads++
			}
		}
	}()
	go func() {
		defer close(readOffsets)
		for i := first; i < end; i++ {
			if ctx.Err() != nil {
				break
			}
			if _, err := p.Publish(PublishRequest{ID: fmt.Sprintf("order-%d", i), Body: body}); err != nil {
				failures <- err
				cancel()
				return
			}
			if (i+1)%16 == 0 {
				if err := p.Compact(); err != nil {
					failures <- err
					cancel()
					return
				}
			}
			if pages := (i + 1) / 4; pages > 0 {
				readOffsets <- uint64(4 * (i % pages))
			}
		}
		failures <- nil
	}()
	var err error
	for range 3 {
		if failure := <-failures; failure != nil {
			err = failure
		}
	}
	wantReads := end - max(first, 3)
	if err == nil && reads != wantReads {
		err = fmt.Errorf("mixed workload completed %d history pages, want %d", reads, wantReads)
	}
	return acks, reads, err
}

func consumeHistoryLoad(ctx context.Context, p *Partition, body []byte, count int, acks *[]time.Duration) error {
	limit := Limits{Messages: 4, Bytes: 4 * len(body)}
	for len(*acks) < count {
		if err := ctx.Err(); err != nil {
			return err
		}
		deliveries, err := p.Pull("fast", "worker", limit)
		if err != nil {
			return err
		}
		if len(deliveries) == 0 {
			time.Sleep(time.Millisecond)
		}
		for _, delivery := range deliveries {
			start := time.Now()
			result, err := p.Ack("fast", "worker", delivery.Instance, delivery.Token)
			elapsed := time.Since(start)
			if err != nil || result != AckAccepted {
				return fmt.Errorf("ack offset %d: %s, %v", delivery.Message.Offset, result, err)
			}
			*acks = append(*acks, elapsed)
			if !bytes.Equal(delivery.Message.Body, body) {
				return fmt.Errorf("wrong delivery payload at %d", delivery.Message.Offset)
			}
		}
	}
	return nil
}
