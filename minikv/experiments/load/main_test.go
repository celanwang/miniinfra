package main

import (
	"testing"
	"time"
)

func TestOfferDistinguishesQueuePressureFromLateScheduler(t *testing.T) {
	queue := make(chan arrival, 1)
	at := time.Unix(20, 0)
	var count counts
	next := offer(queue, &count, at, false, 0, 2)
	next = offer(queue, &count, at, true, next, 5)
	if next != 5 || count.Offered != 5 || count.Waiting != 1 || count.Late != 3 {
		t.Fatalf("next = %d, counts = %+v", next, count)
	}
	if got := <-queue; got.sequence != 0 || !got.at.Equal(at) {
		t.Fatalf("unexpected arrival: %+v", got)
	}
}

func TestScheduleCountsPartialLastInterval(t *testing.T) {
	var normal, auxiliary counts
	start := time.Now().Add(-time.Minute)
	schedule(start, 1250*time.Microsecond, 350*time.Microsecond, 100000, 40,
		make(chan arrival, 1), make(chan arrival, 1), &normal, &auxiliary)
	if normal.Offered != 125 || normal.Late != 125 || auxiliary.Offered != 0 {
		t.Fatalf("normal = %+v, auxiliary = %+v", normal, auxiliary)
	}
}

func TestPressureWriteSizes(t *testing.T) {
	repeated := make(map[int]int)
	adjacent := make(map[int]bool)
	for i := 0; i < 256; i++ {
		repeated[pressureWriteSize("repeated", i)]++
		size := pressureWriteSize("adjacent", i)
		if size > largeSize || size <= largeSize-256 || adjacent[size] {
			t.Fatalf("invalid adjacent size %d at %d", size, i)
		}
		adjacent[size] = true
	}
	for _, size := range []int{256 << 10, 512 << 10, largeSize - 1, largeSize} {
		if repeated[size] != 64 {
			t.Fatalf("size %d repeated %d times, want 64", size, repeated[size])
		}
	}
}

func TestBusinessPressurePlansDeterministicMixedWrites(t *testing.T) {
	plan := &businessPressure{
		stored:   []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"},
		rejected: []string{"r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"},
	}
	counts := make(map[outcome]int)
	storedSizes := make(map[int]bool)
	rejectSizes := make(map[int]bool)
	for i := range pressureWriteCount("business") {
		key, size, want := plan.write(i)
		if size < smallSize || size > largeSize {
			t.Fatalf("illegal size %d at request %d", size, i)
		}
		lane := i % businessWriters
		counts[want]++
		switch want {
		case ok:
			storedSizes[size] = true
			if key != plan.stored[lane] {
				t.Fatalf("write %d lost the lane's existing key", i)
			}
		case noSpace:
			rejectSizes[size] = true
			if key != plan.rejected[lane] || size < 64<<10 {
				t.Fatalf("write %d is not a large insert on the full shard", i)
			}
		default:
			t.Fatalf("unexpected planned status %s", want)
		}
	}
	if counts[ok] != 128 || counts[noSpace] != 384 || len(storedSizes) < 12 || len(rejectSizes) != len(businessRejectSizes) {
		t.Fatalf("write counts %v, distinct sizes stored=%d rejected=%d", counts, len(storedSizes), len(rejectSizes))
	}
}
