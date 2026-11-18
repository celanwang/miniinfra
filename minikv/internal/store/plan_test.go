package store

import (
	"slices"
	"testing"
	"time"
)

func TestPlanPreservesSampleOrderAndPrefix(t *testing.T) {
	entries := []entry{
		{order: 1, value: make([]byte, 8)},
		{order: 5, deadline: time.Second, value: make([]byte, 4)},
		{order: 3, value: make([]byte, 12)},
		{order: 2, deadline: time.Second, value: make([]byte, 6)},
		{order: 4, value: make([]byte, 7)},
		{order: 6, deadline: time.Second, value: make([]byte, 10)},
	}
	for _, tc := range []struct {
		name   string
		delta  int64
		target uint32
		exists bool
		want   []uint32
		ok     bool
		next   int
	}{
		{"slot only", 0, 0, false, []uint32{1}, true, 2},
		{"one victim", 4, 0, false, []uint32{1}, true, 2},
		{"larger second victim", 8, 0, false, []uint32{1, 5}, true, 2},
		{"whole sample", 29, 0, false, []uint32{1, 5, 0, 4}, true, 2},
		{"sample cannot fit", 30, 0, false, nil, false, 2},
		{"exclude first through overwrite", 3, 1, true, []uint32{5}, true, 2},
		{"exclude second through overwrite", 12, 5, true, []uint32{1, 0}, true, 2},
		{"no eviction", 0, 1, true, nil, true, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sh := shard{entries: entries, byteLimit: 100, usedBytes: 100, samples: 4, sampleAt: 4}
			var candidates [maxSamples]uint32
			victims, ok := sh.plan(candidates[:0], tc.target, tc.exists, tc.delta, time.Second)
			if ok != tc.ok || !slices.Equal(victims, tc.want) || sh.sampleAt != tc.next {
				t.Fatalf("plan = (%v, %v, next %d), want (%v, %v, next %d)", victims, ok, sh.sampleAt, tc.want, tc.ok, tc.next)
			}
		})
	}
	sh := shard{entries: entries[:1:1], byteLimit: 100, usedBytes: 100, samples: 4}
	var candidates [maxSamples]uint32
	if victims, ok := sh.plan(candidates[:0], 0, true, 1, time.Second); ok || len(victims) != 0 || sh.sampleAt != 0 {
		t.Fatalf("cannot evict the only slot when it is the overwrite target: (%v, %v, next %d)", victims, ok, sh.sampleAt)
	}
}

func BenchmarkPlanFullShard(b *testing.B) {
	entries := make([]entry, 4096)
	value := make([]byte, 512)
	for i := range entries {
		entries[i] = entry{order: uint64(i + 1), value: value}
		if i%4 == 1 {
			entries[i].deadline = time.Second
		}
	}
	for _, tc := range []struct {
		name string
		need int64
	}{
		{"single", 512}, {"multiple", 1024}, {"reject", 17 * 512},
	} {
		b.Run(tc.name, func(b *testing.B) {
			sh := shard{entries: entries, byteLimit: 4 << 20, usedBytes: 4 << 20, samples: 16}
			var candidates [maxSamples]uint32
			for b.Loop() {
				sh.plan(candidates[:0], 0, false, tc.need, time.Second)
			}
		})
	}
}
