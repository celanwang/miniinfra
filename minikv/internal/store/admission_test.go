package store

import (
	"hash/maphash"
	"testing"
	"time"
)

func TestTwoFillAdmissionOnlyGatesLiveEviction(t *testing.T) {
	const window = time.Second
	s, err := New(Config{
		Shards: 1, BytesPerShard: 2*slotBudget + admissionBytes + 8,
		EntriesPerShard: 2, Samples: 2, AdmissionWindow: window,
	})
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	s.now = func() time.Duration { return now }

	if got := s.Stats(0).UsedBytes; got != 2*slotBudget+admissionBytes {
		t.Fatalf("empty shard bytes = %d", got)
	}
	put(t, s, "a", "aaa", window, true, Stored)
	put(t, s, "b", "bbb", 0, true, Stored)
	put(t, s, "c", "ccc", 0, true, NotCached)
	put(t, s, "a", "new", 0, true, Exists)
	read(t, s, "a", "aaa", Hit)
	read(t, s, "c", "", Miss)
	put(t, s, "c", "ccc", 0, true, Stored)
	read(t, s, "a", "", Miss)
	put(t, s, "d", "ddd", window, false, Stored)
	now = window
	put(t, s, "e", "eee", 0, true, Stored)
	read(t, s, "c", "ccc", Hit)
	read(t, s, "d", "", Miss)
	if stats := s.Stats(0); stats.UsedBytes != 2*slotBudget+admissionBytes+8 ||
		stats.Evicted != 2 || stats.Expired != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestAdmissionFilterForgetsOldEpochs(t *testing.T) {
	a := admission{window: time.Second, seed: maphash.MakeSeed()}
	if a.seen("key", 0) || !a.seen("key", time.Second) {
		t.Fatal("first attempt should be remembered across the epoch boundary")
	}
	if a.seen("key", 3*time.Second) || !a.seen("key", 3*time.Second) {
		t.Fatal("aged attempt should be forgotten and the next attempt remembered")
	}
}
