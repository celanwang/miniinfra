package store

import (
	"testing"
	"time"
)

func testStore(t *testing.T, bytes int64, entries, samples int) (*Store, *time.Duration) {
	t.Helper()
	s, err := New(Config{Shards: 1, BytesPerShard: bytes + int64(entries)*slotBudget, EntriesPerShard: entries, Samples: samples})
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	s.now = func() time.Duration { return now }
	return s, &now
}

func put(t *testing.T, s *Store, key, value string, ttl time.Duration, nx bool, want Result) {
	t.Helper()
	payload := make([]byte, len(value))
	copy(payload, value)
	if got := s.Set(key, payload, ttl, nx); got != want {
		t.Fatalf("Set(%q) = %d, want %d", key, got, want)
	}
}

func read(t *testing.T, s *Store, key, wantValue string, want Result) {
	t.Helper()
	value, result := s.Get(key, func(int64) bool { return true })
	if result != want || string(value) != wantValue {
		t.Fatalf("Get(%q) = %q, %d, want %q, %d", key, value, result, wantValue, want)
	}
}

func TestRejectedOverwriteKeepsOriginalDeadlineAndCandidates(t *testing.T) {
	s, now := testStore(t, 12, 3, 2)
	put(t, s, "a", "old", time.Second, false, Stored)
	put(t, s, "b", "bbb", 0, false, Stored)
	put(t, s, "c", "ccc", 0, false, Stored)
	*now = 500 * time.Millisecond
	put(t, s, "a", "evenlarger", 5*time.Second, false, NoSpace)
	read(t, s, "a", "old", Hit)
	read(t, s, "b", "bbb", Hit)
	read(t, s, "c", "ccc", Hit)
	*now = time.Second
	read(t, s, "a", "", Miss)
	put(t, s, "a", "new", time.Second, true, Stored)
	put(t, s, "a", "ignored", time.Second, true, Exists)
	read(t, s, "a", "new", Hit)
}

func TestEvictionPrefersExpiredAndRepairsMovedIndices(t *testing.T) {
	s, now := testStore(t, 12, 3, 3)
	put(t, s, "a", "aaa", 0, false, Stored)
	put(t, s, "b", "bbb", time.Second, false, Stored)
	put(t, s, "c", "ccc", 0, false, Stored)
	*now = time.Second
	put(t, s, "d", "ddd", 0, false, Stored)
	read(t, s, "a", "aaa", Hit)
	read(t, s, "b", "", Miss)
	read(t, s, "c", "ccc", Hit)
	read(t, s, "d", "ddd", Hit)
	put(t, s, "c", "newnewnew", 0, false, Stored)
	read(t, s, "c", "newnewnew", Hit)
	read(t, s, "a", "", Miss)
	read(t, s, "d", "", Miss)
	if stats := s.Stats(0); stats.Entries != 1 || stats.Expired != 1 || stats.Evicted != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestSweepRespectsBatchAndReplacementDeadline(t *testing.T) {
	s, now := testStore(t, 20, 3, 3)
	put(t, s, "a", "a", time.Second, false, Stored)
	put(t, s, "b", "b", time.Second, false, Stored)
	put(t, s, "c", "c", time.Second, false, Stored)
	*now = time.Second
	put(t, s, "c", "new", time.Second, true, Stored)
	s.Sweep(0, 1)
	if stats := s.Stats(0); stats.Entries != 2 || stats.Expired != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	s.Sweep(0, 3)
	read(t, s, "c", "new", Hit)
	if stats := s.Stats(0); stats.Entries != 1 || stats.Expired != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestSetCopyChecksPublishedCapacityAndDoesNotRetainScratch(t *testing.T) {
	s, _ := testStore(t, 4, 1, 1)
	scratch := make([]byte, 3, 128)
	copy(scratch, "abc")
	if got := s.SetCopy("k", scratch, 0, true); got != Stored {
		t.Fatalf("SetCopy returned %v", got)
	}
	copy(scratch, "xxx")
	read(t, s, "k", "abc", Hit)
	if got := s.Stats(0).UsedBytes; got != slotBudget+4 {
		t.Fatalf("charged %d bytes, want %d", got, slotBudget+4)
	}
}
