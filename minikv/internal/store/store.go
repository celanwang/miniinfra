// Package store implements the sharded in-memory keyspace.
package store

import (
	"cmp"
	"errors"
	"hash/maphash"
	"math"
	"slices"
	"sync"
	"time"
)

type Result uint8

const (
	Hit Result = iota
	Miss
	Stored
	Exists
	NoSpace
	Busy
	Invalid
)

const (
	// slotBudget estimates an entry and its share of the Go map allocation.
	slotBudget = 112
	maxSamples = 64
)

type Config struct {
	Shards          int
	BytesPerShard   int64
	EntriesPerShard int
	Samples         int
}

type Store struct {
	shards []shard
	seed   maphash.Seed
	now    func() time.Duration
}

type entry struct {
	key      string
	value    []byte
	deadline time.Duration
	order    uint64
}

func (e entry) expired(now time.Duration) bool {
	return e.deadline != 0 && now >= e.deadline
}

func (e entry) bytes() int64 {
	return int64(len(e.key)) + int64(cap(e.value))
}

type shard struct {
	mu        sync.RWMutex
	index     map[string]uint32
	entries   []entry
	usedBytes int64
	byteLimit int64
	order     uint64
	samples   int
	sampleAt  int
	sweepAt   int
	evicted   uint64
	expired   uint64
	rejected  uint64
}

func New(config Config) (*Store, error) {
	if config.Shards < 1 || config.EntriesPerShard < 1 ||
		int64(config.EntriesPerShard) > math.MaxUint32 ||
		config.Samples < 1 || config.Samples > maxSamples {
		return nil, errors.New("invalid shard capacity or sample count")
	}

	reserved := int64(config.EntriesPerShard) * slotBudget
	if config.BytesPerShard <= reserved {
		return nil, errors.New("shard byte budget must exceed metadata reservation")
	}

	start := time.Now()
	s := &Store{
		shards: make([]shard, config.Shards),
		seed:   maphash.MakeSeed(),
		now:    func() time.Duration { return time.Since(start) },
	}
	for i := range s.shards {
		sh := &s.shards[i]
		sh.index = make(map[string]uint32, config.EntriesPerShard)
		sh.entries = make([]entry, 0, config.EntriesPerShard)
		sh.usedBytes = reserved
		sh.byteLimit = config.BytesPerShard
		sh.samples = config.Samples
	}
	return s, nil
}

func (s *Store) ShardIndex(key string) int {
	return int(maphash.String(s.seed, key) % uint64(len(s.shards)))
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[s.ShardIndex(key)]
}

// Get borrows key for the lookup and returns an immutable value if reserved.
// reserve runs under the read lock and must return without waiting on I/O.
func (s *Store) Get(key []byte, reserve func(int64) bool) ([]byte, Result) {
	sh := &s.shards[maphash.Bytes(s.seed, key)%uint64(len(s.shards))]
	sh.mu.RLock()
	i, ok := sh.index[string(key)]
	if !ok || sh.entries[i].expired(s.now()) {
		sh.mu.RUnlock()
		return nil, Miss
	}
	value := sh.entries[i].value
	if !reserve(int64(cap(value))) {
		sh.mu.RUnlock()
		return nil, Busy
	}
	sh.mu.RUnlock()
	return value, Hit
}

// Set takes ownership of value on success. Its backing array must be
// immutable after publication. A zero TTL explicitly disables expiration.
func (s *Store) Set(key string, value []byte, ttl time.Duration, onlyMissing bool) Result {
	return s.set(key, value, ttl, onlyMissing, false)
}

// SetCopy borrows value during the call and allocates an exact-capacity copy
// only if the write passes the shard's conditional and capacity checks.
func (s *Store) SetCopy(key string, value []byte, ttl time.Duration, onlyMissing bool) Result {
	return s.set(key, value, ttl, onlyMissing, true)
}

func (s *Store) set(key string, value []byte, ttl time.Duration, onlyMissing, copyValue bool) Result {
	if len(key) == 0 || ttl < 0 {
		return Invalid
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := s.now()
	i, exists := sh.index[key]
	if onlyMissing && exists && !sh.entries[i].expired(now) {
		return Exists
	}

	next := entry{key: key, value: value}
	delta := next.bytes()
	if copyValue {
		delta -= int64(cap(value) - len(value))
	}
	if exists {
		delta -= sh.entries[i].bytes()
	}

	var candidates [maxSamples]uint32
	victims, ok := sh.plan(candidates[:0], i, exists, delta, now)
	if !ok {
		sh.rejected++
		return NoSpace
	}

	if ttl > time.Duration(math.MaxInt64)-now {
		return Invalid
	}
	if copyValue {
		next.value = make([]byte, len(value))
		copy(next.value, value)
	}
	if ttl > 0 {
		published := s.now()
		if ttl > time.Duration(math.MaxInt64)-published {
			return Invalid
		}
		next.deadline = published + ttl
	}
	sh.order++
	next.order = sh.order

	// Removing slots in descending order keeps remaining victim indices valid.
	slices.SortFunc(victims, func(a, b uint32) int {
		return cmp.Compare(b, a)
	})
	for _, victim := range victims {
		sh.remove(victim, now)
	}
	if exists {
		i = sh.index[key]
		next.key = sh.entries[i].key
		sh.entries[i] = next
	} else {
		sh.index[next.key] = uint32(len(sh.entries))
		sh.entries = append(sh.entries, next)
	}
	sh.usedBytes += delta
	return Stored
}

func (sh *shard) plan(candidates []uint32, target uint32, exists bool, delta int64, now time.Duration) ([]uint32, bool) {
	needBytes := delta - (sh.byteLimit - sh.usedBytes)
	needSlot := !exists && len(sh.entries) == cap(sh.entries)
	if needBytes <= 0 && !needSlot {
		return candidates, true
	}
	n := len(sh.entries)
	if n == 0 {
		return nil, false
	}
	start := sh.sampleAt % n
	steps := min(sh.samples, n)
	sh.sampleAt = (start + steps) % n
	for offset := range steps {
		i := uint32((start + offset) % n)
		if !exists || i != target {
			candidates = append(candidates, i)
		}
	}
	slices.SortFunc(candidates, func(a, b uint32) int {
		left, right := sh.entries[a], sh.entries[b]
		if left.expired(now) != right.expired(now) {
			if left.expired(now) {
				return -1
			}
			return 1
		}
		return cmp.Compare(left.order, right.order)
	})
	for i, candidate := range candidates {
		needBytes -= sh.entries[candidate].bytes()
		if needBytes <= 0 {
			return candidates[:i+1], true
		}
	}
	return nil, false
}

func (sh *shard) remove(index uint32, now time.Duration) {
	last := uint32(len(sh.entries) - 1)
	old := sh.entries[index]
	delete(sh.index, old.key)
	sh.usedBytes -= old.bytes()
	if old.expired(now) {
		sh.expired++
	} else {
		sh.evicted++
	}
	if index != last {
		moved := sh.entries[last]
		sh.entries[index] = moved
		sh.index[moved.key] = index
	}
	sh.entries[last] = entry{}
	sh.entries = sh.entries[:last]
}

// Sweep visits at most batch slots in one shard.
func (s *Store) Sweep(shardIndex, batch int) {
	sh := &s.shards[shardIndex]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := s.now()
	for range min(batch, len(sh.entries)) {
		if len(sh.entries) == 0 {
			break
		}
		sh.sweepAt %= len(sh.entries)
		if sh.entries[sh.sweepAt].expired(now) {
			sh.remove(uint32(sh.sweepAt), now)
		} else {
			sh.sweepAt++
		}
	}
}

type ShardStats struct {
	Entries   int
	UsedBytes int64
	Evicted   uint64
	Expired   uint64
	Rejected  uint64
}

func (s *Store) Stats(i int) ShardStats {
	sh := &s.shards[i]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return ShardStats{len(sh.entries), sh.usedBytes, sh.evicted, sh.expired, sh.rejected}
}

func (s *Store) ShardCount() int {
	return len(s.shards)
}
