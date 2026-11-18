package store

import (
	"hash/maphash"
	"time"
)

const (
	admissionBits = 1 << 15
	// Charge both bitmaps and reserve space for the filter header.
	admissionBytes = 2*admissionBits/8 + 64
)

// admission is owned by a shard and accessed under its write lock. Keeping
// the previous epoch avoids dropping all first attempts at an epoch boundary.
type admission struct {
	bits   [2][admissionBits / 64]uint64
	window time.Duration
	epoch  time.Duration
	seed   maphash.Seed
}

func (a *admission) seen(key string, now time.Duration) bool {
	epoch := now / a.window
	current := epoch % 2
	switch {
	case epoch-a.epoch >= 2:
		clear(a.bits[:])
	case epoch != a.epoch:
		clear(a.bits[current][:])
	}
	a.epoch = epoch

	hash := maphash.String(a.seed, key)
	left := hash & (admissionBits - 1)
	right := (hash >> 32) & (admissionBits - 1)
	leftMask := uint64(1) << (left % 64)
	rightMask := uint64(1) << (right % 64)
	seen := false
	for i := range a.bits {
		if a.bits[i][left/64]&leftMask != 0 && a.bits[i][right/64]&rightMask != 0 {
			seen = true
			break
		}
	}
	a.bits[current][left/64] |= leftMask
	a.bits[current][right/64] |= rightMask
	return seen
}
