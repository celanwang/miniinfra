package engine

import (
	"slices"
	"sync"
)

type snapshotRegistry struct {
	mu     sync.Mutex
	active map[*snapshotPin]struct{}
}

type snapshotPin struct {
	db       *DB
	sequence uint64
	owner    *transactionState
}

func (db *DB) pinSnapshot(tx *Txn) Snapshot {
	db.snapshots.mu.Lock()
	defer db.snapshots.mu.Unlock()
	sequence := db.published.Load()
	pin := &snapshotPin{db: db, sequence: sequence}
	if tx != nil {
		pin.owner = tx.state
	}
	db.snapshots.active[pin] = struct{}{}
	return Snapshot{tx: tx, sequence: sequence, pin: pin}
}

// Close releases the statement's view. Copies share the same pin, and Close
// may be called more than once. Callers must finish using the view first.
func (view Snapshot) Close() {
	if view.pin == nil {
		return
	}
	registry := &view.pin.db.snapshots
	registry.mu.Lock()
	delete(registry.active, view.pin)
	registry.mu.Unlock()
}

const maxExactSnapshots = 16

type reclaimViews struct {
	published, oldest uint64
	sequences         [maxExactSnapshots]uint64
	count             int
}

// Copy the active view under the same lock that registers each statement.
// A snapshot registered after this copy starts at or above published.
func (db *DB) captureReclaimViews() reclaimViews {
	db.snapshots.mu.Lock()
	published := db.published.Load()
	views := reclaimViews{published: published, oldest: published}
	exact := len(db.snapshots.active) <= maxExactSnapshots
	for pin := range db.snapshots.active {
		views.oldest = min(views.oldest, pin.sequence)
		if !exact {
			continue
		}
		// Internal callers can keep a writer's view after committing. Its
		// own versions can be visible ahead of the registered sequence.
		if pin.owner != nil {
			owner := pin.owner.sequence.Load()
			if owner != abortedSequence && owner > pin.sequence {
				exact = false
				continue
			}
		}
		views.sequences[views.count] = pin.sequence
		views.count++
	}
	db.snapshots.mu.Unlock()

	if exact {
		slices.Sort(views.sequences[:views.count])
	} else {
		views.count = 0
	}
	return views
}

func (views *reclaimViews) excludes(created, ended uint64) bool {
	if ended == 0 || ended > views.published {
		return false
	}
	if ended <= views.oldest {
		return true
	}
	if views.count == 0 {
		return false
	}
	pins := views.sequences[:views.count]
	next, _ := slices.BinarySearch(pins, created)
	return next == len(pins) || pins[next] >= ended
}
