package engine

import "sync"

type snapshotRegistry struct {
	mu     sync.Mutex
	active map[*snapshotPin]struct{}
}

type snapshotPin struct {
	db       *DB
	sequence uint64
}

func (db *DB) pinSnapshot(tx *Txn) Snapshot {
	db.snapshots.mu.Lock()
	defer db.snapshots.mu.Unlock()
	sequence := db.published.Load()
	pin := &snapshotPin{db: db, sequence: sequence}
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

func (db *DB) oldestSnapshot() uint64 {
	db.snapshots.mu.Lock()
	defer db.snapshots.mu.Unlock()
	oldest := db.published.Load()
	for pin := range db.snapshots.active {
		oldest = min(oldest, pin.sequence)
	}
	return oldest
}
