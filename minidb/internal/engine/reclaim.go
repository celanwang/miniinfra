package engine

import (
	"context"
	"math"
	"time"
)

const (
	abortedSequence = math.MaxUint64
	reclaimInterval = 10 * time.Millisecond
	reclaimBudget   = heapPageRows
)

func (db *DB) runReclaimer(ctx context.Context) {
	defer close(db.reclaimerDone)
	timer := time.NewTicker(reclaimInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			db.reclaimStep()
		}
	}
}

// reclaimStep visits at most one page's worth of slots in a single table.
// Checkpoint walks by page number, so it excludes moves of existing rows.
func (db *DB) reclaimStep() (examined, removed int) {
	db.reclaimMu.Lock()
	defer db.reclaimMu.Unlock()
	if db.closed.Load() {
		return 0, 0
	}

	oldest := db.oldestSnapshot()
	db.catalog.RLock()
	if len(db.tableOrder) == 0 {
		db.catalog.RUnlock()
		return 0, 0
	}
	tbl := db.tableOrder[db.reclaimTable]
	db.reclaimTable = (db.reclaimTable + 1) % len(db.tableOrder)
	db.catalog.RUnlock()

	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if len(tbl.pages) == 0 {
		return 0, 0
	}
	size := (len(tbl.pages)-1)*heapPageRows + len(tbl.pages[len(tbl.pages)-1])
	limit := min(reclaimBudget, size)
	for examined < limit && size != 0 {
		if tbl.reclaimSlot >= size {
			tbl.reclaimSlot = 0
		}
		page, slot := tbl.reclaimSlot/heapPageRows, tbl.reclaimSlot%heapPageRows
		row := tbl.pages[page][slot]
		examined++
		if row.obsolete(oldest) {
			tbl.removeVersion(page, slot, row)
			size--
			removed++
			continue
		}
		tbl.reclaimSlot++
	}
	return examined, removed
}

// Active creations and removals have sequence zero. An aborted removal
// leaves its predecessor live and can be detached even under a pinned view.
func (row *version) obsolete(oldest uint64) bool {
	created := row.created.sequence.Load()
	if created == abortedSequence {
		return true
	}
	if created == 0 || row.ended == nil {
		return false
	}
	ended := row.ended.sequence.Load()
	if ended == abortedSequence {
		row.ended = nil
		return false
	}
	return ended != 0 && ended <= oldest
}

func (tbl *table) removeVersion(page, slot int, row *version) {
	for i := range tbl.indexes {
		idx := &tbl.indexes[i]
		key, _ := key(row.values, idx.def.Columns)
		idx.remove(key, row)
	}
	if row.next != nil {
		row.next.previous = row.previous
	} else if row.previous == nil {
		delete(tbl.heads, row.id)
	} else {
		tbl.heads[row.id] = row.previous
	}
	if row.previous != nil {
		row.previous.next = row.next
	}
	row.previous, row.next = nil, nil

	// Keep heap slots dense by moving the last version. The indexes and
	// chains reference versions, so the move does not change their identity.
	lastPage := len(tbl.pages) - 1
	lastSlot := len(tbl.pages[lastPage]) - 1
	tbl.pages[page][slot] = tbl.pages[lastPage][lastSlot]
	tbl.pages[lastPage][lastSlot] = nil
	tbl.pages[lastPage] = tbl.pages[lastPage][:lastSlot]
	if lastSlot == 0 {
		tbl.pages[lastPage] = nil
		tbl.pages = tbl.pages[:lastPage]
	}
}
