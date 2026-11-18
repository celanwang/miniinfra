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

// reclaimStep shares one page's worth of work across the catalog. A table
// with no rows still consumes one unit so a sparse catalog cannot make a
// sweep unbounded. Checkpoint excludes moves of existing heap slots.
func (db *DB) reclaimStep() (examined, removed int) {
	db.reclaimMu.Lock()
	defer db.reclaimMu.Unlock()
	if db.closed.Load() {
		return 0, 0
	}

	views := db.captureReclaimViews()
	db.catalog.RLock()
	// The catalog appends tables without changing existing entries.
	tables := db.tableOrder
	db.catalog.RUnlock()

	budget := reclaimBudget
	for visited := 0; budget > 0 && visited < len(tables); visited++ {
		tbl := tables[db.reclaimTable]
		db.reclaimTable = (db.reclaimTable + 1) % len(tables)
		scanned, collected := tbl.reclaimWithin(&views, budget)
		examined += scanned
		removed += collected
		budget -= max(1, scanned)
	}
	return examined, removed
}

func (tbl *table) reclaimWithin(views *reclaimViews, budget int) (examined, removed int) {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if len(tbl.pages) == 0 {
		return 0, 0
	}
	size := (len(tbl.pages)-1)*heapPageRows + len(tbl.pages[len(tbl.pages)-1])
	limit := min(budget, size)
	for examined < limit && size != 0 {
		if tbl.reclaimSlot >= size {
			tbl.reclaimSlot = 0
		}
		page, slot := tbl.reclaimSlot/heapPageRows, tbl.reclaimSlot%heapPageRows
		row := tbl.pages[page][slot]
		examined++
		if row.obsolete(views) {
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
func (row *version) obsolete(views *reclaimViews) bool {
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
	return views.excludes(created, ended)
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
