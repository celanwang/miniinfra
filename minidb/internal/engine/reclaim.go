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
	return db.reclaimStepMeasured(nil)
}

type reclaimTiming struct {
	gateWait, prepare time.Duration
	exact             bool
	tables            []reclaimTableTiming
}

type reclaimTableTiming struct {
	table                        *table
	requested, locked, releasing time.Time
	remove                       time.Duration
}

// The optional timing is used by diagnostics. The production reclaimer takes
// the same path without reading clocks or allocating samples.
func (db *DB) reclaimStepMeasured(timing *reclaimTiming) (examined, removed int) {
	var started time.Time
	if timing != nil {
		started = time.Now()
	}
	db.reclaimMu.Lock()
	defer db.reclaimMu.Unlock()
	if timing != nil {
		now := time.Now()
		timing.gateWait = now.Sub(started)
		started = now
	}
	if db.closed.Load() {
		return 0, 0
	}

	views := db.captureReclaimViews()
	db.catalog.RLock()
	// The catalog appends tables without changing existing entries.
	tables := db.tableOrder
	db.catalog.RUnlock()
	if timing != nil {
		timing.prepare = time.Since(started)
		timing.exact = views.count != 0
	}

	budget := reclaimBudget
	for visited := 0; budget > 0 && visited < len(tables); visited++ {
		tbl := tables[db.reclaimTable]
		db.reclaimTable = (db.reclaimTable + 1) % len(tables)
		scanned, collected := tbl.reclaimWithin(&views, budget, timing)
		examined += scanned
		removed += collected
		budget -= max(1, scanned)
	}
	return examined, removed
}

func (tbl *table) reclaimWithin(views *reclaimViews, budget int, timing *reclaimTiming) (examined, removed int) {
	var visit reclaimTableTiming
	if timing != nil {
		visit.table, visit.requested = tbl, time.Now()
	}
	tbl.mu.Lock()
	if timing == nil {
		defer tbl.mu.Unlock()
	} else {
		visit.locked = time.Now()
		defer func() {
			visit.releasing = time.Now()
			tbl.mu.Unlock()
			timing.tables = append(timing.tables, visit)
		}()
	}
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
			if timing == nil {
				tbl.removeVersion(page, slot, row)
			} else {
				started := time.Now()
				tbl.removeVersion(page, slot, row)
				visit.remove += time.Since(started)
			}
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
		tbl.indexes[i].remove(row)
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
