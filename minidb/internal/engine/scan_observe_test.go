package engine

import (
	"runtime"
	"time"
)

// PauseReclamationForScanProbe leaves the server's scans and writers as the
// only users of the table latch during the cancellation measurement.
func (db *DB) PauseReclamationForScanProbe() {
	db.stopReclaimer()
	<-db.reclaimerDone
}

// PinnedScan returns a predicate for the observed statement's release when
// there is one active view and it holds the table latch. The caller arranges
// that there are no other operations on this table during detection.
func (db *DB) PinnedScan(table string) func() bool {
	tbl, ok := db.table(table, db.published.Load())
	if !ok {
		return nil
	}
	db.snapshots.mu.Lock()
	defer db.snapshots.mu.Unlock()
	if len(db.snapshots.active) != 1 {
		return nil
	}
	if tbl.mu.TryLock() {
		tbl.mu.Unlock()
		return nil
	}
	for pin := range db.snapshots.active {
		return func() bool {
			db.snapshots.mu.Lock()
			defer db.snapshots.mu.Unlock()
			_, held := db.snapshots.active[pin]
			return !held
		}
	}
	return nil
}

type ScanMaterialization struct {
	Rows    int
	Indexed bool
	Wall    time.Duration
	Wait    time.Duration
	Bytes   uint64
}

func (db *DB) MeasureScanMaterialization(table string, columns []int, values []Value) (ScanMaterialization, error) {
	tx, err := db.Begin()
	if err != nil {
		return ScanMaterialization{}, err
	}
	defer tx.Rollback()
	view, err := tx.Snapshot()
	if err != nil {
		return ScanMaterialization{}, err
	}
	defer view.Close()
	tx.timing = &transactionTiming{}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	rows, indexed, err := view.Rows(table, columns, values)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	return ScanMaterialization{len(rows), indexed, elapsed, tx.timing.tableWait, after.TotalAlloc - before.TotalAlloc}, err
}
