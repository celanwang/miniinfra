package engine

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

const probeRows = 4096

func TestReclaimLatencyLoad(t *testing.T) {
	if os.Getenv("MINIDB_RECLAIM_LATENCY") == "" {
		t.Skip("set MINIDB_RECLAIM_LATENCY=1 to run the latency probe")
	}
	for _, test := range []struct {
		pins    int
		backlog bool
	}{
		{14, false}, {17, false}, {14, true}, {17, true},
	} {
		for _, enabled := range []bool{false, true} {
			name := fmt.Sprintf("pins%d_debt%t_on%t", test.pins, test.backlog, enabled)
			t.Run(name, func(t *testing.T) {
				probeReclaimer(t, test.pins, test.backlog, enabled)
			})
		}
	}
}

func probeReclaimer(t *testing.T, pins int, backlog, enabled bool) {
	t.Helper()
	db := prepareLatencyProbe(t, pins, backlog)
	before := historyFor(db, 1, "sku-1").heap
	stop := make(chan struct{})
	start := make(chan struct{})
	var results [2][]latencyProbeRequest
	var errorsByWorker [2]error
	var sweeps []timedSweep
	var workers sync.WaitGroup
	if enabled {
		workers.Go(func() {
			<-start
			timer := time.NewTicker(reclaimInterval)
			defer timer.Stop()
			for {
				select {
				case <-stop:
					return
				case scheduled := <-timer.C:
					sweeps = append(sweeps, measureSweep(db, scheduled))
				}
			}
		})
	}
	for i, write := range []bool{false, true} {
		workers.Go(func() {
			<-start
			rate := time.Millisecond
			if write {
				rate = 5 * time.Millisecond
			}
			ticker := time.NewTicker(rate)
			defer ticker.Stop()
			for index := 0; ; index++ {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				id := int64(probeRows/2 + 1 + index%(probeRows/2))
				request, err := latencyRequest(t, db, id, write)
				if err != nil {
					errorsByWorker[i] = err
					return
				}
				results[i] = append(results[i], request)
			}
		})
	}
	close(start)
	time.Sleep(1200 * time.Millisecond)
	close(stop)
	workers.Wait()
	if err := errors.Join(errorsByWorker[:]...); err != nil {
		t.Fatal(err)
	}
	final := historyFor(db, 1, "sku-1").heap
	t.Logf("initial=%d final=%d live_rows=%d", before, final, probeRows)
	if enabled {
		reportSweeps(t, sweeps)
	}
	for i, label := range []string{"reads", "commits"} {
		reportRequests(t, label, results[i], sweeps)
	}
}

func prepareLatencyProbe(t *testing.T, pins int, backlog bool) *DB {
	t.Helper()
	db := openInventory(t, t.TempDir())
	db.stopReclaimer()
	<-db.reclaimerDone
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	tx, view := start(t, db)
	for i := range int64(probeRows) {
		insert(t, view, i+1, fmt.Sprintf("sku-%d", i+1), 1)
	}
	view.Close()
	commit(t, tx)

	if backlog {
		updateProbeRows(t, db)
	}
	for range pins {
		start(t, db)
	}
	if backlog {
		updateProbeRows(t, db)
	}
	return db
}

func updateProbeRows(t *testing.T, db *DB) {
	t.Helper()
	tx, view := start(t, db)
	for range 2 {
		for id := range int64(probeRows / 2) {
			changed, err := view.Mutate(t.Context(), schema.Name, []Value{Int(id + 1)}, incrementProbeQty)
			if err != nil || !changed {
				t.Fatalf("prepare update: %v %v", changed, err)
			}
		}
	}
	view.Close()
	commit(t, tx)
}

func incrementProbeQty(row []Value) ([]Value, bool, error) {
	row[2].Int++
	return row, true, nil
}

type latencyProbeRequest struct {
	total, tableWait time.Duration
	locks            []tableAcquisition
}

func latencyRequest(t *testing.T, db *DB, id int64, write bool) (latencyProbeRequest, error) {
	timing := transactionTiming{acquisitions: make([]tableAcquisition, 0, 3)}
	started := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return latencyProbeRequest{}, err
	}
	defer tx.Rollback()
	tx.timing = &timing
	view, err := tx.Snapshot()
	if err != nil {
		return latencyProbeRequest{}, err
	}
	if write {
		var changed bool
		changed, err = view.Mutate(t.Context(), schema.Name, []Value{Int(id)}, incrementProbeQty)
		if err == nil && !changed {
			err = errors.New("ordinary row disappeared")
		}
	} else {
		var rows [][]Value
		rows, _, err = view.Rows(t.Context(), schema.Name, []int{0}, []Value{Int(id)})
		if err == nil && len(rows) != 1 {
			err = errors.New("ordinary key did not return one visible row")
		}
	}
	view.Close()
	if err != nil {
		return latencyProbeRequest{}, err
	}
	if err := tx.Commit(); err != nil {
		return latencyProbeRequest{}, err
	}
	return latencyProbeRequest{time.Since(started), timing.tableWait, timing.acquisitions}, nil
}

type timedSweep struct {
	scanned, removed int
	dispatch, wall   time.Duration
	timing           reclaimTiming
}

func measureSweep(db *DB, scheduled time.Time) timedSweep {
	start := time.Now()
	sample := timedSweep{
		dispatch: start.Sub(scheduled),
		timing:   reclaimTiming{tables: make([]reclaimTableTiming, 0, 4)},
	}
	sample.scanned, sample.removed = db.reclaimStepMeasured(&sample.timing)
	sample.wall = time.Since(start)
	return sample
}

func reportSweeps(t *testing.T, sweeps []timedSweep) {
	t.Helper()
	var scanned, removed, exact int
	var maxHold time.Duration
	var slowest timedSweep
	var dispatch, waiting, hold, removal []time.Duration
	for _, sweep := range sweeps {
		scanned += sweep.scanned
		removed += sweep.removed
		if sweep.timing.exact {
			exact++
		}
		if sweep.wall > slowest.wall {
			slowest = sweep
		}
		dispatch = append(dispatch, sweep.dispatch)
		wait, held, removedIn := sweepTableTimes(sweep)
		waiting, hold, removal = append(waiting, wait), append(hold, held), append(removal, removedIn)
		for _, visit := range sweep.timing.tables {
			maxHold = max(maxHold, visit.releasing.Sub(visit.locked))
		}
	}
	t.Logf("steps=%d exact=%d scanned=%d removed=%d dispatch_p99=%s table_wait_p99=%s table_hold_p99=%s removal_p99=%s max_hold=%s",
		len(sweeps), exact, scanned, removed, latencyP99(dispatch), latencyP99(waiting), latencyP99(hold), latencyP99(removal), maxHold)
	wait, held, removedIn := sweepTableTimes(slowest)
	t.Logf("slowest_wall=%s gate=%s prepare=%s table_wait=%s table_hold=%s removal=%s",
		slowest.wall, slowest.timing.gateWait, slowest.timing.prepare, wait, held, removedIn)
}

func sweepTableTimes(sweep timedSweep) (wait, hold, removal time.Duration) {
	for _, visit := range sweep.timing.tables {
		wait += visit.locked.Sub(visit.requested)
		hold += visit.releasing.Sub(visit.locked)
		removal += visit.remove
	}
	return wait, hold, removal
}

func reportRequests(t *testing.T, label string, requests []latencyProbeRequest, sweeps []timedSweep) {
	t.Helper()
	var total, wait []time.Duration
	var overlaps int
	var worstOverlap time.Duration
	for _, request := range requests {
		total = append(total, request.total)
		wait = append(wait, request.tableWait)
		blocked := sweepOverlap(request, sweeps)
		if blocked > 0 {
			overlaps++
			worstOverlap = max(worstOverlap, blocked)
		}
	}
	t.Logf("%s=%d p99=%s table_wait_p99=%s overlapped=%d max_overlap=%s",
		label, len(requests), latencyP99(total), latencyP99(wait), overlaps, worstOverlap)
}

func sweepOverlap(request latencyProbeRequest, sweeps []timedSweep) time.Duration {
	var result time.Duration
	for _, lock := range request.locks {
		for _, sweep := range sweeps {
			for _, visit := range sweep.timing.tables {
				if visit.table != lock.table {
					continue
				}
				start, end := lock.requested, lock.locked
				if visit.locked.After(start) {
					start = visit.locked
				}
				if visit.releasing.Before(end) {
					end = visit.releasing
				}
				if end.After(start) {
					result += end.Sub(start)
				}
			}
		}
	}
	return result
}

func latencyP99(times []time.Duration) time.Duration {
	if len(times) == 0 {
		return 0
	}
	slices.Sort(times)
	return times[(99*len(times)+99)/100-1]
}
