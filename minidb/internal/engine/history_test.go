package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

const historyUpdates = 2400

type historyCounts struct {
	heap, primary, secondary int
}

func historyFor(db *DB, sku int64, label string) historyCounts {
	tbl, _ := db.table(schema.Name, db.published.Load())
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	var result historyCounts
	for _, page := range tbl.pages {
		for range page {
			result.heap++
		}
	}
	for range tbl.indexes[0].lookup(lookupKey([]Value{Int(sku)})) {
		result.primary++
	}
	for range tbl.indexes[1].lookup(lookupKey([]Value{String(label)})) {
		result.secondary++
	}
	return result
}

func seedHistory(t testing.TB, dir string, count int) *DB {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	view, err := tx.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	for i, label := range []string{"hot", "ordinary"}[:count] {
		if err := view.Insert(t.Context(), schema.Name, []Value{Int(int64(i + 1)), String(label), Int(0)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func cycleStock(ctx context.Context, db *DB, sku int64, durable bool) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	view, err := tx.Snapshot()
	if err != nil {
		return err
	}
	defer view.Close()
	changed, err := view.Mutate(ctx, schema.Name, []Value{Int(sku)}, func(row []Value) ([]Value, bool, error) {
		row[2].Int++
		return row, true, nil
	})
	if err != nil {
		return err
	}
	if !changed {
		return errors.New("stock row disappeared")
	}
	if durable {
		return tx.Commit()
	}
	return nil
}

func TestHotHistoryAcrossCheckpointAndRestart(t *testing.T) {
	dir := t.TempDir()
	db := seedHistory(t, dir, 1)
	for i := range historyUpdates {
		if err := cycleStock(t.Context(), db, 1, i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("before checkpoint: %+v", historyFor(db, 1, "hot"))
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	counts := waitForHistory(t, db, historyCounts{1, 1, 1})
	t.Logf("after online reclamation: %+v", counts)
	_, view := start(t, db)
	wantStock(t, view, 0, Int(1), historyUpdates/2)
	wantStock(t, view, 1, String("hot"), historyUpdates/2)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openInventory(t, dir)
	_, fresh := start(t, reopened)
	wantStock(t, fresh, 0, Int(1), historyUpdates/2)
	wantStock(t, fresh, 1, String("hot"), historyUpdates/2)
	t.Logf("after restart: %+v", waitForHistory(t, reopened, historyCounts{1, 1, 1}))
}

func waitForHistory(t *testing.T, db *DB, want historyCounts) historyCounts {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if counts := historyFor(db, 1, "hot"); counts == want {
			return counts
		}
		time.Sleep(reclaimInterval)
	}
	t.Fatalf("history = %+v, want %+v", historyFor(db, 1, "hot"), want)
	return historyCounts{}
}

// The hot worker alternates commits and rollbacks while a second client
// updates a disjoint key and syncs every commit.
func BenchmarkMixedVersionHistory(b *testing.B) {
	b.Run("retained", func(b *testing.B) { mixedVersionHistory(b, false) })
	b.Run("online", func(b *testing.B) { mixedVersionHistory(b, true) })
}

func mixedVersionHistory(b *testing.B, collect bool) {
	db := seedHistory(b, b.TempDir(), 2)
	db.stopReclaimer()
	<-db.reclaimerDone

	ctx := b.Context()
	ordinary := make([]time.Duration, b.N)
	var workers sync.WaitGroup
	hotResult := make(chan error, 1)
	start := make(chan struct{})
	stop := make(chan struct{})
	sweeps := make(chan sweepMeasurement, 1)
	if collect {
		go measureReclamation(db, start, stop, sweeps)
	}

	b.ResetTimer()
	workers.Go(func() {
		<-start
		for i := range b.N {
			if err := cycleStock(ctx, db, 1, i%2 == 0); err != nil {
				hotResult <- err
				return
			}
		}
		hotResult <- nil
	})
	close(start)
	var ordinaryErr error
	for i := range b.N {
		now := time.Now()
		ordinaryErr = cycleStock(ctx, db, 2, true)
		ordinary[i] = time.Since(now)
		if ordinaryErr != nil {
			break
		}
	}
	workers.Wait()
	b.StopTimer()
	close(stop)
	if collect {
		result := <-sweeps
		if result.steps > 0 {
			b.ReportMetric(float64(result.total.Nanoseconds())/float64(result.steps), "sweep-mean-ns")
		}
		b.ReportMetric(float64(result.slowest.Nanoseconds()), "sweep-max-ns")
		b.ReportMetric(float64(result.removed), "versions-reclaimed")
	}
	if err := errors.Join(ordinaryErr, <-hotResult); err != nil {
		b.Fatal(err)
	}
	slices.Sort(ordinary)
	b.ReportMetric(float64(ordinary[(len(ordinary)-1)*99/100].Nanoseconds()), "ordinary-p99-ns")
	counts := historyFor(db, 1, "hot")
	b.ReportMetric(float64(counts.heap), "heap-versions")
	b.ReportMetric(float64(counts.primary), "hot-primary")
	b.ReportMetric(float64(counts.secondary), "hot-secondary")
}

type sweepMeasurement struct {
	steps, removed int
	total, slowest time.Duration
}

func measureReclamation(db *DB, start, stop <-chan struct{}, result chan<- sweepMeasurement) {
	<-start
	timer := time.NewTicker(reclaimInterval)
	defer timer.Stop()
	var measurement sweepMeasurement
	for {
		select {
		case <-stop:
			result <- measurement
			return
		case <-timer.C:
			started := time.Now()
			_, removed := db.reclaimStep()
			duration := time.Since(started)
			measurement.steps++
			measurement.removed += removed
			measurement.total += duration
			measurement.slowest = max(measurement.slowest, duration)
		}
	}
}
