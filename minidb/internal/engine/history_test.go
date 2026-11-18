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
	for i := range count {
		if err := view.Insert(t.Context(), schema.Name, []Value{Int(int64(i + 1)), String([]string{"hot", "ordinary"}[i]), Int(0)}); err != nil {
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
	check := func(where string, db *DB, count int) {
		t.Helper()
		got := historyFor(db, 1, "hot")
		t.Logf("%s: %+v", where, got)
		if got != (historyCounts{count, count, count}) {
			t.Fatalf("unexpected history size at %s: %+v", where, got)
		}
		_, view := start(t, db)
		wantStock(t, view, 0, Int(1), historyUpdates/2)
		wantStock(t, view, 1, String("hot"), historyUpdates/2)
	}
	check("before checkpoint", db, historyUpdates+1)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check("after checkpoint", db, historyUpdates+1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	check("after restart", openInventory(t, dir), 1)
}

// Run with -benchtime=2400x. The hot worker alternates commits and rollbacks
// while a second client updates a disjoint key and syncs every commit.
func BenchmarkMixedVersionHistory(b *testing.B) {
	db := seedHistory(b, b.TempDir(), 2)
	ctx := b.Context()
	ordinary := make([]time.Duration, b.N)
	var workers sync.WaitGroup
	hotResult := make(chan error, 1)
	start := make(chan struct{})
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
