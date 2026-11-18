package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestTCPCompletedSelectiveScan(t *testing.T) {
	setting := os.Getenv("MINIDB_SCAN_LOAD")
	if setting == "" {
		t.Skip("set MINIDB_SCAN_LOAD to the number of rows (for example 120000)")
	}
	rows, err := strconv.Atoi(setting)
	if err != nil || rows < 8192 {
		t.Fatalf("invalid MINIDB_SCAN_LOAD %q, need at least 8192", setting)
	}
	store, pool, _ := openTCP(t, t.TempDir())
	store.PauseReclamationForScanProbe()
	pool.SetMaxOpenConns(2)
	seedScanOrders(t, pool, rows)
	for trial := range 5 {
		counts := [2]int{1, 8192}
		if trial%2 != 0 {
			counts[0], counts[1] = counts[1], counts[0]
		}
		checkSelectiveScan(t, pool, rows, true)
		for _, count := range counts {
			for _, withWriter := range []bool{trial%2 != 0, trial%2 == 0} {
				measureCompletedScan(t, store, pool, rows, count, withWriter)
			}
		}
	}
}

type completedScan struct {
	response time.Duration
	err      error
}

func measureCompletedScan(t *testing.T, store *engine.DB, pool *sql.DB, rows, count int, withWriter bool) {
	t.Helper()
	var writer *sql.Tx
	if withWriter {
		writer = beginScanWriter(t, pool)
		defer writer.Rollback()
	}
	var before, after runtime.MemStats
	if !withWriter {
		runtime.ReadMemStats(&before)
	}
	reply := make(chan completedScan, 1)
	go func() {
		started := time.Now()
		err := finishCompletedScan(t.Context(), pool, rows, count)
		reply <- completedScan{time.Since(started), err}
	}()

	var hold, statement, commit time.Duration
	for deadline := time.Now().Add(3 * time.Second); ; runtime.Gosched() {
		if store.PinnedScan(scanTable) != nil {
			started := time.Now()
			if withWriter {
				statement, commit = finishScanWriter(t, writer)
			} else {
				for !store.ScanLatchFree(scanTable) {
					runtime.Gosched()
					if time.Now().After(deadline) {
						t.Fatal("scan did not release the latch")
					}
				}
				hold = time.Since(started)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scan did not acquire the latch")
		}
		select {
		case result := <-reply:
			t.Fatalf("missed the scan latch: %+v", result)
		default:
		}
	}
	result := <-reply
	if result.err != nil {
		t.Fatal(result.err)
	}
	if withWriter {
		t.Logf("completed rows=%d returned=%d writer_statement=%s writer_commit=%s response=%s", rows, count, statement, commit, result.response)
	} else {
		runtime.ReadMemStats(&after)
		t.Logf("completed rows=%d returned=%d allocated=%d latch_observed=%s response=%s",
			rows, count, after.TotalAlloc-before.TotalAlloc, hold, result.response)
	}
}

func finishCompletedScan(ctx context.Context, pool *sql.DB, rows, count int) error {
	first := rows - count + 1
	const scan = "select id, qty from " + scanTable + " where external_ref >= ? and status = 'reserved'"
	result, err := pool.QueryContext(ctx, scan, first)
	if err != nil {
		return err
	}
	defer result.Close()
	var sum, found int64
	for result.Next() {
		var id, qty int64
		if err := result.Scan(&id, &qty); err != nil {
			return err
		}
		if id < int64(first) || id > int64(rows) || qty < 10 {
			return fmt.Errorf("unexpected result row %d, %d", id, qty)
		}
		sum += id
		found++
	}
	if err := result.Err(); err != nil {
		return err
	}
	if want := (int64(first) + int64(rows)) * int64(count) / 2; found != int64(count) || sum != want {
		return fmt.Errorf("returned %d rows with ID sum %d, want %d and %d", found, sum, count, want)
	}
	return nil
}
