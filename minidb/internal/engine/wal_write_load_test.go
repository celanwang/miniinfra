package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestTCPWALWriteLoad(t *testing.T) {
	raw := os.Getenv("MINIDB_WAL_ORDERS")
	if raw == "" {
		t.Skip("set MINIDB_WAL_ORDERS to run the synchronous WAL workload")
	}
	orders, err := strconv.Atoi(raw)
	if err != nil || orders <= 0 {
		t.Fatalf("invalid MINIDB_WAL_ORDERS %q", raw)
	}
	t.Run("small", func(t *testing.T) { runWALWriteLoad(t, orders, false) })
	t.Run("mixed", func(t *testing.T) { runWALWriteLoad(t, orders, true) })
}

func runWALWriteLoad(t *testing.T, orders int, mixed bool) {
	t.Helper()
	const writers = 4
	dir := t.TempDir()
	store, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	drain := store.ObserveCheckpointLoad()
	_, pool, stop := serveTCP(t, store)
	pool.SetMaxOpenConns(writers)
	for _, stmt := range []string{
		"create table wal_orders (id int primary key, request_no text unique, note text)",
		"create table wal_stock (id int primary key, qty int not null)",
		"create table wal_items (order_id int primary key, sku_id int not null)",
		"insert into wal_stock values (1,0),(2,0),(3,0),(4,0)",
	} {
		execSQL(t, pool, stmt)
	}
	drain()
	small, large := strings.Repeat("s", 64), strings.Repeat("L", 80<<10)
	start := make(chan struct{})
	type result struct {
		ordinary, oversized []walOrderTiming
		err                 error
	}
	results := make(chan result, writers)
	for worker := range writers {
		go func() {
			var result result
			<-start
			for i := range orders {
				note := small
				if mixed && (i+13*worker)%64 == 0 {
					note = large
				}
				sample, err := writeWALOrder(t.Context(), pool, worker*orders+i+1, worker+1, note)
				if err != nil {
					result.err = err
					break
				}
				if note == large {
					result.oversized = append(result.oversized, sample)
				} else {
					result.ordinary = append(result.ordinary, sample)
				}
			}
			results <- result
		}()
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	close(start)
	var ordinary, oversized []walOrderTiming
	var failures []error
	for range writers {
		result := <-results
		if result.err != nil {
			failures = append(failures, result.err)
		}
		ordinary = append(ordinary, result.ordinary...)
		oversized = append(oversized, result.oversized...)
	}
	if len(failures) != 0 {
		t.Fatal(failures)
	}
	wall := time.Since(started)
	runtime.ReadMemStats(&after)
	count := float64(writers * orders)
	t.Logf("workload count=%.0f wall=%s commits_per_second=%.1f allocs_per_commit=%.2f bytes_per_commit=%.2f",
		count, wall, count/wall.Seconds(), float64(after.Mallocs-before.Mallocs)/count, float64(after.TotalAlloc-before.TotalAlloc)/count)
	reportWALOrderTiming(t, "small", ordinary)
	reportWALOrderTiming(t, "oversized", oversized)
	reportLockWAL(t, drain(), writers*orders)

	execSQL(t, pool, "checkpoint")
	note := small
	if mixed {
		note = large
	}
	tailID := writers*orders + 1
	if _, err := writeWALOrder(t.Context(), pool, tailID, 1, note); err != nil {
		t.Fatal(err)
	}
	if mixed && checkpointFileSize(t, dir, "minidb.wal") <= 64<<10 {
		t.Fatal("oversized transaction did not produce an oversized WAL frame")
	}
	stop()
	_, recovered, _ := openTCP(t, dir)
	for sku := 1; sku <= writers; sku++ {
		want := int64(orders)
		if sku == 1 {
			want++
		}
		if qty := readQty(t, recovered, "select qty from wal_stock where id = ?", sku); qty != want {
			t.Fatalf("stock %d recovered quantity %d, want %d", sku, qty, want)
		}
	}
	for _, id := range []int{1, tailID} {
		var got string
		if err := recovered.QueryRowContext(t.Context(), "select note from wal_orders where id = ?", id).Scan(&got); err != nil || got != note {
			t.Fatalf("primary key %d recovered note of length %d: %v", id, len(got), err)
		}
		request := "req-" + strconv.Itoa(id)
		if got := readQty(t, recovered, "select id from wal_orders where request_no = ?", request); got != int64(id) {
			t.Fatalf("request %s recovered order %d", request, got)
		}
		const joined = "select i.sku_id from wal_orders o join wal_items i on i.order_id = o.id where o.request_no = ?"
		if sku := readQty(t, recovered, joined, request); sku != 1 {
			t.Fatalf("request %s recovered item SKU %d", request, sku)
		}
	}
}

type walOrderTiming struct {
	sql, commit time.Duration
}

func writeWALOrder(ctx context.Context, pool *sql.DB, id, sku int, note string) (walOrderTiming, error) {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return walOrderTiming{}, err
	}
	defer tx.Rollback()
	started := time.Now()
	if _, err := tx.ExecContext(ctx, "insert into wal_orders values (?,?,?)", id, "req-"+strconv.Itoa(id), note); err != nil {
		return walOrderTiming{}, err
	}
	result, err := tx.ExecContext(ctx, "update wal_stock set qty = qty + 1 where id = ?", sku)
	if err != nil {
		return walOrderTiming{}, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return walOrderTiming{}, fmt.Errorf("stock %d affected %d rows: %v", sku, n, err)
	}
	if _, err := tx.ExecContext(ctx, "insert into wal_items values (?,?)", id, sku); err != nil {
		return walOrderTiming{}, err
	}
	commit := time.Now()
	err = tx.Commit()
	return walOrderTiming{commit.Sub(started), time.Since(commit)}, err
}

func reportWALOrderTiming(t *testing.T, size string, samples []walOrderTiming) {
	t.Helper()
	if len(samples) == 0 {
		return
	}
	var sqlTimes, commits []time.Duration
	for _, sample := range samples {
		sqlTimes = append(sqlTimes, sample.sql)
		commits = append(commits, sample.commit)
	}
	t.Logf("tcp size=%s count=%d sql_p50=%s sql_p99=%s commit_p50=%s commit_p99=%s",
		size, len(samples), lockLoadMedian(sqlTimes), p99(sqlTimes), lockLoadMedian(commits), p99(commits))
}
