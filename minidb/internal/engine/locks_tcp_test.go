package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

const lockLoadUpdate = "update lock_stock set qty = qty + 1 where id = ?"

func TestTCPMultiRowLockLoad(t *testing.T) {
	if os.Getenv("MINIDB_LOCK_LOAD") == "" {
		t.Skip("set MINIDB_LOCK_LOAD=1 for the multi-row lock workload")
	}
	t.Run("disjoint", func(t *testing.T) {
		_, pool, drain := openLockLoad(t)
		const orders = 140
		start := make(chan struct{})
		done := make(chan lockLoadResult, 2)
		for worker := range 2 {
			go func() {
				<-start
				var result lockLoadResult
				for i := range orders {
					sample, err := writeLockOrder(t.Context(), pool, worker*orders+i+1, worker*2+1)
					if err != nil {
						result.err = err
						break
					}
					result.samples = append(result.samples, sample)
				}
				done <- result
			}()
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		cpuBefore := processCPU(t)
		started := time.Now()
		close(start)
		var samples []lockLoadSample
		for range 2 {
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}
			samples = append(samples, result.samples...)
		}
		wall := time.Since(started)
		cpu := processCPU(t) - cpuBefore
		runtime.ReadMemStats(&after)
		count := uint64(len(samples))
		t.Logf("workload orders=%d wall=%s cpu=%s mallocs_per_order=%.1f bytes_per_order=%.1f",
			count, wall, cpu, float64(after.Mallocs-before.Mallocs)/float64(count), float64(after.TotalAlloc-before.TotalAlloc)/float64(count))
		reportLockSQL(t, "disjoint", samples)
		reportLockWAL(t, drain(), 2*orders)
		for sku := 1; sku <= 4; sku++ {
			if qty := readQty(t, pool, "select qty from lock_stock where id = ?", sku); qty != orders {
				t.Fatalf("stock %d has quantity %d", sku, qty)
			}
		}
	})
	t.Run("hotspot", func(t *testing.T) {
		store, pool, drain := openLockLoad(t)
		const rounds = 32
		var hot, ordinary []lockLoadSample
		for i := range rounds {
			holder := beginScanWriter(t, pool)
			defer holder.Rollback()
			execSQL(t, holder, lockLoadUpdate, 1)
			done := make(chan lockLoadResult, 1)
			go func() {
				sample, err := writeLockOrder(t.Context(), pool, i*2+1, 1)
				done <- lockLoadResult{[]lockLoadSample{sample}, err}
			}()
			until(t, "hot order waiting for its first stock row", 2*time.Second, func() bool {
				return store.ObserveReclamation().Waiters == 1
			})
			sample, err := writeLockOrder(t.Context(), pool, i*2+2, 3)
			if err != nil {
				t.Fatal(err)
			}
			ordinary = append(ordinary, sample)
			select {
			case result := <-done:
				t.Fatalf("hot order returned while its holder was still active: %v", result.err)
			default:
			}
			if err := holder.Commit(); err != nil {
				t.Fatal(err)
			}
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}
			hot = append(hot, result.samples...)
		}
		reportLockSQL(t, "hot", hot)
		reportLockSQL(t, "ordinary", ordinary)
		reportLockWAL(t, drain(), 3*rounds)
		for i, want := range []int64{2 * rounds, rounds, rounds, rounds} {
			if qty := readQty(t, pool, "select qty from lock_stock where id = ?", i+1); qty != want {
				t.Fatalf("stock %d has quantity %d, want %d", i+1, qty, want)
			}
		}
	})
}

func TestTCPUniqueWaitFollowsHolderOutcome(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback"} {
		t.Run(outcome, func(t *testing.T) {
			store, pool, _ := openLockLoad(t)
			holder, waiter := beginScanWriter(t, pool), beginScanWriter(t, pool)
			defer holder.Rollback()
			defer waiter.Rollback()
			execSQL(t, holder, "insert into lock_orders values (1,'same-request')")
			execSQL(t, waiter, lockLoadUpdate, 3)
			result := make(chan error, 1)
			go func() {
				_, err := waiter.ExecContext(t.Context(), "insert into lock_orders values (2,'same-request')")
				result <- err
			}()
			until(t, "unique request waiter", 2*time.Second, func() bool {
				return store.ObserveReclamation().Waiters == 1
			})
			var wantID, wantQuantity int64
			if outcome == "commit" {
				if err := holder.Commit(); err != nil {
					t.Fatal(err)
				}
				var conflict *client.Error
				if err := <-result; !errors.As(err, &conflict) || conflict.Code != client.Unique {
					t.Fatalf("waiter did not see committed unique key: %v", err)
				}
				if err := waiter.Commit(); err == nil {
					t.Fatal("waiter committed after unique conflict")
				}
				wantID, wantQuantity = 1, 0
			} else {
				if err := holder.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := <-result; err != nil {
					t.Fatal(err)
				}
				if err := waiter.Commit(); err != nil {
					t.Fatal(err)
				}
				wantID, wantQuantity = 2, 1
			}
			if id := readQty(t, pool, "select id from lock_orders where request_no = ?", "same-request"); id != wantID {
				t.Fatalf("unique key belongs to %d, want %d", id, wantID)
			}
			if qty := readQty(t, pool, "select qty from lock_stock where id = ?", 3); qty != wantQuantity {
				t.Fatalf("waiter's stock has quantity %d, want %d", qty, wantQuantity)
			}
		})
	}
}

func openLockLoad(t *testing.T) (*engine.DB, *sql.DB, func() engine.CheckpointLoadObservation) {
	t.Helper()
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	drain := store.ObserveCheckpointLoad()
	_, pool, _ := serveTCP(t, store)
	for _, stmt := range []string{
		"create table lock_stock (id int primary key, sku text unique, qty int not null)",
		"create table lock_orders (id int primary key, request_no text unique)",
		"create table lock_items (order_id int, sku_id int, primary key (order_id, sku_id), index by_order (order_id))",
		"insert into lock_stock values (1,'one',0),(2,'two',0),(3,'three',0),(4,'four',0)",
	} {
		execSQL(t, pool, stmt)
	}
	drain()
	return store, pool, drain
}

type lockLoadSample struct {
	sql, firstUpdate, commit time.Duration
}

type lockLoadResult struct {
	samples []lockLoadSample
	err     error
}

func writeLockOrder(ctx context.Context, pool *sql.DB, order, sku int) (sample lockLoadSample, err error) {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return sample, err
	}
	defer tx.Rollback()
	started := time.Now()
	if _, err := tx.ExecContext(ctx, "insert into lock_orders values (?,?)", order, fmt.Sprintf("req-%d", order)); err != nil {
		return sample, err
	}
	firstUpdate := time.Now()
	for _, id := range []int{sku, sku + 1} {
		result, err := tx.ExecContext(ctx, lockLoadUpdate, id)
		if err != nil {
			return sample, err
		}
		if id == sku {
			sample.firstUpdate = time.Since(firstUpdate)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return sample, fmt.Errorf("stock %d affected %d rows: %v", id, affected, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "insert into lock_items values (?,?),(?,?)", order, sku, order, sku+1); err != nil {
		return sample, err
	}
	commit := time.Now()
	sample.sql = commit.Sub(started)
	err = tx.Commit()
	sample.commit = time.Since(commit)
	return sample, err
}

func processCPU(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func reportLockSQL(t *testing.T, workload string, samples []lockLoadSample) {
	t.Helper()
	var sqlTimes, updateTimes, commitTimes []time.Duration
	for _, sample := range samples {
		sqlTimes = append(sqlTimes, sample.sql)
		updateTimes = append(updateTimes, sample.firstUpdate)
		commitTimes = append(commitTimes, sample.commit)
	}
	t.Logf("sql mode=%s count=%d sql_p50=%s sql_p99=%s update_p50=%s update_p99=%s commit_p50=%s commit_p99=%s",
		workload, len(samples), lockLoadMedian(sqlTimes), p99(sqlTimes), lockLoadMedian(updateTimes), p99(updateTimes),
		lockLoadMedian(commitTimes), p99(commitTimes))
}

func reportLockWAL(t *testing.T, observation engine.CheckpointLoadObservation, count int) {
	t.Helper()
	if len(observation.Commits) != count {
		t.Fatalf("WAL observed %d commits, want %d", len(observation.Commits), count)
	}
	var queues, writes, syncs []time.Duration
	for _, item := range observation.Commits {
		queues = append(queues, item.Queue)
		writes = append(writes, item.Write)
		syncs = append(syncs, item.Sync)
	}
	t.Logf("wal count=%d queue_p50=%s queue_p99=%s write_p50=%s write_p99=%s sync_p50=%s sync_p99=%s",
		count, lockLoadMedian(queues), p99(queues), lockLoadMedian(writes), p99(writes), lockLoadMedian(syncs), p99(syncs))
}

func lockLoadMedian(samples []time.Duration) time.Duration {
	slices.Sort(samples)
	return samples[len(samples)/2]
}
