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
)

func TestTCPReadLockStateLoad(t *testing.T) {
	raw := os.Getenv("MINIDB_READ_LOCK_ROUNDS")
	if raw == "" {
		t.Skip("set MINIDB_READ_LOCK_ROUNDS for the read transaction workload")
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds < 4 || rounds%4 != 0 {
		t.Fatalf("MINIDB_READ_LOCK_ROUNDS must be a positive multiple of four, got %q", raw)
	}
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			_, pool, drain := openLockLoad(t)
			if err := readLockTransaction(t.Context(), pool, explicit); err != nil {
				t.Fatal(err)
			}
			times := make([]time.Duration, rounds)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := range rounds {
				started := time.Now()
				if err := readLockTransaction(t.Context(), pool, explicit); err != nil {
					t.Fatal(err)
				}
				times[i] = time.Since(started)
			}
			runtime.ReadMemStats(&after)
			reportReadLockAllocations(t, "per_read", rounds, before, after)
			reportReadLockTimes(t, times)
			if count := len(drain().Commits); count != 0 {
				t.Fatalf("read transactions submitted %d WAL records", count)
			}
		})
	}
	t.Run("mixed", func(t *testing.T) {
		_, pool, drain := openLockLoad(t)
		pool.SetMaxOpenConns(2)
		epochs := rounds / 4
		start := make(chan struct{})
		tokens := make(chan struct{}, 1)
		done := make(chan readLockLoadResult, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			defer close(tokens)
			<-start
			result := readLockLoadResult{writes: make([]lockLoadSample, 0, epochs)}
			for i := range epochs {
				sample, err := writeAfterPointRead(ctx, pool, i, tokens)
				if err != nil {
					result.err = err
					break
				}
				result.writes = append(result.writes, sample)
			}
			done <- result
		}()
		reads := make([]time.Duration, 0, rounds)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		close(start)
		for range tokens {
			for range 4 {
				started := time.Now()
				if err := readLockQuantity(ctx, pool, 2, 0); err != nil {
					t.Fatal(err)
				}
				reads = append(reads, time.Since(started))
			}
		}
		result := <-done
		if result.err != nil || len(reads) != rounds || len(result.writes) != epochs {
			t.Fatalf("mixed load: reads=%d writes=%d err=%v", len(reads), len(result.writes), result.err)
		}
		runtime.ReadMemStats(&after)
		reportReadLockAllocations(t, "per_four_reads_and_write", epochs, before, after)
		reportReadLockTimes(t, reads)
		reportLockSQL(t, "read_before_write", result.writes)
		reportLockWAL(t, drain(), epochs)
		if err := readLockQuantity(t.Context(), pool, 1, int64(epochs)); err != nil {
			t.Fatal(err)
		}
	})
}

type readLockLoadResult struct {
	writes []lockLoadSample
	err    error
}

func readLockTransaction(ctx context.Context, pool *sql.DB, explicit bool) error {
	if !explicit {
		return readLockQuantity(ctx, pool, 1, 0)
	}
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := readLockQuantity(ctx, tx, 1, 0); err != nil {
		return err
	}
	return tx.Commit()
}

func readLockQuantity(ctx context.Context, reader orderReader, id int, want int64) error {
	var qty int64
	err := reader.QueryRowContext(ctx, "select qty from lock_stock where id = ?", id).Scan(&qty)
	if err != nil || qty != want {
		return fmt.Errorf("stock %d returned %d, %v, want %d", id, qty, err, want)
	}
	return nil
}

func writeAfterPointRead(ctx context.Context, pool *sql.DB, i int, tokens chan<- struct{}) (lockLoadSample, error) {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return lockLoadSample{}, err
	}
	defer tx.Rollback()
	started := time.Now()
	if err := readLockQuantity(ctx, tx, 1, int64(i)); err != nil {
		return lockLoadSample{}, err
	}
	updating := time.Now()
	executed, err := tx.ExecContext(ctx, lockLoadUpdate, 1)
	if err != nil {
		return lockLoadSample{}, err
	}
	if changes, err := executed.RowsAffected(); err != nil || changes != 1 {
		return lockLoadSample{}, fmt.Errorf("update affected %d rows: %v", changes, err)
	}
	sample := lockLoadSample{sql: time.Since(started), firstUpdate: time.Since(updating)}
	select {
	case <-ctx.Done():
		return sample, ctx.Err()
	case tokens <- struct{}{}:
	}
	committing := time.Now()
	err = tx.Commit()
	sample.commit = time.Since(committing)
	return sample, err
}

func reportReadLockAllocations(t *testing.T, unit string, count int, before, after runtime.MemStats) {
	t.Helper()
	t.Logf("alloc unit=%s count=%d mallocs=%.2f bytes=%.2f", unit, count,
		float64(after.Mallocs-before.Mallocs)/float64(count), float64(after.TotalAlloc-before.TotalAlloc)/float64(count))
}

func reportReadLockTimes(t *testing.T, times []time.Duration) {
	t.Helper()
	t.Logf("read count=%d p50=%s p99=%s", len(times), lockLoadMedian(times), p99(times))
}
