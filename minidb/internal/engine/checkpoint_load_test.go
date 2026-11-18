package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

const (
	checkpointWriters = 4
	checkpointWrites  = 40
)

func TestTCPCheckpointLoad(t *testing.T) {
	setting := os.Getenv("MINIDB_CHECKPOINT_ROWS")
	if setting == "" {
		t.Skip("set MINIDB_CHECKPOINT_ROWS, for example 12000 or 120000")
	}
	rows, err := strconv.Atoi(setting)
	if err != nil || rows < checkpointWriters {
		t.Fatalf("invalid MINIDB_CHECKPOINT_ROWS %q", setting)
	}
	for _, mode := range []string{"control", "checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			runCheckpointLoad(t, rows, mode == "checkpoint")
		})
	}
}

type checkpointWrite struct {
	sqlFinished time.Time
	sql, commit time.Duration
}

type checkpointWriterResult struct {
	transactions []checkpointWrite
	err          error
}

func runCheckpointLoad(t *testing.T, rows int, checkpoint bool) {
	t.Helper()
	dir := t.TempDir()
	store, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	drain := store.ObserveCheckpointLoad()
	_, pool, closeStore := serveTCP(t, store)
	seedCheckpointRows(t, pool, rows)
	drain()

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	warm := make(chan struct{}, checkpointWriters)
	done := make(chan checkpointWriterResult, checkpointWriters)
	for sku := 1; sku <= checkpointWriters; sku++ {
		go func() { done <- writeDuringCheckpoint(ctx, pool, sku, warm) }()
	}
	for range checkpointWriters {
		select {
		case <-warm:
		case result := <-done:
			t.Fatalf("writer ended during warmup: %v", result.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	marker := time.Now()
	var checkpointReply time.Duration
	if checkpoint {
		if _, err := pool.ExecContext(ctx, "checkpoint"); err != nil {
			t.Fatal(err)
		}
		checkpointReply = time.Since(marker)
	}

	var transactions []checkpointWrite
	for range checkpointWriters {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
			transactions = append(transactions, result.transactions...)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	load := drain()
	if len(transactions) != checkpointWriters*checkpointWrites || len(load.Commits) != len(transactions) {
		t.Fatalf("transactions=%d timed commits=%d", len(transactions), len(load.Commits))
	}
	if checkpoint != (len(load.Checkpoints) == 1) || len(load.Checkpoints) > 1 {
		t.Fatalf("checkpoint samples=%d", len(load.Checkpoints))
	}
	reportCheckpointLoad(t, load, transactions, marker, checkpointReply)

	closeStore()
	walBytes := checkpointFileSize(t, dir, "minidb.wal")
	pageBytes := checkpointFileSize(t, dir, "minidb.pages")
	started := time.Now()
	_, recovered, _ := openTCP(t, dir)
	ready := time.Since(started)
	for i := 1; i <= checkpointWriters; i++ {
		if qty := readQty(t, recovered, "select qty from checkpoint_rows where id = ?", i); qty != checkpointWrites {
			t.Fatalf("worker %d recovered %d writes", i, qty)
		}
	}
	if qty := readQty(t, recovered, "select qty from checkpoint_rows where ref = ?", fmt.Sprintf("req-%d", rows)); qty != 0 {
		t.Fatalf("unmodified order has quantity %d", qty)
	}
	t.Logf("reopen rows=%d pages_bytes=%d wal_bytes=%d listener_ready=%s", rows, pageBytes, walBytes, ready)
}

func seedCheckpointRows(t *testing.T, pool *sql.DB, rows int) {
	t.Helper()
	execSQL(t, pool, "create table checkpoint_rows (id int primary key, ref text unique, qty int, note text)")
	for first := 1; first <= rows; first += 1500 {
		var stmt strings.Builder
		stmt.WriteString("insert into checkpoint_rows values ")
		for id := first; id <= min(rows, first+1499); id++ {
			if id != first {
				stmt.WriteByte(',')
			}
			fmt.Fprintf(&stmt, "(%d,'req-%d',0,'confirmed by supplier and packed for shipment from main warehouse')", id, id)
		}
		execSQL(t, pool, stmt.String())
	}
}

func writeDuringCheckpoint(ctx context.Context, pool *sql.DB, id int, warm chan<- struct{}) checkpointWriterResult {
	var result checkpointWriterResult
	for i := range checkpointWrites {
		tx, err := pool.BeginTx(ctx, nil)
		if err != nil {
			result.err = err
			return result
		}
		started := time.Now()
		executed, err := tx.ExecContext(ctx, "update checkpoint_rows set qty = qty + 1 where id = ?", id)
		finished := time.Now()
		if err == nil {
			var affected int64
			affected, err = executed.RowsAffected()
			if err == nil && affected != 1 {
				err = fmt.Errorf("update %d affected %d rows", id, affected)
			}
		}
		if err != nil {
			tx.Rollback()
			result.err = err
			return result
		}
		if err := tx.Commit(); err != nil {
			result.err = err
			return result
		}
		result.transactions = append(result.transactions, checkpointWrite{finished, finished.Sub(started), time.Since(finished)})
		if i == 3 {
			warm <- struct{}{}
		}
	}
	return result
}

func reportCheckpointLoad(t *testing.T, load engine.CheckpointLoadObservation, transactions []checkpointWrite, marker time.Time, reply time.Duration) {
	t.Helper()
	var sqlTimes, commits, queues, writes, syncs []time.Duration
	for _, tx := range transactions {
		if !tx.sqlFinished.Before(marker) {
			sqlTimes, commits = append(sqlTimes, tx.sql), append(commits, tx.commit)
		}
	}
	for _, commit := range load.Commits {
		if !commit.Enqueued.Before(marker) {
			queues = append(queues, commit.Queue)
			writes = append(writes, commit.Write)
			syncs = append(syncs, commit.Sync)
		}
	}
	if len(sqlTimes) == 0 || len(queues) == 0 {
		t.Fatal("no transactions remained after the checkpoint marker")
	}
	t.Logf("foreground transactions=%d update_p99=%s commit_p99=%s queue_p99=%s write_p99=%s sync_p99=%s",
		len(queues), p99(sqlTimes), p99(commits), p99(queues), p99(writes), p99(syncs))
	for _, cp := range load.Checkpoints {
		wal := cp.WAL
		t.Logf("barrier reply=%s admit_wait=%s reclaim_wait=%s build=%s page_write=%s page_writes=%d publish=%s rotate=%s held=%s",
			reply, wal.Started.Sub(wal.Enqueued), cp.ReclaimWait, cp.Pages.Build, cp.Pages.Write, cp.Pages.Writes,
			cp.Pages.Publish, wal.Finished.Sub(wal.Rotating), wal.Finished.Sub(wal.Started))
		var delayed, updates int
		var queueMax, updateMax time.Duration
		for _, tx := range transactions {
			if !tx.sqlFinished.Before(wal.Started) && tx.sqlFinished.Before(wal.Finished) {
				updates++
				updateMax = max(updateMax, tx.sql)
			}
		}
		for _, commit := range load.Commits {
			if commit.Enqueued.Before(wal.Finished) && commit.Enqueued.Add(commit.Queue).After(wal.Started) {
				delayed++
				queueMax = max(queueMax, commit.Queue)
			}
		}
		t.Logf("overlap commits=%d queue_max=%s updates=%d update_max=%s", delayed, queueMax, updates, updateMax)
	}
}

func checkpointFileSize(t *testing.T, dir, name string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
