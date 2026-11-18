package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

const scanTable = "scan_orders"

func TestTCPSelectiveScanCancellation(t *testing.T) {
	setting := os.Getenv("MINIDB_SCAN_LOAD")
	if setting == "" {
		t.Skip("set MINIDB_SCAN_LOAD to the number of rows (for example 120000)")
	}
	rows, err := strconv.Atoi(setting)
	if err != nil || rows < 2 {
		t.Fatalf("invalid MINIDB_SCAN_LOAD %q", setting)
	}
	store, pool, _ := openTCP(t, t.TempDir())
	store.PauseReclamationForScanProbe()
	pool.SetMaxOpenConns(2)
	seedScanOrders(t, pool, rows)
	for i := range 5 {
		for _, indexed := range []bool{i%2 == 0, i%2 != 0} {
			checkSelectiveScan(t, pool, rows, indexed)
		}
	}
	for _, columns := range [][]int{nil, {2}} {
		sample, err := store.MeasureScanMaterialization(scanTable, columns, []engine.Value{engine.Int(int64(rows))})
		if err != nil || sample.Indexed != (len(columns) != 0) {
			t.Fatalf("materialization: %+v, %v", sample, err)
		}
		t.Logf("engine rows=%d indexed=%t materialized=%d wall=%s latch_wait=%s allocated=%d",
			rows, sample.Indexed, sample.Rows, sample.Wall, sample.Wait, sample.Bytes)
	}
	for range 5 {
		writer := beginScanWriter(t, pool)
		statement, commit := finishScanWriter(t, writer)
		t.Logf("idle rows=%d writer_statement=%s writer_commit=%s", rows, statement, commit)
		measureCanceledScan(t, store, pool, rows)
	}
}

func seedScanOrders(t *testing.T, pool *sql.DB, rows int) {
	t.Helper()
	execSQL(t, pool, "create table "+scanTable+" (id int primary key, external_ref int, indexed_ref int, qty int, status text, region text, index by_ref (indexed_ref))")
	for first := 1; first <= rows; first += 2000 {
		var stmt strings.Builder
		stmt.WriteString("insert into " + scanTable + " values ")
		for id := first; id <= min(rows, first+1999); id++ {
			if id != first {
				stmt.WriteByte(',')
			}
			fmt.Fprintf(&stmt, "(%d,%d,%d,10,'reserved','east')", id, id, id)
		}
		execSQL(t, pool, stmt.String())
	}
}

func selectiveScan(indexed bool) string {
	column := "external_ref"
	if indexed {
		column = "indexed_ref"
	}
	return "select id, qty from " + scanTable + " where " + column + " = ?"
}

func checkSelectiveScan(t *testing.T, pool *sql.DB, rows int, indexed bool) {
	t.Helper()
	started := time.Now()
	result, err := pool.QueryContext(t.Context(), selectiveScan(indexed), rows)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	var id, qty int
	if !result.Next() || result.Scan(&id, &qty) != nil || id != rows || qty != 10 || result.Next() || result.Err() != nil {
		t.Fatalf("unexpected scan result: id=%d qty=%d err=%v", id, qty, result.Err())
	}
	t.Logf("tcp rows=%d indexed=%t response=%s", rows, indexed, time.Since(started))
}

func beginScanWriter(t *testing.T, pool *sql.DB) *sql.Tx {
	t.Helper()
	writer, err := pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func finishScanWriter(t *testing.T, writer *sql.Tx) (statement, commit time.Duration) {
	t.Helper()
	defer writer.Rollback()
	started := time.Now()
	if affected := execSQL(t, writer, "update "+scanTable+" set qty = qty + 1 where id = 1"); affected != 1 {
		t.Fatalf("same-table writer affected %d rows", affected)
	}
	statement = time.Since(started)
	started = time.Now()
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	return statement, time.Since(started)
}

func measureCanceledScan(t *testing.T, store *engine.DB, pool *sql.DB, rows int) {
	t.Helper()
	writer := beginScanWriter(t, pool)
	defer writer.Rollback()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	query := make(chan error, 1)
	go func() {
		result, err := pool.QueryContext(ctx, selectiveScan(false), rows)
		if err == nil {
			result.Close()
		}
		query <- err
	}()

	var released func() bool
	for deadline := time.Now().Add(3 * time.Second); released == nil; {
		select {
		case err := <-query:
			t.Fatalf("scan finished before the latch probe observed it: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("scan did not acquire the latch")
		}
		released = store.PinnedScan(scanTable)
		runtime.Gosched()
	}
	started := time.Now()
	cancel()
	if err := <-query; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan: %v", err)
	}
	clientReturned := time.Since(started)
	viewDropped := make(chan time.Duration, 1)
	go func() {
		for !released() && time.Since(started) < 3*time.Second {
			time.Sleep(50 * time.Microsecond)
		}
		viewDropped <- time.Since(started)
	}()

	written, committed := finishScanWriter(t, writer)
	snapshot := <-viewDropped
	if !released() {
		t.Fatal("canceled scan retained its snapshot")
	}
	t.Logf("cancel rows=%d client=%s snapshot=%s writer_statement=%s writer_commit=%s",
		rows, clientReturned, snapshot, written, committed)
	checkSelectiveScan(t, pool, rows, true)
}
