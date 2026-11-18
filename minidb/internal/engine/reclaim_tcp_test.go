package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/server"
)

var writeTables = []string{"stock_00", "stock_01", "stock_02", "stock_03"}

func TestTCPReadCommittedReclamation(t *testing.T) {
	runTCPReclamation(t, 4, 2000, 200*time.Millisecond)
}

func TestTCPMixedReclamationLoad(t *testing.T) {
	if os.Getenv("MINIDB_RECLAIM_LOAD") == "" {
		t.Skip("set MINIDB_RECLAIM_LOAD=1 for the multi-table load experiment")
	}
	runTCPReclamation(t, 48, 2800, 2*time.Second)
}

func openTCP(t *testing.T, dir string) (*engine.DB, *sql.DB, func()) {
	t.Helper()
	store, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, socket, store) }()
	pool := client.Open(socket.Addr().String())
	pool.SetMaxOpenConns(12)
	pool.SetMaxIdleConns(12)
	var once sync.Once
	close := func() {
		once.Do(func() {
			cancel()
			if err := errors.Join(pool.Close(), <-served, store.Close()); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(close)
	return store, pool, close
}

func setupTables(t *testing.T, pool *sql.DB, coldTables, slowRows int) {
	t.Helper()
	for i := range len(writeTables) + coldTables {
		name := fmt.Sprintf("stock_%02d", i)
		execSQL(t, pool, "CREATE TABLE "+name+" (id INT PRIMARY KEY, req TEXT UNIQUE, qty INT)")
		if i < len(writeTables) {
			execSQL(t, pool, "INSERT INTO "+name+" VALUES (1,'hot',0),(2,'ordinary-a',0),(3,'ordinary-b',0)")
		} else {
			execSQL(t, pool, "INSERT INTO "+name+" VALUES (1,'cold',0)")
		}
	}
	execSQL(t, pool, "CREATE TABLE slow_rows (id INT PRIMARY KEY)")
	for from := 1; from <= slowRows; from += 1000 {
		var rows []string
		for id := from; id <= min(from+999, slowRows); id++ {
			rows = append(rows, "("+strconv.Itoa(id)+")")
		}
		execSQL(t, pool, "INSERT INTO slow_rows VALUES "+strings.Join(rows, ","))
	}
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func execSQL(t *testing.T, con sqlExecutor, stmt string, args ...any) int64 {
	t.Helper()
	result, err := con.ExecContext(t.Context(), stmt, args...)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func readQty(t *testing.T, pool *sql.DB, stmt string, key any) int64 {
	t.Helper()
	var qty int64
	if err := pool.QueryRowContext(t.Context(), stmt, key).Scan(&qty); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return qty
}

func until(t *testing.T, what string, budget time.Duration, check func() bool) {
	t.Helper()
	for deadline := time.Now().Add(budget); time.Now().Before(deadline); {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

const versionsPerTable = 2

type writerAttempt struct {
	phase   uint32
	elapsed time.Duration
}
type writerResult struct {
	samples []writerAttempt
	err     error
}
type tcpWriters struct {
	stop      chan struct{}
	phase     atomic.Uint32
	commits   [4]atomic.Int64
	rollbacks atomic.Int64
	finished  chan writerResult
}

func (load *tcpWriters) start(ctx context.Context, pool *sql.DB, id int, stop <-chan struct{}) {
	var result writerResult
	defer func() { load.finished <- result }()
	clock := time.NewTicker(10 * time.Millisecond)
	defer clock.Stop()
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		case <-clock.C:
		}
		phase, start := load.phase.Load(), time.Now()
		shouldCommit := id != 1 || i%2 == 0
		result.err = updateTables(ctx, pool, id, shouldCommit)
		if result.err != nil {
			return
		}
		if shouldCommit {
			load.commits[id].Add(1)
			if id != 1 {
				result.samples = append(result.samples, writerAttempt{phase: phase, elapsed: time.Since(start)})
			}
		} else {
			load.rollbacks.Add(1)
		}
	}
}

func updateTables(ctx context.Context, pool *sql.DB, id int, commit bool) error {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range writeTables {
		for range versionsPerTable {
			result, err := tx.ExecContext(ctx, "UPDATE "+table+" SET qty=qty+1 WHERE id=?", id)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				return fmt.Errorf("update %s affected %d rows, %v", table, n, err)
			}
		}
	}
	if commit {
		return tx.Commit()
	}
	return tx.Rollback()
}

func (load *tcpWriters) totalAttempts() int64 {
	return load.commits[1].Load() + load.commits[2].Load() + load.commits[3].Load() + load.rollbacks.Load()
}

type oldRead struct {
	first, last int64
	err         error
}

func runTCPReclamation(t *testing.T, coldTables, slowRows int, afterRead time.Duration) {
	t.Helper()
	dir := t.TempDir()
	store, pool, closeServer := openTCP(t, dir)
	setupTables(t, pool, coldTables, slowRows)
	reader, err := pool.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	if view := store.ObserveReclamation(); len(view.Snapshots) != 0 {
		t.Fatalf("idle READ COMMITTED transaction pinned %v", view.Snapshots)
	}

	const slowQuery = "SELECT h.qty,v.qty FROM stock_00 h JOIN slow_rows a ON 1=1 JOIN slow_rows b ON a.id>=? AND b.id>=? JOIN stock_03 v ON v.id=h.id WHERE h.id=1"
	read := make(chan oldRead, 1)
	started := time.Now()
	go func() {
		var answer oldRead
		answer.err = reader.QueryRowContext(t.Context(), slowQuery, slowRows, slowRows).Scan(&answer.first, &answer.last)
		read <- answer
	}()
	var pinned uint64
	until(t, "server SELECT pin", time.Second, func() bool {
		view := store.ObserveReclamation()
		if len(view.Snapshots) == 1 {
			pinned = view.Snapshots[0]
			return true
		}
		return false
	})

	ids := []int{1, 1, 1, 2, 3}
	load := tcpWriters{stop: make(chan struct{}), finished: make(chan writerResult, len(ids))}
	var stopped sync.Once
	stop := func() { stopped.Do(func() { close(load.stop) }) }
	defer stop()
	for _, id := range ids {
		go load.start(t.Context(), pool, id, load.stop)
	}
	checkpoint := make(chan error, 1)
	go func() { _, err := pool.ExecContext(t.Context(), "CHECKPOINT"); checkpoint <- err }()
	var held, released engine.VersionObservation
	var peakWaiters int
	var checkpointCrossed bool
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
hold:
	for {
		select {
		case answer := <-read:
			if answer.err != nil || answer.first != 0 || answer.last != 0 {
				t.Fatalf("SELECT crossed its initial view: %+v", answer)
			}
			break hold
		case err := <-checkpoint:
			if err != nil {
				t.Fatal(err)
			}
			checkpointCrossed = slices.Contains(store.ObserveReclamation().Snapshots, pinned)
			checkpoint = nil
		case <-tick.C:
			view := store.ObserveReclamation(writeTables...)
			peakWaiters = max(peakWaiters, view.Waiters)
			if slices.Contains(view.Snapshots, pinned) {
				if next := totalVersions(t, view); next.Heap > held.Heap {
					held = next
				}
			}
		}
	}
	if !checkpointCrossed || held.Required == 0 || peakWaiters == 0 {
		t.Fatalf("missing concurrent checkpoint, retained versions or contention: checkpoint=%v held=%+v waiters=%d", checkpointCrossed, held, peakWaiters)
	}
	pinDuration := time.Since(started)
	heldAttempts := load.totalAttempts()
	load.phase.Store(1)
	released = totalVersions(t, store.ObserveReclamation(writeTables...))
	postStart := time.Now()
	postDeadline := time.NewTimer(afterRead)
	defer postDeadline.Stop()
	var postPeak int
post:
	for {
		select {
		case <-postDeadline.C:
			break post
		case <-tick.C:
			counts := totalVersions(t, store.ObserveReclamation(writeTables...))
			postPeak = max(postPeak, counts.Eligible)
		}
	}
	postEnd := totalVersions(t, store.ObserveReclamation(writeTables...))
	postElapsed := time.Since(postStart)
	stop()

	var phases [2][]time.Duration
	for range ids {
		result := <-load.finished
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, sample := range result.samples {
			phases[sample.phase] = append(phases[sample.phase], sample.elapsed)
		}
	}
	if len(phases[0]) == 0 || len(phases[1]) == 0 {
		t.Fatalf("missing latency samples: %d/%d", len(phases[0]), len(phases[1]))
	}
	drainStart := time.Now()
	atStop := totalVersions(t, store.ObserveReclamation(writeTables...))
	until(t, "heap and indexes to converge", 90*time.Second, func() bool {
		obs := store.ObserveReclamation(writeTables...)
		counts := totalVersions(t, obs)
		return len(obs.Snapshots) == 0 && counts.Heap == 3*len(writeTables)
	})
	drain := time.Since(drainStart)

	var qty int64
	if err := reader.QueryRowContext(t.Context(), "SELECT qty FROM stock_00 WHERE id=1").Scan(&qty); err != nil || qty != load.commits[1].Load()*versionsPerTable || qty == 0 {
		t.Fatalf("the next READ COMMITTED statement did not advance: %d, %v", qty, err)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	t.Logf("cold=%d long_sql=%s lock_waiters_peak=%d committed=%d+%d+%d rolled_back=%d",
		coldTables, pinDuration, peakWaiters, load.commits[1].Load(), load.commits[2].Load(), load.commits[3].Load(), load.rollbacks.Load())
	t.Logf("pinned=%+v released=%+v sustained_end=%+v sustained_peak_eligible=%d stopped=%+v drain=%s",
		held, released, postEnd, postPeak, atStop, drain)
	t.Logf("updates/s pinned=%.0f after=%.0f ordinary_p99 pinned=%s (%d) after=%s (%d)",
		float64(heldAttempts)*versionsPerTable*float64(len(writeTables))/pinDuration.Seconds(),
		float64(load.totalAttempts()-heldAttempts)*versionsPerTable*float64(len(writeTables))/postElapsed.Seconds(),
		p99(phases[0]), len(phases[0]), p99(phases[1]), len(phases[1]))

	testLockedReplacement(t, store, pool)
	closeServer()
	_, recovered, _ := openTCP(t, dir)
	for _, table := range writeTables {
		for id, req := range []string{"hot", "ordinary-a", "ordinary-b"} {
			want := load.commits[id+1].Load() * versionsPerTable
			if table == "stock_00" && id == 1 {
				want = 4242
			}
			pk := readQty(t, recovered, "SELECT qty FROM "+table+" WHERE id=?", id+1)
			unique := readQty(t, recovered, "SELECT qty FROM "+table+" WHERE req=?", req)
			if pk != want || unique != want {
				t.Fatalf("recovered %s %s: pk=%d unique=%d want=%d", table, req, pk, unique, want)
			}
		}
	}
}

func totalVersions(t *testing.T, view engine.ReclaimObservation) engine.VersionObservation {
	t.Helper()
	var sum engine.VersionObservation
	for _, table := range writeTables {
		one, ok := view.Tables[table]
		if !ok || one.Heap != one.Primary || one.Heap != one.Secondary {
			t.Fatalf("heap and index candidates differ on %s: %+v", table, one)
		}
		sum.Heap += one.Heap
		sum.Primary += one.Primary
		sum.Secondary += one.Secondary
		sum.Current += one.Current
		sum.Private += one.Private
		sum.Required += one.Required
		sum.Deferred += one.Deferred
		sum.Eligible += one.Eligible
	}
	return sum
}

func p99(samples []time.Duration) time.Duration {
	slices.Sort(samples)
	return samples[(len(samples)*99+99)/100-1]
}

func testLockedReplacement(t *testing.T, store *engine.DB, pool *sql.DB) {
	t.Helper()
	deleteTx, err := pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer deleteTx.Rollback()
	execSQL(t, deleteTx, "DELETE FROM stock_00 WHERE id=2")
	execSQL(t, deleteTx, "INSERT INTO stock_00 VALUES (2,'ordinary-a',4242)")

	other, err := pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback()
	result := make(chan error, 1)
	go func() {
		count, err := other.ExecContext(t.Context(), "UPDATE stock_00 SET qty=qty+1 WHERE id=2")
		if err == nil {
			if n, affectedErr := count.RowsAffected(); affectedErr != nil || n != 0 {
				err = fmt.Errorf("waiter updated the replacement: %d %v", n, affectedErr)
			}
		}
		result <- err
	}()
	until(t, "writer waiting on the deleted identity", time.Second, func() bool { return store.ObserveReclamation().Waiters == 1 })
	if err := deleteTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := other.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = pool.ExecContext(t.Context(), "INSERT INTO stock_00 VALUES (9,'ordinary-a',1)")
	var conflict *client.Error
	if !errors.As(err, &conflict) || conflict.Code != client.Unique {
		t.Fatalf("expected unique conflict after replacement: %v", err)
	}
}
