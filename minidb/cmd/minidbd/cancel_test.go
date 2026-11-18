package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
)

const reserveStock = "update inventory set available = available - ? where sku = ? and available >= ?"

func TestCanceledOrderReleasesLocks(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	execute(t, db, "insert into inventory values (3, 30)")

	// A keeps SKU 1 locked until C has completed. B has already written an
	// order, a detail and SKU 2 when it tries to acquire the lock from A.
	a := begin(t, db)
	execute(t, a, reserveStock, 1, 1, 1)
	bPool := client.Open(addr)
	defer bPool.Close()
	bPool.SetMaxOpenConns(1)
	b := begin(t, bPool)
	execute(t, b, "insert into orders values (42, 'r42', '1:2,2:3')")
	execute(t, b, reserveStock, 3, 2, 3)
	execute(t, b, "insert into items values (42, 2, 3)")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	bDone := make(chan error, 1)
	go func() {
		_, err := b.ExecContext(ctx, reserveStock, 2, 1, 2)
		bDone <- err
	}()
	select {
	case err := <-bDone:
		t.Fatalf("B finished while A held SKU 1: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	c := begin(t, db)
	cDone := make(chan error, 1)
	cCtx, stopC := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopC()
	go func() {
		result, err := c.ExecContext(cCtx, reserveStock, 1, 2, 1)
		if err == nil {
			var changed int64
			changed, err = result.RowsAffected()
			if err == nil && changed != 1 {
				err = errors.New("competing update did not change one row")
			}
		}
		cDone <- err
	}()

	select {
	case err := <-bDone:
		t.Fatalf("B finished while A held SKU 1: %v", err)
	case err := <-cDone:
		t.Fatalf("C finished while B held SKU 2: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	ordinaryStarted := time.Now()
	ordinaryCtx, stopOrdinary := context.WithTimeout(t.Context(), time.Second)
	defer stopOrdinary()
	if _, err := db.ExecContext(ordinaryCtx, reserveStock, 1, 3, 1); err != nil {
		t.Fatalf("an unrelated durable update was blocked: %v", err)
	}
	ordinary := time.Since(ordinaryStarted)
	started := time.Now()
	cancel()
	var bErr error
	select {
	case bErr = <-bDone:
	case <-time.After(time.Second):
		t.Fatal("the canceled client did not return")
	}
	clientReturned := time.Since(started)
	if !errors.Is(bErr, context.Canceled) {
		t.Fatalf("expected cancellation for a statement before COMMIT: %v", bErr)
	}
	select {
	case err := <-cDone:
		if err != nil {
			t.Fatalf("competing update: %v", err)
		}
	case <-cCtx.Done():
		t.Fatal("B did not release SKU 2")
	}
	released := time.Since(started)
	t.Logf("cancel_to_client=%s cancel_to_competing_update=%s unrelated_durable_update=%s canceled_error=%v",
		clientReturned, released, ordinary, bErr)
	if released > time.Second {
		t.Fatal("disconnect did not release locks before the server lock timeout")
	}

	if err := c.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err == nil {
		t.Fatal("committed on a disconnected transaction")
	}
	if err := a.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertOrderAbsent(t, db)

	// The same pool must acquire a new session. Reusing the original key
	// commits exactly once, and the outcome survives recovery from the WAL.
	if err := reserveOrder(t, bPool).Commit(); err != nil {
		t.Fatal(err)
	}
	t.Logf("cancel_to_retried_commit=%s", time.Since(started))
	assertOrderRecovered(t, db)
	kill()
	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	assertOrderRecovered(t, recovered)
	var available int64
	if err := recovered.QueryRowContext(t.Context(), "select available from inventory where sku = 3").Scan(&available); err != nil || available != 29 {
		t.Fatalf("lost the unrelated commit: %d, %v", available, err)
	}
}

func TestCanceledBeforeSendPreservesTransaction(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	tx := begin(t, db)
	execute(t, tx, "insert into orders values (42, 'r42', '1:2,2:3')")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := tx.ExecContext(ctx, reserveStock, 2, 1, 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled update: %v", err)
	}
	var id, available int64
	if err := tx.QueryRowContext(t.Context(), "select id from orders where request_id = 'r42'").Scan(&id); err != nil || id != 42 {
		t.Fatalf("original statement was lost: %d, %v", id, err)
	}
	if err := tx.QueryRowContext(t.Context(), "select available from inventory where sku = 1").Scan(&available); err != nil || available != 10 {
		t.Fatalf("canceled statement reached the server: %d, %v", available, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertOrderAbsent(t, db)
}

func TestDeadlineWhileWaitingForLock(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	a := begin(t, db)
	execute(t, a, reserveStock, 1, 1, 1)
	b := begin(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := b.ExecContext(ctx, reserveStock, 1, 1, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the application's deadline error: %v", err)
	}
}

func TestUnknownCommitCanRecoverWithoutOrder(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)

	proxy, interrupted := interruptRequest(t, addr, requestFault{statement: "COMMIT", beforeForward: true})
	connection := client.Open(proxy)
	defer connection.Close()
	tx := reserveOrder(t, connection)
	assertUnknownCommit(t, tx.Commit())
	awaitInterruption(t, interrupted)
	kill()

	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	assertOrderAbsent(t, recovered)
	if err := reserveOrder(t, recovered).Commit(); err != nil {
		t.Fatal(err)
	}
	assertOrderRecovered(t, recovered)
}

func TestCanceledAutocommitAfterPublicationIsUnknown(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)

	published, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	proxy, interrupted := interruptRequest(t, addr, requestFault{
		statement: reserveStock,
		afterSuccess: func() {
			close(published)
			<-release
		},
	})
	connection := client.Open(proxy)
	defer connection.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := connection.ExecContext(ctx, reserveStock, 1, 1, 1)
		done <- err
	}()
	select {
	case <-published:
	case err := <-interrupted:
		t.Fatalf("interrupted before server confirmed autocommit: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not confirm autocommit")
	}
	cancel()
	select {
	case err := <-done:
		assertUnknownCommit(t, err)
	case <-time.After(time.Second):
		t.Fatal("client ignored cancellation")
	}
	var available int64
	if err := db.QueryRowContext(t.Context(), "select available from inventory where sku = 1").Scan(&available); err != nil || available != 9 {
		t.Fatalf("the durable update should have run once: %d, %v", available, err)
	}
}

func TestAutocommitLostReplyDoesNotRetry(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)

	proxy, interrupted := interruptRequest(t, addr, requestFault{statement: reserveStock})
	connection := client.Open(proxy)
	defer connection.Close()
	_, err := connection.ExecContext(t.Context(), reserveStock, 1, 1, 1)
	assertUnknownCommit(t, err)
	awaitInterruption(t, interrupted)
}

func assertOrderAbsent(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		"select id from orders where id = 42",
		"select id from orders where request_id = 'r42'",
		"select sku from items where order_id = 42",
	} {
		var id int64
		if err := db.QueryRowContext(t.Context(), stmt).Scan(&id); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s gave %d, %v", stmt, id, err)
		}
	}
	for sku, want := range map[int]int64{1: 10, 2: 20} {
		var available int64
		err := db.QueryRowContext(t.Context(), "select available from inventory where sku = ?", sku).Scan(&available)
		if err != nil || available != want {
			t.Fatalf("inventory %d: available %d want %d: %v", sku, available, want, err)
		}
	}
}
