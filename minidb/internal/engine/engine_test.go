package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var schema = Schema{
	Name:    "inventory",
	Columns: []Column{{Name: "sku", Kind: Integer, NotNull: true}, {Name: "label", Kind: Text, NotNull: true}, {Name: "available", Kind: Integer, NotNull: true}},
	Indexes: []Index{{Name: "primary", Unique: true, Primary: true, Columns: []int{0}}, {Name: "label", Unique: true, Columns: []int{1}}},
}

func start(t *testing.T, db *DB) (*Txn, Snapshot) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tx.Rollback)
	return tx, snapshot(t, tx)
}

func snapshot(t *testing.T, tx *Txn) Snapshot {
	t.Helper()
	view, err := tx.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(view.Close)
	return view
}

func openInventory(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insert(t *testing.T, view Snapshot, sku int64, label string, available int64) {
	t.Helper()
	if err := view.Insert(t.Context(), "inventory", []Value{Int(sku), String(label), Int(available)}); err != nil {
		t.Fatal(err)
	}
}

func commit(t *testing.T, tx *Txn) {
	t.Helper()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func seed(t *testing.T, db *DB) {
	t.Helper()
	if err := db.CreateTable(schema); err != nil {
		t.Fatal(err)
	}
	tx, view := start(t, db)
	defer view.Close()
	for i := range int64(800) {
		insert(t, view, i+1, fmt.Sprintf("sku-%d", i+1), 1)
	}
	commit(t, tx)
}

func decrease(view Snapshot, id int64) (bool, error) {
	return view.Mutate(context.Background(), "inventory", []Value{Int(id)}, func(row []Value) ([]Value, bool, error) {
		if row[2].Int == 0 {
			return row, false, nil
		}
		row[2].Int--
		return row, true, nil
	})
}

func waitForLock(t *testing.T, db *DB, waiter *Txn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		db.locks.mu.Lock()
		_, waiting := db.locks.waiting[waiter]
		db.locks.mu.Unlock()
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("transaction never entered the wait queue")
}

func wantStock(t *testing.T, view Snapshot, column int, value Value, stock int64) {
	t.Helper()
	rows, indexed, err := view.Rows("inventory", []int{column}, []Value{value})
	if err != nil || !indexed || len(rows) != 1 || rows[0][2] != Int(stock) {
		t.Fatalf("rows = %v, indexed = %v, err = %v, want stock %d", rows, indexed, err, stock)
	}
}

func TestRowWaitDoesNotBlockDisjointCommitAndRechecksStock(t *testing.T) {
	dir := t.TempDir()
	db := openInventory(t, dir)
	seed(t, db)
	reader, before := start(t, db)
	first, firstView := start(t, db)
	second, secondView := start(t, db)
	independent, independentView := start(t, db)
	if changed, err := decrease(firstView, 1); !changed || err != nil {
		t.Fatalf("first decrease: %v %v", changed, err)
	}
	result := make(chan error, 1)
	go func() {
		changed, err := decrease(secondView, 1)
		if changed {
			err = errors.New("second transaction also took the last item")
		}
		result <- err
	}()
	waitForLock(t, db, second)
	if changed, err := decrease(independentView, 766); !changed || err != nil {
		t.Fatalf("independent decrease: %v %v", changed, err)
	}
	commit(t, independent)
	between := snapshot(t, reader)
	wantStock(t, between, 0, Int(1), 1)
	wantStock(t, between, 0, Int(766), 0)
	commit(t, first)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	wantStock(t, before, 1, String("sku-1"), 1)
	wantStock(t, snapshot(t, reader), 1, String("sku-1"), 0)
	second.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openInventory(t, dir)
	_, fresh := start(t, db)
	wantStock(t, fresh, 0, Int(1), 0)
	wantStock(t, fresh, 1, String("sku-766"), 0)
}

func TestWaiterSkipsDeletedIdentityAfterSameKeyReinsert(t *testing.T) {
	db := openInventory(t, t.TempDir())
	seed(t, db)
	deleter, changing := start(t, db)
	waiter, before := start(t, db)
	if removed, err := changing.Mutate(t.Context(), "inventory", []Value{Int(1)}, func([]Value) ([]Value, bool, error) {
		return nil, true, nil
	}); !removed || err != nil {
		t.Fatalf("delete: %v %v", removed, err)
	}
	result := make(chan error, 1)
	go func() {
		changed, err := decrease(before, 1)
		if changed {
			err = errors.New("waiter modified the replacement")
		}
		result <- err
	}()
	waitForLock(t, db, waiter)
	insert(t, changing, 1, "sku-1", 9)
	commit(t, deleter)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	wantStock(t, before, 0, Int(1), 1)
	wantStock(t, snapshot(t, waiter), 1, String("sku-1"), 9)
}

func TestUniqueKeyWaitAndDeadlockVictim(t *testing.T) {
	db := openInventory(t, t.TempDir())
	seed(t, db)
	_, left := start(t, db)
	second, right := start(t, db)
	insert(t, left, 801, "request", 1)
	insert(t, right, 802, "another", 1)
	result := make(chan error, 1)
	go func() {
		result <- right.Insert(context.Background(), "inventory", []Value{Int(803), String("request"), Int(1)})
	}()
	waitForLock(t, db, second)
	err := left.Insert(t.Context(), "inventory", []Value{Int(804), String("another"), Int(1)})
	if !errors.Is(err, ErrDeadlock) {
		t.Fatalf("wanted deadlock, got %v", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	commit(t, second)
	_, fresh := start(t, db)
	rows, _, err := fresh.Rows("inventory", []int{1}, []Value{String("request")})
	if err != nil || len(rows) != 1 || rows[0][0] != Int(803) {
		t.Fatalf("aborted row still owns unique key: %v %v", rows, err)
	}
}
