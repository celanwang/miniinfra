package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadTransactionDoesNotEnterManagerOnFinish(t *testing.T) {
	db := openInventory(t, t.TempDir())
	seed(t, db)
	for _, ending := range []string{"commit", "rollback"} {
		t.Run(ending, func(t *testing.T) {
			tx, view := start(t, db)
			wantStock(t, view, 0, Int(1), 1)
			view.Close()
			if tx.held != nil || tx.done != nil {
				t.Fatal("a read-only transaction initialized lock ownership")
			}
			db.locks.mu.Lock()
			result := make(chan error, 1)
			go func() {
				if ending == "commit" {
					result <- tx.Commit()
				} else {
					tx.Rollback()
					result <- nil
				}
			}()
			select {
			case err := <-result:
				db.locks.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				db.locks.mu.Unlock()
				<-result
				t.Fatal("finishing a read transaction took the lock-manager mutex")
			}
		})
	}
}

func TestFirstLockWaitClearsEdgeWithoutOwnership(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
	}{
		{"cancel", context.Canceled},
		{"timeout", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openInventory(t, t.TempDir())
			db.lockTimeout = 200 * time.Millisecond
			owner, view := start(t, db)
			view.Close()
			waiter, view := start(t, db)
			view.Close()
			r := resource{table: "orders", key: "order-1"}
			if err := owner.acquire(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- waiter.acquire(ctx, r) }()
			waitForLock(t, db, waiter)
			db.locks.mu.Lock()
			owns := waiter.held != nil || waiter.done != nil
			db.locks.mu.Unlock()
			if tc.want == context.Canceled {
				cancel()
			}
			if err := <-result; !errors.Is(err, ErrLockWait) || !errors.Is(err, tc.want) {
				t.Fatalf("first acquire returned %v, want %v", err, tc.want)
			}
			if owns || waiter.done != nil || waiter.held != nil || !waiter.finished {
				t.Fatal("failed first acquire initialized ownership or left a transaction active")
			}
			db.locks.mu.Lock()
			_, waiting := db.locks.waiting[waiter]
			stillOwned := db.locks.holders[r] == owner && owner.done != nil
			db.locks.mu.Unlock()
			if waiting || !stillOwned {
				t.Fatal("failed acquisition lost the owner or left a waiting edge")
			}
		})
	}
}
