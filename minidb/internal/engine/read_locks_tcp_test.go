package engine_test

import (
	"fmt"
	"testing"
	"time"
)

func TestTCPReadThenLockedNoopWakesFirstWaiter(t *testing.T) {
	for _, ending := range []string{"commit", "rollback"} {
		t.Run(ending, func(t *testing.T) {
			store, pool, drain := openLockLoad(t)
			holder := beginScanWriter(t, pool)
			defer holder.Rollback()
			if err := readLockQuantity(t.Context(), holder, 1, 0); err != nil {
				t.Fatal(err)
			}
			const noop = "update lock_stock set qty = qty + 1 where id = 1 and qty > 20"
			if count := execSQL(t, holder, noop); count != 0 {
				t.Fatalf("conditional update affected %d rows", count)
			}
			waiter := beginScanWriter(t, pool)
			defer waiter.Rollback()
			result := make(chan error, 1)
			go func() {
				updated, err := waiter.ExecContext(t.Context(), lockLoadUpdate, 1)
				if err == nil {
					var count int64
					count, err = updated.RowsAffected()
					if err == nil && count != 1 {
						err = fmt.Errorf("resumed update affected %d rows", count)
					}
				}
				result <- err
			}()
			until(t, "first lock waiter", 2*time.Second, func() bool {
				return store.ObserveReclamation().Waiters == 1
			})
			// Reacquiring while the waiter sleeps must keep the same signal.
			if count := execSQL(t, holder, noop); count != 0 {
				t.Fatalf("repeated conditional update affected %d rows", count)
			}
			var err error
			if ending == "commit" {
				err = holder.Commit()
			} else {
				err = holder.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("ending a no-op transaction left its first waiter blocked")
			}
			if err := waiter.Commit(); err != nil {
				t.Fatal(err)
			}
			if count := len(drain().Commits); count != 1 {
				t.Fatalf("saw %d WAL writes, want just the resumed update", count)
			}
			if err := readLockQuantity(t.Context(), pool, 1, 1); err != nil {
				t.Fatal(err)
			}
		})
	}
}
