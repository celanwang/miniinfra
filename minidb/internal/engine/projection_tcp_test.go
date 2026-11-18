package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTCPCanceledJoinPreservesCommittedProjection(t *testing.T) {
	store, pool, _ := openTCP(t, t.TempDir())
	const slowRows = 1200
	setupTables(t, pool, 0, slowRows)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	old := stockRead(ctx, pool, slowRows, false)
	until(t, "join's statement snapshot", time.Second, func() bool {
		return len(store.ObserveReclamation().Snapshots) == 1
	})
	execSQL(t, pool, "update stock_00 set qty = qty + 1 where id = 2")
	cancel()
	select {
	case result := <-old:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled join returned %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("join did not return after cancellation")
	}
	until(t, "canceled join releases its snapshot", time.Second, func() bool {
		return len(store.ObserveReclamation().Snapshots) == 0
	})
	const self = "select a.qty, b.qty, a.qty from stock_00 a join stock_00 b on a.id = b.id where a.id = 2"
	var first, second, repeated int
	err := pool.QueryRowContext(t.Context(), self).Scan(&first, &second, &repeated)
	if err != nil || first != 1 || second != 1 || repeated != 1 {
		t.Fatalf("new statement returned %d/%d/%d, %v", first, second, repeated, err)
	}
}
