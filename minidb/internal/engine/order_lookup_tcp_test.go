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

const (
	orderLookup = "select id, amount from orders where tenant_id = ? and request_no = ?"
	orderUpdate = "update orders set amount = amount + 1 where id = ?"
)

func TestTCPOrderLookupIndexOrder(t *testing.T) {
	setting := os.Getenv("MINIDB_ORDER_LOOKUP_ROWS")
	if setting == "" {
		t.Skip("set MINIDB_ORDER_LOOKUP_ROWS, for example 120000")
	}
	rows, err := strconv.Atoi(setting)
	if err != nil || rows < 2048 {
		t.Fatalf("invalid MINIDB_ORDER_LOOKUP_ROWS %q, need at least 2048", setting)
	}
	for _, name := range []string{"tenant_first", "unique_first"} {
		t.Run(name, func(t *testing.T) {
			store, pool, _ := openTCP(t, t.TempDir())
			store.PauseReclamationForScanProbe()
			pool.SetMaxOpenConns(2)
			createLookupOrders(t, pool, name == "unique_first")
			target := rows / 2
			seedLookupOrders(t, pool, rows, target)
			for _, key := range []struct {
				index string
				value int
				want  int
			}{
				{"by_tenant", 1, rows + 4096},
				{"request_no_key", target, 9},
				{"request_no_key", rows + 2, 0},
			} {
				n, ok := store.IndexCandidates("orders", key.index, []engine.Value{engine.Int(int64(key.value))})
				if !ok || n != key.want {
					t.Fatalf("index %s key %d count=%d want=%d", key.index, key.value, n, key.want)
				}
				t.Logf("index=%s key=%d candidates=%d", key.index, key.value, n)
			}
			for trial := range 24 {
				keys := [2]int{target, rows + 2}
				if trial%2 != 0 {
					keys[0], keys[1] = keys[1], keys[0]
				}
				for _, key := range keys {
					wantID := 0
					if key == target {
						wantID = target
					}
					started := time.Now()
					if err := checkOrderLookup(t.Context(), pool, 1, key, wantID, 18); err != nil {
						t.Fatal(err)
					}
					t.Logf("point hit=%t response=%s", key == target, time.Since(started))
				}
				measureLookupAndWriter(t, store, pool, target, rows+1)
			}
		})
	}
}

func TestTCPOrderLookupPaths(t *testing.T) {
	store, pool, _ := openTCP(t, t.TempDir())
	store.PauseReclamationForScanProbe()
	createLookupOrders(t, pool, false)
	execSQL(t, pool, "insert into orders values (1,1,40,10),(2,1,41,20),(3,2,42,30),(4,1,null,40),(5,1,null,50)")
	check := func(reader orderReader, tenant int, request any, wantID, wantAmount int) {
		t.Helper()
		if err := checkOrderLookup(t.Context(), reader, tenant, request, wantID, wantAmount); err != nil {
			t.Fatal(err)
		}
	}
	check(pool, 1, 40, 1, 10)
	check(pool, 1, 99, 0, 0)
	check(pool, 2, 40, 0, 0)
	check(pool, 1, nil, 0, 0)
	if amount := readQty(t, pool, "select amount from orders where tenant_id = ?", 2); amount != 30 {
		t.Fatalf("ordinary index returned %d", amount)
	}

	tx := beginScanWriter(t, pool)
	defer tx.Rollback()
	execSQL(t, tx, orderUpdate, 1)
	check(pool, 1, 40, 1, 10)
	check(tx, 1, 40, 1, 11)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check(pool, 1, 40, 1, 11)

	tx = beginScanWriter(t, pool)
	defer tx.Rollback()
	execSQL(t, tx, "delete from orders where id = 1")
	execSQL(t, tx, "insert into orders values (6,1,40,60)")
	check(pool, 1, 40, 1, 11)
	check(tx, 1, 40, 6, 60)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	check(pool, 1, 40, 6, 60)
	if n, ok := store.IndexCandidates("orders", "request_no_key", []engine.Value{engine.Int(40)}); !ok || n != 3 {
		t.Fatalf("expected three retained candidates for the same unique key, got %d", n)
	}

	execSQL(t, pool, "create table partitioned_orders (id int primary key, tenant_id int, request_no int, amount int, index by_tenant (tenant_id), unique (tenant_id, request_no))")
	execSQL(t, pool, "insert into partitioned_orders values (1,1,40,10),(2,2,40,20),(3,2,null,30),(4,2,null,40)")
	execSQL(t, pool, "create table requests (id int primary key, tenant_id int, request_no int)")
	execSQL(t, pool, "insert into requests values (1,2,40),(2,1,99),(3,2,null)")
	const join = "select o.amount from requests r join partitioned_orders o on o.request_no = r.request_no and o.tenant_id = r.tenant_id where r.id = ?"
	if amount := readQty(t, pool, join, 1); amount != 20 {
		t.Fatalf("composite join returned %d", amount)
	}
	for _, requestID := range []int{2, 3} {
		var amount int
		err := pool.QueryRowContext(t.Context(), join, requestID).Scan(&amount)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("join for request %d returned %d, %v", requestID, amount, err)
		}
	}
}

func createLookupOrders(t *testing.T, pool *sql.DB, uniqueFirst bool) {
	t.Helper()
	indexes := "index by_tenant (tenant_id), unique (request_no)"
	if uniqueFirst {
		indexes = "unique (request_no), index by_tenant (tenant_id)"
	}
	execSQL(t, pool, "create table orders (id int primary key, tenant_id int not null, request_no int, amount int not null, "+indexes+")")
}

func seedLookupOrders(t *testing.T, pool *sql.DB, rows, target int) {
	t.Helper()
	for first := 1; first <= rows; first += 2000 {
		var stmt strings.Builder
		stmt.WriteString("insert into orders values ")
		for id := first; id <= min(rows, first+1999); id++ {
			if id != first {
				stmt.WriteByte(',')
			}
			fmt.Fprintf(&stmt, "(%d,1,%d,10)", id, id)
		}
		execSQL(t, pool, stmt.String())
	}
	execSQL(t, pool, "insert into orders values (?,2,?,10)", rows+1, rows+1)
	for range 8 {
		tx := beginScanWriter(t, pool)
		defer tx.Rollback()
		for id := 2; id <= 512; id++ {
			execSQL(t, tx, orderUpdate, id)
		}
		execSQL(t, tx, orderUpdate, target)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

type orderReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func checkOrderLookup(ctx context.Context, reader orderReader, tenant int, request any, wantID, wantAmount int) error {
	var id, amount int
	err := reader.QueryRowContext(ctx, orderLookup, tenant, request).Scan(&id, &amount)
	if wantID == 0 {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("request %v should be absent: id=%d amount=%d err=%v", request, id, amount, err)
		}
	} else if err != nil || id != wantID || amount != wantAmount {
		return fmt.Errorf("request %v: id=%d amount=%d err=%v", request, id, amount, err)
	}
	return nil
}

func measureLookupAndWriter(t *testing.T, store *engine.DB, pool *sql.DB, target, writerID int) {
	t.Helper()
	writer := beginScanWriter(t, pool)
	defer writer.Rollback()
	reply := make(chan completedScan, 1)
	go func() {
		started := time.Now()
		err := checkOrderLookup(t.Context(), pool, 1, target, target, 18)
		reply <- completedScan{time.Since(started), err}
	}()

	var result completedScan
	var foundReply, held bool
	for deadline := time.Now().Add(time.Second); !foundReply && !held; runtime.Gosched() {
		select {
		case result = <-reply:
			foundReply = true
		default:
			held = store.PinnedScan("orders") != nil
		}
		if time.Now().After(deadline) {
			t.Fatal("lookup neither took the table latch nor returned")
		}
	}
	started := time.Now()
	if execSQL(t, writer, orderUpdate, writerID) != 1 {
		t.Fatal("same-table update did not affect one row")
	}
	statement := time.Since(started)
	started = time.Now()
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	commit := time.Since(started)
	if !foundReply {
		result = <-reply
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	t.Logf("paired held=%t response=%s writer_statement=%s writer_commit=%s", held, result.response, statement, commit)
}
