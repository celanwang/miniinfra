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
