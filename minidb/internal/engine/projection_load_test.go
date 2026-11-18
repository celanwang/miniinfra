package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const projectionRows = 512

func TestTCPProjectionLoad(t *testing.T) {
	raw := os.Getenv("MINIDB_PROJECTION_QUERIES")
	if raw == "" {
		t.Skip("set MINIDB_PROJECTION_QUERIES for the projection workload")
	}
	queries, err := strconv.Atoi(raw)
	if err != nil || queries <= 0 {
		t.Fatalf("invalid MINIDB_PROJECTION_QUERIES %q", raw)
	}
	_, pool, _ := openTCP(t, t.TempDir())
	seedProjectionRows(t, pool)
	for _, query := range []projectionQuery{
		{"unique", "select qty, id, qty from projection_orders where request_no = ?", "req-512", 512, 512, false},
		{"heap", "select qty, id, qty from projection_orders where id >= ?", 385, 385, 512, false},
		{"secondary", "select qty, id, qty from projection_orders where bucket = ?", 3, 385, 512, false},
		{"join", "select i.qty, o.id, i.qty from projection_orders o join projection_items i on i.order_id = o.id where o.bucket = ?", 3, 385, 512, true},
	} {
		t.Run(query.name, func(t *testing.T) {
			readProjection(t, pool, query)
			response := make([]time.Duration, queries)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := range queries {
				started := time.Now()
				readProjection(t, pool, query)
				response[i] = time.Since(started)
			}
			runtime.ReadMemStats(&after)
			t.Logf("projection queries=%d mallocs_per_query=%.2f bytes_per_query=%.2f response_p50=%s response_p99=%s",
				queries, float64(after.Mallocs-before.Mallocs)/float64(queries),
				float64(after.TotalAlloc-before.TotalAlloc)/float64(queries), lockLoadMedian(response), p99(response))
		})
	}
}

type projectionQuery struct {
	name, stmt  string
	arg         any
	first, last int
	join        bool
}

func seedProjectionRows(t *testing.T, pool *sql.DB) {
	t.Helper()
	execSQL(t, pool, "create table projection_orders (id int primary key, request_no text unique, bucket int, qty int, index by_bucket (bucket))")
	execSQL(t, pool, "create table projection_items (order_id int, slot int, qty int, primary key (order_id, slot), index by_order (order_id))")
	var orders, items strings.Builder
	orders.WriteString("insert into projection_orders values ")
	items.WriteString("insert into projection_items values ")
	for id := 1; id <= projectionRows; id++ {
		if id > 1 {
			orders.WriteByte(',')
			items.WriteByte(',')
		}
		fmt.Fprintf(&orders, "(%d,'req-%d',%d,%d)", id, id, (id-1)/128, id*10)
		fmt.Fprintf(&items, "(%d,1,%d),(%d,2,%d)", id, id*10+1, id, id*10+2)
	}
	execSQL(t, pool, orders.String())
	execSQL(t, pool, items.String())
}

func readProjection(t *testing.T, pool *sql.DB, query projectionQuery) {
	t.Helper()
	rows, err := pool.QueryContext(context.Background(), query.stmt, query.arg)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil || !slices.Equal(columns, []string{"qty", "id", "qty"}) {
		t.Fatalf("projection columns are %v, %v", columns, err)
	}
	var seen [projectionRows + 1]uint8
	for rows.Next() {
		var qty, id, repeated int
		if err := rows.Scan(&qty, &id, &repeated); err != nil {
			t.Fatal(err)
		}
		if id < query.first || id > query.last || qty != repeated {
			t.Fatalf("unexpected projected row (%d, %d, %d)", qty, id, repeated)
		}
		offset := qty - id*10
		if !query.join && offset != 0 || query.join && (offset < 1 || offset > 2) {
			t.Fatalf("quantity %d has incorrect prefix %d", qty, id)
		}
		bit := uint8(1) << uint(offset)
		if seen[id]&bit != 0 {
			t.Fatalf("duplicate row (%d, %d)", id, qty)
		}
		seen[id] |= bit
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	mask := uint8(1)
	if query.join {
		mask = 6
	}
	for id := query.first; id <= query.last; id++ {
		if seen[id] != mask {
			t.Fatalf("incomplete results for %d: rows=%b want=%b", id, seen[id], mask)
		}
	}
}
