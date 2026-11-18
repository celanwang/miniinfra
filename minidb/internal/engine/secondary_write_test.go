package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestTCPSecondaryIndexWritesRecover(t *testing.T) {
	dir := t.TempDir()
	store, pool, closeStore := openTCP(t, dir)
	execSQL(t, pool, "create table secondary_stock (id int primary key, request_id text, region text, bin_label text, qty int not null, index by_location (region, bin_label), unique (request_id))")
	const insert = "insert into secondary_stock values (?,?,?,?,?)"
	for _, row := range [][]any{
		{1, "req-1", "east\x00rack", "shelf", 10},
		{2, "req-2", "east\x00rack", "shelf", 20},
		{3, "req-3", "east", "rack\x00shelf", 30},
		{4, nil, nil, "spare", 40},
		{5, nil, nil, "spare", 50},
	} {
		execSQL(t, pool, insert, row...)
	}
	const byLocation = "select id from secondary_stock where region = ? and bin_label = ?"
	const byRequest = "select id from secondary_stock where request_id = ?"
	const move = "update secondary_stock set region = ?, bin_label = ?, qty = ? where id = ?"
	locationCount := func(region, bin string, want int) bool {
		key := []engine.Value{engine.String(region), engine.String(bin)}
		got, ok := store.IndexCandidates("secondary_stock", "by_location", key)
		return ok && got == want
	}
	check := func(conn secondaryQueryer, query string, expected []int, args ...any) {
		t.Helper()
		checkSecondaryIDs(t, conn, query, expected, args...)
	}
	check(pool, byLocation, []int{1, 2}, "east\x00rack", "shelf")
	check(pool, byLocation, []int{3}, "east", "rack\x00shelf")
	check(pool, byLocation, nil, nil, "spare")
	check(pool, byRequest, nil, nil)
	check(pool, "select id from secondary_stock", []int{1, 2, 3, 4, 5})

	_, err := pool.ExecContext(t.Context(), insert, 6, "req-2", "west", "rack-b", 60)
	requireSecondaryUnique(t, err)
	_, err = pool.ExecContext(t.Context(), "update secondary_stock set request_id = ?, region = ? where id = ?", "req-2", "west", 1)
	requireSecondaryUnique(t, err)
	check(pool, byLocation, []int{1, 2}, "east\x00rack", "shelf")
	check(pool, byRequest, []int{1}, "req-1")
	check(pool, "select id from secondary_stock where id = ?", nil, 6)

	active := beginScanWriter(t, pool)
	defer active.Rollback()
	execSQL(t, active, move, "west", "rack-b", 11, 1)
	check(active, byLocation, []int{1}, "west", "rack-b")
	check(active, byLocation, []int{2}, "east\x00rack", "shelf")
	check(pool, byLocation, nil, "west", "rack-b")
	check(pool, byLocation, []int{1, 2}, "east\x00rack", "shelf")
	// Roll back both a chain head on the shared key and the only entry
	// on the temporary key.
	execSQL(t, active, move, "east\x00rack", "shelf", 12, 1)
	if err := active.Rollback(); err != nil {
		t.Fatal(err)
	}
	until(t, "reclamation of rolled-back secondary entries", 2*time.Second, func() bool {
		return locationCount("east\x00rack", "shelf", 2) && locationCount("west", "rack-b", 0)
	})
	check(pool, byLocation, nil, "west", "rack-b")
	check(pool, byLocation, []int{1, 2}, "east\x00rack", "shelf")
	execSQL(t, pool, move, "west", "rack-b", 12, 1)
	until(t, "reclamation of committed old secondary key", 2*time.Second, func() bool {
		return locationCount("east\x00rack", "shelf", 1) && locationCount("west", "rack-b", 1)
	})
	check(pool, byLocation, []int{2}, "east\x00rack", "shelf")

	checkRecovered := func(oldLocation []int, nullKeys int, qty int64) {
		t.Helper()
		check(pool, byLocation, oldLocation, "east\x00rack", "shelf")
		check(pool, byLocation, []int{1}, "west", "rack-b")
		check(pool, byLocation, []int{3}, "east", "rack\x00shelf")
		check(pool, byLocation, nil, nil, "spare")
		check(pool, byRequest, nil, nil)
		check(pool, byRequest, []int{1}, "req-1")
		check(pool, byRequest, []int{2}, "req-2")
		if got := readQty(t, pool, "select qty from secondary_stock where id = ?", 2); got != qty {
			t.Fatalf("quantity is %d, want %d", got, qty)
		}
		if got, ok := store.IndexCandidates("secondary_stock", "by_location", []engine.Value{{}, engine.String("spare")}); !ok || got != nullKeys {
			t.Fatalf("non-unique index contains %d NULL keys, want %d", got, nullKeys)
		}
	}
	closeStore()
	store, pool, closeStore = openTCP(t, dir)
	checkRecovered([]int{2}, 2, 20)

	execSQL(t, pool, "checkpoint")
	execSQL(t, pool, move, nil, "spare", 21, 2)
	closeStore()
	store, pool, _ = openTCP(t, dir)
	checkRecovered(nil, 3, 21)
}

type secondaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func checkSecondaryIDs(t *testing.T, queryer secondaryQueryer, stmt string, expected []int, args ...any) {
	t.Helper()
	rows, err := queryer.QueryContext(t.Context(), stmt, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if rows.Err() != nil || !slices.Equal(ids, expected) {
		t.Fatalf("%s %q returned %v (%v), want %v", stmt, args, ids, rows.Err(), expected)
	}
}

func requireSecondaryUnique(t *testing.T, err error) {
	t.Helper()
	var conflict *client.Error
	if !errors.As(err, &conflict) || conflict.Code != client.Unique {
		t.Fatalf("wanted a unique key conflict, got %v", err)
	}
}
