package query

import (
	"errors"
	"slices"
	"testing"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestOrderQueriesAndPartialRollback(t *testing.T) {
	db, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, reader := NewSession(db), NewSession(db)
	defer first.Close()
	defer reader.Close()
	exec := func(s *Session, sql string, args ...engine.Value) Result {
		t.Helper()
		result, err := s.Execute(t.Context(), sql, args)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return result
	}
	for _, sql := range []string{
		"create table inventory (sku int primary key, available int not null)",
		"create table orders (id int primary key, request text not null unique)",
		"create table items (order_id int, sku int, quantity int not null, primary key (order_id, sku), index by_order (order_id))",
		"insert into inventory values (1, 10), (2, 20)",
		"begin",
		"insert into orders values (42, 'r-42')",
		"update inventory set available = available - 2 where sku = 1 and available >= 2",
	} {
		exec(first, sql)
	}
	if result := exec(first, "update inventory set available = available - 21 where sku = 2 and available >= 21"); result.Affected != 0 {
		t.Fatal("insufficient stock should affect zero rows")
	}
	exec(first, "rollback")
	if got := exec(reader, "select available from inventory where sku = 1").Rows[0][0]; got != engine.Int(10) {
		t.Fatalf("rollback left stock at %v", got)
	}
	exec(first, "begin")
	exec(first, "insert into orders values (?, ?)", engine.Int(42), engine.String("r-42"))
	exec(first, "update inventory set available = available - 2 where sku = 1 and available >= 2")
	exec(first, "update inventory set available = available - 3 where sku = 2 and available >= 3")
	exec(first, "insert into items values (42, 1, 2), (42, 2, 3)")
	const join = "select o.id, i.sku from orders o join items i on o.id = i.order_id where o.request = 'r-42'"
	exec(reader, "begin")
	if len(exec(first, join).Rows) != 2 || len(exec(reader, join).Rows) != 0 {
		t.Fatal("query disagreed about visibility of uncommitted changes")
	}
	exec(first, "commit")
	if len(exec(reader, join).Rows) != 2 {
		t.Fatal("next statement did not advance its snapshot")
	}
	exec(reader, "rollback")
	for _, sql := range []string{
		"select id from orders where id = 42",
		"select id from orders where request = 'r-42'",
		"select id from orders where id > 41",
	} {
		rows := exec(reader, sql).Rows
		if len(rows) != 1 || rows[0][0] != engine.Int(42) {
			t.Fatalf("%s gave %v", sql, rows)
		}
	}
	exec(first, "begin")
	if _, err := first.Execute(t.Context(), "insert into orders values (43, 'r-42')", nil); !errors.Is(err, engine.ErrUnique) {
		t.Fatalf("wanted unique conflict, got %v", err)
	}
	if _, err := first.Execute(t.Context(), "insert into orders values (44, 'unexpected')", nil); !errors.Is(err, ErrTransaction) {
		t.Fatalf("statement after rollback ran in autocommit: %v", err)
	}
	if _, err := first.Execute(t.Context(), "commit", nil); !errors.Is(err, ErrTransaction) {
		t.Fatalf("failed explicit transaction could commit: %v", err)
	}
}

func TestSingleTableFilterAgreesWithIndexAndJoin(t *testing.T) {
	db, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writer, reader := NewSession(db), NewSession(db)
	defer writer.Close()
	defer reader.Close()
	exec := func(session *Session, stmt string, args ...engine.Value) Result {
		t.Helper()
		result, err := session.Execute(t.Context(), stmt, args)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		return result
	}
	exec(writer, "create table offers (id int primary key, unindexed int, indexed int, candidate int, note text, approved bool, index by_key (indexed))")
	exec(writer, "insert into offers values (1,3,3,3,null,true), (2,3,3,null,'null operand',true), (3,3,3,4,'unapproved',false), (4,null,null,4,'null key',true), (5,8,8,8,'other key',true)")
	const heap = "select o.note, 7, o.id from offers o where o.unindexed = ? and o.candidate >= o.unindexed and o.approved = true"
	const indexed = "select o.note, 7, o.id from offers o where o.indexed = ? and o.candidate >= o.indexed and o.approved = true"
	const join = "select o.note, 7, o.id from offers o join offers i on i.id = o.id where i.indexed = ? and o.candidate >= i.indexed and o.approved = true"
	check := func(session *Session, stmt string, filter engine.Value, want []engine.Value) {
		t.Helper()
		result := exec(session, stmt, filter)
		if want == nil {
			if len(result.Rows) != 0 {
				t.Fatalf("%s returned rows for NULL: %v", stmt, result.Rows)
			}
			return
		}
		if len(result.Rows) != 1 || !slices.Equal(result.Rows[0], want) || !slices.Equal(result.Columns, []string{"note", "value", "id"}) {
			t.Fatalf("%s returned %+v, want %v", stmt, result, want)
		}
	}
	old := []engine.Value{{}, engine.Int(7), engine.Int(1)}
	updated := []engine.Value{engine.String("replacement"), engine.Int(7), engine.Int(2)}
	for _, stmt := range []string{heap, indexed, join} {
		check(reader, stmt, engine.Int(3), old)
		check(reader, stmt, engine.Value{}, nil)
	}
	exec(reader, "begin")
	exec(writer, "begin")
	exec(writer, "update offers set approved = false where id = 1")
	exec(writer, "update offers set candidate = 5, note = 'replacement' where id = 2")
	for _, stmt := range []string{heap, indexed, join} {
		check(reader, stmt, engine.Int(3), old)
		check(writer, stmt, engine.Int(3), updated)
	}
	exec(writer, "commit")
	for _, stmt := range []string{heap, indexed, join} {
		check(reader, stmt, engine.Int(3), updated)
	}
	exec(reader, "rollback")
}
