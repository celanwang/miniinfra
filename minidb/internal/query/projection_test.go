package query

import (
	"slices"
	"testing"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestProjectionRowsRemainIndependent(t *testing.T) {
	db, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	session := NewSession(db)
	defer session.Close()
	execute := func(t *testing.T, stmt string) Result {
		t.Helper()
		result, err := session.Execute(t.Context(), stmt, nil)
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		return result
	}
	execute(t, "create table offers (id int primary key, req text unique, aisle text, qty int, index by_aisle (aisle))")
	execute(t, "insert into offers values (1,'r1','east',10),(2,'r2','east',20),(3,'r3','west',30),(4,'r4','east',40)")

	const projection = "select qty, id, qty from offers where "
	for _, tc := range []struct {
		name, condition string
		ids             []int64
	}{
		{"heap", "id >= 1", []int64{1, 2, 3, 4}},
		{"ordinary index", "aisle = 'east'", []int64{1, 2, 4}},
		{"unique index", "req = 'r1'", []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := func(result Result) {
				t.Helper()
				if !slices.Equal(result.Columns, []string{"qty", "id", "qty"}) {
					t.Fatalf("unexpected columns %v", result.Columns)
				}
				var ids []int64
				for _, row := range result.Rows {
					if len(row) != 3 {
						t.Fatalf("unexpected projected row %v", row)
					}
					id := row[1].Int
					if row[0] != engine.Int(id*10) || row[2] != row[0] {
						t.Fatalf("unexpected projected row %v", row)
					}
					ids = append(ids, id)
				}
				slices.Sort(ids)
				if !slices.Equal(ids, tc.ids) {
					t.Fatalf("got IDs %v, want %v", ids, tc.ids)
				}
			}
			stmt := projection + tc.condition
			result := execute(t, stmt)
			check(result)
			changeProjection(t, result.Rows, 2)
			check(execute(t, stmt))
		})
	}

	const join = "select a.id, b.id, b.qty, a.qty, b.qty, c.id from offers a join offers b on b.aisle = a.aisle join offers c on c.id = a.id"
	result := execute(t, join)
	pairs := make(map[[2]int64]bool)
	for _, row := range result.Rows {
		if len(row) != 6 {
			t.Fatalf("unexpected joined projection %v", row)
		}
		a, b := row[0].Int, row[1].Int
		pair := [2]int64{a, b}
		if a < 1 || a > 4 || b < 1 || b > 4 || (a == 3) != (b == 3) || pairs[pair] ||
			row[2] != engine.Int(b*10) || row[3] != engine.Int(a*10) || row[4] != row[2] || row[5] != row[0] {
			t.Fatalf("unexpected joined projection %v", row)
		}
		pairs[pair] = true
	}
	if len(pairs) != 10 {
		t.Fatalf("got %d joined pairs, want ten", len(pairs))
	}
	changeProjection(t, result.Rows, 5)
	beforeUpdate := execute(t, projection+"req = 'r1'")
	execute(t, "update offers set qty = 99 where id = 1")
	afterUpdate := execute(t, projection+"req = 'r1'")
	if len(beforeUpdate.Rows) != 1 || len(afterUpdate.Rows) != 1 ||
		len(beforeUpdate.Rows[0]) != 3 || len(afterUpdate.Rows[0]) != 3 ||
		beforeUpdate.Rows[0][0] != engine.Int(10) || afterUpdate.Rows[0][0] != engine.Int(99) {
		t.Fatalf("separate statements returned %v then %v", beforeUpdate.Rows, afterUpdate.Rows)
	}
}

func changeProjection(t *testing.T, rows [][]engine.Value, repeated int) {
	t.Helper()
	before := make([][]engine.Value, len(rows))
	for i, row := range rows {
		before[i] = slices.Clone(row)
	}
	rows[0][0] = engine.Int(-1)
	if rows[0][repeated] != before[0][0] {
		t.Fatalf("changing a projected cell changed its repeated value: %v", rows[0])
	}
	for i := 1; i < len(rows); i++ {
		if !slices.Equal(rows[i], before[i]) {
			t.Fatalf("changing one result changed sibling row %d: %v", i, rows[i])
		}
	}
}
