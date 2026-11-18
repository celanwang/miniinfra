package engine

import (
	"fmt"
	"testing"
)

func TestPinnedStatementSurvivesCommitsAndReclamation(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	reader, pinned := start(t, db)
	const updates = 40
	for i := range updates {
		if err := cycleStock(t.Context(), db, 1, i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	want := updates/2 + 1
	waitForHistory(t, db, historyCounts{want, want, want})
	wantStock(t, pinned, 0, Int(1), 0)
	wantStock(t, pinned, 1, String("hot"), 0)
	rows, _, err := pinned.Rows(schema.Name, nil, nil)
	if err != nil || len(rows) != 1 || rows[0][2] != Int(0) {
		t.Fatalf("heap scan lost the pinned row: %v %v", rows, err)
	}

	newView := snapshot(t, reader)
	wantStock(t, newView, 1, String("hot"), updates/2)
	pinned.Close()
	pinned.Close()
	waitForHistory(t, db, historyCounts{1, 1, 1})
	wantStock(t, newView, 0, Int(1), updates/2)
}

func TestReclaimKeepsActiveVersionAndAbortedPredecessor(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	writer, active := start(t, db)
	changed, err := active.Mutate(t.Context(), schema.Name, []Value{Int(1)}, func(row []Value) ([]Value, bool, error) {
		row[1], row[2] = String("uncommitted"), Int(9)
		return row, true, nil
	})
	if err != nil || !changed {
		t.Fatalf("update: %v %v", changed, err)
	}

	if _, removed := db.reclaimStep(); removed != 0 {
		t.Fatalf("reclaimed %d rows during an active write", removed)
	}
	wantStock(t, active, 1, String("uncommitted"), 9)
	reader, before := start(t, db)
	wantStock(t, before, 1, String("hot"), 0)
	writer.Rollback()
	waitForHistory(t, db, historyCounts{1, 1, 1})
	wantStock(t, before, 1, String("hot"), 0)
	if err := cycleStock(t.Context(), db, 1, true); err != nil {
		t.Fatal(err)
	}
	wantStock(t, snapshot(t, reader), 1, String("hot"), 1)
	before.Close()
	active.Close()
	waitForHistory(t, db, historyCounts{1, 1, 1})
}

func TestReclaimDropsEmptyIndexPagesAndDeletedIdentities(t *testing.T) {
	db := openInventory(t, t.TempDir())
	seed(t, db)
	tx, view := start(t, db)
	for id := range int64(800) {
		changed, err := view.Mutate(t.Context(), schema.Name, []Value{Int(id + 1)}, func([]Value) ([]Value, bool, error) {
			return nil, true, nil
		})
		if err != nil || !changed {
			t.Fatalf("delete %d: %v %v", id+1, changed, err)
		}
	}
	view.Close()
	commit(t, tx)
	waitForHistory(t, db, historyCounts{})
	tbl, _ := db.table(schema.Name, db.published.Load())
	tbl.mu.RLock()
	for _, idx := range tbl.indexes {
		if len(idx.root.keys) != 0 || len(idx.root.children) != 0 {
			t.Error("empty index retained keys or child pages")
		}
	}
	if len(tbl.pages) != 0 || len(tbl.heads) != 0 {
		t.Error("deleted rows retained heap pages or row directory entries")
	}
	tbl.mu.RUnlock()

	fresh, next := start(t, db)
	for i := range int64(800) {
		insert(t, next, i+1, fmt.Sprintf("sku-%d", i+1), 3)
	}
	next.Close()
	commit(t, fresh)
	_, read := start(t, db)
	wantStock(t, read, 0, Int(800), 3)
	wantStock(t, read, 1, String("sku-1"), 3)
}
