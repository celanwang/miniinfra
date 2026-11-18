package engine

import (
	"fmt"
	"testing"
)

func TestPinnedStatementsKeepOnlyVisibleHistory(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	reader, oldest := start(t, db)
	const updates = 40
	var middle Snapshot
	for i := range updates {
		if i == updates/2 {
			middle = snapshot(t, reader)
		}
		if err := cycleStock(t.Context(), db, 1, i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	waitForHistory(t, db, historyCounts{3, 3, 3})
	for _, previous := range []struct {
		view Snapshot
		qty  int64
	}{{oldest, 0}, {middle, updates / 4}} {
		wantStock(t, previous.view, 0, Int(1), previous.qty)
		wantStock(t, previous.view, 1, String("hot"), previous.qty)
		rows, _, err := previous.view.Rows(t.Context(), schema.Name, nil, nil)
		if err != nil || len(rows) != 1 || rows[0][2] != Int(previous.qty) {
			t.Fatalf("heap scan lost the pinned row: %v %v", rows, err)
		}
	}

	latest := snapshot(t, reader)
	wantStock(t, latest, 1, String("hot"), updates/2)
	oldest.Close()
	oldest.Close()
	waitForHistory(t, db, historyCounts{2, 2, 2})
	wantStock(t, middle, 1, String("hot"), updates/4)
	middle.Close()
	waitForHistory(t, db, historyCounts{1, 1, 1})
	wantStock(t, latest, 0, Int(1), updates/2)
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

func TestReclaimStepRespectsHeapSlotBudget(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	db.stopReclaimer()
	<-db.reclaimerDone
	for range reclaimBudget + 20 {
		if err := cycleStock(t.Context(), db, 1, false); err != nil {
			t.Fatal(err)
		}
	}
	examined, removed := db.reclaimStep()
	if examined != reclaimBudget || removed > examined {
		t.Fatalf("examined %d and removed %d versions with a %d-slot budget", examined, removed, reclaimBudget)
	}
	// Slot swaps can leave some dead rows for the next pass.
	for range 2 {
		db.reclaimStep()
	}
	if got := historyFor(db, 1, "hot"); got != (historyCounts{1, 1, 1}) {
		t.Fatalf("rollback backlog did not drain: %+v", got)
	}
}

func TestReclaimSharesSlotBudgetAcrossSparseTables(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	db.stopReclaimer()
	<-db.reclaimerDone

	empty := schema
	empty.Name = "empty"
	if err := db.CreateTable(empty); err != nil {
		t.Fatal(err)
	}
	small := schema
	small.Name = "small"
	if err := db.CreateTable(small); err != nil {
		t.Fatal(err)
	}
	tx, view := start(t, db)
	if err := view.Insert(t.Context(), small.Name, []Value{Int(10), String("small"), Int(1)}); err != nil {
		t.Fatal(err)
	}
	view.Close()
	commit(t, tx)

	for range reclaimBudget + 20 {
		if err := cycleStock(t.Context(), db, 1, false); err != nil {
			t.Fatal(err)
		}
	}
	// Begin this sweep at the two quiet tables. The empty visit consumes
	// budget without examining a heap slot. The busy table gets the rest.
	db.reclaimTable = 1
	examined, removed := db.reclaimStep()
	if examined != reclaimBudget-1 || removed < reclaimBudget-3 {
		t.Fatalf("scanned %d and removed %d across three tables", examined, removed)
	}
	for range 2 {
		db.reclaimStep()
	}
	if got := historyFor(db, 1, "hot"); got != (historyCounts{1, 1, 1}) {
		t.Fatalf("busy table did not converge: %+v", got)
	}
}

func TestReclaimKeepsVersionsForSnapshotsRegisteredAfterCapture(t *testing.T) {
	db := seedHistory(t, t.TempDir(), 1)
	db.stopReclaimer()
	<-db.reclaimerDone

	_, oldest := start(t, db)
	if err := cycleStock(t.Context(), db, 1, true); err != nil {
		t.Fatal(err)
	}
	views := db.captureReclaimViews()
	_, late := start(t, db)
	if err := cycleStock(t.Context(), db, 1, true); err != nil {
		t.Fatal(err)
	}
	tbl, _ := db.table(schema.Name, db.published.Load())
	if _, removed := tbl.reclaimWithin(&views, reclaimBudget, nil); removed != 0 {
		t.Fatal("sweep reclaimed a version ended after capture")
	}
	wantStock(t, oldest, 1, String("hot"), 0)
	wantStock(t, late, 1, String("hot"), 1)
	late.Close()
	if _, removed := db.reclaimStep(); removed != 1 {
		t.Fatalf("released version remained in a later sweep: removed=%d", removed)
	}
}

func TestReclaimFallsBackForManyPinsAndCompletedWriterViews(t *testing.T) {
	t.Run("many pins", func(t *testing.T) {
		db := seedHistory(t, t.TempDir(), 1)
		db.stopReclaimer()
		<-db.reclaimerDone

		reader, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Rollback()
		var pins []Snapshot
		for range maxExactSnapshots + 1 {
			pins = append(pins, snapshot(t, reader))
		}
		for range 4 {
			if err := cycleStock(t.Context(), db, 1, true); err != nil {
				t.Fatal(err)
			}
		}
		if _, removed := db.reclaimStep(); removed != 0 {
			t.Fatal("sweep exceeded the bound on exact snapshot searches")
		}
		pins[0].Close()
		if _, removed := db.reclaimStep(); removed != 3 {
			t.Fatalf("bounded snapshot search did not resume: removed=%d", removed)
		}
		wantStock(t, pins[1], 1, String("hot"), 0)
	})

	t.Run("completed writer view", func(t *testing.T) {
		db := seedHistory(t, t.TempDir(), 1)
		db.stopReclaimer()
		<-db.reclaimerDone

		writer, view := start(t, db)
		changed, err := view.Mutate(t.Context(), schema.Name, []Value{Int(1)}, func(row []Value) ([]Value, bool, error) {
			row[2] = Int(5)
			return row, true, nil
		})
		if err != nil || !changed {
			t.Fatalf("update: %v %v", changed, err)
		}
		commit(t, writer)
		if err := cycleStock(t.Context(), db, 1, true); err != nil {
			t.Fatal(err)
		}

		db.reclaimStep()
		wantStock(t, view, 1, String("hot"), 5)
		view.Close()
		if _, removed := db.reclaimStep(); removed != 2 {
			t.Fatalf("finished writer's version remained pinned: removed=%d", removed)
		}
	})
}
