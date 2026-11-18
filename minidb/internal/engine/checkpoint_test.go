package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointRecoversPagesAndReplaysOnlyLaterCommits(t *testing.T) {
	dir := t.TempDir()
	db := openInventory(t, dir)
	seed(t, db)
	walPath := filepath.Join(dir, "minidb.wal")
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}

	active, view := start(t, db)
	if changed, err := decrease(view, 1); !changed || err != nil {
		t.Fatalf("update before checkpoint: %v %v", changed, err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	pageFile, err := os.Stat(filepath.Join(dir, checkpointFile))
	if err != nil || pageFile.Size() <= 4096 {
		t.Fatalf("checkpoint did not persist data pages: %v %v", pageFile, err)
	}
	after, err := os.Stat(walPath)
	if err != nil || after.Size() >= before.Size() {
		t.Fatalf("checkpoint did not reclaim old WAL: %v %v", after, err)
	}

	// An active transaction spanning the checkpoint belongs to the WAL tail.
	commit(t, active)
	aborted, pending := start(t, db)
	if changed, err := decrease(pending, 2); !changed || err != nil {
		t.Fatalf("uncommitted update: %v %v", changed, err)
	}
	aborted.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openInventory(t, dir)
	_, recovered := start(t, reopened)
	wantStock(t, recovered, 0, Int(1), 0)
	wantStock(t, recovered, 1, String("sku-2"), 1)
	wantStock(t, recovered, 0, Int(800), 1)
	if err := reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	_, latest := start(t, openInventory(t, dir))
	wantStock(t, latest, 1, String("sku-1"), 0)
}
