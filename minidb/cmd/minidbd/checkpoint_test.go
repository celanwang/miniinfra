package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/pages"
)

func TestOrderSurvivesCheckpointPowerLoss(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	t.Cleanup(func() { db.Close() })

	setup(t, db)
	execute(t, db, "checkpoint")
	oldPages := readDatabaseFile(t, dir, "minidb.pages")
	tx := reserveOrder(t, db)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	oldWAL := readDatabaseFile(t, dir, "minidb.wal")

	execute(t, db, "checkpoint")
	newPages := readDatabaseFile(t, dir, "minidb.pages")
	if len(newPages) < 2*pages.Size || bytes.Equal(newPages, oldPages) {
		t.Fatal("second checkpoint did not include the committed order")
	}
	if size := len(readDatabaseFile(t, dir, "minidb.wal")); size >= len(oldWAL) {
		t.Fatal("second checkpoint did not recycle the WAL containing the order")
	}
	kill()

	// Recreate the durable directory at each power-loss point using files
	// emitted by the real daemon. The writer fills the header after the
	// data pages, so the interrupted staging file starts with a zero page.
	torn := make([]byte, pages.Size+pages.Size/2)
	copy(torn[pages.Size:], newPages[pages.Size:len(torn)])
	for _, crash := range []struct {
		name               string
		published, staging []byte
	}{
		{name: "half_page_before_publish", published: oldPages, staging: torn},
		{name: "published_before_WAL_recycle", published: newPages},
	} {
		t.Run(crash.name, func(t *testing.T) {
			image := t.TempDir()
			writeDatabaseFile(t, image, "minidb.pages", crash.published)
			writeDatabaseFile(t, image, "minidb.wal", oldWAL)
			if crash.staging != nil {
				writeDatabaseFile(t, image, "minidb.pages.next", crash.staging)
			}
			addr, _ := launch(t, image)
			db := client.Open(addr)
			defer db.Close()
			assertOrderRecovered(t, db)
			execute(t, db, "checkpoint")
			assertOrderRecovered(t, db)
			if _, err := os.Stat(filepath.Join(image, "minidb.pages.next")); !os.IsNotExist(err) {
				t.Fatalf("checkpoint left a staged file: %v", err)
			}
		})
	}
}

func readDatabaseFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeDatabaseFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
