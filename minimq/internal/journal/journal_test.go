package journal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryRequiresDurableHeadBeforeCollection(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "partition")
	j, err := Open(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Checkpoint([]byte("state"), nil); err != nil {
		j.Close()
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Lose access to the directory after the new HEAD has been read, while
	// both generations are still available and the journal file is open.
	moved := filepath.Join(root, "moved")
	recovered, err := Open(dir, func(string) error { return os.Rename(dir, moved) }, nil)
	if recovered != nil {
		recovered.Close()
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery accepted HEAD without syncing its directory: %v", err)
	}
	if _, err := os.Stat(generationPath(moved, "journal", 1)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, dir); err != nil {
		t.Fatal(err)
	}
	recovered, err = Open(dir, func(path string) error {
		data, err := os.ReadFile(path)
		if err == nil && string(data) != "state" {
			t.Errorf("recovered checkpoint = %q", data)
		}
		return err
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.Collect(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(generationPath(dir, "journal", 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("obsolete journal after recovery: %v", err)
	}
}
