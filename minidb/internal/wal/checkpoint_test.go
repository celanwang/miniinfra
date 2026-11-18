package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointKeepsWALUntilPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	log, err := Open(path, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	var lsn int64
	if err := log.Commit([]byte("before"), func(end int64) { lsn = end }); err != nil {
		t.Fatal(err)
	}
	checkOldLog := func(end int64) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil || int64(len(data)) != lsn || end != lsn || !bytes.HasSuffix(data, []byte("before")) {
			t.Errorf("checkpoint at %d did not retain WAL through %d: %q %v", end, lsn, data, err)
		}
	}

	interrupted := errors.New("failed to sync checkpoint")
	if err := log.Checkpoint(func(end int64) error {
		checkOldLog(end)
		return interrupted
	}); !errors.Is(err, interrupted) {
		t.Fatalf("checkpoint returned %v", err)
	}
	checkOldLog(lsn)
	if err := log.Checkpoint(func(end int64) error {
		checkOldLog(end)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithCheckpoint(path, func() (int64, error) { return lsn, nil }, func([]byte) error { return nil }); err == nil {
		t.Fatal("replacing the WAL released database ownership")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != int64(rotatedHeaderSize) {
		t.Fatalf("old WAL records survived rotation: %v %v", info, err)
	}
	if err := log.Commit([]byte("after"), func(end int64) {
		if end != lsn+headerSize+int64(len("after")) {
			t.Errorf("rotation moved logical position backwards: %d", end)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, func([]byte) error { return nil }); err == nil {
		t.Fatal("rotated WAL opened without its checkpoint")
	}
	var recovered []string
	again, err := OpenWithCheckpoint(path, func() (int64, error) { return lsn, nil }, func(data []byte) error {
		recovered = append(recovered, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if len(recovered) != 1 || recovered[0] != "after" {
		t.Fatalf("wrong records after rotation: %v", recovered)
	}
}
