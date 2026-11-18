package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestQueuedCommitsStayOnTheirSideOfCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(magic); err != nil {
		file.Close()
		t.Fatal(err)
	}

	log := &Log{file: file, path: path, queue: make(chan request, 3), done: make(chan struct{})}
	first, checkpoint, second := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	enqueued := time.Now()
	var published, base int64
	var firstTiming, secondTiming CommitTiming
	log.queue <- request{
		payload: []byte("before"),
		result:  first, timing: &firstTiming, enqueued: enqueued,
		publish: func(end int64) { published = end },
	}
	log.queue <- request{result: checkpoint, checkpoint: func(end int64) error {
		base = end
		if published != base {
			return fmt.Errorf("checkpoint ran before publication: %d != %d", published, base)
		}
		return nil
	}}
	log.queue <- request{
		payload: []byte("after"),
		result:  second, timing: &secondTiming, enqueued: enqueued,
		publish: func(int64) {},
	}
	go log.run(int64(len(magic)))
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(<-first, <-checkpoint, <-second); err != nil {
		t.Fatal(err)
	}
	if firstTiming.BatchSize != 1 || secondTiming.BatchSize != 1 {
		t.Fatalf("batch crossed a checkpoint: %d, %d", firstTiming.BatchSize, secondTiming.BatchSize)
	}
	var records []string
	log, err = OpenWithCheckpoint(path, func() (int64, error) { return base, nil }, func(payload []byte) error {
		records = append(records, string(payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if len(records) != 1 || records[0] != "after" {
		t.Fatalf("incorrect checkpoint tail: %v", records)
	}
}
