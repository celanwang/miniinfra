package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestReplayPreservesPublicationOrderAndTruncatesIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	var published []string
	log, err := Open(path, func([]byte) error { return errors.New("unexpected record") })
	if err != nil {
		t.Fatal(err)
	}
	var clients sync.WaitGroup
	for _, value := range []string{"one", "two", "three", "four"} {
		clients.Go(func() {
			if err := log.Commit([]byte(value), func(int64) { published = append(published, value) }); err != nil {
				t.Error(err)
			}
		})
	}
	clients.Wait()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var torn [headerSize + 1]byte
	binary.BigEndian.PutUint32(torn[:4], 5)
	binary.BigEndian.PutUint32(torn[8:12], crc32.ChecksumIEEE(torn[:8]))
	if _, err := file.Write(torn[:]); err != nil {
		t.Fatal(err)
	}
	file.Close()
	var recovered []string
	log, err = Open(path, func(data []byte) error {
		recovered = append(recovered, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, published) || len(recovered) != 4 {
		t.Fatalf("replayed %v, published %v", recovered, published)
	}
	if _, err := Open(path, func([]byte) error { return nil }); err == nil {
		t.Fatal("second writer opened the same WAL")
	}
	if err := log.Commit([]byte("after restart"), func(int64) {}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, err = Open(path, func(data []byte) error {
		recovered = append(recovered, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if len(recovered) != 9 || recovered[8] != "after restart" {
		t.Fatalf("failed to replay after tail repair: %v", recovered)
	}
}

func TestReplayRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	log, err := Open(path, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Commit([]byte("record"), func(int64) {}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'X'}, int64(len(magic)+headerSize)); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if log, err := Open(path, func([]byte) error { return nil }); err == nil {
		log.Close()
		t.Fatal("opened a WAL with a corrupted record")
	}
}

func TestBatchIsWrittenBeforeFirstPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteString(magic); err != nil {
		t.Fatal(err)
	}
	log := &Log{file: file, queue: make(chan request, 2), done: make(chan struct{})}
	first := make(chan error, 1)
	second := make(chan error, 1)
	expectedEnd := int64(len(magic) + 2*headerSize + len("firstsecond"))
	log.queue <- request{payload: []byte("first"), result: first, publish: func(end int64) {
		info, err := os.Stat(path)
		if err != nil || info.Size() != expectedEnd {
			t.Errorf("first publication did not follow the batch write: %v %v", info, err)
		}
		if end != int64(len(magic)+headerSize+len("first")) {
			t.Errorf("wrong first transaction end: %d", end)
		}
	}}
	log.queue <- request{payload: []byte("second"), result: second, publish: func(end int64) {
		if end != expectedEnd {
			t.Errorf("wrong second transaction end: %d", end)
		}
	}}
	go log.run(int64(len(magic)))
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(<-first, <-second); err != nil {
		t.Fatal(err)
	}
}

func TestWriteFailurePreventsPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	log, err := Open(path, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := log.file.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := log.Commit([]byte("failed write"), func(int64) { t.Error("published a failed transaction") }); err == nil {
			t.Fatal("acknowledged a failed write")
		}
	}
	if err := log.Close(); err == nil {
		t.Fatal("WAL did not report the write failure at close")
	}
}
