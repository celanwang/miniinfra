package minimq

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var pageLimit = Limits{Messages: 20, Bytes: 4096}

func openTest(t *testing.T, dir string, cfg Config) *Partition {
	t.Helper()
	p, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func publishTest(t *testing.T, p *Partition, id string) PublishResult {
	t.Helper()
	result, err := p.Publish(PublishRequest{ID: id, Body: []byte(id)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPublishAndRecoverCommittedHistory(t *testing.T) {
	dir := t.TempDir()
	p := openTest(t, dir, Config{})
	first := publishTest(t, p, "first")
	second := publishTest(t, p, "second")
	if first.Offset != 0 || second.Offset != 1 {
		t.Fatalf("unexpected offsets: %v %v", first, second)
	}
	if _, err := Open(dir, Config{}); err == nil {
		t.Fatal("second writer acquired the directory")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "journal-0000000000000001")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("partial uncommitted tail")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, Config{})
	if got := publishTest(t, p, "first"); got != first {
		t.Fatalf("publish retry = %v, want %v", got, first)
	}
	if _, err := p.Publish(PublishRequest{ID: "first", Body: []byte("changed")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("same ID with different body: %v", err)
	}
	page, err := p.Read(0, pageLimit)
	if err != nil || page.Next != 2 || len(page.Messages) != 2 || string(page.Messages[1].Body) != "second" {
		t.Fatalf("history = %+v, %v", page, err)
	}
	page.Messages[0].Body[0] = 'x'
	page, err = p.Read(0, Limits{Messages: 1, Bytes: 5})
	if err != nil || page.Next != 1 || len(page.Messages) != 1 || string(page.Messages[0].Body) != "first" {
		t.Fatalf("limited history = %+v, %v", page, err)
	}
}

func TestCommittedCorruptionFailsOpen(t *testing.T) {
	dir := t.TempDir()
	p := openTest(t, dir, Config{})
	publishTest(t, p, "first")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "journal-0000000000000001")
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, 9); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Config{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("opening committed damaged frame: %v", err)
	}
}

func testClock(at *time.Time) func() (TimeBounds, error) {
	return func() (TimeBounds, error) {
		return TimeBounds{Lower: *at, Upper: *at}, nil
	}
}
