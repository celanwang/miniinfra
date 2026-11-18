package journal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkPrepareWALFrame(b *testing.B) {
	for _, workload := range []struct {
		name string
		size int
	}{
		{"ack", 564},
		{"retry", 64_417},
		{"dead-letter", 11_255_874},
	} {
		b.Run(workload.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte("A"), workload.size)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				frame, err := EncodeFrame(payload)
				if err != nil || len(frame) != len(payload)+FrameHeaderBytes {
					b.Fatalf("frame size = %d, %v", len(frame), err)
				}
			}
		})
	}
}

func TestAppendFramePreservesLegacyBytes(t *testing.T) {
	start := []byte("prefix")
	frame, err := AppendFrame(start, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := []byte{3, 0, 0, 0, 0xc2, 0x41, 0x24, 0x35, 'a', 'b', 'c'}
	if !bytes.Equal(frame, append(bytes.Clone(start), legacy...)) {
		t.Fatalf("frame = %x", frame)
	}
	payload, n, err := ReadFrame(bytes.NewReader(frame[len(start):]), int64(len(legacy)))
	if err != nil || string(payload) != "abc" || n != int64(len(legacy)) {
		t.Fatalf("read frame = %q, %d, %v", payload, n, err)
	}
}

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
