package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
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
				frame, err := PrepareFrame(payload)
				if err != nil || frame.Size() != len(payload)+FrameHeaderBytes {
					b.Fatalf("frame size = %d, %v", frame.Size(), err)
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

func TestAppendWALFramesPreserveBytesAndBarriers(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	var written int
	var synced int
	j.ObserveWrite = func(time.Duration) { written++ }
	j.ObserveSync = func(time.Duration) { synced++ }
	var offset int64
	for _, size := range []int{564, splitFrameAt - 1, splitFrameAt, splitFrameAt + 1} {
		payload := bytes.Repeat([]byte{0x9d}, size)
		frame, err := PrepareFrame(payload)
		if err != nil || frame.Size() != size+8 {
			t.Fatalf("prepare %d bytes = %d, %v", size, frame.Size(), err)
		}
		written, synced = 0, 0
		if err := j.Append(frame); err != nil {
			t.Fatal(err)
		}
		wantsWrites := 2 // The WAL and HEAD staging file.
		if size >= splitFrameAt {
			wantsWrites++
		}
		if written != wantsWrites || synced != 3 {
			t.Fatalf("append %d bytes called %d writes and %d syncs", size, written, synced)
		}
		var header [8]byte
		binary.LittleEndian.PutUint32(header[:4], uint32(size))
		binary.LittleEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
		actual := make([]byte, len(header)+size)
		if _, err := j.file.ReadAt(actual, offset); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual[:8], header[:]) || !bytes.Equal(actual[8:], payload) {
			t.Fatalf("append %d bytes differs from the original frame bytes", size)
		}
		offset += int64(len(actual))
		if j.End() != offset {
			t.Fatalf("end = %d, want %d", j.End(), offset)
		}
	}
}

func TestSplitFrameBodyWriteFailurePreservesHEAD(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := PrepareFrame([]byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(initial); err != nil {
		t.Fatal(err)
	}
	end := j.End()
	pending, err := PrepareFrame(make([]byte, splitFrameAt))
	if err != nil {
		t.Fatal(err)
	}
	j.ObserveWrite = func(time.Duration) { _ = j.file.Close() }
	err = j.Append(pending)
	if !errors.Is(err, os.ErrClosed) || j.End() != end || j.Append(initial) != err {
		t.Fatalf("failed body append = %v, journal end = %d, want %d and fail-stop", err, j.End(), end)
	}
	_ = j.Close()
	var recovered []string
	j, err = Open(dir, nil, func(payload []byte) error {
		recovered = append(recovered, string(payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	info, err := j.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != end || len(recovered) != 1 || recovered[0] != "committed" {
		t.Fatalf("recovered end = %d, records = %q", info.Size(), recovered)
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
