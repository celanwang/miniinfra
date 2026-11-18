package pages

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointValidatesEveryPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	payload := bytes.Repeat([]byte("checkpoint record\n"), 9000)
	meta := Metadata{LSN: 1234, Sequence: 4, LastRowID: 20}
	var timing SaveTiming
	if err := SaveWithTiming(path, meta, func(out io.Writer) error {
		_, err := out.Write(payload)
		return err
	}, &timing); err != nil {
		t.Fatal(err)
	}
	pages := (len(payload) + Size - dataOffset - 1) / (Size - dataOffset)
	if want := 1 + (pages*Size+writeBufferSize-1)/writeBufferSize; timing.Writes != want {
		t.Fatalf("checkpoint used %d writes, want %d", timing.Writes, want)
	}
	decode := func(header Metadata, in io.Reader) error {
		t.Helper()
		if meta != header {
			t.Fatal(header)
		}
		got, err := io.ReadAll(in)
		if err == nil && !bytes.Equal(got, payload) {
			t.Fatal("different checkpoint data")
		}
		return err
	}
	if lsn, err := Load(path, decode); err != nil || lsn != meta.LSN {
		t.Fatalf("load = %d, %v", lsn, err)
	}
	interrupted := errors.New("interrupted encoding")
	if err := Save(path, Metadata{LSN: 5678}, func(out io.Writer) error {
		if _, err := out.Write(payload); err != nil {
			return err
		}
		return interrupted
	}); !errors.Is(err, interrupted) {
		t.Fatalf("staged checkpoint returned %v", err)
	}
	if lsn, err := Load(path, decode); err != nil || lsn != meta.LSN {
		t.Fatalf("published checkpoint changed after encode failure: %d, %v", lsn, err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{'X'}, Size*2+dataOffset); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, err := Load(path, decode); err == nil {
		t.Fatal("accepted a damaged checkpoint page")
	}
}
