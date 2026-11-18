package pages

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointValidatesEveryPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	payload := bytes.Repeat([]byte("checkpoint record\n"), 500)
	meta := Metadata{LSN: 1234, Sequence: 4, LastRowID: 20}
	if err := Save(path, meta, func(out io.Writer) error {
		_, err := out.Write(payload)
		return err
	}); err != nil {
		t.Fatal(err)
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
