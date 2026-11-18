package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/query"
)

const (
	MaxFrame       = 4 << 20
	readBufferSize = 4096
)

type Request struct {
	SQL  string
	Args []engine.Value
}
type Response struct {
	Result        query.Result
	Code          string
	Message       string
	InTransaction *bool `json:",omitempty"`
}

func Failure(err error) Response {
	code := "execution"
	switch {
	case errors.Is(err, engine.ErrUnique):
		code = "unique"
	case errors.Is(err, engine.ErrDeadlock):
		code = "deadlock"
	case errors.Is(err, engine.ErrLockWait):
		code = "lock_wait"
	case errors.Is(err, engine.ErrCommitUnknown):
		code = "commit_unknown"
	case errors.Is(err, engine.ErrConstraint):
		code = "constraint"
	case errors.Is(err, engine.ErrUnavailable):
		code = "unavailable"
	}
	return Response{Code: code, Message: err.Error()}
}

// FrameReader scans a stream of newline-delimited protocol messages.
type FrameReader struct {
	input    io.Reader
	scanner  *bufio.Scanner
	unread   []byte // Non-nil when Read should restart after the current frame.
	grew     bool
	sawShort bool
}

func Reader(in io.Reader) *FrameReader {
	reader := &FrameReader{input: in}
	reader.reset(nil)
	return reader
}

func (reader *FrameReader) reset(unread []byte) {
	in := reader.input
	if len(unread) != 0 {
		// Copy the lookahead so it does not keep the old buffer alive.
		in = io.MultiReader(bytes.NewReader(bytes.Clone(unread)), in)
	}
	reader.scanner = bufio.NewScanner(in)
	reader.scanner.Buffer(make([]byte, readBufferSize), MaxFrame)
	reader.scanner.Split(reader.split)
	reader.unread, reader.grew, reader.sawShort = nil, false, false
}

func (reader *FrameReader) split(data []byte, terminal bool) (int, []byte, error) {
	n, frame, err := bufio.ScanLines(data, terminal)
	if n > readBufferSize {
		reader.grew, reader.sawShort = true, false
	} else if reader.grew && n != 0 && !terminal {
		// Keep the grown buffer through a single short frame. Replace it
		// once normal traffic resumes and the lookahead fits the small buffer.
		if reader.sawShort && len(data)-n <= readBufferSize {
			reader.unread = data[n:]
		}
		reader.sawShort = true
	}
	return n, frame, err
}

func (reader *FrameReader) Err() error { return reader.scanner.Err() }

func Read(reader *FrameReader, dst any) error {
	if !reader.scanner.Scan() {
		if err := reader.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	frame := reader.scanner.Bytes()
	if reader.unread != nil {
		reader.reset(reader.unread)
	}
	if !utf8.Valid(frame) {
		return errors.New("protocol frame must be valid UTF-8")
	}
	return json.Unmarshal(frame, dst)
}

func Encode(message any) ([]byte, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if len(data)+1 >= MaxFrame {
		return nil, fmt.Errorf("protocol message exceeds %d bytes", MaxFrame)
	}
	return append(data, '\n'), nil
}
