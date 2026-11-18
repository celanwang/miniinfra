package wire

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
	"github.com/celanwang/miniinfra/minidb/internal/query"
)

const MaxFrame = 4 << 20

type Request struct {
	SQL  string
	Args []engine.Value
}
type Response struct {
	Result  query.Result
	Code    string
	Message string
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

func Reader(in io.Reader) *bufio.Scanner {
	reader := bufio.NewScanner(in)
	reader.Buffer(make([]byte, 4096), MaxFrame)
	return reader
}

func Read(reader *bufio.Scanner, dst any) error {
	if !reader.Scan() {
		if err := reader.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	return json.Unmarshal(reader.Bytes(), dst)
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
