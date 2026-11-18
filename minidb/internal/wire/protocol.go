package wire

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/celanwang/miniinfra/minidb/internal/sqltypes"
)

const MaxFrame = 4 << 20

type Request struct {
	SQL  string
	Args []sqltypes.Value
}
type Response struct {
	Result        sqltypes.Result
	Code          string
	Message       string
	InTransaction *bool `json:",omitempty"`
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
	frame := reader.Bytes()
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
