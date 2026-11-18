package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/celanwang/miniinfra/minidb/internal/sqltypes"
)

type Kind = sqltypes.Kind
type Value = sqltypes.Value

const (
	Null    = sqltypes.Null
	Integer = sqltypes.Integer
	Text    = sqltypes.Text
	Boolean = sqltypes.Boolean
)

func Int(v int64) Value     { return sqltypes.Int(v) }
func String(v string) Value { return sqltypes.String(v) }
func Bool(v bool) Value     { return sqltypes.Bool(v) }

type Column struct {
	Name    string
	Kind    Kind
	NotNull bool
}

type Index struct {
	Name    string
	Columns []int
	Unique  bool
	Primary bool
}

type Schema struct {
	Name    string
	Columns []Column
	Indexes []Index
}

func (s Schema) Column(name string) int {
	return slices.IndexFunc(s.Columns, func(c Column) bool { return c.Name == name })
}

func (s Schema) validate() error {
	if s.Name == "" || !utf8.ValidString(s.Name) || len(s.Columns) == 0 {
		return fmt.Errorf("%w: table name and columns are required", ErrSchema)
	}
	columns := make(map[string]bool)
	for _, column := range s.Columns {
		if column.Name == "" || !utf8.ValidString(column.Name) || columns[column.Name] || column.Kind < Integer || column.Kind > Boolean {
			return fmt.Errorf("%w: invalid or repeated column %q", ErrSchema, column.Name)
		}
		columns[column.Name] = true
	}
	names := make(map[string]bool)
	var primary int
	for _, idx := range s.Indexes {
		if idx.Name == "" || !utf8.ValidString(idx.Name) || names[idx.Name] || len(idx.Columns) == 0 {
			return fmt.Errorf("%w: invalid or repeated index %q", ErrSchema, idx.Name)
		}
		names[idx.Name] = true
		seen := make(map[int]bool)
		for _, pos := range idx.Columns {
			if pos < 0 || pos >= len(s.Columns) || seen[pos] || (idx.Primary && !s.Columns[pos].NotNull) {
				return fmt.Errorf("%w: invalid indexed column in %q", ErrSchema, idx.Name)
			}
			seen[pos] = true
		}
		if idx.Primary {
			primary++
			if !idx.Unique {
				return fmt.Errorf("%w: primary index must be unique", ErrSchema)
			}
		}
	}
	if primary != 1 {
		return fmt.Errorf("%w: exactly one primary key is required", ErrSchema)
	}
	return nil
}

func (s Schema) checkRow(values []Value) error {
	if len(values) != len(s.Columns) {
		return fmt.Errorf("%w: wrong number of values", ErrConstraint)
	}
	for i, v := range values {
		c := s.Columns[i]
		if (v.Kind == Null && c.NotNull) || (v.Kind != Null && v.Kind != c.Kind) ||
			(v.Kind == Boolean && v.Int != 0 && v.Int != 1) || (v.Kind == Text && !utf8.ValidString(v.Text)) {
			return fmt.Errorf("%w: invalid value for %s.%s", ErrConstraint, s.Name, c.Name)
		}
	}
	return nil
}

func key(values []Value, columns []int) (string, bool) {
	var encoded strings.Builder
	hasNull := false
	for _, column := range columns {
		v := values[column]
		encoded.WriteByte(byte(v.Kind))
		switch v.Kind {
		case Null:
			hasNull = true
		case Integer, Boolean:
			var data [8]byte
			binary.BigEndian.PutUint64(data[:], uint64(v.Int)^(1<<63))
			encoded.Write(data[:])
		case Text:
			for i := range len(v.Text) {
				encoded.WriteByte(v.Text[i])
				if v.Text[i] == 0 {
					encoded.WriteByte(255)
				}
			}
			encoded.WriteString("\x00\x00")
		}
	}
	return encoded.String(), hasNull
}

func lookupKey(values []Value) string {
	columns := make([]int, len(values))
	for i := range columns {
		columns[i] = i
	}
	encoded, _ := key(values, columns)
	return encoded
}

var (
	ErrSchema        = errors.New("invalid schema")
	ErrConstraint    = errors.New("constraint violation")
	ErrUnique        = errors.New("unique key conflict")
	ErrDeadlock      = errors.New("deadlock")
	ErrLockWait      = errors.New("lock wait canceled")
	ErrInactive      = errors.New("transaction is inactive")
	ErrCommitUnknown = errors.New("commit result unknown")
	ErrUnavailable   = errors.New("database unavailable")
)
