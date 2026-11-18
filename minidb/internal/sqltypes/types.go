// Package sqltypes contains the data shared by SQL execution and the wire protocol.
package sqltypes

type Kind uint8

const (
	Null Kind = iota
	Integer
	Text
	Boolean
)

type Value struct {
	Kind Kind
	Int  int64
	Text string
}

func Int(v int64) Value     { return Value{Kind: Integer, Int: v} }
func String(v string) Value { return Value{Kind: Text, Text: v} }
func Bool(v bool) Value {
	if v {
		return Value{Kind: Boolean, Int: 1}
	}
	return Value{Kind: Boolean}
}

type Result struct {
	Columns  []string
	Rows     [][]Value
	Affected int64
}
