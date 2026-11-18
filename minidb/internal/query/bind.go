package query

import (
	"fmt"
	"strings"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

type field struct {
	name, qualifier string
	kind            engine.Kind
}
type boundValue struct {
	position int
	literal  engine.Value
	kind     engine.Kind
}
type condition struct {
	left, right boundValue
	operator    string
}

func bind(expr expression, fields []field) (boundValue, error) {
	if expr.column == "" {
		return boundValue{position: -1, literal: expr.value, kind: expr.value.Kind}, nil
	}
	pos := -1
	for i, field := range fields {
		if field.name == expr.column && (expr.qualifier == "" || field.qualifier == expr.qualifier) {
			if pos != -1 {
				return boundValue{}, fmt.Errorf("ambiguous column %q", expr.column)
			}
			pos = i
		}
	}
	if pos < 0 {
		return boundValue{}, fmt.Errorf("unknown column %q", expr.column)
	}
	return boundValue{position: pos, kind: fields[pos].kind}, nil
}

func bindConditions(filters []predicate, fields []field) ([]condition, error) {
	var result []condition
	for _, predicate := range filters {
		left, err := bind(predicate.left, fields)
		if err != nil {
			return nil, err
		}
		right, err := bind(predicate.right, fields)
		if err != nil {
			return nil, err
		}
		if left.kind != engine.Null && right.kind != engine.Null && left.kind != right.kind {
			return nil, fmt.Errorf("cannot compare columns or values of different types")
		}
		result = append(result, condition{left: left, right: right, operator: predicate.operator})
	}
	return result, nil
}

func (value boundValue) get(row []engine.Value) engine.Value {
	if value.position >= 0 {
		return row[value.position]
	}
	return value.literal
}

func (filter condition) matches(row []engine.Value) bool {
	left, right := filter.left.get(row), filter.right.get(row)
	if left.Kind == engine.Null || right.Kind == engine.Null {
		return false
	}
	var comparison int
	switch {
	case left.Kind == engine.Text:
		comparison = strings.Compare(left.Text, right.Text)
	case left.Int < right.Int:
		comparison = -1
	case left.Int > right.Int:
		comparison = 1
	}
	switch filter.operator {
	case "=":
		return comparison == 0
	case "<>", "!=":
		return comparison != 0
	case "<":
		return comparison < 0
	case ">":
		return comparison > 0
	case "<=":
		return comparison <= 0
	default:
		return comparison >= 0
	}
}

func matches(conditions []condition, row []engine.Value) bool {
	for _, filter := range conditions {
		if !filter.matches(row) {
			return false
		}
	}
	return true
}

func lookupValues(columns []int, prefix []engine.Value, filters []condition) ([]engine.Value, bool) {
	values := make([]engine.Value, len(columns))
	for i, column := range columns {
		found := false
		pos := column + len(prefix)
		for _, filter := range filters {
			if filter.operator != "=" {
				continue
			}
			left, right := filter.left, filter.right
			if right.position == pos {
				left, right = right, left
			}
			if left.position == pos && right.position < len(prefix) {
				values[i] = right.get(prefix)
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return values, true
}

func fieldsFor(source source, schema engine.Schema) []field {
	var fields []field
	for _, col := range schema.Columns {
		fields = append(fields, field{name: col.Name, qualifier: source.alias, kind: col.Kind})
	}
	return fields
}
