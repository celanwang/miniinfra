package query

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

type Result struct {
	Columns  []string
	Rows     [][]engine.Value
	Affected int64
}

var (
	ErrSyntax      = errors.New("invalid SQL")
	ErrTransaction = errors.New("invalid transaction state")
)

// A Session is owned by one client connection.
type Session struct {
	failed bool
	db     *engine.DB
	tx     *engine.Txn
}

func NewSession(db *engine.DB) *Session { return &Session{db: db} }

func (s *Session) Close() {
	s.failed = false
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
}

func (s *Session) abort() {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
		s.failed = true
	}
}

func (s *Session) Execute(ctx context.Context, sql string, args []engine.Value) (Result, error) {
	if err := ctx.Err(); err != nil {
		s.abort()
		return Result{}, err
	}
	stmt, err := parse(sql, args)
	if err != nil {
		s.abort()
		return Result{}, fmt.Errorf("%w: %w", ErrSyntax, err)
	}
	if s.failed && stmt.verb != "rollback" {
		if stmt.verb == "commit" {
			s.Close()
		}
		return Result{}, fmt.Errorf("%w: transaction was rolled back", ErrTransaction)
	}
	switch stmt.verb {
	case "begin":
		if s.tx != nil {
			s.abort()
			return Result{}, fmt.Errorf("%w: transaction already active", ErrTransaction)
		}
		s.tx, err = s.db.Begin()
		return Result{}, err
	case "rollback":
		s.Close()
		return Result{}, nil
	case "commit":
		if s.tx == nil {
			return Result{}, fmt.Errorf("%w: no active transaction", ErrTransaction)
		}
		tx := s.tx
		s.tx = nil
		return Result{}, tx.Commit()
	case "checkpoint":
		if s.tx != nil {
			s.abort()
			return Result{}, fmt.Errorf("%w: CHECKPOINT requires autocommit", ErrTransaction)
		}
		return Result{}, s.db.Checkpoint()
	case "create":
		if s.tx != nil {
			s.abort()
			return Result{}, fmt.Errorf("%w: CREATE TABLE requires autocommit", ErrTransaction)
		}
		return Result{}, s.db.CreateTable(stmt.schema)
	}

	tx := s.tx
	implicit := tx == nil
	if implicit {
		tx, err = s.db.Begin()
		if err != nil {
			return Result{}, err
		}
	}
	view, err := tx.Snapshot()
	var result Result
	if err == nil {
		switch stmt.verb {
		case "select":
			result, err = selectRows(ctx, view, stmt)
		case "insert":
			result, err = insertRows(ctx, view, stmt)
		case "update", "delete":
			result, err = mutateRow(ctx, view, stmt)
		}
	}
	if err != nil {
		if implicit {
			tx.Rollback()
		} else {
			s.abort()
		}
		return Result{}, err
	}
	if implicit {
		if err := tx.Commit(); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}

func insertRows(ctx context.Context, view engine.Snapshot, stmt statement) (Result, error) {
	schema, err := view.Schema(stmt.table.name)
	if err != nil {
		return Result{}, err
	}
	columns := make([]int, len(schema.Columns))
	for i := range columns {
		columns[i] = i
	}
	if len(stmt.columns) != 0 {
		columns = make([]int, len(stmt.columns))
		seen := make(map[int]bool)
		for i, name := range stmt.columns {
			column := schema.Column(name)
			if column < 0 || seen[column] {
				return Result{}, fmt.Errorf("%w: invalid or repeated column %q", ErrSyntax, name)
			}
			columns[i], seen[column] = column, true
		}
	}
	for _, expressions := range stmt.values {
		if len(columns) != len(expressions) {
			return Result{}, fmt.Errorf("%w: wrong number of INSERT values", ErrSyntax)
		}
		row := make([]engine.Value, len(schema.Columns))
		for i, expr := range expressions {
			if expr.column != "" {
				return Result{}, fmt.Errorf("%w: INSERT requires literals or parameters", ErrSyntax)
			}
			row[columns[i]] = expr.value
		}
		if err := view.Insert(ctx, stmt.table.name, row); err != nil {
			return Result{}, err
		}
	}
	return Result{Affected: int64(len(stmt.values))}, nil
}

func mutateRow(ctx context.Context, view engine.Snapshot, stmt statement) (Result, error) {
	schema, err := view.Schema(stmt.table.name)
	if err != nil {
		return Result{}, err
	}
	fields := fieldsFor(source{alias: stmt.table.name}, schema)
	filter, err := bindConditions(stmt.filter, fields)
	if err != nil {
		return Result{}, err
	}
	var primary []int
	for _, index := range schema.Indexes {
		if index.Primary {
			primary = index.Columns
			break
		}
	}
	key, found := lookupValues(primary, nil, filter)
	if !found {
		return Result{}, fmt.Errorf("%w: UPDATE and DELETE require equality on every primary key column", ErrSyntax)
	}

	type calculation struct {
		position    int
		left, right boundValue
		operator    string
	}
	var calculations []calculation
	seen := make(map[int]bool)
	for _, update := range stmt.updates {
		position := schema.Column(update.column)
		if position < 0 || seen[position] {
			return Result{}, fmt.Errorf("%w: invalid or repeated column %q", ErrSyntax, update.column)
		}
		seen[position] = true
		left, err := bind(update.value.left, fields)
		if err != nil {
			return Result{}, err
		}
		right, err := bind(update.value.right, fields)
		if err != nil {
			return Result{}, err
		}
		if left.kind != engine.Null && left.kind != schema.Columns[position].Kind {
			return Result{}, fmt.Errorf("%w: invalid type for %q", ErrSyntax, update.column)
		}
		if update.value.operator != "" && (schema.Columns[position].Kind != engine.Integer ||
			right.kind != engine.Integer && right.kind != engine.Null) {
			return Result{}, fmt.Errorf("%w: arithmetic requires integer operands", ErrSyntax)
		}
		calculations = append(calculations, calculation{position: position, left: left, right: right, operator: update.value.operator})
	}
	changed, err := view.Mutate(ctx, stmt.table.name, key, func(current []engine.Value) ([]engine.Value, bool, error) {
		if !matches(filter, current) {
			return nil, false, nil
		}
		if stmt.verb == "delete" {
			return nil, true, nil
		}
		values := slices.Clone(current)
		for _, calculation := range calculations {
			value, err := calculate(calculation.left.get(current), calculation.right.get(current), calculation.operator)
			if err != nil {
				return nil, false, err
			}
			values[calculation.position] = value
		}
		return values, true, nil
	})
	if changed {
		return Result{Affected: 1}, err
	}
	return Result{}, err
}

func calculate(left, right engine.Value, operator string) (engine.Value, error) {
	if operator == "" {
		return left, nil
	}
	if left.Kind == engine.Null || right.Kind == engine.Null {
		return engine.Value{}, nil
	}
	a, b := left.Int, right.Int
	if operator == "+" {
		if result := a + b; (b >= 0 && result >= a) || (b < 0 && result < a) {
			return engine.Int(result), nil
		}
	} else if result := a - b; (b >= 0 && result <= a) || (b < 0 && result > a) {
		return engine.Int(result), nil
	}
	return engine.Value{}, fmt.Errorf("integer overflow")
}
