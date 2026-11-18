package query

import (
	"context"
	"fmt"
	"slices"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

type scan struct {
	table  string
	schema engine.Schema
	on     []condition
}

func selectRows(ctx context.Context, view engine.Snapshot, stmt statement) (Result, error) {
	schema, err := view.Schema(stmt.table.name)
	if err != nil {
		return Result{}, err
	}
	fields := fieldsFor(stmt.table, schema)
	aliases := map[string]bool{stmt.table.alias: true}
	scans := []scan{{table: stmt.table.name, schema: schema}}
	for _, join := range stmt.joins {
		if aliases[join.table.alias] {
			return Result{}, fmt.Errorf("duplicate table alias %q", join.table.alias)
		}
		aliases[join.table.alias] = true
		schema, err := view.Schema(join.table.name)
		if err != nil {
			return Result{}, err
		}
		fields = append(fields, fieldsFor(join.table, schema)...)
		on, err := bindConditions(join.on, fields)
		if err != nil {
			return Result{}, err
		}
		scans = append(scans, scan{table: join.table.name, schema: schema, on: on})
	}
	where, err := bindConditions(stmt.filter, fields)
	if err != nil {
		return Result{}, err
	}

	result := Result{}
	var projections []boundValue
	if len(stmt.projections) == 0 {
		for i, field := range fields {
			result.Columns = append(result.Columns, field.name)
			projections = append(projections, boundValue{position: i})
		}
	} else {
		for _, expr := range stmt.projections {
			bound, err := bind(expr, fields)
			if err != nil {
				return Result{}, err
			}
			name := expr.column
			if name == "" {
				name = "value"
			}
			result.Columns = append(result.Columns, name)
			projections = append(projections, bound)
		}
	}
	const maxRows = 10_000
	var visit func(prefix []engine.Value, depth int) error
	visit = func(prefix []engine.Value, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth == len(scans) {
			if !matches(where, prefix) {
				return nil
			}
			if len(result.Rows) == maxRows {
				return fmt.Errorf("query result exceeds %d rows", maxRows)
			}
			projected := make([]engine.Value, len(projections))
			for i, value := range projections {
				projected[i] = value.get(prefix)
			}
			result.Rows = append(result.Rows, projected)
			return nil
		}
		plan := scans[depth]
		filters := slices.Concat(plan.on, where)
		var columns []int
		var values []engine.Value
		for _, index := range plan.schema.Indexes {
			if key, found := lookupValues(index.Columns, prefix, filters); found {
				columns, values = index.Columns, key
				break
			}
		}
		rows, _, err := view.Rows(plan.table, columns, values)
		if err != nil {
			return err
		}
		for _, row := range rows {
			joined := append(prefix, row...)
			if matches(plan.on, joined) {
				if err := visit(joined, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return result, visit(nil, 0)
}
