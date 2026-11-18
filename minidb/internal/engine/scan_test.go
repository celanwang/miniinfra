package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestRowsStopMidScan(t *testing.T) {
	db := openInventory(t, t.TempDir())
	def := schema
	def.Indexes = append(slices.Clone(def.Indexes), Index{Name: "by_available", Columns: []int{2}})
	if err := db.CreateTable(def); err != nil {
		t.Fatal(err)
	}
	writer, before := start(t, db)
	for id := range int64(4 * heapPageRows) {
		insert(t, before, id+1, fmt.Sprint(id), 1)
	}
	before.Close()
	commit(t, writer)
	_, view := start(t, db)
	defer view.Close()

	for _, tc := range []struct {
		name    string
		columns []int
	}{
		{name: "heap"},
		{name: "index", columns: []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Observe cancellation after one page of candidates.
			interrupted := &scanCancelContext{Context: ctx, cancel: cancel, checks: 4}
			examined := 0
			rows, indexed, err := view.RowsMatching(interrupted, def.Name, tc.columns, []Value{Int(1)}, func([]Value) bool {
				examined++
				return false
			})
			if !errors.Is(err, context.Canceled) || len(rows) != 0 || indexed != (len(tc.columns) > 0) || examined != heapPageRows {
				t.Fatalf("canceled scan examined %d candidates and returned %d rows indexed=%t err=%v", examined, len(rows), indexed, err)
			}
			rows, _, err = view.Rows(t.Context(), def.Name, tc.columns, []Value{Int(1)})
			if err != nil || len(rows) != 4*heapPageRows {
				t.Fatalf("live scan returned %d rows: %v", len(rows), err)
			}
		})
	}
}

type scanCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (ctx *scanCancelContext) Err() error {
	ctx.checks--
	if ctx.checks == 0 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}
