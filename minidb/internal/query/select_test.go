package query

import (
	"slices"
	"testing"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestChooseEqualityLookup(t *testing.T) {
	indexes := []engine.Index{
		{Name: "tenant", Columns: []int{0}},
		{Name: "amount", Columns: []int{2}},
		{Name: "tenant_request", Columns: []int{0, 1}, Unique: true},
		{Name: "request", Columns: []int{1}, Unique: true},
	}
	equal := func(column, other int, literal engine.Value) condition {
		return condition{operator: "=", left: boundValue{position: column}, right: boundValue{position: other, literal: literal}}
	}
	tenant, request, amount := engine.Int(2), engine.String("order-8"), engine.Int(21)
	byTenant := equal(0, -1, tenant)
	byRequest := equal(1, -1, request)
	byAmount := equal(2, -1, amount)
	nullRequest := equal(1, -1, engine.Value{})

	for _, tc := range []struct {
		name    string
		indexes []engine.Index
		prefix  []engine.Value
		filters []condition
		columns []int
		values  []engine.Value
	}{
		{"unique before ordinary", []engine.Index{indexes[0], indexes[3]}, nil, []condition{byTenant, byRequest}, []int{1}, []engine.Value{request}},
		{"complete composite", indexes, nil, []condition{byRequest, byTenant}, []int{0, 1}, []engine.Value{tenant, request}},
		{"ordinary fallback", indexes, nil, []condition{byAmount, byTenant}, []int{0}, []engine.Value{tenant}},
		{"no complete key", indexes[:3], nil, []condition{byRequest}, nil, nil},
		{"nullable unique fallback", indexes, nil, []condition{byTenant, nullRequest}, []int{0}, []engine.Value{tenant}},
		{"join composite", indexes, []engine.Value{tenant, request}, []condition{equal(1, 3, engine.Value{}), equal(2, 0, engine.Value{})}, []int{0, 1}, []engine.Value{tenant, request}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns, values := chooseEqualityLookup(tc.indexes, tc.prefix, tc.filters)
			if !slices.Equal(columns, tc.columns) || !slices.Equal(values, tc.values) {
				t.Fatalf("lookup returned (%v, %v), want (%v, %v)", columns, values, tc.columns, tc.values)
			}
		})
	}
}
