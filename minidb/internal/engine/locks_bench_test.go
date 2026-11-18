package engine

import (
	"context"
	"testing"
)

func BenchmarkLockAcquisition(b *testing.B) {
	for _, tc := range []struct {
		name     string
		repeated bool
		kind     string
	}{
		{"row_new", false, ""},
		{"row_repeated", true, ""},
		{"unique_new", false, "request"},
		{"unique_repeated", true, "request"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := &DB{lockTimeout: defaultLockTimeout, locks: lockManager{
				holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn),
			}}
			tx := &Txn{db: db, held: make(map[resource]bool)}
			r := resource{table: "orders", namespace: tc.kind, key: "existing-key"}
			ctx := context.Background()
			if err := db.locks.acquire(ctx, tx, r); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if !tc.repeated {
					delete(db.locks.holders, r)
					delete(tx.held, r)
				}
				if err := db.locks.acquire(ctx, tx, r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
