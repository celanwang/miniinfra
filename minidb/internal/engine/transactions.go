package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/wal"
)

// A transaction belongs to one session. Calls within a session are sequential.
type Txn struct {
	timing   *transactionTiming
	db       *DB
	state    *transactionState
	held     map[resource]bool
	done     chan struct{}
	finished bool
	changes  []change
}

func (db *DB) Begin() (*Txn, error) {
	if err := db.available(); err != nil {
		return nil, err
	}
	return &Txn{db: db, state: &transactionState{}, held: make(map[resource]bool), done: make(chan struct{})}, nil
}

func (tx *Txn) active() error {
	if tx.finished {
		return ErrInactive
	}
	return tx.db.available()
}

func (tx *Txn) Commit() error {
	if tx.finished {
		return ErrInactive
	}
	defer tx.finish()
	if err := tx.db.available(); err != nil {
		return err
	}
	if len(tx.changes) == 0 {
		return nil
	}
	var timing *wal.CommitTiming
	if tx.timing != nil {
		timing = &tx.timing.wal
	}
	return tx.db.persist(record{Changes: tx.changes}, func(sequence uint64) {
		tx.state.sequence.Store(sequence)
	}, timing)
}

func (tx *Txn) Rollback() {
	if !tx.finished {
		tx.finish()
	}
}

func (tx *Txn) finish() {
	tx.finished = true
	tx.state.sequence.CompareAndSwap(0, abortedSequence)
	tx.db.locks.release(tx)
	tx.changes = nil
}

// Snapshot is shared by every operator of a SQL statement.
type Snapshot struct {
	tx       *Txn
	sequence uint64
	pin      *snapshotPin
}

func (tx *Txn) Snapshot() (Snapshot, error) {
	if err := tx.active(); err != nil {
		return Snapshot{}, err
	}
	return tx.db.pinSnapshot(tx), nil
}

func (view Snapshot) table(name string) (*table, error) {
	tbl, ok := view.tx.db.table(name, view.sequence)
	if !ok {
		return nil, fmt.Errorf("%w: unknown table %s", ErrSchema, name)
	}
	return tbl, nil
}

func (view Snapshot) Schema(name string) (Schema, error) {
	tbl, err := view.table(name)
	if err != nil {
		return Schema{}, err
	}
	return tbl.schema, nil
}

func (view Snapshot) visible(row *version) bool {
	return view.sees(row.created) && !view.sees(row.ended)
}

func (view Snapshot) sees(state *transactionState) bool {
	if state == nil {
		return false
	}
	seq := state.sequence.Load()
	if seq == abortedSequence {
		return false
	}
	if view.tx != nil && state == view.tx.state {
		return true
	}
	return seq != 0 && seq <= view.sequence
}

// Rows returns all visible rows, or probes an equality index when the supplied
// columns match one. indexed reports whether the probe was performed.
func (view Snapshot) Rows(name string, columns []int, values []Value) (rows [][]Value, indexed bool, err error) {
	tbl, err := view.table(name)
	if err != nil {
		return nil, false, err
	}
	view.tx.lockTable(tbl, tbl.mu.RLock)
	defer tbl.mu.RUnlock()
	if len(columns) != 0 {
		for i := range tbl.indexes {
			if !slices.Equal(tbl.indexes[i].def.Columns, columns) {
				continue
			}
			for row := range tbl.indexes[i].lookup(lookupKey(values)) {
				if view.visible(row) {
					rows = append(rows, slices.Clone(row.values))
				}
			}
			return rows, true, nil
		}
	}
	for _, page := range tbl.pages {
		for _, row := range page {
			if view.visible(row) {
				rows = append(rows, slices.Clone(row.values))
			}
		}
	}
	return rows, false, nil
}

func (view Snapshot) Insert(ctx context.Context, name string, values []Value) error {
	tbl, err := view.table(name)
	if err != nil {
		return err
	}
	if err := tbl.schema.checkRow(values); err != nil {
		return err
	}
	tx := view.tx
	id := tx.db.rowID.Add(1)
	return tx.write(ctx, tbl, nil, change{Table: name, ID: id, Kind: "insert", Values: slices.Clone(values)})
}

// Mutate selects the original logical row through the statement snapshot,
// then holds its write lock while evaluating the current version.
func (view Snapshot) Mutate(ctx context.Context, name string, primary []Value, evaluate func([]Value) ([]Value, bool, error)) (bool, error) {
	tbl, err := view.table(name)
	if err != nil {
		return false, err
	}
	if len(primary) != len(tbl.indexes[tbl.primary].def.Columns) {
		return false, fmt.Errorf("%w: wrong number of primary key values", ErrSchema)
	}
	var id uint64
	view.tx.lockTable(tbl, tbl.mu.RLock)
	for row := range tbl.indexes[tbl.primary].lookup(lookupKey(primary)) {
		if view.visible(row) {
			id = row.id
			break
		}
	}
	tbl.mu.RUnlock()
	if id == 0 {
		return false, nil
	}

	tx := view.tx
	if err := tx.acquire(ctx, resource{table: name, key: strconv.FormatUint(id, 10)}); err != nil {
		return false, err
	}
	current := Snapshot{tx: tx, sequence: tx.db.published.Load()}
	tx.lockTable(tbl, tbl.mu.RLock)
	var old *version
	for row := tbl.heads[id]; row != nil; row = row.previous {
		if current.visible(row) {
			old = row
			break
		}
	}
	tbl.mu.RUnlock()
	if old == nil {
		return false, nil
	}
	values, changed, err := evaluate(slices.Clone(old.values))
	if err != nil || !changed {
		return false, err
	}
	kind := "delete"
	if values != nil {
		if err := tbl.schema.checkRow(values); err != nil {
			return false, err
		}
		kind = "update"
	}
	err = tx.write(ctx, tbl, old, change{Table: name, ID: id, Kind: kind, Values: slices.Clone(values)})
	return err == nil, err
}

func (tx *Txn) acquire(ctx context.Context, r resource) error {
	err := tx.db.locks.acquire(ctx, tx, r)
	if errors.Is(err, ErrDeadlock) || errors.Is(err, ErrLockWait) {
		tx.Rollback()
	}
	return err
}

func (tx *Txn) write(ctx context.Context, tbl *table, old *version, ch change) error {
	// DDL cannot add indexes to an existing table in this version. Definitions
	// and index slots are stable while their tree roots change under tbl.mu.
	for _, def := range tbl.schema.Indexes {
		if !def.Unique {
			continue
		}
		var keys []string
		if old != nil {
			if encoded, null := key(old.values, def.Columns); !null {
				keys = append(keys, encoded)
			}
		}
		if ch.Kind != "delete" {
			if encoded, null := key(ch.Values, def.Columns); !null {
				keys = append(keys, encoded)
			}
		}
		slices.Sort(keys)
		for _, encoded := range slices.Compact(keys) {
			r := resource{table: ch.Table, namespace: def.Name, key: encoded}
			if err := tx.acquire(ctx, r); err != nil {
				return err
			}
		}
	}

	tx.lockTable(tbl, tbl.mu.Lock)
	defer tbl.mu.Unlock()
	if ch.Kind != "delete" {
		current := Snapshot{tx: tx, sequence: tx.db.published.Load()}
		for i := range tbl.indexes {
			idx := &tbl.indexes[i]
			encoded, null := key(ch.Values, idx.def.Columns)
			if !idx.def.Unique || null {
				continue
			}
			for row := range idx.lookup(encoded) {
				if row.id != ch.ID && current.visible(row) {
					return fmt.Errorf("%w: %s.%s", ErrUnique, tbl.schema.Name, idx.def.Name)
				}
			}
		}
	}
	tbl.apply(ch, tx.state, old)
	tx.changes = append(tx.changes, ch)
	return nil
}

type transactionTiming struct {
	rowWait, uniqueWait, lockManagerWait, tableWait time.Duration
	wal                                             wal.CommitTiming
	acquisitions                                    []tableAcquisition
}

type tableAcquisition struct {
	table             *table
	requested, locked time.Time
}

func (tx *Txn) lockTable(tbl *table, lock func()) {
	if tx.timing == nil {
		lock()
		return
	}
	start := time.Now()
	lock()
	locked := time.Now()
	tx.timing.tableWait += locked.Sub(start)
	if tx.timing.acquisitions != nil {
		tx.timing.acquisitions = append(tx.timing.acquisitions, tableAcquisition{tbl, start, locked})
	}
}
