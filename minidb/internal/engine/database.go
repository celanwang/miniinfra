package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/wal"
)

type DB struct {
	log         *wal.Log
	catalog     sync.RWMutex
	ddl         sync.Mutex
	tables      map[string]*table
	locks       lockManager
	lockTimeout time.Duration
	rowID       atomic.Uint64
	published   atomic.Uint64
	closed      atomic.Bool
	failed      atomic.Bool
}

type transactionState struct {
	sequence atomic.Uint64
}

type version struct {
	id       uint64
	values   []Value
	created  *transactionState
	ended    *transactionState
	previous *version
}

type table struct {
	mu      sync.RWMutex
	schema  Schema
	since   uint64
	indexes []index
	primary int
	pages   [][]*version
	heads   map[uint64]*version
}

type change struct {
	Table  string
	ID     uint64
	Kind   string
	Values []Value
}

type record struct {
	Create  *Schema
	Changes []change
}

func Open(dir string) (*DB, error) {
	db := &DB{
		tables:      make(map[string]*table),
		locks:       lockManager{holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn)},
		lockTimeout: defaultLockTimeout,
	}
	log, err := wal.Open(filepath.Join(dir, "minidb.wal"), db.replay)
	if err != nil {
		return nil, err
	}
	db.log = log
	return db, nil
}

func (db *DB) Close() error {
	db.closed.Store(true)
	return db.log.Close()
}

func (db *DB) CreateTable(schema Schema) error {
	if err := schema.validate(); err != nil {
		return err
	}
	db.ddl.Lock()
	defer db.ddl.Unlock()
	if err := db.available(); err != nil {
		return err
	}
	if _, exists := db.table(schema.Name, db.published.Load()); exists {
		return fmt.Errorf("%w: table %s already exists", ErrSchema, schema.Name)
	}
	schema.Columns = slices.Clone(schema.Columns)
	schema.Indexes = slices.Clone(schema.Indexes)
	for i := range schema.Indexes {
		schema.Indexes[i].Columns = slices.Clone(schema.Indexes[i].Columns)
	}
	return db.persist(record{Create: &schema}, func(sequence uint64) {
		db.install(schema, sequence)
	})
}

func (db *DB) install(schema Schema, sequence uint64) {
	tbl := &table{schema: schema, since: sequence, heads: make(map[uint64]*version)}
	for i, def := range schema.Indexes {
		tbl.indexes = append(tbl.indexes, index{def: def, root: &indexPage{}})
		if def.Primary {
			tbl.primary = i
		}
	}
	db.catalog.Lock()
	db.tables[schema.Name] = tbl
	db.catalog.Unlock()
}

func (db *DB) table(name string, snap uint64) (*table, bool) {
	db.catalog.RLock()
	tbl := db.tables[name]
	db.catalog.RUnlock()
	return tbl, tbl != nil && tbl.since <= snap
}

func (db *DB) available() error {
	if db.closed.Load() || db.failed.Load() {
		return ErrUnavailable
	}
	return nil
}

func (db *DB) persist(change record, publish func(uint64)) error {
	bytes, err := json.Marshal(change)
	if err != nil {
		return err
	}
	err = db.log.Commit(bytes, func(int64) {
		next := db.published.Load() + 1
		publish(next)
		db.published.Store(next)
	})
	switch {
	case err == nil, errors.Is(err, wal.ErrTooLarge), errors.Is(err, wal.ErrClosed):
		return err
	default:
		db.failed.Store(true)
		return fmt.Errorf("%w: %w", ErrCommitUnknown, err)
	}
}

func (db *DB) replay(bytes []byte) error {
	var rec record
	if err := json.Unmarshal(bytes, &rec); err != nil {
		return err
	}
	next := db.published.Load() + 1
	if rec.Create != nil {
		if err := rec.Create.validate(); err != nil {
			return err
		}
		if _, exists := db.table(rec.Create.Name, next); exists || len(rec.Changes) != 0 {
			return errors.New("invalid catalog record")
		}
		db.install(*rec.Create, next)
	} else {
		if len(rec.Changes) == 0 {
			return errors.New("empty transaction record")
		}
		state := &transactionState{}
		state.sequence.Store(next)
		for _, ch := range rec.Changes {
			tbl, ok := db.table(ch.Table, next)
			if !ok || ch.ID == 0 {
				return errors.New("invalid row record")
			}
			old := tbl.heads[ch.ID]
			switch ch.Kind {
			case "insert":
				if old != nil {
					return errors.New("duplicated row identity")
				}
			case "update", "delete":
				if old == nil || old.ended != nil {
					return errors.New("missing live row")
				}
			default:
				return errors.New("unknown row operation")
			}
			if ch.Kind != "delete" {
				if err := tbl.schema.checkRow(ch.Values); err != nil {
					return err
				}
			}
			tbl.apply(ch, state, old)
			db.rowID.Store(max(ch.ID, db.rowID.Load()))
		}
	}
	db.published.Store(next)
	return nil
}

const heapPageRows = 128

func (tbl *table) apply(ch change, state *transactionState, old *version) {
	if old != nil {
		old.ended = state
	}
	if ch.Kind == "delete" {
		return
	}
	row := &version{id: ch.ID, values: ch.Values, created: state, previous: tbl.heads[ch.ID]}
	if len(tbl.pages) == 0 || len(tbl.pages[len(tbl.pages)-1]) == heapPageRows {
		tbl.pages = append(tbl.pages, make([]*version, 0, heapPageRows))
	}
	page := len(tbl.pages) - 1
	tbl.pages[page] = append(tbl.pages[page], row)
	tbl.heads[ch.ID] = row
	for i := range tbl.indexes {
		idx := &tbl.indexes[i]
		encoded, _ := key(ch.Values, idx.def.Columns)
		idx.add(encoded, row)
	}
}
