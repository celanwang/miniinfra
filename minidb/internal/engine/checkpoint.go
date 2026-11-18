package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/pages"
	"github.com/celanwang/miniinfra/minidb/internal/wal"
)

const checkpointFile = "minidb.pages"

type checkpointRecord struct {
	Schema *Schema
	ID     uint64
	Values []Value
}

type checkpointTiming struct {
	WAL         wal.CheckpointTiming
	Pages       pages.SaveTiming
	ReclaimWait time.Duration
}

type checkpointObserver struct {
	commit     func(wal.CommitTiming)
	checkpoint func(checkpointTiming)
}

// Checkpoint persists the latest published view. Active transactions may
// keep writing private versions, but publication waits at the WAL barrier.
func (db *DB) Checkpoint() error {
	if err := db.available(); err != nil {
		return err
	}
	if observer := db.checkpointObserver; observer != nil {
		var timing checkpointTiming
		err := db.log.CheckpointWithTiming(func(lsn int64) error {
			return db.writeCheckpoint(lsn, &timing)
		}, &timing.WAL)
		if err == nil {
			observer.checkpoint(timing)
		}
		return err
	}
	return db.log.Checkpoint(db.saveCheckpoint)
}

func (db *DB) saveCheckpoint(lsn int64) error {
	return db.writeCheckpoint(lsn, nil)
}

func (db *DB) writeCheckpoint(lsn int64, timing *checkpointTiming) error {
	var started time.Time
	if timing != nil {
		started = time.Now()
	}
	db.reclaimMu.Lock()
	defer db.reclaimMu.Unlock()
	var pageTiming *pages.SaveTiming
	if timing != nil {
		timing.ReclaimWait = time.Since(started)
		pageTiming = &timing.Pages
	}
	view := db.pinSnapshot(nil)
	defer view.Close()
	meta := pages.Metadata{LSN: lsn, Sequence: view.sequence, LastRowID: db.rowID.Load()}
	db.catalog.RLock()
	tables := maps.Clone(db.tables)
	db.catalog.RUnlock()
	return pages.SaveWithTiming(db.checkpointPath, meta, func(out io.Writer) error {
		enc := json.NewEncoder(out)
		var records []checkpointRecord
		for _, name := range slices.Sorted(maps.Keys(tables)) {
			tbl := tables[name]
			if err := enc.Encode(checkpointRecord{Schema: &tbl.schema}); err != nil {
				return err
			}
			tbl.mu.RLock()
			count := len(tbl.pages)
			tbl.mu.RUnlock()
			for i := range count {
				tbl.mu.RLock()
				for _, row := range tbl.pages[i] {
					if view.visible(row) {
						records = append(records, checkpointRecord{ID: row.id, Values: row.values})
					}
				}
				tbl.mu.RUnlock()
				for _, record := range records {
					if err := enc.Encode(record); err != nil {
						return err
					}
				}
				clear(records)
				records = records[:0]
			}
		}
		return nil
	}, pageTiming)
}

func (db *DB) loadCheckpoint() (int64, error) {
	lsn, err := pages.Load(db.checkpointPath, db.restorePages)
	if err != nil {
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return lsn, nil
}

func (db *DB) restorePages(meta pages.Metadata, in io.Reader) error {
	decoder := json.NewDecoder(in)
	state := &transactionState{}
	state.sequence.Store(meta.Sequence)
	var tbl *table
	for {
		var record checkpointRecord
		if err := decoder.Decode(&record); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if meta.Sequence == 0 {
			return errors.New("nonempty checkpoint with no published commits")
		}
		if record.Schema != nil {
			schema := *record.Schema
			if err := schema.validate(); err != nil {
				return err
			}
			if _, exists := db.tables[schema.Name]; exists || record.ID != 0 || len(record.Values) != 0 {
				return errors.New("invalid checkpoint table")
			}
			db.install(schema, meta.Sequence)
			tbl = db.tables[schema.Name]
			continue
		}
		if tbl == nil || record.ID == 0 || record.ID > meta.LastRowID || tbl.heads[record.ID] != nil {
			return errors.New("invalid checkpoint row identity")
		}
		if err := tbl.schema.checkRow(record.Values); err != nil {
			return err
		}
		tbl.apply(change{ID: record.ID, Kind: "insert", Values: record.Values}, state, nil)
	}
	db.published.Store(meta.Sequence)
	db.rowID.Store(meta.LastRowID)
	return nil
}
