package engine

import (
	"sync"

	"github.com/celanwang/miniinfra/minidb/internal/wal"
)

type CheckpointLoadObservation struct {
	Commits     []wal.CommitTiming
	Checkpoints []checkpointTiming
}

// ObserveCheckpointLoad installs the observer before the TCP listener starts.
func (db *DB) ObserveCheckpointLoad() func() CheckpointLoadObservation {
	var mu sync.Mutex
	var sample CheckpointLoadObservation
	db.checkpointObserver = &checkpointObserver{
		commit: func(timing wal.CommitTiming) {
			mu.Lock()
			sample.Commits = append(sample.Commits, timing)
			mu.Unlock()
		},
		checkpoint: func(timing checkpointTiming) {
			mu.Lock()
			sample.Checkpoints = append(sample.Checkpoints, timing)
			mu.Unlock()
		},
	}
	return func() CheckpointLoadObservation {
		mu.Lock()
		defer mu.Unlock()
		collected := sample
		sample = CheckpointLoadObservation{}
		return collected
	}
}
