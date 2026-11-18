package engine

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type resource struct {
	table     string
	namespace string
	key       string
}

type lockManager struct {
	mu      sync.Mutex
	holders map[resource]*Txn
	waiting map[*Txn]*Txn
}

func (locks *lockManager) acquire(ctx context.Context, tx *Txn, r resource) error {
	ctx, cancel := context.WithTimeout(ctx, tx.db.lockTimeout)
	defer cancel()
	for {
		var started time.Time
		if tx.timing != nil {
			started = time.Now()
		}
		locks.mu.Lock()
		if tx.timing != nil {
			tx.timing.lockManagerWait += time.Since(started)
		}
		delete(locks.waiting, tx)
		if err := ctx.Err(); err != nil {
			locks.mu.Unlock()
			return fmt.Errorf("%w: %w", ErrLockWait, err)
		}
		owner := locks.holders[r]
		if owner == nil || owner == tx {
			locks.holders[r] = tx
			tx.held[r] = true
			locks.mu.Unlock()
			return nil
		}
		for other := owner; other != nil; other = locks.waiting[other] {
			if other == tx {
				locks.mu.Unlock()
				return ErrDeadlock
			}
		}
		locks.waiting[tx] = owner
		locks.mu.Unlock()
		if tx.timing != nil {
			started = time.Now()
		}
		select {
		case <-owner.done:
		case <-ctx.Done():
		}
		if tx.timing != nil {
			if r.namespace == "" {
				tx.timing.rowWait += time.Since(started)
			} else {
				tx.timing.uniqueWait += time.Since(started)
			}
		}
	}
}

func (locks *lockManager) release(tx *Txn) {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	delete(locks.waiting, tx)
	for r := range tx.held {
		delete(locks.holders, r)
	}
	close(tx.done)
}

const defaultLockTimeout = 5 * time.Second
