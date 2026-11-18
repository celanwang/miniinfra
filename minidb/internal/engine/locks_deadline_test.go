package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLockRejectsCancellationAndExpirationBeforeGrant(t *testing.T) {
	db := &DB{lockTimeout: defaultLockTimeout, locks: lockManager{
		holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn),
	}}
	tx := &Txn{db: db, held: make(map[resource]bool)}
	r := resource{table: "orders", key: "order"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	check := func(ctx context.Context, want error) {
		t.Helper()
		if err := db.locks.acquire(ctx, tx, r); !errors.Is(err, ErrLockWait) || !errors.Is(err, want) {
			t.Fatalf("acquire = %v, want a lock wait with %v", err, want)
		}
	}
	check(ctx, context.Canceled)
	if len(tx.held) != 0 || len(db.locks.holders) != 0 {
		t.Fatal("granted an unowned lock to a canceled request")
	}
	if err := db.locks.acquire(t.Context(), tx, r); err != nil {
		t.Fatal(err)
	}
	check(ctx, context.Canceled)
	expired, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	check(expired, context.DeadlineExceeded)
	db.lockTimeout = 0
	check(t.Context(), context.DeadlineExceeded)
}

func TestLockDeadlineIncludesManagerWait(t *testing.T) {
	db := &DB{lockTimeout: 30 * time.Millisecond, locks: lockManager{
		holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn),
	}}
	tx := &Txn{db: db, held: make(map[resource]bool)}
	r := resource{table: "orders", key: "order"}
	entered := make(chan struct{}, 1)
	result := make(chan error, 1)
	db.locks.mu.Lock()
	go func() {
		result <- db.locks.acquire(lockDeadlineObserver{t.Context(), entered}, tx, r)
	}()
	<-entered
	time.Sleep(2 * db.lockTimeout)
	db.locks.mu.Unlock()
	if err := <-result; !errors.Is(err, ErrLockWait) || !errors.Is(err, context.DeadlineExceeded) || len(tx.held) != 0 {
		t.Fatalf("acquire after deadline: holders=%v err=%v", tx.held, err)
	}
}

func TestLockWaitUsesEarlierCallerDeadline(t *testing.T) {
	db := &DB{lockTimeout: defaultLockTimeout, locks: lockManager{
		holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn),
	}}
	owner := &Txn{db: db, held: make(map[resource]bool), done: make(chan struct{})}
	waiter := &Txn{db: db, held: make(map[resource]bool)}
	r := resource{table: "orders", key: "order"}
	if err := db.locks.acquire(t.Context(), owner, r); err != nil {
		t.Fatal(err)
	}
	defer db.locks.release(owner)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- db.locks.acquire(ctx, waiter, r) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrLockWait) || !errors.Is(err, context.DeadlineExceeded) || len(waiter.held) != 0 {
			t.Fatalf("caller deadline: held=%v err=%v", waiter.held, err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait ignored the caller's earlier deadline")
	}
}

type lockDeadlineObserver struct {
	context.Context
	entered chan<- struct{}
}

func (ctx lockDeadlineObserver) Deadline() (time.Time, bool) {
	select {
	case ctx.entered <- struct{}{}:
	default:
	}
	return ctx.Context.Deadline()
}

func TestLockWaitKeepsDeadlineAcrossOwnerChanges(t *testing.T) {
	const timeout = 400 * time.Millisecond
	db := &DB{lockTimeout: timeout, locks: lockManager{
		holders: make(map[resource]*Txn), waiting: make(map[*Txn]*Txn),
	}}
	var owners [3]*Txn
	for i := range owners {
		owners[i] = &Txn{db: db, held: make(map[resource]bool), done: make(chan struct{})}
	}
	waiter := &Txn{db: db, held: make(map[resource]bool)}
	r := resource{table: "orders", namespace: "request", key: "order"}
	if err := db.locks.acquire(t.Context(), owners[0], r); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result := make(chan error, 1)
	go func() { result <- db.locks.acquire(t.Context(), waiter, r) }()

	for i, pause := range []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 0} {
		for {
			db.locks.mu.Lock()
			waiting := db.locks.waiting[waiter] == owners[i]
			db.locks.mu.Unlock()
			if waiting {
				break
			}
			select {
			case err := <-result:
				t.Fatalf("waiter left before observing owner %d: %v", i, err)
			case <-time.After(time.Millisecond):
			}
		}
		if i == len(owners)-1 {
			break
		}
		<-time.After(time.Until(started.Add(pause)))
		// Simulate another owner acquiring the lock before the awakened
		// waiter reacquires the lock-manager mutex.
		db.locks.mu.Lock()
		delete(owners[i].held, r)
		db.locks.holders[r] = owners[i+1]
		owners[i+1].held[r] = true
		close(owners[i].done)
		db.locks.mu.Unlock()
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrLockWait) || !errors.Is(err, context.DeadlineExceeded) || len(waiter.held) != 0 {
			t.Fatalf("waiter completed with %v, held %v", err, waiter.held)
		}
	case <-time.After(time.Until(started.Add(timeout + 150*time.Millisecond))):
		t.Fatal("wait extended past the original deadline")
	}
}
