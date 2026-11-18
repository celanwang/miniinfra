package minimq

import (
	"bytes"
	"sort"
	"time"
)

type DeadLetter struct {
	ID, Subscription, Instance string
	Message                    Message
	Attempts                   int
	Reason                     string
	SourceDeadLetterID         string
	DeadAt, Expires            time.Time
}

type FailureResult struct {
	RetryAt      time.Time
	DeadLetterID string
}

// Fail settles the current attempt, either scheduling a retry or creating a dead letter.
func (p *Partition) Fail(name, worker, instance, token, reason string) (FailureResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return FailureResult{}, err
	}
	if !validID(reason) {
		return FailureResult{}, ErrInvalid
	}
	update, err := p.lookupTask(name, instance)
	if err != nil {
		return FailureResult{}, err
	}
	now, clockErr := p.clock()
	if err := validateLease(update.State.Lease, now, clockErr, worker, token); err != nil {
		return FailureResult{}, err
	}
	tx := transaction{Lower: now.Lower}
	result, err := p.recordFailure(&tx, update, reason, now.Upper, now.Upper)
	if err != nil {
		return FailureResult{}, err
	}
	if err := p.commit(tx); err != nil {
		return FailureResult{}, err
	}
	return result, nil
}

func (p *Partition) recordFailure(tx *transaction, update taskUpdate, reason string, at, settledAt time.Time) (FailureResult, error) {
	cfg := p.subscriptions[update.Subscription].Config
	if update.State.Attempts < cfg.MaxAttempts {
		update.State.Lease = leaseState{}
		update.State.ReadyAt = at.Add(cfg.RetryDelay)
		update.State.Remaining -= transitionReserve
		tx.ReserveDelta -= transitionReserve
		tx.Tasks = append(tx.Tasks, update)
		return FailureResult{RetryAt: update.State.ReadyAt}, nil
	}
	id, err := newToken()
	if err != nil {
		return FailureResult{}, err
	}
	tx.Dead = append(tx.Dead, DeadLetter{
		ID: id, Subscription: update.Subscription, Instance: update.instance(),
		Message: p.messageForTask(update), Attempts: update.State.Attempts,
		Reason: reason, DeadAt: settledAt, Expires: settledAt.Add(p.config.DeadRetention),
		SourceDeadLetterID: p.redrives[update.Redrive].SourceDeadLetterID,
	})
	tx.ReserveDelta -= update.State.Remaining
	update.State = taskState{Finished: true}
	tx.Tasks = append(tx.Tasks, update)
	return FailureResult{DeadLetterID: id}, nil
}

// AdvanceRetries settles a bounded batch of leases known to have expired.
func (p *Partition) AdvanceRetries(limit Limits) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	if !limit.valid() {
		return 0, ErrInvalid
	}
	now, err := p.clock()
	if err != nil {
		return 0, err
	}
	return p.expireLocked("", now, limit)
}

func (p *Partition) expireLocked(name string, now TimeBounds, limit Limits) (int, error) {
	var expired []taskUpdate
	for subName, sub := range p.subscriptions {
		if name != "" && name != subName {
			continue
		}
		for offset, state := range sub.Tasks {
			if state.Lease.Token != "" && !now.Lower.Before(state.Lease.Deadline) {
				expired = append(expired, taskUpdate{Subscription: subName, Offset: offset, State: state})
			}
		}
	}
	for _, task := range p.redrives {
		if (name == "" || name == task.Subscription) && task.State.Lease.Token != "" &&
			!now.Lower.Before(task.State.Lease.Deadline) {
			expired = append(expired, task.update())
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		a, b := expired[i], expired[j]
		if !a.State.Lease.Deadline.Equal(b.State.Lease.Deadline) {
			return a.State.Lease.Deadline.Before(b.State.Lease.Deadline)
		}
		if a.Subscription != b.Subscription {
			return a.Subscription < b.Subscription
		}
		return a.instance() < b.instance()
	})
	tx := transaction{Lower: now.Lower}
	for _, update := range expired {
		size := 0
		if update.State.Attempts >= p.subscriptions[update.Subscription].Config.MaxAttempts {
			size = len(p.messageForTask(update).Body)
		}
		if len(tx.Tasks) == limit.Messages || size > limit.Bytes {
			break
		}
		if _, err := p.recordFailure(&tx, update, "lease expired", update.State.Lease.Deadline, now.Upper); err != nil {
			return 0, err
		}
		limit.Bytes -= size
	}
	if len(tx.Tasks) == 0 {
		if len(expired) > 0 {
			return 0, ErrInvalid
		}
		return 0, nil
	}
	if err := p.commit(tx); err != nil {
		return 0, err
	}
	return len(tx.Tasks), nil
}

func (p *Partition) GetDeadLetter(name, id string) (DeadLetter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return DeadLetter{}, err
	}
	now, err := p.clock()
	if err != nil {
		return DeadLetter{}, err
	}
	letter, err := p.deadLetter(name, id, now)
	letter.Message.Body = bytes.Clone(letter.Message.Body)
	return letter, err
}

func (p *Partition) deadLetter(name, id string, now TimeBounds) (DeadLetter, error) {
	letter, found := p.dead[id]
	if !found || letter.Subscription != name || !now.Lower.Before(letter.Expires) {
		return DeadLetter{}, ErrNotFound
	}
	return letter, nil
}

// ListDeadLetters lists retained letters by ID, strictly after the supplied cursor.
func (p *Partition) ListDeadLetters(name, after string, limit int) ([]DeadLetter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxBatchMessages {
		return nil, ErrInvalid
	}
	if p.subscriptions[name] == nil {
		return nil, ErrSubscription
	}
	now, err := p.clock()
	if err != nil {
		return nil, err
	}
	var ids []string
	for id, letter := range p.dead {
		if id > after && letter.Subscription == name && now.Lower.Before(letter.Expires) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	var letters []DeadLetter
	bytesLeft := MaxBatchBytes
	for _, id := range ids[:min(limit, len(ids))] {
		letter := p.dead[id]
		if len(letter.Message.Body) > bytesLeft {
			break
		}
		bytesLeft -= len(letter.Message.Body)
		letter.Message.Body = bytes.Clone(letter.Message.Body)
		letters = append(letters, letter)
	}
	return letters, nil
}
