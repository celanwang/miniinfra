package minimq

import (
	"bytes"
	"slices"
	"strings"
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
	reader := historyReader{dir: p.journal.Dir()}
	defer reader.close()
	result, err := p.recordFailure(&tx, &reader, update, reason, now.Upper, now.Upper)
	if err != nil {
		return FailureResult{}, err
	}
	if err := p.commit(tx); err != nil {
		return FailureResult{}, err
	}
	return result, nil
}

func (p *Partition) recordFailure(tx *transaction, reader *historyReader, update taskUpdate, reason string, at, settledAt time.Time) (FailureResult, error) {
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
	task := p.redrives[update.Redrive]
	msg := task.Message
	if update.Redrive == "" {
		var start time.Time
		if p.timings != nil {
			start = time.Now()
		}
		msg, err = p.readHistoryLocked(reader, update.Offset, p.locateHistory(update.Offset))
		if p.timings != nil {
			p.timings.retry.read += time.Since(start)
		}
		if err != nil {
			return FailureResult{}, err
		}
	}
	tx.Dead = append(tx.Dead, DeadLetter{
		ID: id, Subscription: update.Subscription, Instance: update.instance(),
		Message: msg, Attempts: update.State.Attempts,
		Reason: reason, DeadAt: settledAt, Expires: settledAt.Add(p.config.DeadRetention),
		SourceDeadLetterID: task.SourceDeadLetterID,
	})
	tx.ReserveDelta -= update.State.Remaining
	update.State = taskState{Finished: true}
	tx.Tasks = append(tx.Tasks, update)
	return FailureResult{DeadLetterID: id}, nil
}

// AdvanceRetries settles a bounded batch of leases known to have expired.
func (p *Partition) AdvanceRetries(limit Limits) (int, error) {
	sample := p.beginOperation()
	defer p.endOperation("retry", sample)
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

type expiredLease struct {
	update   taskUpdate
	instance string
}

func compareExpiredLeases(a, b expiredLease) int {
	if deadline := a.update.State.Lease.Deadline.Compare(b.update.State.Lease.Deadline); deadline != 0 {
		return deadline
	}
	if subscription := strings.Compare(a.update.Subscription, b.update.Subscription); subscription != 0 {
		return subscription
	}
	return strings.Compare(a.instance, b.instance)
}

func (p *Partition) expireLocked(name string, now TimeBounds, limit Limits) (int, error) {
	var stats *retryTiming
	var start time.Time
	if p.timings != nil {
		stats = &p.timings.retry
		start = time.Now()
	}
	var expired []expiredLease
	order := func() {
		var began time.Time
		if stats != nil {
			began = time.Now()
		}
		slices.SortFunc(expired, compareExpiredLeases)
		if stats != nil {
			stats.order += time.Since(began)
		}
	}
	collect := func(update taskUpdate) {
		if stats != nil {
			stats.candidates++
		}
		expired = append(expired, expiredLease{update: update, instance: update.instance()})
		// A batch can only commit a prefix of this order. A candidate
		// behind the first limit.Messages can never enter that prefix.
		if len(expired) == 2*limit.Messages {
			order()
			clear(expired[limit.Messages:])
			expired = expired[:limit.Messages]
		}
	}
	scanRedrives := name == ""
	for subName, sub := range p.subscriptions {
		if name != "" && name != subName {
			continue
		}
		scanRedrives = scanRedrives || sub.redriveCount != 0
		if stats != nil {
			stats.visited += len(sub.Tasks)
		}
		for offset, state := range sub.Tasks {
			if state.Lease.Token != "" && !now.Lower.Before(state.Lease.Deadline) {
				collect(taskUpdate{Subscription: subName, Offset: offset, State: state})
			}
		}
	}
	if scanRedrives {
		if stats != nil {
			stats.visited += len(p.redrives)
		}
		for _, task := range p.redrives {
			if (name == "" || name == task.Subscription) && task.State.Lease.Token != "" &&
				!now.Lower.Before(task.State.Lease.Deadline) {
				collect(task.update())
			}
		}
	}
	if stats != nil {
		stats.scan = time.Since(start) - stats.order
	}
	order()
	expired = expired[:min(len(expired), limit.Messages)]
	if stats != nil {
		start = time.Now()
	}
	tx := transaction{Lower: now.Lower}
	reader := historyReader{dir: p.journal.Dir()}
	if stats != nil {
		reader.decodeTime = &stats.decode
	}
	defer reader.close()
	for _, candidate := range expired {
		update := candidate.update
		size := 0
		if update.State.Attempts >= p.subscriptions[update.Subscription].Config.MaxAttempts {
			if update.Redrive != "" {
				size = len(p.redrives[update.Redrive].Message.Body)
			} else {
				size = p.locateHistory(update.Offset).record.bodyBytes
			}
		}
		if size > limit.Bytes {
			break
		}
		if _, err := p.recordFailure(&tx, &reader, update, "lease expired", update.State.Lease.Deadline, now.Upper); err != nil {
			return 0, err
		}
		if stats != nil {
			stats.deadBytes += size
		}
		limit.Bytes -= size
	}
	if stats != nil {
		stats.prepare = time.Since(start)
		stats.settled, stats.dead = len(tx.Tasks), len(tx.Dead)
		start = time.Now()
	}
	if len(tx.Tasks) == 0 {
		if len(expired) > 0 {
			return 0, ErrInvalid
		}
		return 0, nil
	}
	err := p.commit(tx)
	if stats != nil {
		stats.commit = time.Since(start)
	}
	if err != nil {
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
	sample := p.beginOperation()
	defer func() { p.endOperation("dead-list", sample) }()
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
	var started time.Time
	if p.timings != nil {
		started = time.Now()
	}
	var ids []string
	for id, letter := range p.dead {
		if id <= after || letter.Subscription != name || !now.Lower.Before(letter.Expires) {
			continue
		}
		// Once full, only an ID before the current last entry can enter the page.
		if len(ids) == limit && id >= ids[len(ids)-1] {
			continue
		}
		if ids == nil {
			ids = make([]string, 0, limit)
		}
		at, _ := slices.BinarySearch(ids, id)
		if len(ids) < limit {
			ids = append(ids, "")
		}
		copy(ids[at+1:], ids[at:])
		ids[at] = id
	}
	if p.timings != nil {
		sample.page.selectIDs = time.Since(started)
		started = time.Now()
	}
	var letters []DeadLetter
	bytesLeft := MaxBatchBytes
	for _, id := range ids {
		letter := p.dead[id]
		if len(letter.Message.Body) > bytesLeft {
			break
		}
		bytesLeft -= len(letter.Message.Body)
		letter.Message.Body = bytes.Clone(letter.Message.Body)
		letters = append(letters, letter)
	}
	if p.timings != nil {
		sample.page.clone = time.Since(started)
	}
	return letters, nil
}
