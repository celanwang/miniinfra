package minimq

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrSubscription   = errors.New("subscription does not exist")
	ErrLeaseExpired   = errors.New("lease has expired or was superseded")
	ErrOutcomeUnknown = errors.New("acknowledgment outcome is unknown")
)

const (
	maxSubscriptions  = 32
	transitionReserve = 8192
)

type SubscriptionConfig struct {
	LeaseDuration time.Duration
	MaxAttempts   int
	Window        uint64
	RetryDelay    time.Duration
}

func (s SubscriptionConfig) defaults() SubscriptionConfig {
	if s.LeaseDuration == 0 {
		s.LeaseDuration = 30 * time.Second
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 3
	}
	if s.Window == 0 {
		s.Window = 4096
	}
	return s
}

func (s SubscriptionConfig) valid() bool {
	return s.LeaseDuration > 0 && s.LeaseDuration <= 24*time.Hour &&
		s.MaxAttempts > 0 && s.MaxAttempts <= 32 &&
		s.Window > 0 && s.Window <= 65536 &&
		s.RetryDelay >= 0 && s.RetryDelay <= 24*time.Hour
}

func (s SubscriptionConfig) taskReserve(bodyBytes int) int64 {
	return int64(s.MaxAttempts*2+2)*transitionReserve + 2*int64(bodyBytes)
}

type Delivery struct {
	Message  Message
	Instance string
	Token    string
	Attempt  int
	Deadline time.Time
}

type AckResult string

const (
	AckAccepted           AckResult = "accepted"
	AckPreviouslyAccepted AckResult = "previously_accepted"
)

type registration struct {
	Name   string
	Config SubscriptionConfig
}

type subscription struct {
	Config        SubscriptionConfig
	Registered    uint64
	Floor         uint64
	Tasks         map[uint64]taskState
	NextRedriveAt time.Time
}

type leaseState struct {
	Token, Worker string
	Deadline      time.Time
}

type taskState struct {
	Attempts  int
	Remaining int64
	Lease     leaseState
	ReadyAt   time.Time
	Finished  bool
}

type taskUpdate struct {
	Subscription  string
	Offset        uint64
	Redrive       string
	NextRedriveAt time.Time
	State         taskState
}

type ackReceipt struct {
	Subscription, Worker, Instance, Token string
	Expires                               time.Time
}

func (p *Partition) publicationReserve(bodyBytes int) int64 {
	var reserve int64
	for _, sub := range p.subscriptions {
		reserve += sub.Config.taskReserve(bodyBytes)
	}
	return reserve
}

func (p *Partition) applySubscriptions(tx transaction) error {
	if r := tx.Register; r != nil {
		if _, exists := p.subscriptions[r.Name]; exists {
			return fmt.Errorf("%w: repeated subscription registration", ErrCorrupt)
		}
		p.subscriptions[r.Name] = &subscription{
			Config: r.Config, Registered: tx.Sequence, Floor: p.nextOffset(),
			Tasks: make(map[uint64]taskState),
		}
	}
	for _, u := range tx.Tasks {
		sub := p.subscriptions[u.Subscription]
		if u.Redrive != "" {
			task, exists := p.redrives[u.Redrive]
			if sub == nil || !exists || task.Subscription != u.Subscription {
				return fmt.Errorf("%w: invalid redrive state update", ErrCorrupt)
			}
			if !u.NextRedriveAt.IsZero() {
				sub.NextRedriveAt = u.NextRedriveAt
			}
			if u.State.Finished {
				delete(p.redrives, u.Redrive)
			} else {
				task.State = u.State
				p.redrives[u.Redrive] = task
			}
			continue
		}
		if sub == nil || u.Offset < sub.Floor || u.Offset >= p.nextOffset() {
			return fmt.Errorf("%w: invalid subscription update", ErrCorrupt)
		}
		sub.Tasks[u.Offset] = u.State
	}
	if tx.Ack != nil {
		p.acknowledgments[tx.Ack.Token] = *tx.Ack
	}
	for _, sub := range p.subscriptions {
		for sub.Floor < p.nextOffset() {
			if !sub.Tasks[sub.Floor].Finished && p.locateHistory(sub.Floor).record.accepted > sub.Registered {
				break
			}
			delete(sub.Tasks, sub.Floor)
			sub.Floor++
		}
	}
	return nil
}

// Register starts a durable subscription at the next accepted publication.
func (p *Partition) Register(name string, config SubscriptionConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	config = config.defaults()
	if !validID(name) || !config.valid() {
		return ErrInvalid
	}
	if existing := p.subscriptions[name]; existing != nil {
		if existing.Config == config {
			return nil
		}
		return ErrConflict
	}
	if len(p.subscriptions) >= maxSubscriptions {
		return ErrCapacity
	}
	now, err := p.clock()
	if err != nil {
		return err
	}
	return p.commit(transaction{Lower: now.Lower, Register: &registration{Name: name, Config: config}})
}

func newToken() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

// Pull grants a batch of independent durable leases to worker.
func (p *Partition) Pull(name, worker string, limit Limits) ([]Delivery, error) {
	sample := p.beginOperation()
	sample.worker = worker
	defer p.endOperation("pull", sample)
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	if !validID(worker) || !limit.valid() {
		return nil, ErrInvalid
	}
	sub := p.subscriptions[name]
	if sub == nil {
		return nil, ErrSubscription
	}
	now, err := p.clock()
	if err != nil {
		return nil, err
	}
	if _, err := p.expireLocked(name, now, Limits{Messages: MaxBatchMessages, Bytes: MaxBatchBytes}); err != nil {
		return nil, err
	}
	var deliveries []Delivery
	tx := transaction{Lower: now.Lower}
	if !now.Lower.Before(sub.NextRedriveAt) {
		if task, found := p.readyRedrive(name, now, limit.Bytes); found {
			update := task.update()
			update.NextRedriveAt = now.Upper.Add(redriveInterval)
			msg := task.Message
			msg.Body = bytes.Clone(msg.Body)
			delivery, err := p.grant(&tx, update, msg, worker, now)
			if err != nil {
				return nil, err
			}
			deliveries = append(deliveries, delivery)
			limit.Bytes -= len(task.Message.Body)
		}
	}
	end := min(p.nextOffset(), sub.Floor+sub.Config.Window)
	reader := historyReader{dir: p.journal.Dir()}
	defer reader.close()
	for offset := sub.Floor; offset < end && len(deliveries) < limit.Messages; offset++ {
		state, known := sub.Tasks[offset]
		if state.Finished || state.Attempts >= sub.Config.MaxAttempts ||
			state.Lease.Token != "" || now.Lower.Before(state.ReadyAt) {
			continue
		}
		loc := p.locateHistory(offset)
		if loc.record.accepted <= sub.Registered || loc.record.bodyBytes > limit.Bytes {
			continue
		}
		if !known {
			state.Remaining = sub.Config.taskReserve(loc.record.bodyBytes)
		}
		msg, err := p.readHistoryLocked(&reader, offset, loc)
		if err != nil {
			return nil, err
		}
		delivery, err := p.grant(&tx, taskUpdate{Subscription: name, Offset: offset, State: state}, msg, worker, now)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
		limit.Bytes -= len(msg.Body)
	}
	if len(deliveries) != 0 {
		if err := p.commit(tx); err != nil {
			return nil, err
		}
	}
	return deliveries, nil
}

func historyInstance(offset uint64) string { return fmt.Sprintf("history:%d", offset) }

func (u taskUpdate) instance() string {
	if u.Redrive != "" {
		return u.Redrive
	}
	return historyInstance(u.Offset)
}

func (p *Partition) grant(tx *transaction, update taskUpdate, msg Message, worker string, now TimeBounds) (Delivery, error) {
	token, err := newToken()
	if err != nil {
		return Delivery{}, err
	}
	update.State.Attempts++
	update.State.Remaining -= transitionReserve
	deadline := now.Upper.Add(p.subscriptions[update.Subscription].Config.LeaseDuration)
	update.State.Lease = leaseState{Token: token, Worker: worker, Deadline: deadline}
	tx.Tasks = append(tx.Tasks, update)
	tx.ReserveDelta -= transitionReserve
	return Delivery{
		Message: msg, Instance: update.instance(), Token: token, Attempt: update.State.Attempts, Deadline: deadline,
	}, nil
}

func (p *Partition) lookupTask(name, instance string) (taskUpdate, error) {
	sub := p.subscriptions[name]
	if sub == nil {
		return taskUpdate{}, ErrSubscription
	}
	if strings.HasPrefix(instance, "redrive:") {
		task, exists := p.redrives[instance]
		if !exists || task.Subscription != name {
			return taskUpdate{}, ErrOutcomeUnknown
		}
		return task.update(), nil
	}
	var offset uint64
	if _, err := fmt.Sscanf(instance, "history:%d", &offset); err != nil || historyInstance(offset) != instance {
		return taskUpdate{}, ErrInvalid
	}
	state, exists := sub.Tasks[offset]
	if !exists || state.Finished {
		return taskUpdate{}, ErrOutcomeUnknown
	}
	return taskUpdate{Subscription: name, Offset: offset, State: state}, nil
}

func validateLease(current leaseState, now TimeBounds, clockErr error, worker, token string) error {
	if current.Token == "" || current.Token != token || current.Worker != worker {
		return ErrLeaseExpired
	}
	if clockErr != nil {
		return clockErr
	}
	if !now.Lower.Before(current.Deadline) {
		return ErrLeaseExpired
	}
	if !now.Upper.Before(current.Deadline) {
		return ErrClockUncertain
	}
	return nil
}

// Ack confirms a single lease and can be retried with the same token.
func (p *Partition) Ack(name, worker, instance, token string) (AckResult, error) {
	sample := p.beginOperation()
	sample.worker = worker
	defer p.endOperation("ack", sample)
	if err := p.checkOpen(); err != nil {
		return "", err
	}
	sub := p.subscriptions[name]
	if sub == nil {
		return "", ErrSubscription
	}
	now, clockErr := p.clock()
	if receipt, ok := p.acknowledgments[token]; ok && receipt.Subscription == name && receipt.Worker == worker && receipt.Instance == instance {
		if clockErr != nil || now.Lower.Before(receipt.Expires) {
			return AckPreviouslyAccepted, nil
		}
		return "", ErrOutcomeUnknown
	}
	update, err := p.lookupTask(name, instance)
	if err != nil {
		return "", err
	}
	if err := validateLease(update.State.Lease, now, clockErr, worker, token); err != nil {
		return "", err
	}
	remaining := update.State.Remaining
	update.State = taskState{Finished: true}
	tx := transaction{
		Lower: now.Lower, ReserveDelta: -remaining,
		Tasks: []taskUpdate{update},
		Ack:   &ackReceipt{Subscription: name, Worker: worker, Instance: instance, Token: token, Expires: now.Upper.Add(p.config.DedupRetention)},
	}
	if err := p.commit(tx); err != nil {
		return "", err
	}
	return AckAccepted, nil
}
