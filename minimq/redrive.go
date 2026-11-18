package minimq

import (
	"fmt"
	"time"
)

const redriveInterval = 100 * time.Millisecond

type RedriveResult struct {
	Instance           string
	SourceDeadLetterID string
}

type redriveReceipt struct {
	Result  RedriveResult
	Expires time.Time
}

type redriveTask struct {
	Instance, Subscription, SourceDeadLetterID string
	Message                                    Message
	Sequence                                   uint64
	State                                      taskState
}

type redriveAdmission struct {
	ReceiptKey string
	Receipt    redriveReceipt
	Task       redriveTask
}

func (t redriveTask) update() taskUpdate {
	return taskUpdate{Subscription: t.Subscription, Redrive: t.Instance, State: t.State}
}

func (p *Partition) applyRedrive(r redriveAdmission) error {
	if _, found := p.redrives[r.Task.Instance]; found || p.subscriptions[r.Task.Subscription] == nil {
		return fmt.Errorf("%w: invalid redrive admission", ErrCorrupt)
	}
	p.redrives[r.Task.Instance] = r.Task
	p.redriveReceipts[r.ReceiptKey] = r.Receipt
	return nil
}

// Redrive enqueues a new task for the original subscription. Within receipt
// retention, repeating an operation ID in that subscription returns the same instance.
func (p *Partition) Redrive(name, deadID, operationID string) (RedriveResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return RedriveResult{}, err
	}
	if !validID(operationID) || !validID(deadID) {
		return RedriveResult{}, ErrInvalid
	}
	sub := p.subscriptions[name]
	if sub == nil {
		return RedriveResult{}, ErrSubscription
	}
	now, err := p.clock()
	if err != nil {
		return RedriveResult{}, err
	}
	key := fmt.Sprintf("%d:%s%s", len(name), name, operationID)
	if receipt, found := p.redriveReceipts[key]; found && now.Lower.Before(receipt.Expires) {
		if receipt.Result.SourceDeadLetterID != deadID {
			return RedriveResult{}, ErrConflict
		}
		return receipt.Result, nil
	}
	dead, err := p.deadLetter(name, deadID, now)
	if err != nil {
		return RedriveResult{}, err
	}
	token, err := newToken()
	if err != nil {
		return RedriveResult{}, err
	}
	result := RedriveResult{Instance: "redrive:" + token, SourceDeadLetterID: deadID}
	reserve := sub.Config.taskReserve(len(dead.Message.Body))
	// Stored bodies are immutable. The task retains a reference in memory
	// and carries the full body in its durable record.
	tx := transaction{
		Lower: now.Lower, ReserveDelta: reserve,
		Redrive: &redriveAdmission{
			ReceiptKey: key,
			Receipt:    redriveReceipt{Result: result, Expires: now.Upper.Add(p.config.DedupRetention)},
			Task: redriveTask{
				Instance: result.Instance, Subscription: name, SourceDeadLetterID: deadID,
				Message: dead.Message, Sequence: p.sequence + 1, State: taskState{Remaining: reserve},
			},
		},
	}
	if err := p.commit(tx); err != nil {
		return RedriveResult{}, err
	}
	return result, nil
}

func (p *Partition) readyRedrive(name string, now TimeBounds, bytes int) (redriveTask, bool) {
	var chosen redriveTask
	for _, task := range p.redrives {
		if task.Subscription != name || task.State.Lease.Token != "" ||
			now.Lower.Before(task.State.ReadyAt) || len(task.Message.Body) > bytes {
			continue
		}
		if chosen.Instance == "" || task.Sequence < chosen.Sequence {
			chosen = task
		}
	}
	return chosen, chosen.Instance != ""
}
