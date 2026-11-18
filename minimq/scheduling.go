package minimq

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("record not found or retention expired")

type scheduledPublication struct {
	ID        string
	Body      []byte
	DeliverAt time.Time
	Sequence  uint64
	Receipt   publishReceipt
}

type scheduleEntry struct {
	ID       string
	At       time.Time
	Sequence uint64
}

type scheduleQueue []scheduleEntry

func (q scheduleQueue) Len() int { return len(q) }
func (q scheduleQueue) Less(i, j int) bool {
	if q[i].At.Equal(q[j].At) {
		return q[i].Sequence < q[j].Sequence
	}
	return q[i].At.Before(q[j].At)
}
func (q scheduleQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *scheduleQueue) Push(v any)   { *q = append(*q, v.(scheduleEntry)) }
func (q *scheduleQueue) Pop() any {
	last := len(*q) - 1
	entry := (*q)[last]
	(*q)[last] = scheduleEntry{}
	*q = (*q)[:last]
	return entry
}

func scheduleReserve(size int) int64 { return 2*int64(size) + transitionReserve }

func (p *Partition) schedule(req PublishRequest, digest [32]byte, now TimeBounds) (PublishResult, error) {
	result := PublishResult{ID: req.ID, Scheduled: true}
	tx := transaction{
		Lower:        now.Lower,
		ReserveDelta: p.publicationReserve(len(req.Body)) + scheduleReserve(len(req.Body)),
		Schedule: &scheduledPublication{
			ID: req.ID, Body: bytes.Clone(req.Body), DeliverAt: req.DeliverAt.UTC(), Sequence: p.sequence + 1,
			Receipt: publishReceipt{Digest: digest, Result: result, DeliverAt: req.DeliverAt.UTC()},
		},
	}
	if err := p.commit(tx); err != nil {
		return PublishResult{}, err
	}
	return result, nil
}

func (p *Partition) applySchedule(tx transaction) error {
	if item := tx.Schedule; item != nil {
		if _, found := p.scheduled[item.ID]; found {
			return fmt.Errorf("%w: repeated schedule", ErrCorrupt)
		}
		p.scheduled[item.ID] = *item
		p.publishes[item.ID] = item.Receipt
		heap.Push(&p.due, scheduleEntry{ID: item.ID, At: item.DeliverAt, Sequence: item.Sequence})
	}
	for _, pub := range tx.Due {
		id := pub.Message.ID
		item, found := p.scheduled[id]
		if !found || item.Sequence != pub.AcceptedSequence || p.due.Len() == 0 ||
			p.due[0].ID != id || item.DeliverAt.After(tx.Lower) {
			return fmt.Errorf("%w: invalid scheduled publication", ErrCorrupt)
		}
		if err := p.applyPublication(pub, item.Sequence); err != nil {
			return err
		}
		heap.Pop(&p.due)
		delete(p.scheduled, id)
	}
	return nil
}

// PublishStatus reports whether a known publication still awaits its history offset.
func (p *Partition) PublishStatus(id string) (PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return PublishResult{}, err
	}
	receipt, ok := p.publishes[id]
	if !ok {
		return PublishResult{}, ErrNotFound
	}
	if _, pending := p.scheduled[id]; pending {
		return receipt.Result, nil
	}
	now, err := p.clock()
	if err != nil {
		return PublishResult{}, err
	}
	if !now.Lower.Before(receipt.Expires) {
		return PublishResult{}, ErrNotFound
	}
	return PublishResult{ID: id, Offset: receipt.VisibleOffset}, nil
}

// AdvanceScheduled publishes an ordered batch known to have reached its release time.
func (p *Partition) AdvanceScheduled(limit Limits) (int, error) {
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
	if p.due.Len() == 0 || p.due[0].At.After(now.Lower) {
		return 0, nil
	}
	tx := transaction{Lower: now.Lower}
	var selected []scheduleEntry
	for p.due.Len() != 0 && len(selected) < limit.Messages && !p.due[0].At.After(now.Lower) {
		item := p.scheduled[p.due[0].ID]
		if len(item.Body) > limit.Bytes {
			break
		}
		selected = append(selected, heap.Pop(&p.due).(scheduleEntry))
		offset := p.nextOffset() + uint64(len(tx.Due))
		receipt := item.Receipt
		receipt.VisibleOffset = offset
		receipt.Expires = now.Upper.Add(p.config.DedupRetention)
		tx.Due = append(tx.Due, publication{
			Message: Message{ID: item.ID, Offset: offset, Body: item.Body, PublishedAt: now.Upper},
			Receipt: receipt, AcceptedSequence: item.Sequence,
		})
		tx.ReserveDelta -= scheduleReserve(len(item.Body))
		limit.Bytes -= len(item.Body)
	}
	for _, entry := range selected {
		heap.Push(&p.due, entry)
	}
	if len(selected) == 0 {
		return 0, fmt.Errorf("%w: due message exceeds byte limit", ErrInvalid)
	}
	if err := p.commit(tx); err != nil {
		return 0, err
	}
	return len(selected), nil
}
