// Package minimq implements the durable state of a single message partition.
package minimq

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/celanwang/miniinfra/minimq/internal/journal"
)

var (
	ErrClosed         = errors.New("partition is closed")
	ErrConflict       = errors.New("request ID already used for a different request")
	ErrInvalid        = errors.New("invalid request")
	ErrCapacity       = errors.New("partition storage budget exhausted")
	ErrClockUncertain = errors.New("partition time cannot be determined")
	ErrCorrupt        = journal.ErrCorrupt
)

// TimeBounds is a trusted interval containing the current instant.
type TimeBounds struct {
	Lower, Upper time.Time
}

type Config struct {
	Clock            func() (TimeBounds, error)
	CapacityBytes    int64
	DedupRetention   time.Duration
	HistoryRetention time.Duration
	DeadRetention    time.Duration
	SegmentMessages  int
}

func (c Config) defaults() Config {
	if c.Clock == nil {
		c.Clock = func() (TimeBounds, error) {
			now := time.Now().UTC()
			return TimeBounds{Lower: now, Upper: now}, nil
		}
	}
	if c.CapacityBytes == 0 {
		c.CapacityBytes = 1 << 30
	}
	if c.DedupRetention == 0 {
		c.DedupRetention = 24 * time.Hour
	}
	if c.HistoryRetention == 0 {
		c.HistoryRetention = 24 * time.Hour
	}
	if c.DeadRetention == 0 {
		c.DeadRetention = 7 * 24 * time.Hour
	}
	if c.SegmentMessages == 0 {
		c.SegmentMessages = 256
	}
	return c
}

const (
	MaxMessageBytes    = 1 << 20
	MaxBatchBytes      = 8 << 20
	MaxBatchMessages   = 256
	maxIdentifierBytes = 256
)

type Limits struct {
	Messages, Bytes int
}

func (l Limits) valid() bool {
	return l.Messages > 0 && l.Messages <= MaxBatchMessages &&
		l.Bytes > 0 && l.Bytes <= MaxBatchBytes
}

type Message struct {
	ID          string
	Offset      uint64
	Body        []byte
	PublishedAt time.Time
}

type PublishRequest struct {
	ID        string
	Body      []byte
	DeliverAt time.Time
}

type PublishResult struct {
	ID        string
	Offset    uint64
	Scheduled bool
}

type HistoryPage struct {
	Messages []Message
	Next     uint64
}

type OffsetGap struct {
	Requested, First uint64
}

func (g *OffsetGap) Error() string {
	return fmt.Sprintf("offset %d is below readable start %d", g.Requested, g.First)
}

type publishReceipt struct {
	Digest        [32]byte
	Result        PublishResult
	Expires       time.Time
	VisibleOffset uint64
	DeliverAt     time.Time
}

type transaction struct {
	Sequence     uint64
	Lower        time.Time
	Publish      *publication
	Schedule     *scheduledPublication
	Due          []publication
	Dead         []DeadLetter
	Redrive      *redriveAdmission
	Register     *registration
	Tasks        []taskUpdate
	Ack          *ackReceipt
	ReserveDelta int64
}

type publication struct {
	Message          Message
	Receipt          publishReceipt
	AcceptedSequence uint64
}

type storedMessage struct {
	Message
	AcceptedSequence uint64
}

type Partition struct {
	mu      sync.Mutex
	config  Config
	journal *journal.Journal
	closed  bool
	failed  error

	sequence        uint64
	timeLower       time.Time
	firstOffset     uint64
	history         []storedMessage
	publishes       map[string]publishReceipt
	subscriptions   map[string]*subscription
	acknowledgments map[string]ackReceipt
	reserved        int64
	scheduled       map[string]scheduledPublication
	due             scheduleQueue
	dead            map[string]DeadLetter
	redrives        map[string]redriveTask
	redriveReceipts map[string]redriveReceipt
	segments        []segment
	checkpointBytes int64
}

// Open loads the partition in dir and takes exclusive ownership until Close.
func Open(dir string, config Config) (*Partition, error) {
	config = config.defaults()
	if config.CapacityBytes < 4096 || config.DedupRetention < 0 ||
		config.HistoryRetention < 0 || config.DeadRetention < 0 || config.SegmentMessages < 1 ||
		config.SegmentMessages > MaxBatchMessages {
		return nil, ErrInvalid
	}
	p := &Partition{
		config: config, publishes: make(map[string]publishReceipt),
		subscriptions:   make(map[string]*subscription),
		acknowledgments: make(map[string]ackReceipt),
		scheduled:       make(map[string]scheduledPublication),
		dead:            make(map[string]DeadLetter),
		redrives:        make(map[string]redriveTask),
		redriveReceipts: make(map[string]redriveReceipt),
	}
	j, err := journal.Open(dir, p.restore, func(data []byte) error {
		var tx transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			return err
		}
		return p.apply(tx)
	})
	if err != nil {
		return nil, err
	}
	p.journal = j
	if err := p.collectFiles(); err != nil {
		j.Close()
		return nil, err
	}
	return p, nil
}

func (p *Partition) checkOpen() error {
	if p.closed {
		return ErrClosed
	}
	return p.failed
}

func (p *Partition) clock() (TimeBounds, error) {
	now, err := p.config.Clock()
	if err != nil {
		return TimeBounds{}, fmt.Errorf("%w: %v", ErrClockUncertain, err)
	}
	if now.Lower.IsZero() || now.Upper.Before(now.Lower) || now.Lower.Before(p.timeLower) {
		return TimeBounds{}, ErrClockUncertain
	}
	return TimeBounds{Lower: now.Lower.UTC(), Upper: now.Upper.UTC()}, nil
}

func (p *Partition) nextOffset() uint64 {
	return p.firstOffset + uint64(len(p.history))
}

func (p *Partition) apply(tx transaction) error {
	if tx.Sequence != p.sequence+1 || tx.Lower.Before(p.timeLower) {
		return fmt.Errorf("%w: transaction sequence or time regressed", ErrCorrupt)
	}
	if pub := tx.Publish; pub != nil {
		if err := p.applyPublication(*pub, tx.Sequence); err != nil {
			return err
		}
	}
	if err := p.applySchedule(tx); err != nil {
		return err
	}
	if tx.Redrive != nil {
		if err := p.applyRedrive(*tx.Redrive); err != nil {
			return err
		}
	}
	if err := p.applySubscriptions(tx); err != nil {
		return err
	}
	for _, letter := range tx.Dead {
		if _, exists := p.dead[letter.ID]; exists {
			return fmt.Errorf("%w: repeated dead letter", ErrCorrupt)
		}
		p.dead[letter.ID] = letter
	}
	p.reserved += tx.ReserveDelta
	if p.reserved < 0 {
		return fmt.Errorf("%w: negative completion budget", ErrCorrupt)
	}
	p.sequence, p.timeLower = tx.Sequence, tx.Lower
	return nil
}

func (p *Partition) applyPublication(pub publication, acceptedSequence uint64) error {
	if pub.Message.Offset != p.nextOffset() {
		return fmt.Errorf("%w: unexpected message offset", ErrCorrupt)
	}
	p.history = append(p.history, storedMessage{Message: pub.Message, AcceptedSequence: acceptedSequence})
	p.publishes[pub.Message.ID] = pub.Receipt
	return nil
}

func (p *Partition) commit(tx transaction) error {
	tx.Sequence = p.sequence + 1
	data, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	frame, err := journal.EncodeFrame(data)
	if err != nil {
		return err
	}
	budget := p.config.CapacityBytes/2 - 4096
	if int64(len(frame))+p.reserved+tx.ReserveDelta > budget-p.storageBytes() {
		return ErrCapacity
	}
	available, err := p.journal.Available()
	if err != nil {
		return err
	}
	if int64(len(frame))+p.reserved+tx.ReserveDelta+8192 > available {
		return ErrCapacity
	}
	if err := p.journal.Append(frame); err != nil {
		p.failed = err
		return err
	}
	p.failed = p.apply(tx)
	return p.failed
}

func validID(id string) bool {
	return len(id) > 0 && len(id) <= maxIdentifierBytes && utf8.ValidString(id)
}

func (p *Partition) Publish(req PublishRequest) (PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return PublishResult{}, err
	}
	if !validID(req.ID) || len(req.Body) > MaxMessageBytes {
		return PublishResult{}, ErrInvalid
	}
	now, err := p.clock()
	if err != nil {
		return PublishResult{}, err
	}
	digest := sha256.Sum256(req.Body)
	_, pending := p.scheduled[req.ID]
	if old, found := p.publishes[req.ID]; found && (pending || now.Lower.Before(old.Expires)) {
		if digest != old.Digest || !req.DeliverAt.Equal(old.DeliverAt) {
			return PublishResult{}, ErrConflict
		}
		return old.Result, nil
	}
	if req.DeliverAt.After(now.Lower) {
		return p.schedule(req, digest, now)
	}
	result := PublishResult{ID: req.ID, Offset: p.nextOffset()}
	tx := transaction{Lower: now.Lower, ReserveDelta: p.publicationReserve(len(req.Body)), Publish: &publication{
		Message: Message{ID: req.ID, Offset: result.Offset, Body: bytes.Clone(req.Body), PublishedAt: now.Upper},
		Receipt: publishReceipt{Digest: digest, Result: result, VisibleOffset: result.Offset, DeliverAt: req.DeliverAt.UTC(), Expires: now.Upper.Add(p.config.DedupRetention)},
	}}
	if err := p.commit(tx); err != nil {
		return PublishResult{}, err
	}
	return result, nil
}

func (p *Partition) Read(offset uint64, limit Limits) (HistoryPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return HistoryPage{}, err
	}
	if !limit.valid() {
		return HistoryPage{}, ErrInvalid
	}
	if offset < p.firstOffset {
		return HistoryPage{}, &OffsetGap{Requested: offset, First: p.firstOffset}
	}
	page := HistoryPage{Next: offset}
	for page.Next < p.nextOffset() && len(page.Messages) < limit.Messages {
		msg := p.history[page.Next-p.firstOffset].Message
		if len(msg.Body) > limit.Bytes {
			if len(page.Messages) == 0 {
				return HistoryPage{}, fmt.Errorf("%w: next message requires %d bytes", ErrInvalid, len(msg.Body))
			}
			break
		}
		limit.Bytes -= len(msg.Body)
		msg.Body = bytes.Clone(msg.Body)
		page.Messages = append(page.Messages, msg)
		page.Next++
	}
	return page, nil
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.journal.Close()
}
