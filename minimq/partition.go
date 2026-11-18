// Package minimq implements the durable state of a single message partition.
package minimq

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

type PartitionStatus struct {
	FirstOffset, NextOffset                 uint64
	StoredBytes, ReservedBytes              int64
	Scheduled, DeadLetters, PendingRedrives int
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
	historyReadMu sync.Mutex
	mu            sync.Mutex
	config        Config
	journal       *journal.Journal
	closed        bool
	failed        error
	timings       *partitionTimings

	sequence        uint64
	timeLower       time.Time
	firstOffset     uint64
	readStart       atomic.Pointer[uint64]
	tailOffset      uint64
	tail            []storedMessage
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
	checkpointEnd   uint64
}

type operationTiming struct {
	arrived, locked, finished time.Time
	syncTime                  time.Duration
	syncCalls                 int
	worker                    string
	checkpoint                checkpointTiming
}

type checkpointTiming struct {
	prepare, encode, frame, write, cleanup, cleanupSync time.Duration
	tailFirst                                           uint64
	tailRecords, tailBytes, imageBytes, stagedSegments  int
}

// An optional observer, installed before opening the workload to concurrent
// calls. Its counters are updated under the partition lock.
type partitionTimings struct {
	record           func(string, operationTiming)
	time             time.Duration
	calls            int
	checkpoint       checkpointTiming
	checkpointLocked func()
}

func (p *Partition) observe(record func(string, operationTiming)) {
	timings := &partitionTimings{record: record}
	p.timings = timings
	p.journal.ObserveSync = func(elapsed time.Duration) {
		timings.time += elapsed
		timings.calls++
	}
	p.journal.ObserveWrite = func(elapsed time.Duration) {
		timings.checkpoint.write += elapsed
	}
}

func (p *Partition) beginOperation() operationTiming {
	if p.timings == nil {
		p.mu.Lock()
		return operationTiming{}
	}
	arrived := time.Now()
	p.mu.Lock()
	return operationTiming{
		arrived: arrived, locked: time.Now(), syncTime: p.timings.time, syncCalls: p.timings.calls,
	}
}

func (p *Partition) endOperation(operation string, sample operationTiming) {
	if timing := p.timings; timing != nil {
		sample.finished = time.Now()
		sample.syncTime = timing.time - sample.syncTime
		sample.syncCalls = timing.calls - sample.syncCalls
		if operation == "checkpoint" {
			sample.checkpoint = timing.checkpoint
		}
		timing.record(operation, sample)
	}
	p.mu.Unlock()
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
	return p.tailOffset + uint64(len(p.tail))
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
	p.tail = append(p.tail, storedMessage{Message: pub.Message, AcceptedSequence: acceptedSequence})
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

func (p *Partition) planHistoryRead(offset uint64, limit Limits) ([]historyLocation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	if !limit.valid() {
		return nil, ErrInvalid
	}
	if offset < p.firstOffset {
		return nil, &OffsetGap{Requested: offset, First: p.firstOffset}
	}
	if offset >= p.nextOffset() {
		return nil, nil
	}
	end := offset + min(p.nextOffset()-offset, uint64(limit.Messages))
	batch := make([]historyLocation, 0, end-offset)
	for next := offset; next < end; next++ {
		loc := p.locateHistory(next)
		if loc.record.bodyBytes > limit.Bytes {
			if len(batch) == 0 {
				return nil, fmt.Errorf("%w: next message requires %d bytes", ErrInvalid, loc.record.bodyBytes)
			}
			break
		}
		limit.Bytes -= loc.record.bodyBytes
		batch = append(batch, loc)
	}
	if batch[0].segment != nil {
		p.readStart.Store(&offset)
	}
	return batch, nil
}

func (p *Partition) finishHistoryRead(err error) {
	if err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.failed == nil {
			p.failed = err
		}
	}
	p.readStart.Store(nil)
}

func (p *Partition) Read(offset uint64, limit Limits) (HistoryPage, error) {
	// Serialize history reads to bound scratch space and disk concurrency per
	// partition. The selected range pins its sealed segments until the read ends.
	p.historyReadMu.Lock()
	defer p.historyReadMu.Unlock()
	batch, err := p.planHistoryRead(offset, limit)
	if err != nil {
		return HistoryPage{}, err
	}
	page := HistoryPage{Next: offset}
	if len(batch) == 0 {
		return page, nil
	}
	reader := historyReader{dir: p.journal.Dir()}
	defer func() {
		reader.close()
		p.finishHistoryRead(err)
	}()
	page.Messages = make([]Message, 0, len(batch))
	for _, loc := range batch {
		var msg Message
		msg, err = reader.read(page.Next, loc)
		if err != nil {
			return HistoryPage{}, err
		}
		page.Messages = append(page.Messages, msg)
		page.Next++
	}
	return page, nil
}

func (p *Partition) Close() error {
	p.historyReadMu.Lock()
	defer p.historyReadMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.journal.Close()
}

// Status reports committed state and physically accounted storage.
func (p *Partition) Status() (PartitionStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return PartitionStatus{}, err
	}
	return PartitionStatus{
		FirstOffset: p.firstOffset, NextOffset: p.nextOffset(),
		StoredBytes: p.storageBytes(), ReservedBytes: p.reserved,
		Scheduled: len(p.scheduled), DeadLetters: len(p.dead), PendingRedrives: len(p.redrives),
	}, nil
}
