package minimq

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/celanwang/miniinfra/minimq/internal/journal"
)

type segment struct {
	Base   uint64
	Count  int
	Bytes  int64
	Newest time.Time
}

func (s segment) name() string { return fmt.Sprintf("history-%016x", s.Base) }

type checkpoint struct {
	Sequence      uint64
	TimeLower     time.Time
	First         uint64
	Tail          []storedMessage
	Segments      []segment
	Publishes     map[string]publishReceipt
	Subscriptions map[string]*subscription
	Acks          map[string]ackReceipt
	Scheduled     map[string]scheduledPublication
	Dead          map[string]DeadLetter
	Reserved      int64
}

func (p *Partition) storageBytes() int64 {
	size := p.journal.End() + p.checkpointBytes
	for _, s := range p.segments {
		size += s.Bytes
	}
	return size
}

func (p *Partition) collectFiles() error {
	keep := make(map[string]bool, len(p.segments))
	for _, s := range p.segments {
		keep[s.name()] = true
	}
	return p.journal.Collect(keep)
}

func encodeSegment(messages []storedMessage) ([]byte, error) {
	var data bytes.Buffer
	for _, msg := range messages {
		payload, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		frame, err := journal.EncodeFrame(payload)
		if err != nil {
			return nil, err
		}
		data.Write(frame)
	}
	return data.Bytes(), nil
}

func (p *Partition) readSegment(dir string, s segment) error {
	f, err := os.Open(filepath.Join(dir, s.name()))
	if err != nil {
		return fmt.Errorf("%w: open history segment: %v", ErrCorrupt, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != s.Bytes || s.Base != p.nextOffset() || s.Count < 1 || s.Count > MaxBatchMessages {
		return fmt.Errorf("%w: history segment metadata", ErrCorrupt)
	}
	remaining := s.Bytes
	for range s.Count {
		data, size, err := journal.ReadFrame(f, remaining)
		if err != nil {
			return err
		}
		var msg storedMessage
		if err := json.Unmarshal(data, &msg); err != nil || msg.Offset != p.nextOffset() {
			return fmt.Errorf("%w: invalid segment record", ErrCorrupt)
		}
		p.history = append(p.history, msg)
		remaining -= size
	}
	if remaining != 0 {
		return fmt.Errorf("%w: unexpected segment tail", ErrCorrupt)
	}
	return nil
}

func (p *Partition) restore(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%w: read checkpoint: %v", ErrCorrupt, err)
	}
	if len(data) <= sha256.Size {
		return fmt.Errorf("%w: truncated checkpoint", ErrCorrupt)
	}
	payload := data[:len(data)-sha256.Size]
	digest := sha256.Sum256(payload)
	if !bytes.Equal(digest[:], data[len(payload):]) {
		return fmt.Errorf("%w: checkpoint checksum", ErrCorrupt)
	}
	var image checkpoint
	if err := json.Unmarshal(payload, &image); err != nil {
		return fmt.Errorf("%w: decode checkpoint: %v", ErrCorrupt, err)
	}
	p.firstOffset = image.First
	for _, s := range image.Segments {
		if err := p.readSegment(filepath.Dir(path), s); err != nil {
			return err
		}
	}
	for _, msg := range image.Tail {
		if msg.Offset != p.nextOffset() {
			return fmt.Errorf("%w: invalid checkpoint history tail", ErrCorrupt)
		}
		p.history = append(p.history, msg)
	}
	p.sequence, p.timeLower, p.reserved = image.Sequence, image.TimeLower, image.Reserved
	p.segments, p.checkpointBytes = image.Segments, int64(len(data))
	p.publishes, p.subscriptions = image.Publishes, image.Subscriptions
	p.acknowledgments, p.dead = image.Acks, image.Dead
	for _, item := range image.Scheduled {
		if err := p.applySchedule(transaction{Schedule: &item}); err != nil {
			return err
		}
	}
	return nil
}

// Compact installs a checkpoint and reclaims the eligible history prefix.
func (p *Partition) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	now, err := p.clock()
	if err != nil {
		return err
	}
	return p.compact(now)
}

func (p *Partition) compact(now TimeBounds) error {
	image := checkpoint{
		Sequence: p.sequence, TimeLower: now.Lower, First: p.firstOffset, Reserved: p.reserved,
		Segments:  append([]segment(nil), p.segments...),
		Publishes: maps.Clone(p.publishes), Acks: maps.Clone(p.acknowledgments),
		Subscriptions: p.subscriptions, Scheduled: p.scheduled, Dead: maps.Clone(p.dead),
	}
	maps.DeleteFunc(image.Publishes, func(id string, r publishReceipt) bool {
		_, pending := p.scheduled[id]
		return !pending && !now.Lower.Before(r.Expires)
	})
	maps.DeleteFunc(image.Acks, func(_ string, r ackReceipt) bool { return !now.Lower.Before(r.Expires) })
	maps.DeleteFunc(image.Dead, func(_ string, d DeadLetter) bool { return !now.Lower.Before(d.Expires) })

	var files = make(map[string][]byte)
	sealed := 0
	for _, s := range p.segments {
		sealed += s.Count
	}
	width := p.config.SegmentMessages
	for ; len(p.history)-sealed >= width; sealed += width {
		chunk := p.history[sealed : sealed+width]
		data, err := encodeSegment(chunk)
		if err != nil {
			return err
		}
		s := segment{Base: chunk[0].Offset, Count: width, Bytes: int64(len(data))}
		for _, msg := range chunk {
			if msg.PublishedAt.After(s.Newest) {
				s.Newest = msg.PublishedAt
			}
		}
		image.Segments = append(image.Segments, s)
		files[s.name()] = data
	}
	floor := p.nextOffset()
	for _, sub := range p.subscriptions {
		floor = min(floor, sub.Floor)
	}
	removed := 0
	for _, s := range image.Segments {
		end := s.Base + uint64(s.Count)
		if end > floor || now.Lower.Before(s.Newest.Add(p.config.HistoryRetention)) {
			break
		}
		delete(files, s.name())
		removed++
		image.First = end
	}
	image.Segments = image.Segments[removed:]
	image.Tail = p.history[sealed:]
	if p.journal.End() == 0 && len(files) == 0 && removed == 0 &&
		len(image.Acks) == len(p.acknowledgments) && len(image.Publishes) == len(p.publishes) &&
		len(image.Dead) == len(p.dead) {
		return nil
	}
	payload, err := json.Marshal(image)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	payload = append(payload, digest[:]...)
	scratch := int64(len(payload)) + 8192
	for _, data := range files {
		scratch += int64(len(data))
	}
	available, err := p.journal.Available()
	if err != nil {
		return err
	}
	if scratch > available || scratch+p.storageBytes() > p.config.CapacityBytes {
		return ErrCapacity
	}
	for name, data := range files {
		if err := journal.InstallFile(p.journal.Dir(), filepath.Join(p.journal.Dir(), name), data); err != nil {
			p.failed = fmt.Errorf("partition requires recovery after writing history segment: %w", err)
			return p.failed
		}
	}
	if err := p.journal.Checkpoint(payload); err != nil {
		p.failed = err
		return err
	}
	first := image.First - p.firstOffset
	clear(p.history[:first])
	p.history = p.history[first:]
	p.firstOffset, p.segments = image.First, image.Segments
	p.timeLower, p.checkpointBytes = image.TimeLower, int64(len(payload))
	p.publishes, p.acknowledgments, p.dead = image.Publishes, image.Acks, image.Dead
	if err := p.collectFiles(); err != nil {
		p.failed = fmt.Errorf("partition requires recovery after file collection: %w", err)
		return p.failed
	}
	return nil
}
