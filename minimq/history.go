package minimq

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/celanwang/miniinfra/minimq/internal/journal"
)

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

type segment struct {
	Base   uint64
	Count  int
	Bytes  int64
	Newest time.Time

	// Rebuilt from validated frames. These positions are never persisted.
	records []segmentRecord
}

type segmentRecord struct {
	position  int64
	accepted  uint64
	bodyBytes int
}

func (s *segment) name() string { return fmt.Sprintf("history-%016x", s.Base) }

// In addition to the base64 body, the frame contains a bounded ID, fixed
// fields and the eight-byte length/checksum header.
const maxHistoryFrame = (MaxMessageBytes+2)/3*4 + 4096

func encodeSegment(s *segment, messages []storedMessage, stats *checkpointTiming) ([]byte, error) {
	var data bytes.Buffer
	s.records = make([]segmentRecord, 0, len(messages))
	for _, msg := range messages {
		start := checkpointStart(stats)
		payload, err := json.Marshal(msg)
		if stats != nil {
			stats.encode += time.Since(start)
			start = time.Now()
		}
		if err != nil {
			return nil, err
		}
		data.Grow(journal.FrameHeaderBytes + len(payload))
		frame, err := journal.AppendFrame(data.AvailableBuffer(), payload)
		if err != nil {
			return nil, err
		}
		s.records = append(s.records, segmentRecord{
			position: int64(data.Len()), accepted: msg.AcceptedSequence, bodyBytes: len(msg.Body),
		})
		// The frame uses data's spare capacity. Write advances its length.
		data.Write(frame)
		if stats != nil {
			stats.frame += time.Since(start)
		}
	}
	s.Bytes = int64(data.Len())
	return data.Bytes(), nil
}

// recoveryBody validates the serialized body and reuses the decode buffer
// across frames. Only its length is copied into the segment index.
type recoveryBody struct {
	scratch []byte
	size    int
}

func (b *recoveryBody) UnmarshalJSON(raw []byte) error {
	// Segment writers use encoding/json's canonical base64 string (or null).
	// Keep encoding/json's decoding rules for the other valid representations.
	if len(raw) < 2 || raw[0] != '"' || bytes.IndexByte(raw, '\\') >= 0 {
		var body []byte
		if err := json.Unmarshal(raw, &body); err != nil {
			return err
		}
		b.size = len(body)
		return nil
	}
	encoded := raw[1 : len(raw)-1]
	if len(encoded) > base64.StdEncoding.EncodedLen(MaxMessageBytes) {
		return fmt.Errorf("body exceeds %d bytes", MaxMessageBytes)
	}
	size := base64.StdEncoding.DecodedLen(len(encoded))
	if cap(b.scratch) < size {
		b.scratch = make([]byte, size)
	}
	var err error
	b.size, err = base64.StdEncoding.Decode(b.scratch[:size], encoded)
	return err
}

type segmentScanner struct {
	frame []byte
	body  []byte
}

func (scan *segmentScanner) index(dir string, s *segment, expected uint64) error {
	f, err := os.Open(filepath.Join(dir, s.name()))
	if err != nil {
		return fmt.Errorf("%w: open history segment: %v", ErrCorrupt, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != s.Bytes || s.Base != expected || s.Count < 1 || s.Count > MaxBatchMessages ||
		s.Base+uint64(s.Count) < s.Base {
		return fmt.Errorf("%w: history segment metadata", ErrCorrupt)
	}
	s.records = make([]segmentRecord, s.Count)
	var pos int64
	for i := range s.records {
		data, size, err := journal.ReadFrameInto(f, min(s.Bytes-pos, maxHistoryFrame), scan.frame)
		if err != nil {
			return fmt.Errorf("history offset %d: %w", s.Base+uint64(i), err)
		}
		scan.frame = data
		msg := struct {
			ID               string
			Offset           uint64
			Body             recoveryBody
			PublishedAt      time.Time
			AcceptedSequence uint64
		}{Body: recoveryBody{scratch: scan.body}}
		if err := json.Unmarshal(data, &msg); err != nil || msg.Offset != s.Base+uint64(i) || msg.Body.size > MaxMessageBytes {
			return fmt.Errorf("%w: invalid history record at offset %d", ErrCorrupt, s.Base+uint64(i))
		}
		scan.body = msg.Body.scratch
		s.records[i] = segmentRecord{position: pos, accepted: msg.AcceptedSequence, bodyBytes: msg.Body.size}
		pos += size
	}
	if pos != s.Bytes {
		return fmt.Errorf("%w: unexpected segment tail", ErrCorrupt)
	}
	return nil
}

type historyLocation struct {
	segment *segment
	record  segmentRecord
	tail    Message
}

// locateHistory takes an offset in the retained committed range under mu.
func (p *Partition) locateHistory(offset uint64) historyLocation {
	if offset >= p.tailOffset {
		msg := p.tail[offset-p.tailOffset]
		return historyLocation{
			record: segmentRecord{accepted: msg.AcceptedSequence, bodyBytes: len(msg.Body)},
			tail:   msg.Message,
		}
	}
	index := sort.Search(len(p.segments), func(i int) bool {
		s := &p.segments[i]
		return s.Base+uint64(s.Count) > offset
	})
	s := &p.segments[index]
	return historyLocation{segment: s, record: s.records[offset-s.Base]}
}

// A batch keeps at most one segment descriptor and one encoded frame buffer.
type historyReader struct {
	dir        string
	file       *os.File
	segment    *segment
	frame      []byte
	decodeTime *time.Duration
}

func (r *historyReader) close() {
	if r.file != nil {
		_ = r.file.Close()
		r.file, r.segment = nil, nil
	}
}

func (r *historyReader) read(offset uint64, loc historyLocation) (Message, error) {
	s := loc.segment
	if s == nil {
		msg := loc.tail
		msg.Body = bytes.Clone(msg.Body)
		return msg, nil
	}
	if r.segment != s {
		r.close()
		f, err := os.Open(filepath.Join(r.dir, s.name()))
		if err != nil {
			return Message{}, fmt.Errorf("%w: open history at offset %d: %v", ErrCorrupt, offset, err)
		}
		r.file, r.segment = f, s
	}
	remaining := s.Bytes - loc.record.position
	reader := io.NewSectionReader(r.file, loc.record.position, remaining)
	data, _, err := journal.ReadFrameInto(reader, min(remaining, maxHistoryFrame), r.frame)
	if err != nil {
		return Message{}, fmt.Errorf("history offset %d: %w", offset, err)
	}
	r.frame = data
	var msg storedMessage
	var began time.Time
	if r.decodeTime != nil {
		began = time.Now()
	}
	err = json.Unmarshal(data, &msg)
	if r.decodeTime != nil {
		*r.decodeTime += time.Since(began)
	}
	if err != nil || msg.Offset != offset ||
		msg.AcceptedSequence != loc.record.accepted || len(msg.Body) != loc.record.bodyBytes {
		return Message{}, fmt.Errorf("%w: invalid history record at offset %d", ErrCorrupt, offset)
	}
	return msg.Message, nil
}

func (p *Partition) readHistoryLocked(r *historyReader, offset uint64, loc historyLocation) (Message, error) {
	msg, err := r.read(offset, loc)
	if err != nil {
		p.failed = err
	}
	return msg, err
}

// planHistoryRead selects a page and pins its sealed segments under mu.
// The caller holds historyReadMu until it closes the reader and releases the pin.
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

// finishHistoryRead clears the pin after the reader has closed its segment.
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

// Read returns a page of committed history starting at offset.
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
