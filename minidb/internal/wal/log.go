package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/celanwang/miniinfra/minidb/internal/disk"
)

const (
	magic      = "MINIDB-WAL-1\n"
	headerSize = 12
	MaxPayload = 16 << 20
	maxBatch   = 64
)

var (
	ErrClosed   = errors.New("WAL is closed")
	ErrTooLarge = errors.New("transaction exceeds WAL record limit")
)

// Log writes checksummed transaction records. A complete record is the
// transaction's commit boundary. Publish runs in log order, after sync.
type Log struct {
	file     *os.File
	lock     *os.File
	path     string
	queue    chan request
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	closeErr error
}

type request struct {
	checkpoint func(int64) error
	payload    []byte
	publish    func(end int64)
	result     chan error
}

func Open(path string, replay func([]byte) error) (*Log, error) {
	return OpenWithCheckpoint(path, func() (int64, error) { return 0, nil }, replay)
}

// OpenWithCheckpoint loads the published checkpoint while holding a lock that
// stays on the same inode across WAL replacements. A zero position means
// recovery starts with a legacy WAL and no checkpoint.
func OpenWithCheckpoint(path string, load func() (int64, error), replay func([]byte) error) (*Log, error) {
	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			lock.Close()
		}
	}()
	if err := lockFile(lock); err != nil {
		return nil, err
	}
	checkpoint, err := load()
	if err != nil {
		return nil, err
	}
	mode := os.O_RDWR
	if checkpoint == 0 {
		mode |= os.O_CREATE
	}
	file, err := os.OpenFile(path, mode, 0600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !opened {
			file.Close()
		}
	}()
	// Retain the inode lock for older binaries that used the data file alone.
	if err := lockFile(file); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 && checkpoint == 0 {
		if _, err := file.WriteString(magic); err != nil {
			return nil, err
		}
	}

	offset, base, err := replayStart(file, checkpoint)
	if err != nil {
		return nil, err
	}
	tail, err := scan(file, offset, replay)
	if err != nil {
		return nil, err
	}
	// Adopt valid records retained by the OS and any visible rename before
	// serving reads.
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := disk.SyncDirectory(path); err != nil {
		return nil, err
	}
	if _, err := file.Seek(tail, io.SeekStart); err != nil {
		return nil, err
	}
	log := &Log{
		file: file, lock: lock, path: path,
		queue: make(chan request, maxBatch), done: make(chan struct{}),
	}
	go log.run(base + (tail - offset))
	opened = true
	return log, nil
}

func scan(file *os.File, end int64, replay func([]byte) error) (int64, error) {
	var header [headerSize]byte
	for {
		n, err := file.ReadAt(header[:], end)
		if errors.Is(err, io.EOF) && n == 0 {
			return end, nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if crc32.ChecksumIEEE(header[:8]) != binary.BigEndian.Uint32(header[8:]) {
			return 0, fmt.Errorf("WAL header checksum mismatch at %d", end)
		}
		size := binary.BigEndian.Uint32(header[:4])
		if size == 0 || size > MaxPayload {
			return 0, fmt.Errorf("invalid WAL record size at %d", end)
		}
		payload := make([]byte, size)
		if _, err := file.ReadAt(payload, end+headerSize); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
			return 0, fmt.Errorf("WAL checksum mismatch at %d", end)
		}
		if err := replay(payload); err != nil {
			return 0, fmt.Errorf("replay WAL at %d: %w", end, err)
		}
		end += headerSize + int64(size)
	}
	if err := file.Truncate(end); err != nil {
		return 0, err
	}
	return end, nil
}

func (log *Log) Commit(payload []byte, publish func(end int64)) error {
	if len(payload) == 0 || len(payload) > MaxPayload {
		return ErrTooLarge
	}
	return log.submit(request{payload: payload, publish: publish})
}

func (log *Log) submit(item request) error {
	item.result = make(chan error, 1)
	log.mu.RLock()
	if log.closed {
		log.mu.RUnlock()
		return ErrClosed
	}
	log.queue <- item
	log.mu.RUnlock()
	return <-item.result
}

func (log *Log) run(end int64) {
	defer close(log.done)
	var failure error
	for first := range log.queue {
		if first.checkpoint != nil {
			failure = log.checkpoint(first, end, failure)
			continue
		}
		batch := []request{first}
		var barrier request
	collect:
		for len(batch) < maxBatch {
			select {
			case next, ok := <-log.queue:
				if !ok {
					break collect
				}
				if next.checkpoint != nil {
					barrier = next
					break collect
				}
				batch = append(batch, next)
			default:
				break collect
			}
		}
		if failure == nil {
			failure = log.sync(batch)
		}
		for _, item := range batch {
			if failure == nil {
				end += int64(headerSize + len(item.payload))
				item.publish(end)
			}
			item.result <- failure
		}
		if barrier.checkpoint != nil {
			failure = log.checkpoint(barrier, end, failure)
		}
	}
	log.closeErr = errors.Join(failure, log.file.Close())
	if log.lock != nil {
		log.closeErr = errors.Join(log.closeErr, log.lock.Close())
	}
}

func (log *Log) sync(batch []request) error {
	// Bound staging memory even when the batch contains large transactions.
	var header [headerSize]byte
	var buffer bytes.Buffer
	for _, item := range batch {
		binary.BigEndian.PutUint32(header[:4], uint32(len(item.payload)))
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(item.payload))
		binary.BigEndian.PutUint32(header[8:], crc32.ChecksumIEEE(header[:8]))
		buffer.Write(header[:])
		if buffer.Len()+len(item.payload) > 64<<10 {
			if _, err := buffer.WriteTo(log.file); err != nil {
				return fmt.Errorf("write WAL: %w", err)
			}
			if _, err := log.file.Write(item.payload); err != nil {
				return fmt.Errorf("write WAL: %w", err)
			}
			continue
		}
		buffer.Write(item.payload)
	}
	if _, err := buffer.WriteTo(log.file); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}
	if err := log.file.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	return nil
}

func (log *Log) Close() error {
	log.mu.Lock()
	if !log.closed {
		log.closed = true
		close(log.queue)
	}
	log.mu.Unlock()
	<-log.done
	return log.closeErr
}
