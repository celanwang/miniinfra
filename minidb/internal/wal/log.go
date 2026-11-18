package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
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
	queue    chan request
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	closeErr error
}

type request struct {
	payload []byte
	publish func(end int64)
	result  chan error
}

func Open(path string, replay func([]byte) error) (*Log, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			file.Close()
		}
	}()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock WAL: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		if _, err := file.WriteString(magic); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return nil, err
	}

	end, err := scan(file, replay)
	if err != nil {
		return nil, err
	}
	// Make any valid records retained by the OS durable before serving reads.
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if _, err := file.Seek(end, io.SeekStart); err != nil {
		return nil, err
	}
	log := &Log{file: file, queue: make(chan request, maxBatch), done: make(chan struct{})}
	go log.run(end)
	opened = true
	return log, nil
}

func scan(file *os.File, replay func([]byte) error) (int64, error) {
	version := make([]byte, len(magic))
	if _, err := file.ReadAt(version, 0); err != nil || string(version) != magic {
		return 0, errors.New("invalid or unsupported WAL header")
	}
	end := int64(len(magic))
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
	return end, file.Sync()
}

func (log *Log) Commit(payload []byte, publish func(end int64)) error {
	if len(payload) == 0 || len(payload) > MaxPayload {
		return ErrTooLarge
	}
	result := make(chan error, 1)
	log.mu.RLock()
	if log.closed {
		log.mu.RUnlock()
		return ErrClosed
	}
	log.queue <- request{payload: payload, publish: publish, result: result}
	log.mu.RUnlock()
	return <-result
}

func (log *Log) run(end int64) {
	defer close(log.done)
	var failure error
	for first := range log.queue {
		batch := []request{first}
	collect:
		for len(batch) < maxBatch {
			select {
			case next, ok := <-log.queue:
				if !ok {
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
	}
	log.closeErr = errors.Join(failure, log.file.Close())
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
