package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"syscall"

	"github.com/celanwang/miniinfra/minidb/internal/disk"
)

const (
	rotatedMagic      = "MINIDB-WAL-2\n"
	rotatedHeaderSize = len(rotatedMagic) + 8 + 4
)

// Checkpoint pauses publication at the end of the durable WAL. install must
// publish and sync the checkpoint before returning nil. Commits queued behind
// this operation are written after any successful rotation.
func (log *Log) Checkpoint(install func(int64) error) error {
	return log.submit(request{checkpoint: install})
}

func (log *Log) checkpoint(item request, end int64, failure error) error {
	if failure != nil {
		item.result <- failure
		return failure
	}
	if err := item.checkpoint(end); err != nil {
		item.result <- fmt.Errorf("publish checkpoint: %w", err)
		return nil
	}
	// Any replacement error may have happened after rename. Stop appending
	// until restart has resolved which WAL generation is on disk.
	failure = log.rotate(end)
	item.result <- failure
	return failure
}

func (log *Log) rotate(base int64) error {
	staged, err := disk.Stage(log.path)
	if err != nil {
		return fmt.Errorf("stage new WAL: %w", err)
	}
	defer os.Remove(staged.Name())
	published := false
	defer func() {
		if !published {
			staged.Close()
		}
	}()
	if err := lockFile(staged); err != nil {
		return err
	}
	var header [rotatedHeaderSize]byte
	copy(header[:], rotatedMagic)
	binary.BigEndian.PutUint64(header[len(rotatedMagic):], uint64(base))
	binary.BigEndian.PutUint32(header[len(header)-4:], crc32.ChecksumIEEE(header[:len(header)-4]))
	if _, err := staged.Write(header[:]); err != nil {
		return fmt.Errorf("write new WAL header: %w", err)
	}
	if err := disk.Publish(staged, log.path); err != nil {
		return fmt.Errorf("publish new WAL: %w", err)
	}
	log.file.Close()
	log.file = staged
	published = true
	return nil
}

func lockFile(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock %s: %w", file.Name(), err)
	}
	return nil
}

func replayStart(file *os.File, checkpoint int64) (offset, end int64, err error) {
	var header [rotatedHeaderSize]byte
	if _, err := file.ReadAt(header[:len(magic)], 0); err != nil {
		return 0, 0, fmt.Errorf("read WAL header: %w", err)
	}
	offset, end = int64(len(magic)), int64(len(magic))
	switch string(header[:len(magic)]) {
	case magic:
	case rotatedMagic:
		if _, err := file.ReadAt(header[len(magic):], int64(len(magic))); err != nil {
			return 0, 0, fmt.Errorf("read rotated WAL header: %w", err)
		}
		if crc32.ChecksumIEEE(header[:len(header)-4]) != binary.BigEndian.Uint32(header[len(header)-4:]) {
			return 0, 0, errors.New("WAL base checksum mismatch")
		}
		base := binary.BigEndian.Uint64(header[len(magic):])
		if base < uint64(len(magic)) || base > math.MaxInt64 {
			return 0, 0, errors.New("invalid WAL base position")
		}
		if checkpoint == 0 {
			return 0, 0, errors.New("rotated WAL requires its checkpoint")
		}
		offset, end = int64(len(header)), int64(base)
	default:
		return 0, 0, errors.New("invalid or unsupported WAL header")
	}
	if checkpoint == 0 {
		return offset, end, nil
	}
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, 0, err
	}
	if checkpoint < end || checkpoint-end > size-offset {
		return 0, 0, fmt.Errorf("checkpoint position %d is outside WAL starting at %d", checkpoint, end)
	}
	return offset + (checkpoint - end), checkpoint, nil
}
