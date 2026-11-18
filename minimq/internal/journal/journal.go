// Package journal stores a sequence of length- and CRC-framed local transactions.
package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const (
	maxFrame         = 1 << 28
	headSize         = 24
	FrameHeaderBytes = 8
)

var ErrCorrupt = errors.New("corrupt partition storage")

type Journal struct {
	dir        string
	lock, file *os.File
	generation uint64
	end        int64
	failed     error

	// ObserveSync is optional. Set it before serving requests.
	ObserveSync  func(time.Duration)
	ObserveWrite func(time.Duration)
}

func generationPath(dir, name string, generation uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%016x", name, generation))
}

// Open holds an exclusive directory lock through Close. Recovery never interprets
// data after the published commit boundary.
func Open(dir string, restore func(string) error, replay func([]byte) error) (_ *Journal, err error) {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if err := syncDirectory(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("lock partition: %w", err)
	}
	j := &Journal{dir: dir, lock: lock}
	defer func() {
		if err != nil {
			j.Close()
		}
	}()
	data, err := os.ReadFile(filepath.Join(dir, "HEAD"))
	if errors.Is(err, os.ErrNotExist) {
		if err := j.start(); err != nil {
			return nil, err
		}
		return j, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) != headSize || string(data[:4]) != "MMQ1" ||
		crc32.ChecksumIEEE(data[:20]) != binary.LittleEndian.Uint32(data[20:]) {
		return nil, fmt.Errorf("%w: invalid HEAD", ErrCorrupt)
	}
	j.generation = binary.LittleEndian.Uint64(data[4:])
	j.end = int64(binary.LittleEndian.Uint64(data[12:]))
	if j.generation == 0 || j.end < 0 {
		return nil, fmt.Errorf("%w: invalid committed range", ErrCorrupt)
	}
	j.file, err = os.OpenFile(generationPath(dir, "journal", j.generation), os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open committed journal: %w", err)
	}
	info, err := j.file.Stat()
	if err != nil {
		return nil, err
	}
	if j.end > info.Size() {
		return nil, fmt.Errorf("%w: journal shorter than committed range", ErrCorrupt)
	}
	if j.generation > 1 {
		if err := restore(generationPath(dir, "checkpoint", j.generation)); err != nil {
			return nil, err
		}
	}
	reader := io.NewSectionReader(j.file, 0, j.end)
	for pos := int64(0); pos < j.end; {
		payload, size, err := ReadFrame(reader, j.end-pos)
		if err != nil {
			return nil, fmt.Errorf("journal position %d: %w", pos, err)
		}
		if err := replay(payload); err != nil {
			return nil, fmt.Errorf("%w: journal position %d: %v", ErrCorrupt, pos, err)
		}
		pos += size
	}
	// A process can exit between renaming HEAD and syncing its directory.
	// Make the recovered generation durable before exposing it or collecting
	// the files needed to read the previous generation.
	if err := syncDirectory(dir); err != nil {
		return nil, fmt.Errorf("sync recovered HEAD: %w", err)
	}
	if info.Size() > j.end {
		if err := j.file.Truncate(j.end); err != nil {
			return nil, err
		}
	}
	return j, nil
}

func (j *Journal) start() error {
	j.generation = 1
	path := generationPath(j.dir, "journal", 1)
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if ownedFile(name, "checkpoint-") || ownedFile(name, "history-") ||
			(ownedFile(name, "journal-") && name != filepath.Base(path)) {
			return fmt.Errorf("%w: HEAD missing for existing partition", ErrCorrupt)
		}
	}
	info, err := os.Stat(path)
	if err == nil && info.Size() != 0 {
		return fmt.Errorf("%w: HEAD missing for nonempty journal", ErrCorrupt)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	j.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if err := j.sync(j.file); err != nil {
		return err
	}
	if err := openAndSync(j.dir, j.sync); err != nil {
		return err
	}
	return j.installHead(1, 0)
}

func (j *Journal) End() int64  { return j.end }
func (j *Journal) Dir() string { return j.dir }

func (j *Journal) Available() (int64, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(j.dir, &stats); err != nil {
		return 0, err
	}
	return int64(stats.Bavail) * int64(stats.Bsize), nil
}

func EncodeFrame(payload []byte) ([]byte, error) {
	return AppendFrame(nil, payload)
}

// AppendFrame appends a length/CRC frame without a temporary frame buffer.
func AppendFrame(dst, payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > maxFrame {
		return nil, fmt.Errorf("invalid frame size: %d", len(payload))
	}
	dst = slices.Grow(dst, FrameHeaderBytes+len(payload))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.ChecksumIEEE(payload))
	return append(dst, payload...), nil
}

func ReadFrame(reader io.Reader, remaining int64) ([]byte, int64, error) {
	return ReadFrameInto(reader, remaining, nil)
}

// ReadFrameInto validates a frame and reuses scratch when it has enough capacity.
func ReadFrameInto(reader io.Reader, remaining int64, scratch []byte) ([]byte, int64, error) {
	var header [FrameHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, 0, fmt.Errorf("%w: frame header: %v", ErrCorrupt, err)
	}
	size := int64(binary.LittleEndian.Uint32(header[:]))
	if size == 0 || size > maxFrame || size+FrameHeaderBytes > remaining {
		return nil, 0, fmt.Errorf("%w: invalid frame length", ErrCorrupt)
	}
	data := scratch
	if cap(data) < int(size) {
		data = make([]byte, int(size))
	}
	data = data[:size]
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, 0, fmt.Errorf("%w: frame body: %v", ErrCorrupt, err)
	}
	if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[4:]) {
		return nil, 0, fmt.Errorf("%w: frame checksum", ErrCorrupt)
	}
	return data, size + FrameHeaderBytes, nil
}

func (j *Journal) Append(frame []byte) error {
	if j.failed != nil {
		return j.failed
	}
	if err := j.append(frame); err != nil {
		j.failed = fmt.Errorf("partition requires recovery after journal write: %w", err)
		return j.failed
	}
	return nil
}

func (j *Journal) append(frame []byte) error {
	if _, err := j.file.WriteAt(frame, j.end); err != nil {
		return err
	}
	if err := j.sync(j.file); err != nil {
		return err
	}
	end := j.end + int64(len(frame))
	if err := j.installHead(j.generation, end); err != nil {
		return err
	}
	j.end = end
	return nil
}

func (j *Journal) installHead(generation uint64, end int64) error {
	data := make([]byte, headSize)
	copy(data, "MMQ1")
	binary.LittleEndian.PutUint64(data[4:], generation)
	binary.LittleEndian.PutUint64(data[12:], uint64(end))
	binary.LittleEndian.PutUint32(data[20:], crc32.ChecksumIEEE(data[:20]))
	if err := j.stageFile(filepath.Join(j.dir, "HEAD"), data); err != nil {
		return err
	}
	return openAndSync(j.dir, j.sync)
}

// Checkpoint installs new immutable files and rotates to a fresh journal.
// Segment names must be new to the currently committed generation.
func (j *Journal) Checkpoint(data []byte, segments map[string][]byte) error {
	if j.failed != nil {
		return j.failed
	}
	if err := j.checkpoint(data, segments); err != nil {
		j.failed = fmt.Errorf("partition requires recovery after checkpoint write: %w", err)
		return j.failed
	}
	return nil
}

func (j *Journal) checkpoint(data []byte, segments map[string][]byte) error {
	next := j.generation + 1
	for name, segment := range segments {
		if err := j.stageFile(filepath.Join(j.dir, name), segment); err != nil {
			return fmt.Errorf("stage %s: %w", name, err)
		}
	}
	if err := j.stageFile(generationPath(j.dir, "checkpoint", next), data); err != nil {
		return err
	}
	file, err := os.OpenFile(generationPath(j.dir, "journal", next), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := j.sync(file); err != nil {
		file.Close()
		return err
	}
	// Publish no reference to these files until all of their directory entries
	// are durable. One barrier covers the segments, image and next journal.
	if err := openAndSync(j.dir, j.sync); err != nil {
		file.Close()
		return err
	}
	if err := j.installHead(next, 0); err != nil {
		file.Close()
		return err
	}
	previous := j.file
	j.file, j.generation, j.end = file, next, 0
	return previous.Close()
}

// Collect removes obsolete files owned by the partition after HEAD is durable.
func (j *Journal) Collect(historyFiles map[string]bool) error {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return err
	}
	journalName := filepath.Base(generationPath(j.dir, "journal", j.generation))
	checkpointName := filepath.Base(generationPath(j.dir, "checkpoint", j.generation))
	changed := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == journalName || name == checkpointName || historyFiles[name] {
			continue
		}
		if !strings.HasPrefix(name, ".minimq-") && !ownedFile(name, "journal-") &&
			!ownedFile(name, "checkpoint-") && !ownedFile(name, "history-") {
			continue
		}
		if err := os.Remove(filepath.Join(j.dir, name)); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return openAndSync(j.dir, j.sync)
	}
	return nil
}

func ownedFile(name, prefix string) bool {
	id, found := strings.CutPrefix(name, prefix)
	if !found || len(id) != 16 {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// stageFile syncs file contents and renames it. The caller syncs the directory
// before publishing any durable reference to its name.
func (j *Journal) stageFile(path string, data []byte) error {
	temp, err := os.CreateTemp(j.dir, ".minimq-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	var start time.Time
	if j.ObserveWrite != nil {
		start = time.Now()
	}
	_, writeErr := temp.Write(data)
	if j.ObserveWrite != nil {
		j.ObserveWrite(time.Since(start))
	}
	if writeErr == nil {
		writeErr = j.sync(temp)
	}
	if err := errors.Join(writeErr, temp.Close()); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

func syncDirectory(dir string) error {
	return openAndSync(dir, (*os.File).Sync)
}

func openAndSync(dir string, syncFile func(*os.File) error) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(syncFile(f), f.Close())
}

func (j *Journal) sync(f *os.File) error {
	if j.ObserveSync == nil {
		return f.Sync()
	}
	start := time.Now()
	err := f.Sync()
	j.ObserveSync(time.Since(start))
	return err
}

func (j *Journal) Close() error {
	var err error
	if j.file != nil {
		err = j.file.Close()
	}
	return errors.Join(err, j.lock.Close())
}
