package pages

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/celanwang/miniinfra/minidb/internal/disk"
)

const (
	Size           = 4096
	dataOffset     = 16
	magic          = "MINIDB-PAGES-1\n"
	metadataOffset = 4 + len(magic)
)

type Metadata struct {
	LSN       int64
	Sequence  uint64
	LastRowID uint64
}

func Save(path string, meta Metadata, encode func(io.Writer) error) error {
	file, err := disk.Stage(path)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Seek(Size, io.SeekStart); err != nil {
		return err
	}
	writer := pageWriter{out: file}
	if err := encode(&writer); err != nil {
		return err
	}
	if err := writer.flush(); err != nil {
		return err
	}
	var header [Size]byte
	copy(header[4:], magic)
	binary.BigEndian.PutUint64(header[metadataOffset:], uint64(meta.LSN))
	binary.BigEndian.PutUint64(header[metadataOffset+8:], meta.Sequence)
	binary.BigEndian.PutUint64(header[metadataOffset+16:], meta.LastRowID)
	binary.BigEndian.PutUint64(header[metadataOffset+24:], writer.number)
	seal(&header)
	if _, err := file.WriteAt(header[:], 0); err != nil {
		return err
	}
	return disk.Publish(file, path)
}

// Load opens only the published generation. The LSN in its header covers
// every record in the image, and the file must contain all declared pages.
func Load(path string, decode func(Metadata, io.Reader) error) (int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var header [Size]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return 0, fmt.Errorf("read checkpoint header: %w", err)
	}
	if !valid(&header) || string(header[4:metadataOffset]) != magic {
		return 0, errors.New("invalid checkpoint header")
	}
	lsn := binary.BigEndian.Uint64(header[metadataOffset:])
	if lsn == 0 || lsn > math.MaxInt64 {
		return 0, errors.New("invalid checkpoint WAL position")
	}
	count := binary.BigEndian.Uint64(header[metadataOffset+24:])
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size()%Size != 0 || count != uint64(info.Size()/Size-1) {
		return 0, errors.New("checkpoint page count does not match file size")
	}
	meta := Metadata{
		LSN:       int64(lsn),
		Sequence:  binary.BigEndian.Uint64(header[metadataOffset+8:]),
		LastRowID: binary.BigEndian.Uint64(header[metadataOffset+16:]),
	}
	reader := pageReader{in: file, count: count}
	if err := decode(meta, &reader); err != nil {
		return 0, err
	}
	if reader.number != count || len(reader.remaining) != 0 {
		return 0, errors.New("checkpoint decoder did not consume all pages")
	}
	return meta.LSN, nil
}

func seal(page *[Size]byte) {
	binary.BigEndian.PutUint32(page[:4], crc32.ChecksumIEEE(page[4:]))
}

func valid(page *[Size]byte) bool {
	return crc32.ChecksumIEEE(page[4:]) == binary.BigEndian.Uint32(page[:4])
}

type pageWriter struct {
	out    io.Writer
	page   [Size]byte
	number uint64
	used   int
}

func (w *pageWriter) Write(data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		n := copy(w.page[dataOffset+w.used:], data)
		w.used += n
		total += n
		data = data[n:]
		if dataOffset+w.used == Size {
			if err := w.flush(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func (w *pageWriter) flush() error {
	if w.used == 0 {
		return nil
	}
	w.number++
	binary.BigEndian.PutUint32(w.page[4:8], uint32(w.used))
	binary.BigEndian.PutUint64(w.page[8:16], w.number)
	seal(&w.page)
	n, err := w.out.Write(w.page[:])
	if err != nil {
		return err
	}
	if n != Size {
		return io.ErrShortWrite
	}
	clear(w.page[:])
	w.used = 0
	return nil
}

type pageReader struct {
	in            io.Reader
	page          [Size]byte
	count, number uint64
	remaining     []byte
}

func (r *pageReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(r.remaining) == 0 {
		if r.number == r.count {
			return 0, io.EOF
		}
		r.number++
		if _, err := io.ReadFull(r.in, r.page[:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("read checkpoint page %d: %w", r.number, err)
		}
		payload := binary.BigEndian.Uint32(r.page[4:8])
		if !valid(&r.page) || binary.BigEndian.Uint64(r.page[8:16]) != r.number ||
			payload == 0 || payload > Size-dataOffset {
			return 0, fmt.Errorf("invalid checkpoint page %d", r.number)
		}
		r.remaining = r.page[dataOffset : dataOffset+int(payload)]
	}
	n := copy(dst, r.remaining)
	r.remaining = r.remaining[n:]
	return n, nil
}
