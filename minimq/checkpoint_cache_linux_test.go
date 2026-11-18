//go:build linux && (amd64 || arm64)

package minimq

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

func checkCheckpointCache(dir string, cold bool) (int, int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	var resident, pages int
	for _, entry := range files {
		if !strings.HasPrefix(entry.Name(), "history-") {
			continue
		}
		n, total, err := checkpointSegmentCache(filepath.Join(dir, entry.Name()), cold)
		if err != nil {
			return 0, 0, err
		}
		resident += n
		pages += total
	}
	return resident, pages, nil
}

func checkpointSegmentCache(path string, cold bool) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	if cold {
		const dontNeed = 4 // POSIX_FADV_DONTNEED
		_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, dontNeed, 0, 0)
		if errno != 0 {
			return 0, 0, fmt.Errorf("evict %s: %w", path, errno)
		}
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return 0, 0, err
	}
	defer syscall.Munmap(data)
	pageSize := os.Getpagesize()
	bits := make([]byte, (len(data)+pageSize-1)/pageSize)
	_, _, errno := syscall.Syscall(syscall.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&bits[0])))
	if errno != 0 {
		return 0, 0, fmt.Errorf("check page cache for %s: %w", path, errno)
	}
	resident := 0
	for _, bit := range bits {
		resident += int(bit & 1)
	}
	return resident, len(bits), nil
}
