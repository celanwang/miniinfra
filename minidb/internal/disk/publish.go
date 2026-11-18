package disk

import (
	"errors"
	"os"
	"path/filepath"
)

// Stage replaces any unpublished file left by a crashed writer. The caller
// must hold exclusive ownership of the database directory.
func Stage(path string) (*os.File, error) {
	name := path + ".next"
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
}

// Publish makes the staged file durable before the rename, then syncs the
// directory. A failure after rename leaves either generation recoverable.
func Publish(file *os.File, path string) error {
	if err := file.Sync(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return SyncDirectory(path)
}

func SyncDirectory(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
