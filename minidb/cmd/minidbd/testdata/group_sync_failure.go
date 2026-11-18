// This file is compiled into the isolated acceptance-test daemon by an overlay.
package wal

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

func (log *Log) syncWithFailure(batch []request) error {
	if _, err := os.Stat(log.path + ".armed"); errors.Is(err, os.ErrNotExist) {
		return log.file.Sync()
	} else if err != nil {
		return err
	}
	info, err := log.file.Stat()
	if err != nil {
		return err
	}
	end := info.Size()
	if _, err := os.Stat(log.path + ".at-confirmed-sync"); errors.Is(err, os.ErrNotExist) {
		if len(batch) != 1 {
			return fmt.Errorf("expected one confirmed commit, got %d", len(batch))
		}
		// Hold this actual sync while the next two TCP commits enqueue.
		if err := os.WriteFile(log.path+".at-confirmed-sync", []byte(strconv.FormatInt(end, 10)), 0600); err != nil {
			return err
		}
		deadline := time.Now().Add(10 * time.Second)
		for len(log.queue) < 2 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(log.queue) != 2 {
			return errors.New("two commits did not queue behind the confirmed commit")
		}
		return log.file.Sync()
	} else if err != nil {
		return err
	}

	if len(batch) != 2 {
		return fmt.Errorf("expected two commits in the failing batch, got %d", len(batch))
	}
	// The normal writer appended both frames. Simulate one possible image
	// left by a failed sync: a complete first frame and a torn last payload.
	if err := log.file.Truncate(end - int64(len(batch[1].payload)/2)); err != nil {
		return err
	}
	if err := os.WriteFile(log.path+".at-failed-sync", nil, 0600); err != nil {
		return err
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(log.path + ".release-failed-sync"); err == nil {
			return syscall.EIO
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("failed sync was not released")
}
