//go:build linux || darwin

package localupgrade

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type journalLock struct {
	file *os.File
}

func acquireJournalLock(path string) (*journalLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("lock upgrade journal: %w", err)
		}
		return &journalLock{file: file}, nil
	}
}

func (l *journalLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(l.file.Fd()), unix.LOCK_UN), l.file.Close())
}
