//go:build windows

package coordinatedupgrade

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

type journalLock struct {
	file *os.File
}

func acquireJournalLock(path string) (*journalLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	err = windows.LockFileEx(
		windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, ^uint32(0), ^uint32(0),
		&windows.Overlapped{},
	)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock coordinated upgrade journal: %w", err)
	}
	return &journalLock{file: file}, nil
}

func (l *journalLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := windows.UnlockFileEx(
		windows.Handle(l.file.Fd()), 0, ^uint32(0), ^uint32(0), &windows.Overlapped{},
	)
	return errors.Join(err, l.file.Close())
}
