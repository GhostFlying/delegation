//go:build darwin

package appserver

import (
	"errors"

	"golang.org/x/sys/unix"
)

func waitForProcessExit(pid int) error {
	queue, err := unix.Kqueue()
	if err != nil {
		return err
	}
	defer unix.Close(queue)
	change := unix.Kevent_t{
		Ident:  uint64(pid),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ONESHOT,
		Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(queue, []unix.Kevent_t{change}, nil, nil); err != nil {
		return err
	}
	for {
		events := make([]unix.Kevent_t, 1)
		count, err := unix.Kevent(queue, nil, events, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if count == 1 {
			return nil
		}
	}
}

// Darwin reports EPERM when killpg targets a group whose only remaining
// member is the already-exited, not-yet-reaped leader.
func isExitedProcessGroupError(err error) bool {
	return errors.Is(err, unix.EPERM)
}
