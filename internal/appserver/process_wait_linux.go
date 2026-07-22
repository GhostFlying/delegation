//go:build linux

package appserver

import (
	"errors"

	"golang.org/x/sys/unix"
)

func waitForProcessExit(pid int) error {
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func isExitedProcessGroupError(error) bool {
	return false
}
