//go:build linux

package appserver

import (
	"os/exec"
	"syscall"
	"time"
)

func preparePlatformOwnedProcess(command *exec.Cmd, _ string, _ time.Duration) (ownedProcess, error) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	return &processGroupOwner{}, nil
}
