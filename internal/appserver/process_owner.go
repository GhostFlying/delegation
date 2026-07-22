package appserver

import (
	"errors"
	"os"
	"os/exec"
	"time"
)

type processWaitResult struct {
	exitErr    error
	cleanupErr error
}

func (r processWaitResult) err() error {
	return errors.Join(r.exitErr, r.cleanupErr)
}

type ownedProcess interface {
	Attach(*os.Process) error
	// Wait reaps the direct process. exitErr reports the direct process status;
	// cleanupErr is non-nil only when platform-owned descendant cleanup could
	// not be confirmed.
	Wait(*exec.Cmd) processWaitResult
	Terminate() error
}

func prepareOwnedProcess(
	command *exec.Cmd,
	supervisorBinary string,
	closeTimeout time.Duration,
) (ownedProcess, error) {
	return preparePlatformOwnedProcess(command, supervisorBinary, closeTimeout)
}
