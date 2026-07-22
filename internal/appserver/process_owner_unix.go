//go:build linux || darwin

package appserver

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type processGroupOwner struct {
	process     *os.Process
	pgid        int
	once        sync.Once
	mu          sync.Mutex
	groupUnsafe bool
	forced      bool
	err         error
	waitForExit func(int) error
	killGroup   func(int, unix.Signal) error
}

func (o *processGroupOwner) Attach(process *os.Process) error {
	o.process = process
	o.pgid = process.Pid
	return nil
}

func (o *processGroupOwner) Wait(command *exec.Cmd) processWaitResult {
	if o.process == nil {
		return processWaitResult{cleanupErr: errors.New("app-server process group is not attached")}
	}
	waitForExit := o.waitForExit
	if waitForExit == nil {
		waitForExit = waitForProcessExit
	}
	if err := waitForExit(o.process.Pid); err != nil {
		// If the platform exit monitor is unavailable, prioritize reaping the
		// child and avoid signaling a group whose leader might have been reused.
		o.mu.Lock()
		o.groupUnsafe = true
		o.mu.Unlock()
		return processWaitResult{
			exitErr:    command.Wait(),
			cleanupErr: fmt.Errorf("monitor app-server process-group exit: %w", err),
		}
	}
	// The direct child remains waitable, so its PID/PGID cannot be reused while
	// remaining group members are terminated.
	terminateErr := o.terminate(true)
	waitErr := command.Wait()
	o.mu.Lock()
	forced := o.forced
	o.mu.Unlock()
	if forced && isExpectedForcedUnixExit(waitErr) {
		waitErr = nil
	}
	return processWaitResult{exitErr: waitErr, cleanupErr: terminateErr}
}

func (o *processGroupOwner) Terminate() error {
	o.mu.Lock()
	o.forced = true
	o.mu.Unlock()
	return o.terminate(false)
}

func isExpectedForcedUnixExit(err error) bool {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return false
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

func (o *processGroupOwner) terminate(leaderExited bool) error {
	o.once.Do(func() {
		o.mu.Lock()
		groupUnsafe := o.groupUnsafe
		o.mu.Unlock()
		// This owns the app-server process group, not an OS containment
		// boundary. Codex deliberately creates separate groups for MCP and
		// shell children and owns their pipe/PDEATHSIG lifecycle. On macOS,
		// cleanup additionally tracks processes that remain discoverable in
		// the live ancestry tree; an immediately daemonizing double-fork may
		// reparent before discovery. Hostile same-UID detachment is outside
		// M2's threat model.
		if o.pgid > 0 && !groupUnsafe {
			killGroup := o.killGroup
			if killGroup == nil {
				killGroup = unix.Kill
			}
			if err := killGroup(-o.pgid, unix.SIGKILL); err != nil &&
				!errors.Is(err, unix.ESRCH) &&
				!(leaderExited && isExitedProcessGroupError(err)) {
				o.err = errors.Join(o.err, err)
			}
		}
		if o.process != nil {
			if err := o.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				o.err = errors.Join(o.err, err)
			}
		}
	})
	return o.err
}
