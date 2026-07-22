//go:build darwin

package appserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	darwinSupervisorArgument            = "__darwin-app-server-supervisor"
	darwinSupervisorModeEnvironment     = "_DELEGATION_DARWIN_SUPERVISOR"
	darwinSupervisorTargetEnvironment   = "_DELEGATION_DARWIN_SUPERVISOR_TARGET"
	darwinSupervisorWatchdogEnvironment = "_DELEGATION_DARWIN_SUPERVISOR_WATCHDOG_FD"
	darwinSupervisorTimeoutEnvironment  = "_DELEGATION_DARWIN_SUPERVISOR_TIMEOUT"
	darwinSupervisorCleanupExitCode     = 125
)

var errDarwinSupervisorCleanup = errors.New("Darwin app-server cleanup was not confirmed")

type darwinSupervisorTarget struct {
	Binary string   `json:"binary"`
	Args   []string `json:"args"`
}

type darwinSupervisorChildResult struct {
	monitorErr error
	containErr error
	waitErr    error
}

// RunDarwinSupervisorIfRequested must run before normal argument dispatch in
// any executable that calls Start on Darwin.
func RunDarwinSupervisorIfRequested() (bool, int) {
	if os.Getenv(darwinSupervisorModeEnvironment) != "1" ||
		len(os.Args) != 2 || os.Args[1] != darwinSupervisorArgument {
		return false, 0
	}
	if err := runDarwinSupervisor(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "delegation: managed app-server supervisor: %v\n", err)
		return true, supervisorExitCode(err)
	}
	return true, 0
}

func runDarwinSupervisor() error {
	var target darwinSupervisorTarget
	if err := json.Unmarshal([]byte(os.Getenv(darwinSupervisorTargetEnvironment)), &target); err != nil {
		return errors.New("invalid child specification")
	}
	if !filepath.IsAbs(target.Binary) || len(target.Args) == 0 || len(target.Args) > 32 {
		return errors.New("invalid child command")
	}
	watchdogFD, err := strconv.Atoi(os.Getenv(darwinSupervisorWatchdogEnvironment))
	if err != nil || watchdogFD < 3 || watchdogFD > 1024 {
		return errors.New("invalid watchdog descriptor")
	}
	closeTimeout, err := time.ParseDuration(os.Getenv(darwinSupervisorTimeoutEnvironment))
	if err != nil || closeTimeout < time.Millisecond || closeTimeout > time.Minute {
		return errors.New("invalid close timeout")
	}
	watchdog := os.NewFile(uintptr(watchdogFD), "delegation-app-server-watchdog")
	if watchdog == nil {
		return errors.New("watchdog descriptor is unavailable")
	}
	defer watchdog.Close()
	unix.CloseOnExec(watchdogFD)

	child := exec.Command(target.Binary, target.Args...)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Env = supervisorChildEnvironment(os.Environ())
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	childInput, err := child.StdinPipe()
	if err != nil {
		return fmt.Errorf("open managed app-server input: %w", err)
	}
	if err := child.Start(); err != nil {
		_ = childInput.Close()
		return fmt.Errorf("start managed app-server: %w", err)
	}
	tracker, err := newDarwinDescendantTracker(child.Process.Pid)
	if err != nil {
		_ = childInput.Close()
		killErr := killDarwinChildGroup(child)
		return errors.Join(
			markDarwinSupervisorCleanup(errors.Join(err, killErr)),
			child.Wait(),
		)
	}
	go func() {
		_, _ = io.Copy(childInput, os.Stdin)
		_ = childInput.Close()
	}()

	childDone := make(chan darwinSupervisorChildResult, 1)
	go func() {
		monitorErr := waitForProcessExit(child.Process.Pid)
		containErr := tracker.Terminate()
		childDone <- darwinSupervisorChildResult{
			monitorErr: monitorErr,
			containErr: containErr,
			waitErr:    child.Wait(),
		}
	}()

	type watchdogResult struct {
		force bool
		err   error
	}
	watchdogDone := make(chan watchdogResult, 1)
	go func() {
		var buffer [1]byte
		count, err := watchdog.Read(buffer[:])
		if count == 1 {
			result := watchdogResult{force: true}
			if buffer[0] != darwinSupervisorForceByte {
				result.err = errors.New("watchdog received an invalid force request")
			}
			watchdogDone <- result
			return
		}
		if errors.Is(err, io.EOF) {
			err = nil
		} else if err == nil {
			err = errors.New("watchdog returned no data")
		}
		watchdogDone <- watchdogResult{err: err}
	}()

	select {
	case child := <-childDone:
		return darwinSupervisorObservedChild(tracker, child, false)
	case fatalErr := <-tracker.Fatal():
		child, cleanupErr := awaitDarwinSupervisorTermination(tracker.Terminate, childDone, closeTimeout)
		if cleanupErr != nil {
			return errors.Join(fatalErr, cleanupErr)
		}
		return errors.Join(fatalErr, darwinSupervisorChildError(child, true))
	case watchdog := <-watchdogDone:
		if watchdog.force || watchdog.err != nil {
			child, cleanupErr := awaitDarwinSupervisorTermination(tracker.Terminate, childDone, closeTimeout)
			if cleanupErr != nil {
				return errors.Join(watchdog.err, cleanupErr)
			}
			return errors.Join(
				watchdog.err,
				darwinSupervisorObservedChild(tracker, child, true),
			)
		}
		timer := time.NewTimer(closeTimeout)
		defer timer.Stop()
		select {
		case child := <-childDone:
			return darwinSupervisorObservedChild(tracker, child, false)
		case fatalErr := <-tracker.Fatal():
			child, cleanupErr := awaitDarwinSupervisorTermination(tracker.Terminate, childDone, closeTimeout)
			if cleanupErr != nil {
				return errors.Join(fatalErr, cleanupErr)
			}
			return errors.Join(fatalErr, darwinSupervisorChildError(child, true))
		case <-timer.C:
			child, cleanupErr := awaitDarwinSupervisorTermination(tracker.Terminate, childDone, closeTimeout)
			if cleanupErr != nil {
				return cleanupErr
			}
			return darwinSupervisorObservedChild(tracker, child, true)
		}
	}
}

func awaitDarwinSupervisorTermination(
	terminate func() error,
	childDone <-chan darwinSupervisorChildResult,
	timeout time.Duration,
) (darwinSupervisorChildResult, error) {
	type outcome struct {
		child      darwinSupervisorChildResult
		containErr error
	}
	finished := make(chan outcome, 1)
	go func() {
		containErr := terminate()
		finished <- outcome{child: <-childDone, containErr: containErr}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-finished:
		if result.child.containErr == nil {
			result.child.containErr = result.containErr
		}
		return result.child, nil
	case <-timer.C:
		return darwinSupervisorChildResult{}, markDarwinSupervisorCleanup(
			errors.New("managed app-server did not exit after forced cleanup"),
		)
	}
}

func darwinSupervisorObservedChild(
	tracker *darwinDescendantTracker,
	child darwinSupervisorChildResult,
	expectedTermination bool,
) error {
	select {
	case fatalErr := <-tracker.Fatal():
		return errors.Join(fatalErr, darwinSupervisorChildError(child, true))
	default:
		return darwinSupervisorChildError(child, expectedTermination)
	}
}

func darwinSupervisorChildError(
	child darwinSupervisorChildResult,
	expectedTermination bool,
) error {
	waitErr := child.waitErr
	if expectedTermination || child.monitorErr != nil {
		waitErr = darwinExpectedTerminationError(waitErr)
	}
	return errors.Join(
		child.monitorErr,
		markDarwinSupervisorCleanup(child.containErr),
		waitErr,
	)
}

func markDarwinSupervisorCleanup(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(errDarwinSupervisorCleanup, err)
}

func darwinExpectedTerminationError(err error) error {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return err
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	if ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
		return nil
	}
	return err
}

func killDarwinChildGroup(child *exec.Cmd) error {
	err := unix.Kill(-child.Process.Pid, unix.SIGKILL)
	if err == nil || errors.Is(err, unix.ESRCH) {
		return nil
	}
	return errors.Join(err, child.Process.Kill())
}

func supervisorChildEnvironment(environment []string) []string {
	for _, name := range []string{
		darwinSupervisorModeEnvironment,
		darwinSupervisorTargetEnvironment,
		darwinSupervisorWatchdogEnvironment,
		darwinSupervisorTimeoutEnvironment,
	} {
		environment = removeEnvironment(environment, name)
	}
	return environment
}

func supervisorExitCode(err error) int {
	if errors.Is(err, errDarwinSupervisorCleanup) {
		return darwinSupervisorCleanupExitCode
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if exitError.ExitCode() == darwinSupervisorCleanupExitCode {
			return 1
		}
		return exitError.ExitCode()
	}
	return 1
}
