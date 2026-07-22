package appserver

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type jobOwner struct {
	job           windows.Handle
	process       *os.Process
	attached      bool
	terminateJob  func(windows.Handle, uint32) error
	closeJob      func(windows.Handle) error
	assignProcess func(windows.Handle, *os.Process) error
	resumeProcess func(int) error
	forced        atomic.Bool
	once          sync.Once
	requestErr    error
	cleanupErr    error
}

func preparePlatformOwnedProcess(command *exec.Cmd, _ string, _ time.Duration) (ownedProcess, error) {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	information := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	information.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&information)),
		uint32(unsafe.Sizeof(information)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	return &jobOwner{job: job}, nil
}

func (o *jobOwner) Attach(process *os.Process) error {
	o.process = process
	assignProcess := assignProcessToJob
	if o.assignProcess != nil {
		assignProcess = o.assignProcess
	}
	if err := assignProcess(o.job, process); err != nil {
		return err
	}
	o.attached = true
	resumeProcess := resumeSuspendedProcess
	if o.resumeProcess != nil {
		resumeProcess = o.resumeProcess
	}
	if err := resumeProcess(process.Pid); err != nil {
		return fmt.Errorf("resume job-owned process: %w", err)
	}
	return nil
}

func assignProcessToJob(job windows.Handle, process *os.Process) error {
	var assignmentErr error
	if err := process.WithHandle(func(handle uintptr) {
		assignmentErr = windows.AssignProcessToJobObject(job, windows.Handle(handle))
	}); err != nil {
		return err
	}
	return assignmentErr
}

func resumeSuspendedProcess(processID int) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("snapshot process threads: %w", err)
	}
	defer windows.CloseHandle(snapshot)

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return fmt.Errorf("enumerate process threads: %w", err)
	}
	resumed := 0
	for {
		if entry.OwnerProcessID == uint32(processID) {
			thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if err != nil {
				return fmt.Errorf("open suspended process thread %d: %w", entry.ThreadID, err)
			}
			_, resumeErr := windows.ResumeThread(thread)
			closeErr := windows.CloseHandle(thread)
			if err := errors.Join(resumeErr, closeErr); err != nil {
				return fmt.Errorf("resume process thread %d: %w", entry.ThreadID, err)
			}
			resumed++
		}
		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				break
			}
			return fmt.Errorf("enumerate process threads: %w", err)
		}
	}
	if resumed == 0 {
		return errors.New("suspended process has no discoverable thread")
	}
	return nil
}

func (o *jobOwner) Wait(command *exec.Cmd) processWaitResult {
	waitErr := command.Wait()
	var exitError *exec.ExitError
	if o.forced.Load() && errors.As(waitErr, &exitError) && exitError.ExitCode() == 1 {
		waitErr = nil
	}
	_ = o.release(true)
	return processWaitResult{exitErr: waitErr, cleanupErr: o.cleanupErr}
}

func (o *jobOwner) Terminate() error {
	o.forced.Store(true)
	return o.release(false)
}

func (o *jobOwner) release(rootExited bool) error {
	o.once.Do(func() {
		if o.job != 0 {
			terminateJob := windows.TerminateJobObject
			if o.terminateJob != nil {
				terminateJob = o.terminateJob
			}
			closeJob := windows.CloseHandle
			if o.closeJob != nil {
				closeJob = o.closeJob
			}
			// KILL_ON_JOB_CLOSE terminates any descendants left after the root
			// exits. TerminateJobObject returns ERROR_INVALID_PARAMETER for an
			// already-empty job, so reserve it for active termination.
			var terminateErr error
			if !rootExited {
				terminateErr = terminateJob(o.job, 1)
			}
			closeErr := closeJob(o.job)
			if errors.Is(terminateErr, windows.ERROR_INVALID_PARAMETER) && closeErr == nil {
				terminateErr = nil
			}
			o.requestErr = errors.Join(o.requestErr, terminateErr)
			o.cleanupErr = errors.Join(o.cleanupErr, closeErr)
			o.job = 0
		}
		// The direct process is a fallback only when assignment failed. Once
		// attached, the job is authoritative and a redundant Process.Kill can
		// race Cmd.Wait into ERROR_INVALID_PARAMETER on Windows.
		if !rootExited && !o.attached && o.process != nil {
			if err := o.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				o.requestErr = errors.Join(o.requestErr, err)
			}
		}
	})
	return errors.Join(o.requestErr, o.cleanupErr)
}
