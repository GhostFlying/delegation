//go:build windows

package appserver

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestPrepareOwnedWindowsProcessStartsSuspended(t *testing.T) {
	command := exec.Command("unused")
	owner, err := preparePlatformOwnedProcess(command, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Terminate()
	if command.SysProcAttr == nil || command.SysProcAttr.CreationFlags&windows.CREATE_SUSPENDED == 0 {
		t.Fatalf("creation flags = %#x", command.SysProcAttr.CreationFlags)
	}
}

func TestJobOwnerAssignsBeforeResumingProcess(t *testing.T) {
	steps := make([]string, 0, 2)
	owner := &jobOwner{
		job: windows.Handle(1),
		assignProcess: func(job windows.Handle, process *os.Process) error {
			steps = append(steps, "assign")
			if job != windows.Handle(1) || process.Pid != 42 {
				t.Fatalf("assignment = job %d, process %d", job, process.Pid)
			}
			return nil
		},
		resumeProcess: func(processID int) error {
			steps = append(steps, "resume")
			if processID != 42 {
				t.Fatalf("resumed process = %d", processID)
			}
			return nil
		},
	}
	if err := owner.Attach(&os.Process{Pid: 42}); err != nil {
		t.Fatal(err)
	}
	if !owner.attached || len(steps) != 2 || steps[0] != "assign" || steps[1] != "resume" {
		t.Fatalf("attached = %v, steps = %#v", owner.attached, steps)
	}
}

func TestJobOwnerKeepsJobAuthorityWhenResumeFails(t *testing.T) {
	resumeErr := errors.New("injected resume failure")
	owner := &jobOwner{
		job:           windows.Handle(1),
		assignProcess: func(windows.Handle, *os.Process) error { return nil },
		resumeProcess: func(int) error { return resumeErr },
	}
	err := owner.Attach(&os.Process{Pid: 42})
	if !errors.Is(err, resumeErr) || !owner.attached {
		t.Fatalf("Attach() = %v, attached = %v", err, owner.attached)
	}
}

func TestJobOwnerNormalizesEmptyJobTerminationAfterHandleClose(t *testing.T) {
	owner := &jobOwner{
		job:      windows.Handle(1),
		attached: true,
		terminateJob: func(windows.Handle, uint32) error {
			return windows.ERROR_INVALID_PARAMETER
		},
		closeJob: func(windows.Handle) error { return nil },
	}
	if err := owner.Terminate(); err != nil {
		t.Fatalf("Terminate() error = %v", err)
	}
}

func TestJobOwnerRetainsEmptyJobErrorWhenHandleCloseFails(t *testing.T) {
	closeErr := errors.New("injected job handle close failure")
	owner := &jobOwner{
		job:      windows.Handle(1),
		attached: true,
		terminateJob: func(windows.Handle, uint32) error {
			return windows.ERROR_INVALID_PARAMETER
		},
		closeJob: func(windows.Handle) error { return closeErr },
	}
	err := owner.Terminate()
	if !errors.Is(err, windows.ERROR_INVALID_PARAMETER) || !errors.Is(err, closeErr) {
		t.Fatalf("Terminate() error = %v", err)
	}
	if !errors.Is(owner.cleanupErr, closeErr) ||
		errors.Is(owner.cleanupErr, windows.ERROR_INVALID_PARAMETER) {
		t.Fatalf("cleanup error = %v", owner.cleanupErr)
	}
}
