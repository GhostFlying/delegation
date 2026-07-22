//go:build linux || darwin

package appserver

import (
	"errors"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProcessGroupMonitorFallbackDoesNotSignalReapedGroup(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "exit 0")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	killGroupCalls := 0
	monitorErr := errors.New("monitor unavailable")
	owner := &processGroupOwner{
		waitForExit: func(int) error { return monitorErr },
		killGroup: func(int, unix.Signal) error {
			killGroupCalls++
			return nil
		},
	}
	if err := owner.Attach(command.Process); err != nil {
		t.Fatal(err)
	}
	result := owner.Wait(command)
	if result.exitErr != nil || !errors.Is(result.cleanupErr, monitorErr) {
		t.Fatalf("Wait() result = %#v, want classified monitor failure", result)
	}
	if err := owner.Terminate(); err != nil {
		t.Fatalf("Terminate() error = %v", err)
	}
	if killGroupCalls != 0 {
		t.Fatalf("reaped process group was signaled %d times", killGroupCalls)
	}
}
