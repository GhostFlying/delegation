//go:build darwin

package appserver

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDarwinAppServerRequiresExplicitSupervisorBinary(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	_, err = validateOptions(Options{Binary: executable, CodexHome: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "supervisor binary") {
		t.Fatalf("validateOptions() error = %v", err)
	}
}

func TestDarwinAppServerUsesExplicitSupervisorBinary(t *testing.T) {
	command := exec.Command("/usr/bin/true", "app-server", "--listen", "stdio://")
	const supervisor = "/explicit/delegation"
	owner, err := preparePlatformOwnedProcess(command, supervisor, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Terminate(); err != nil {
			t.Errorf("terminate supervisor owner: %v", err)
		}
	})
	if command.Path != supervisor ||
		len(command.Args) != 2 ||
		command.Args[0] != supervisor ||
		command.Args[1] != darwinSupervisorArgument {
		t.Fatalf("supervisor command = %q", command.Args)
	}
	if len(command.ExtraFiles) != 1 || command.ExtraFiles[0] == nil {
		t.Fatalf("supervisor extra files = %#v", command.ExtraFiles)
	}
	if command.SysProcAttr == nil || !command.SysProcAttr.Setpgid {
		t.Fatalf("supervisor process attributes = %#v", command.SysProcAttr)
	}
	environment := make(map[string]string, len(command.Env))
	for _, entry := range command.Env {
		name, value, found := strings.Cut(entry, "=")
		if found {
			environment[name] = value
		}
	}
	if environment[darwinSupervisorModeEnvironment] != "1" ||
		environment[darwinSupervisorWatchdogEnvironment] != "3" ||
		environment[darwinSupervisorTimeoutEnvironment] != time.Second.String() {
		t.Fatalf("supervisor environment = %#v", environment)
	}
	var target darwinSupervisorTarget
	if err := json.Unmarshal([]byte(environment[darwinSupervisorTargetEnvironment]), &target); err != nil {
		t.Fatal(err)
	}
	if target.Binary != "/usr/bin/true" ||
		!slices.Equal(target.Args, []string{"app-server", "--listen", "stdio://"}) {
		t.Fatalf("supervisor target = %#v", target)
	}
}

func TestDarwinSupervisorOwnerDoesNotKillCleanerAfterForce(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	command := exec.Command("/bin/sh", "-c", "sleep 10")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	}()
	owner := &darwinSupervisorOwner{writer: writer}
	if err := owner.Attach(command.Process); err != nil {
		t.Fatal(err)
	}
	if err := owner.Terminate(); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(darwinContainmentTimeout + 500*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if err := command.Process.Signal(syscall.Signal(0)); err != nil &&
		!errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("inspect cleaner process: %v", err)
	} else if errors.Is(err, os.ErrProcessDone) {
		t.Fatal("owner killed the cleaner process after its local deadline")
	}
}

func TestDarwinSupervisorOwnerClassifiesCleanupExit(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "exit 125")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	result := (&darwinSupervisorOwner{}).Wait(command)
	if result.exitErr != nil || result.cleanupErr == nil {
		t.Fatalf("Wait() result = %#v", result)
	}
}

func TestDarwinSupervisorExitCodeReservesCleanupStatus(t *testing.T) {
	if code := supervisorExitCode(errDarwinSupervisorCleanup); code != darwinSupervisorCleanupExitCode {
		t.Fatalf("cleanup exit code = %d", code)
	}
	command := exec.Command("/bin/sh", "-c", "exit 125")
	err := command.Run()
	if code := supervisorExitCode(err); code == darwinSupervisorCleanupExitCode {
		t.Fatalf("ordinary child exit used reserved cleanup code %d", code)
	}
}

func TestDarwinSupervisorClassifiesMonitorAndContainmentErrors(t *testing.T) {
	monitorErr := errors.New("injected exit monitor failure")
	err := darwinSupervisorChildError(darwinSupervisorChildResult{monitorErr: monitorErr}, false)
	if !errors.Is(err, monitorErr) || errors.Is(err, errDarwinSupervisorCleanup) {
		t.Fatalf("monitor-only error = %v", err)
	}

	containErr := errors.New("injected containment failure")
	err = darwinSupervisorChildError(darwinSupervisorChildResult{containErr: containErr}, false)
	if !errors.Is(err, containErr) || !errors.Is(err, errDarwinSupervisorCleanup) {
		t.Fatalf("containment error = %v", err)
	}
}

func TestDarwinSupervisorChildResultRetainsQueuedFatalError(t *testing.T) {
	fatalErr := errors.New("injected tracker fatal error")
	tracker := &darwinDescendantTracker{fatal: make(chan error, 1)}
	tracker.fatal <- fatalErr
	err := darwinSupervisorObservedChild(tracker, darwinSupervisorChildResult{}, false)
	if !errors.Is(err, fatalErr) {
		t.Fatalf("observed child error = %v", err)
	}
}
