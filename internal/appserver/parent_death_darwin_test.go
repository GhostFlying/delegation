//go:build darwin

package appserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	parentDeathHelperEnvironment  = "DELEGATION_APP_SERVER_PARENT_DEATH_HELPER"
	parentDeathHomeEnvironment    = "DELEGATION_APP_SERVER_PARENT_DEATH_HOME"
	parentDeathPIDFileEnvironment = "DELEGATION_APP_SERVER_PARENT_DEATH_PID_FILE"
	parentDeathConnectorHelper    = "connector"
	parentDeathAppServerHelper    = "app-server"
	parentDeathHeartbeatHelper    = "heartbeat"
)

func TestDarwinSupervisorKillsDetachedDescendantAfterConnectorSIGKILL(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := t.TempDir() + "/detached-heartbeat"
	pidFile := t.TempDir() + "/detached-pid"
	home := t.TempDir()
	command := exec.Command(executable, "-test.run=^$")
	command.Env = setEnvironment(os.Environ(), parentDeathHelperEnvironment, parentDeathConnectorHelper)
	command.Env = setEnvironment(command.Env, parentDeathHomeEnvironment, home)
	command.Env = setEnvironment(command.Env, helperFileEnvironment, heartbeat)
	command.Env = setEnvironment(command.Env, parentDeathPIDFileEnvironment, pidFile)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	var detachedIdentity darwinProcessIdentity
	t.Cleanup(func() {
		cleanupDetachedTestProcess(pidFile, detachedIdentity)
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	waitForFileGrowth(t, heartbeat)
	detachedIdentity = readDetachedTestIdentity(t, pidFile)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("connector helper did not report its hard termination")
	}
	waited = true
	assertFileStopsGrowing(t, heartbeat)
	assertDetachedTestProcessGone(t, detachedIdentity)
	if stderr.Len() != 0 {
		t.Fatalf("connector helper stderr = %q", stderr.String())
	}
}

func TestDarwinSupervisorCloseKillsDetachedDescendant(t *testing.T) {
	heartbeat := t.TempDir() + "/detached-heartbeat"
	pidFile := t.TempDir() + "/detached-pid"
	client := startDetachedDarwinTestClient(t, heartbeat, pidFile)
	var detachedIdentity darwinProcessIdentity
	t.Cleanup(func() {
		cleanupDetachedTestProcess(pidFile, detachedIdentity)
		_ = client.Close(context.Background())
	})
	waitForFileGrowth(t, heartbeat)
	detachedIdentity = readDetachedTestIdentity(t, pidFile)
	err := client.Close(context.Background())
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close error = %v, want ErrCloseTimeout", err)
	}
	if errors.Is(err, ErrProcessExitUnconfirmed) {
		t.Fatalf("Close did not confirm forced app-server exit: %v", err)
	}
	assertFileStopsGrowing(t, heartbeat)
	assertDetachedTestProcessGone(t, detachedIdentity)
}

func startDetachedDarwinTestClient(t *testing.T, heartbeat, pidFile string) *Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	client, err := Start(context.Background(), Options{
		Binary: executable, SupervisorBinary: executable,
		CodexHome: t.TempDir(), CloseTimeout: 50 * time.Millisecond,
		Environment: map[string]string{
			parentDeathHelperEnvironment:  parentDeathAppServerHelper,
			helperFileEnvironment:         heartbeat,
			parentDeathPIDFileEnvironment: pidFile,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func runParentDeathHelperIfRequested() (bool, int) {
	switch os.Getenv(parentDeathHelperEnvironment) {
	case "":
		return false, 0
	case parentDeathConnectorHelper:
		return true, runParentDeathConnectorHelper()
	case parentDeathAppServerHelper:
		return true, runDetachedAppServerHelper()
	case parentDeathHeartbeatHelper:
		return true, runDetachedHeartbeatHelper()
	default:
		_, _ = fmt.Fprintln(os.Stderr, "invalid Darwin parent-death helper mode")
		return true, 1
	}
}

func runParentDeathConnectorHelper() int {
	executable, err := os.Executable()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	client, err := Start(context.Background(), Options{
		Binary: executable, SupervisorBinary: executable,
		CodexHome:    os.Getenv(parentDeathHomeEnvironment),
		CloseTimeout: 50 * time.Millisecond,
		Environment: map[string]string{
			parentDeathHelperEnvironment:  parentDeathAppServerHelper,
			helperFileEnvironment:         os.Getenv(helperFileEnvironment),
			parentDeathPIDFileEnvironment: os.Getenv(parentDeathPIDFileEnvironment),
		},
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer client.Close(context.Background())
	for {
		time.Sleep(time.Hour)
	}
}

func runDetachedAppServerHelper() int {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), MaxMessageBytes+1)
	var initialize struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &initialize) != nil || initialize.Method != "initialize" {
		return detachedHelperFailure("missing initialize request")
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"id": initialize.ID, "result": map[string]string{"server": "detached-test-helper"},
	}); err != nil {
		return detachedHelperFailure("write initialize response: %v", err)
	}
	var initialized struct {
		Method string `json:"method"`
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &initialized) != nil || initialized.Method != "initialized" {
		return detachedHelperFailure("missing initialized notification")
	}
	executable, err := os.Executable()
	if err != nil {
		return detachedHelperFailure("resolve heartbeat helper: %v", err)
	}
	child := exec.Command(executable, "-test.run=^$")
	child.Env = setEnvironment(os.Environ(), parentDeathHelperEnvironment, parentDeathHeartbeatHelper)
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		return detachedHelperFailure("start detached heartbeat helper: %v", err)
	}
	if err := child.Process.Release(); err != nil {
		return detachedHelperFailure("release detached heartbeat helper: %v", err)
	}
	for scanner.Scan() {
	}
	if err := scanner.Err(); err != nil {
		return detachedHelperFailure("read app-server input: %v", err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func runDetachedHeartbeatHelper() int {
	pidFile := os.Getenv(parentDeathPIDFileEnvironment)
	info, exists, err := readDarwinProcessInfo(os.Getpid())
	if err != nil || !exists {
		return detachedHelperFailure("read detached process identity: %v", err)
	}
	identity := info.Identity
	identityText := fmt.Sprintf("%d %d %d", identity.PID, identity.StartSec, identity.StartUsec)
	if err := os.WriteFile(pidFile, []byte(identityText), 0o600); err != nil {
		return detachedHelperFailure("write detached pid: %v", err)
	}
	for {
		file, err := os.OpenFile(
			os.Getenv(helperFileEnvironment), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600,
		)
		if err != nil {
			return detachedHelperFailure("open heartbeat: %v", err)
		}
		_, writeErr := file.WriteString("x")
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return detachedHelperFailure("write heartbeat: %v %v", writeErr, closeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readDetachedTestIdentity(t *testing.T, pidFile string) darwinProcessIdentity {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		recorded, err := readDetachedIdentityFile(pidFile)
		if err == nil {
			info, exists, infoErr := readDarwinProcessInfo(recorded.PID)
			if infoErr == nil && exists && info.Identity == recorded {
				return recorded
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("detached test process identity was unavailable")
	return darwinProcessIdentity{}
}

func assertDetachedTestProcessGone(t *testing.T, identity darwinProcessIdentity) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		info, exists, err := readDarwinProcessInfo(identity.PID)
		if err == nil && (!exists || info.Identity != identity) {
			return
		}
		if err != nil {
			t.Fatalf("inspect detached test process: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("detached test process %d still exists", identity.PID)
}

func cleanupDetachedTestProcess(pidFile string, expected darwinProcessIdentity) {
	recorded, err := readDetachedIdentityFile(pidFile)
	if err != nil || expected.PID != 0 && recorded != expected {
		return
	}
	info, exists, err := readDarwinProcessInfo(recorded.PID)
	if err != nil || !exists || info.Identity != recorded {
		return
	}
	process, err := os.FindProcess(recorded.PID)
	if err == nil {
		_ = process.Kill()
	}
}

func readDetachedIdentityFile(path string) (darwinProcessIdentity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return darwinProcessIdentity{}, err
	}
	var identity darwinProcessIdentity
	count, err := fmt.Sscanf(
		strings.TrimSpace(string(data)),
		"%d %d %d",
		&identity.PID,
		&identity.StartSec,
		&identity.StartUsec,
	)
	if err != nil || count != 3 || identity.PID <= 0 || identity.StartSec <= 0 {
		return darwinProcessIdentity{}, errors.New("invalid detached process identity file")
	}
	return identity, nil
}

func detachedHelperFailure(format string, args ...any) int {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	return 2
}
