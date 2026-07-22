//go:build linux || windows

package appserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	parentDeathHelperEnvironment  = "DELEGATION_APP_SERVER_PARENT_DEATH_HELPER"
	parentDeathHomeEnvironment    = "DELEGATION_APP_SERVER_PARENT_DEATH_HOME"
	parentDeathPIDFileEnvironment = "DELEGATION_APP_SERVER_PARENT_DEATH_PID_FILE"
	parentDeathConnectorHelper    = "connector"
	parentDeathAppServerHelper    = "app-server"
)

func TestPlatformOwnerKillsAppServerAfterConnectorHardDeath(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := filepath.Join(t.TempDir(), "app-server-heartbeat")
	pidFile := filepath.Join(t.TempDir(), "app-server-pid")
	command := exec.Command(executable, "-test.run=^$")
	command.Env = setEnvironment(os.Environ(), parentDeathHelperEnvironment, parentDeathConnectorHelper)
	command.Env = setEnvironment(command.Env, parentDeathHomeEnvironment, t.TempDir())
	command.Env = setEnvironment(command.Env, helperFileEnvironment, heartbeat)
	command.Env = setEnvironment(command.Env, parentDeathPIDFileEnvironment, pidFile)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	cleanupAppServer := true
	t.Cleanup(func() {
		if cleanupAppServer {
			cleanupParentDeathProcess(pidFile)
		}
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	waitForFileGrowth(t, heartbeat)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("connector helper did not report its hard termination")
	}
	waited = true
	assertFileStopsGrowing(t, heartbeat)
	cleanupAppServer = false
	if stderr.Len() != 0 {
		t.Fatalf("connector helper stderr = %q", stderr.String())
	}
}

func runParentDeathHelperIfRequested() (bool, int) {
	switch os.Getenv(parentDeathHelperEnvironment) {
	case "":
		return false, 0
	case parentDeathConnectorHelper:
		return true, runParentDeathConnectorHelper()
	case parentDeathAppServerHelper:
		return true, runParentDeathAppServerHelper()
	default:
		_, _ = fmt.Fprintln(os.Stderr, "invalid parent-death helper mode")
		return true, 1
	}
}

func runParentDeathConnectorHelper() int {
	executable, err := os.Executable()
	if err != nil {
		return parentDeathHelperFailure("resolve app-server helper: %v", err)
	}
	client, err := Start(context.Background(), Options{
		Binary: executable, CodexHome: os.Getenv(parentDeathHomeEnvironment),
		Environment: map[string]string{
			parentDeathHelperEnvironment:  parentDeathAppServerHelper,
			helperFileEnvironment:         os.Getenv(helperFileEnvironment),
			parentDeathPIDFileEnvironment: os.Getenv(parentDeathPIDFileEnvironment),
		},
	})
	if err != nil {
		return parentDeathHelperFailure("start app-server helper: %v", err)
	}
	defer client.Close(context.Background())
	for {
		time.Sleep(time.Hour)
	}
}

func runParentDeathAppServerHelper() int {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64<<10), MaxMessageBytes+1)
	var initialize struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &initialize) != nil || initialize.Method != "initialize" {
		return parentDeathHelperFailure("missing initialize request")
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"id": initialize.ID, "result": map[string]string{"server": "parent-death-test-helper"},
	}); err != nil {
		return parentDeathHelperFailure("write initialize response: %v", err)
	}
	var initialized struct {
		Method string `json:"method"`
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &initialized) != nil || initialized.Method != "initialized" {
		return parentDeathHelperFailure("missing initialized notification")
	}
	if err := os.WriteFile(
		os.Getenv(parentDeathPIDFileEnvironment),
		[]byte(strconv.Itoa(os.Getpid())),
		0o600,
	); err != nil {
		return parentDeathHelperFailure("write app-server pid: %v", err)
	}
	for {
		file, err := os.OpenFile(
			os.Getenv(helperFileEnvironment),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0o600,
		)
		if err != nil {
			return parentDeathHelperFailure("open heartbeat: %v", err)
		}
		_, writeErr := file.WriteString("x")
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return parentDeathHelperFailure("write heartbeat: %v %v", writeErr, closeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cleanupParentDeathProcess(pidFile string) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return
	}
	process, err := os.FindProcess(pid)
	if err == nil {
		_ = process.Kill()
	}
}

func parentDeathHelperFailure(format string, args ...any) int {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	return 2
}
