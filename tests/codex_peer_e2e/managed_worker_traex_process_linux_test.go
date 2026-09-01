//go:build integration && live && linux

package codex_peer_e2e

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func traeXLiveAppServerPIDs(cliHome string, excluded int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == excluded {
			continue
		}
		processRoot := filepath.Join("/proc", entry.Name())
		command, err := os.ReadFile(filepath.Join(processRoot, "comm"))
		if err != nil || !isTraeXLiveProcessCommand(strings.TrimSpace(string(command))) {
			continue
		}
		environment, err := os.ReadFile(filepath.Join(processRoot, "environ"))
		if err != nil ||
			!containsTraeXLiveProcessValue(environment, "TRAECLI_HOME", cliHome) {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(processRoot, "cmdline"))
		if err != nil || !containsTraeXLiveProcessArguments(
			cmdline, "app-server", "--listen", "stdio://",
		) {
			continue
		}
		running, err := traeXLiveProcessRunning(pid)
		if err == nil && running {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

func traeXLiveProcessRunning(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, err
}
