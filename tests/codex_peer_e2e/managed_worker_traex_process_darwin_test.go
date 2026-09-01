//go:build integration && live && darwin

package codex_peer_e2e

import (
	"errors"

	"golang.org/x/sys/unix"
)

func traeXLiveAppServerPIDs(cliHome string, excluded int) ([]int, error) {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, process := range processes {
		pid := int(process.Proc.P_pid)
		if pid <= 0 || pid == excluded {
			continue
		}
		metadata, err := unix.SysctlRaw("kern.procargs2", pid)
		if err != nil {
			continue
		}
		command := cString(process.Proc.P_comm[:])
		if command != "traex" && command != "traecli" {
			continue
		}
		if !containsTraeXLiveProcessValue(metadata, "TRAECLI_HOME", cliHome) ||
			!containsTraeXLiveProcessArguments(
				metadata, "app-server", "--listen", "stdio://",
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

func cString(value []byte) string {
	for index, current := range value {
		if current == 0 {
			return string(value[:index])
		}
	}
	return string(value)
}

func traeXLiveProcessRunning(pid int) (bool, error) {
	err := unix.Kill(pid, 0)
	if err == nil || errors.Is(err, unix.EPERM) {
		return true, nil
	}
	if errors.Is(err, unix.ESRCH) {
		return false, nil
	}
	return false, err
}
