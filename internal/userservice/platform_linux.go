//go:build linux

package userservice

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var runSystemctl = func(args ...string) (userServiceCommandResult, error) {
	return executeUserServiceCommand("systemctl", args...)
}

func platformPrepare(binaryPath, configPath string) (Result, error) {
	descriptor, err := RenderSystemd(binaryPath, configPath)
	if err != nil {
		return Result{}, err
	}
	path, err := linuxServicePath()
	if err != nil {
		return Result{}, err
	}
	state, err := installManagedFile(path, descriptor)
	return Result{State: state, Kind: descriptor.Kind, Artifact: path}, err
}

func platformActivate(result Result, _, _ string) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate systemd user service from state %s", result.State)
	}
	reloaded, err := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if err != nil || reloaded.ExitCode != 0 {
		return reconcileSystemdFailure(result, errors.Join(err, commandFailure("reload systemd user manager", reloaded)))
	}
	enabled, err := runSystemctl(
		"--user", "--no-ask-password", "enable", "--now", SystemdUnitName,
	)
	if err != nil || enabled.ExitCode != 0 {
		return reconcileSystemdFailure(result, errors.Join(err, commandFailure("enable systemd user service", enabled)))
	}
	isEnabled, isActive, err := querySystemdState()
	if err != nil || !isEnabled || !isActive {
		result.State = StateIndeterminate
		return result, errors.Join(err, errors.New("systemd user service did not become enabled and active"))
	}
	result.State = StateActive
	return result, nil
}

func reconcileSystemdFailure(result Result, activationErr error) (Result, error) {
	enabled, active, queryErr := querySystemdState()
	if queryErr != nil {
		result.State = StateIndeterminate
		return result, errors.Join(activationErr, queryErr)
	}
	if enabled && active {
		result.State = StateActive
		return result, nil
	}
	if enabled || active {
		result.State = StateIndeterminate
	}
	return result, activationErr
}

func querySystemdState() (bool, bool, error) {
	enabled, err := runSystemctl(
		"--user", "--no-ask-password", "is-enabled", "--quiet", SystemdUnitName,
	)
	if err != nil {
		return false, false, err
	}
	active, err := runSystemctl(
		"--user", "--no-ask-password", "is-active", "--quiet", SystemdUnitName,
	)
	if err != nil {
		return enabled.ExitCode == 0, false, err
	}
	return enabled.ExitCode == 0, active.ExitCode == 0, nil
}

func linuxServicePath() (string, error) {
	configHome := os.Getenv("XDG_CONFIG_HOME")
	if configHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		configHome = filepath.Join(home, ".config")
	}
	if !filepath.IsAbs(configHome) {
		return "", errors.New("XDG_CONFIG_HOME must be absolute")
	}
	return filepath.Join(configHome, "systemd", "user", SystemdUnitName), nil
}
