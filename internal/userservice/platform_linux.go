//go:build linux

package userservice

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var runSystemctl = func(args ...string) (userServiceCommandResult, error) {
	return executeUserServiceCommand("systemctl", args...)
}

var waitForLinuxServiceReady = waitForServiceReady

func platformPrepare(role ServiceRole, binaryPath, configPath string) (Result, error) {
	descriptor, err := RenderSystemd(role, binaryPath, configPath)
	if err != nil {
		return Result{}, err
	}
	path, err := linuxServicePath(role)
	if err != nil {
		return Result{}, err
	}
	state, err := installManagedFile(path, descriptor)
	return Result{State: state, Kind: descriptor.Kind, Artifact: path, Role: role}, err
}

func platformActivate(result Result, binaryPath, configPath string) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate systemd user service from state %s", result.State)
	}
	spec, err := specFor(result.Role)
	if err != nil {
		return result, err
	}
	reloaded, err := runSystemctl("--user", "--no-ask-password", "daemon-reload")
	if err != nil || reloaded.ExitCode != 0 {
		return reconcileSystemdFailure(
			result, binaryPath, configPath,
			errors.Join(err, commandFailure("reload systemd user manager", reloaded)),
		)
	}
	matched, err := querySystemdIdentity(result, binaryPath, configPath)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.New("loaded systemd unit is shadowed or has drop-in overrides")
	}
	enabled, err := runSystemctl(
		"--user", "--no-ask-password", "enable", "--now", spec.systemdUnit,
	)
	if err != nil || enabled.ExitCode != 0 {
		return reconcileSystemdFailure(
			result, binaryPath, configPath,
			errors.Join(err, commandFailure("enable systemd user service", enabled)),
		)
	}
	isEnabled, isActive, err := querySystemdState(result.Role)
	if err != nil || !isEnabled || !isActive {
		result.State = StateIndeterminate
		return result, errors.Join(err, errors.New("systemd user service did not become enabled and active"))
	}
	matched, err = querySystemdIdentity(result, binaryPath, configPath)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.New("systemd unit identity changed during activation")
	}
	if err := waitForLinuxServiceReady(configPath); err != nil {
		result.State = StateIndeterminate
		return result, fmt.Errorf("systemd user service did not become ready: %w", err)
	}
	result.State = StateActive
	return result, nil
}

func reconcileSystemdFailure(
	result Result,
	binaryPath, configPath string,
	activationErr error,
) (Result, error) {
	matched, identityErr := querySystemdIdentity(result, binaryPath, configPath)
	if identityErr != nil {
		result.State = StateIndeterminate
		return result, errors.Join(activationErr, identityErr)
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.Join(activationErr, errors.New("loaded systemd unit is shadowed or has drop-in overrides"))
	}
	enabled, active, queryErr := querySystemdState(result.Role)
	if queryErr != nil {
		result.State = StateIndeterminate
		return result, errors.Join(activationErr, queryErr)
	}
	if enabled && active {
		if readyErr := waitForLinuxServiceReady(configPath); readyErr != nil {
			result.State = StateIndeterminate
			return result, errors.Join(activationErr, fmt.Errorf("systemd user service did not become ready: %w", readyErr))
		}
		result.State = StateActive
		return result, nil
	}
	if enabled || active {
		result.State = StateIndeterminate
	}
	return result, activationErr
}

func querySystemdIdentity(result Result, binaryPath, configPath string) (bool, error) {
	descriptor, err := RenderSystemd(result.Role, binaryPath, configPath)
	if err != nil {
		return false, err
	}
	content, err := os.ReadFile(result.Artifact)
	if err != nil {
		return false, fmt.Errorf("read prepared systemd unit: %w", err)
	}
	if !bytes.Equal(content, descriptor.Content) {
		return false, nil
	}
	spec, err := specFor(result.Role)
	if err != nil {
		return false, err
	}
	show, err := runSystemctl(
		"--user", "--no-ask-password", "show", spec.systemdUnit,
		"--property=FragmentPath", "--property=DropInPaths",
	)
	if err != nil || show.ExitCode != 0 {
		return false, errors.Join(err, commandFailure("inspect loaded systemd unit", show))
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(show.Output)), "\n") {
		name, value, found := strings.Cut(line, "=")
		if !found || name == "" {
			return false, errors.New("systemd returned malformed unit identity")
		}
		if _, exists := properties[name]; exists {
			return false, errors.New("systemd returned duplicate unit identity properties")
		}
		properties[name] = value
	}
	fragment, hasFragment := properties["FragmentPath"]
	dropIns, hasDropIns := properties["DropInPaths"]
	if len(properties) != 2 || !hasFragment || !hasDropIns {
		return false, errors.New("systemd omitted required unit identity properties")
	}
	return filepath.Clean(fragment) == filepath.Clean(result.Artifact) && strings.TrimSpace(dropIns) == "", nil
}

func querySystemdState(role ServiceRole) (bool, bool, error) {
	spec, err := specFor(role)
	if err != nil {
		return false, false, err
	}
	enabled, err := runSystemctl(
		"--user", "--no-ask-password", "is-enabled", "--quiet", spec.systemdUnit,
	)
	if err != nil {
		return false, false, err
	}
	active, err := runSystemctl(
		"--user", "--no-ask-password", "is-active", "--quiet", spec.systemdUnit,
	)
	if err != nil {
		return enabled.ExitCode == 0, false, err
	}
	return enabled.ExitCode == 0, active.ExitCode == 0, nil
}

func linuxServicePath(role ServiceRole) (string, error) {
	spec, err := specFor(role)
	if err != nil {
		return "", err
	}
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
	return filepath.Join(configHome, "systemd", "user", spec.systemdUnit), nil
}
