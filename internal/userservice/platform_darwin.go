//go:build darwin

package userservice

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var runLaunchctl = func(args ...string) (userServiceCommandResult, error) {
	return executeUserServiceCommand("/bin/launchctl", args...)
}

func platformPrepare(binaryPath, configPath string) (Result, error) {
	descriptor, err := RenderLaunchAgent(binaryPath, configPath)
	if err != nil {
		return Result{}, err
	}
	path, err := darwinServicePath()
	if err != nil {
		return Result{}, err
	}
	state, err := installManagedFile(path, descriptor)
	return Result{State: state, Kind: descriptor.Kind, Artifact: path}, err
}

func platformActivate(result Result, _, _ string) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate LaunchAgent from state %s", result.State)
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + LaunchAgentName
	domainStatus, err := runLaunchctl("print", domain)
	if err != nil || domainStatus.ExitCode != 0 {
		return result, errors.Join(err, commandFailure("inspect LaunchAgent domain", domainStatus))
	}
	status, loaded, err := printLaunchAgent(target)
	if err != nil {
		return result, err
	}
	if loaded {
		if filepath.Clean(status.Path) != filepath.Clean(result.Artifact) {
			return result, errors.New("LaunchAgent label is loaded from an unmanaged path")
		}
	}
	enabled, err := runLaunchctl("enable", target)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if enabled.ExitCode != 0 {
		result.State = StateIndeterminate
		return result, userServiceCommandError("enable LaunchAgent", enabled)
	}
	if !loaded {
		bootstrapped, err := runLaunchctl("bootstrap", domain, result.Artifact)
		if err != nil || bootstrapped.ExitCode != 0 {
			reconciled, nowLoaded, printErr := printLaunchAgent(target)
			if printErr != nil || !nowLoaded || filepath.Clean(reconciled.Path) != filepath.Clean(result.Artifact) {
				result.State = StateIndeterminate
				return result, errors.Join(
					err,
					commandFailure("bootstrap LaunchAgent", bootstrapped),
					printErr,
				)
			}
		}
	}
	kicked, kickErr := runLaunchctl("kickstart", target)
	status, loaded, printErr := printLaunchAgent(target)
	if loaded && printErr == nil && filepath.Clean(status.Path) == filepath.Clean(result.Artifact) &&
		status.State == "running" {
		result.State = StateActive
		return result, nil
	}
	result.State = StateIndeterminate
	return result, errors.Join(
		kickErr,
		commandFailure("start LaunchAgent", kicked),
		printErr,
		errors.New("LaunchAgent did not reach the managed running state"),
	)
}

type launchAgentStatus struct {
	Path  string
	State string
}

func printLaunchAgent(target string) (launchAgentStatus, bool, error) {
	result, err := runLaunchctl("print", target)
	if err != nil {
		return launchAgentStatus{}, false, err
	}
	if result.ExitCode != 0 {
		return launchAgentStatus{}, false, nil
	}
	status, err := parseLaunchAgentStatus(result)
	return status, true, err
}

func parseLaunchAgentStatus(result userServiceCommandResult) (launchAgentStatus, error) {
	if result.Truncated {
		return launchAgentStatus{}, errors.New("launchctl service description exceeds the output limit")
	}
	scanner := bufio.NewScanner(bytes.NewReader(result.Output))
	var status launchAgentStatus
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		key, value, found := strings.Cut(line, " = ")
		if !found || (key != "path" && key != "state") {
			continue
		}
		value = strings.TrimSpace(value)
		switch key {
		case "path":
			if status.Path != "" {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate paths")
			}
			status.Path = value
		case "state":
			if status.State != "" {
				return launchAgentStatus{}, errors.New("launchctl service description contains duplicate states")
			}
			status.State = value
		}
	}
	if err := scanner.Err(); err != nil {
		return launchAgentStatus{}, fmt.Errorf("parse launchctl service description: %w", err)
	}
	if status.Path == "" || !filepath.IsAbs(status.Path) {
		return launchAgentStatus{}, errors.New("launchctl service description has no absolute managed path")
	}
	return status, nil
}

func darwinServicePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(home) {
		return "", errors.New("user home must be absolute")
	}
	return filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist"), nil
}
