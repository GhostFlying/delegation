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

var waitForDarwinServiceReady = waitForServiceReady

func platformPrepare(role ServiceRole, invocation Invocation) (Result, error) {
	descriptor, err := RenderLaunchAgent(role, invocation)
	if err != nil {
		return Result{}, err
	}
	path, err := darwinServicePath(role)
	if err != nil {
		return Result{}, err
	}
	state, err := installManagedFile(path, descriptor)
	return Result{State: state, Kind: descriptor.Kind, Artifact: path, Role: role}, err
}

func platformActivate(result Result, invocation Invocation) (Result, error) {
	if result.State != StatePrepared && result.State != StateActive {
		return result, fmt.Errorf("cannot activate LaunchAgent from state %s", result.State)
	}
	spec, err := specFor(result.Role)
	if err != nil {
		return result, err
	}
	matched, err := launchAgentDefinitionMatches(result, invocation)
	if err != nil {
		result.State = StateIndeterminate
		return result, err
	}
	if !matched {
		result.State = StateForeignConflict
		return result, errors.New("prepared LaunchAgent definition changed before activation")
	}
	domain := fmt.Sprintf("gui/%d", os.Geteuid())
	target := domain + "/" + spec.launchAgent
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
			result.State = StateForeignConflict
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
	if loaded {
		bootedOut, bootoutErr := runLaunchctl("bootout", target)
		reconciled, stillLoaded, printErr := printLaunchAgent(target)
		if printErr != nil {
			result.State = StateIndeterminate
			return result, errors.Join(bootoutErr, commandFailure("unload LaunchAgent", bootedOut), printErr)
		}
		if stillLoaded {
			if filepath.Clean(reconciled.Path) != filepath.Clean(result.Artifact) {
				result.State = StateForeignConflict
				return result, errors.Join(bootoutErr, errors.New("LaunchAgent identity changed during unload"))
			}
			result.State = StateIndeterminate
			return result, errors.Join(
				bootoutErr,
				commandFailure("unload LaunchAgent", bootedOut),
				errors.New("managed LaunchAgent remained loaded after bootout"),
			)
		}
	}
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
	kicked, kickErr := runLaunchctl("kickstart", target)
	status, loaded, printErr := printLaunchAgent(target)
	if loaded && printErr == nil && filepath.Clean(status.Path) != filepath.Clean(result.Artifact) {
		result.State = StateForeignConflict
		return result, errors.Join(kickErr, errors.New("LaunchAgent identity changed during activation"))
	}
	if loaded && printErr == nil && filepath.Clean(status.Path) == filepath.Clean(result.Artifact) &&
		status.State == "running" {
		matched, definitionErr := launchAgentDefinitionMatches(result, invocation)
		if definitionErr != nil {
			result.State = StateIndeterminate
			return result, definitionErr
		}
		if !matched {
			result.State = StateForeignConflict
			return result, errors.New("LaunchAgent definition changed during activation")
		}
		if err := waitForDarwinServiceReady(invocation.ConfigPath); err != nil {
			result.State = StateIndeterminate
			return result, fmt.Errorf("LaunchAgent did not become ready: %w", err)
		}
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

func launchAgentDefinitionMatches(result Result, invocation Invocation) (bool, error) {
	descriptor, err := RenderLaunchAgent(result.Role, invocation)
	if err != nil {
		return false, err
	}
	state, content, err := inspectManagedFile(result.Artifact, KindLaunchAgent)
	if err != nil {
		return false, fmt.Errorf("inspect prepared LaunchAgent definition: %w", err)
	}
	return state == StatePrepared && bytes.Equal(content, descriptor.Content), nil
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
	type field struct {
		value  string
		indent int
	}
	var paths []field
	var states []field
	for scanner.Scan() {
		raw := scanner.Text()
		line := strings.TrimSpace(raw)
		key, value, found := strings.Cut(line, " = ")
		if !found || (key != "path" && key != "state") {
			continue
		}
		candidate := field{
			value:  strings.TrimSpace(value),
			indent: len(raw) - len(strings.TrimLeft(raw, " \t")),
		}
		switch key {
		case "path":
			paths = append(paths, candidate)
		case "state":
			states = append(states, candidate)
		}
	}
	if err := scanner.Err(); err != nil {
		return launchAgentStatus{}, fmt.Errorf("parse launchctl service description: %w", err)
	}
	if len(paths) == 0 {
		return launchAgentStatus{}, errors.New("launchctl service description has no absolute managed path")
	}
	topLevelIndent := paths[0].indent
	for _, candidate := range paths[1:] {
		topLevelIndent = min(topLevelIndent, candidate.indent)
	}
	var status launchAgentStatus
	for _, candidate := range paths {
		if candidate.indent != topLevelIndent {
			continue
		}
		if status.Path != "" {
			return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level paths")
		}
		status.Path = candidate.value
	}
	for _, candidate := range states {
		if candidate.indent != topLevelIndent {
			continue
		}
		if status.State != "" {
			return launchAgentStatus{}, errors.New("launchctl service description contains duplicate top-level states")
		}
		status.State = candidate.value
	}
	if status.Path == "" || !filepath.IsAbs(status.Path) {
		return launchAgentStatus{}, errors.New("launchctl service description has no absolute managed path")
	}
	return status, nil
}

func darwinServicePath(role ServiceRole) (string, error) {
	spec, err := specFor(role)
	if err != nil {
		return "", err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(home) {
		return "", errors.New("user home must be absolute")
	}
	return filepath.Join(home, "Library", "LaunchAgents", spec.launchAgent+".plist"), nil
}
