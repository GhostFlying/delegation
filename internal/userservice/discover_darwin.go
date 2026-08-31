//go:build darwin

package userservice

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
)

func platformDiscoverUpgradeSource(
	ctx context.Context, role ServiceRole, expected Invocation,
) (Invocation, error) {
	if err := ctx.Err(); err != nil {
		return Invocation{}, err
	}
	spec, err := specFor(role, expected.InstanceID)
	if err != nil {
		return Invocation{}, err
	}
	artifact, err := darwinServicePath(role, expected.InstanceID)
	if err != nil {
		return Invocation{}, err
	}
	state, content, err := inspectManagedFile(artifact, KindLaunchAgent)
	if err != nil {
		return Invocation{}, err
	}
	if state != StatePrepared {
		return Invocation{}, errors.New("LaunchAgent definition is absent or not managed")
	}
	status, loaded, err := printLaunchAgent(launchAgentUpgradeTarget(spec.launchAgent))
	if err != nil {
		return Invocation{}, err
	}
	if !loaded || filepath.Clean(status.Path) != filepath.Clean(artifact) ||
		status.State != "running" || status.PID <= 0 {
		return Invocation{}, errors.New("LaunchAgent is not exact and running")
	}
	binaryPath := status.Program
	if status.ArgumentsPresent {
		if len(status.Arguments) == 0 {
			return Invocation{}, errors.New("LaunchAgent has empty managed arguments")
		}
		if binaryPath != "" && filepath.Clean(binaryPath) != filepath.Clean(status.Arguments[0]) {
			return Invocation{}, errors.New("LaunchAgent program and argv executable disagree")
		}
		binaryPath = status.Arguments[0]
	}
	if binaryPath == "" {
		return Invocation{}, errors.New("LaunchAgent does not expose its managed executable")
	}
	source := expected
	source.BinaryPath = binaryPath
	if status.ArgumentsPresent && !slices.Equal(status.Arguments, launchAgentArguments(source)) {
		return Invocation{}, errors.New("LaunchAgent arguments do not match the requested service")
	}
	descriptor, err := RenderLaunchAgent(role, source)
	if err != nil {
		return Invocation{}, err
	}
	if !bytes.Equal(content, descriptor.Content) {
		return Invocation{}, errors.New("LaunchAgent is not the exact managed source definition")
	}
	return source, nil
}
