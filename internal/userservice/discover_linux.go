//go:build linux

package userservice

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
)

func platformDiscoverUpgradeSource(
	ctx context.Context, role ServiceRole, expected Invocation,
) (Invocation, error) {
	artifact, err := linuxServicePath(role, expected.InstanceID)
	if err != nil {
		return Invocation{}, err
	}
	state, content, err := inspectManagedFile(artifact, KindSystemd)
	if err != nil {
		return Invocation{}, err
	}
	if state != StatePrepared {
		return Invocation{}, errors.New("systemd service definition is absent or not managed")
	}
	source, err := discoverSystemdInvocation(content, role, expected)
	if err != nil {
		return Invocation{}, err
	}
	spec, err := specFor(role, expected.InstanceID)
	if err != nil {
		return Invocation{}, err
	}
	status, err := inspectSystemdUpgradeStatus(ctx, spec.systemdUnit)
	if err != nil {
		return Invocation{}, err
	}
	if filepath.Clean(status.FragmentPath) != filepath.Clean(artifact) ||
		strings.TrimSpace(status.DropInPaths) != "" || status.ActiveState != "active" ||
		(status.UnitFileState != "enabled" && status.UnitFileState != "enabled-runtime") ||
		status.MainPID <= 0 || status.ControlGroup == "" {
		return Invocation{}, errors.New("systemd service is not exact, enabled, and running")
	}
	return source, nil
}

func discoverSystemdInvocation(
	content []byte, role ServiceRole, expected Invocation,
) (Invocation, error) {
	environmentArgument := ""
	if expected.EnvironmentFile != "" {
		environmentArgument = " --environment-file " + systemdQuote(expected.EnvironmentFile)
	}
	suffix := " service run --config " + systemdQuote(expected.ConfigPath) + environmentArgument
	var encodedBinary string
	for _, line := range strings.Split(string(content), "\n") {
		if !strings.HasPrefix(line, "ExecStart=") {
			continue
		}
		if encodedBinary != "" || !strings.HasSuffix(line, suffix) {
			return Invocation{}, errors.New("systemd service has an unexpected ExecStart")
		}
		encodedBinary = strings.TrimSuffix(strings.TrimPrefix(line, "ExecStart="), suffix)
	}
	if encodedBinary == "" {
		return Invocation{}, errors.New("systemd service has no exact managed ExecStart")
	}
	binaryPath, err := unquoteSystemdPath(encodedBinary)
	if err != nil {
		return Invocation{}, err
	}
	source := expected
	source.BinaryPath = binaryPath
	descriptor, err := RenderSystemd(role, source)
	if err != nil {
		return Invocation{}, err
	}
	if !bytes.Equal(content, descriptor.Content) {
		return Invocation{}, errors.New("systemd service is not the exact managed source definition")
	}
	return source, nil
}

func unquoteSystemdPath(value string) (string, error) {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return "", errors.New("systemd managed executable is not quoted")
	}
	encoded := value[1 : len(value)-1]
	var result strings.Builder
	for index := 0; index < len(encoded); index++ {
		character := encoded[index]
		switch character {
		case '\\':
			index++
			if index >= len(encoded) || (encoded[index] != '\\' && encoded[index] != '"') {
				return "", errors.New("systemd managed executable has invalid escaping")
			}
			result.WriteByte(encoded[index])
		case '%', '$':
			index++
			if index >= len(encoded) || encoded[index] != character {
				return "", errors.New("systemd managed executable has invalid expansion escaping")
			}
			result.WriteByte(character)
		default:
			result.WriteByte(character)
		}
	}
	return result.String(), nil
}
