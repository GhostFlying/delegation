//go:build windows

package userservice

import (
	"context"
	"errors"
	"slices"

	"golang.org/x/sys/windows"
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
	result, err := runTaskCommand("/Query", "/TN", spec.scheduled, "/XML")
	if err != nil || result.ExitCode != 0 {
		return Invocation{}, errors.Join(err, taskCommandFailure("query scheduled task for bootstrap upgrade", result))
	}
	current, err := parseTaskDefinition(result.Output)
	if err != nil {
		return Invocation{}, err
	}
	if !taskOwned(current, role, expected.InstanceID) || !current.Enabled {
		return Invocation{}, errors.New("Scheduled Task is not the exact enabled managed service")
	}
	if current.ActionCommand == "" || current.ActionArguments == "" {
		return Invocation{}, errors.New("Scheduled Task has no exact managed Exec action")
	}
	arguments, err := windows.DecomposeCommandLine("delegation-bootstrap " + current.ActionArguments)
	if err != nil || len(arguments) == 0 {
		return Invocation{}, errors.Join(err, errors.New("Scheduled Task has invalid managed arguments"))
	}
	arguments = arguments[1:]
	source := expected
	source.BinaryPath = current.ActionCommand
	wantArguments := []string{"service", "run", "--config", expected.ConfigPath}
	if expected.EnvironmentFile != "" {
		wantArguments = append(wantArguments, "--environment-file", expected.EnvironmentFile)
	}
	if !slices.Equal(arguments, wantArguments) {
		return Invocation{}, errors.New("Scheduled Task arguments do not match the requested service")
	}
	sid, err := windowsUserSID()
	if err != nil {
		return Invocation{}, err
	}
	descriptor, err := RenderScheduledTask(role, source, sid, windows.EscapeArg)
	if err != nil {
		return Invocation{}, err
	}
	want, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		return Invocation{}, err
	}
	want.Enabled = true
	equal, err := taskDefinitionsEquivalent(want, current, windowsTaskUserIDsEqual)
	if err != nil || !equal {
		return Invocation{}, errors.Join(err, errors.New("Scheduled Task is not the exact managed source definition"))
	}
	pids, err := queryUpgradePIDs(role, expected.InstanceID)
	if err != nil {
		return Invocation{}, err
	}
	if len(pids) == 0 {
		return Invocation{}, errors.New("Scheduled Task has no running engine process")
	}
	return source, nil
}
