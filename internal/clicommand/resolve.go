package clicommand

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexcommand"
	"github.com/GhostFlying/delegation/internal/hostkind"
)

type Launch struct {
	CommandPath      string
	RuntimePath      string
	Environment      map[string]string
	UnsetEnvironment []string
}

func Resolve(kind hostkind.Kind, command string) (Launch, error) {
	switch kind {
	case hostkind.Codex:
		resolved, err := codexcommand.Resolve(command)
		if err != nil {
			return Launch{}, err
		}
		return Launch{
			CommandPath:      resolved.CommandPath,
			RuntimePath:      resolved.NativePath,
			Environment:      resolved.Environment,
			UnsetEnvironment: resolved.UnsetEnvironment,
		}, nil
	case hostkind.TraeX:
		commandPath, err := exec.LookPath(command)
		if err != nil {
			return Launch{}, err
		}
		commandPath, err = filepath.Abs(commandPath)
		if err != nil {
			return Launch{}, fmt.Errorf("resolve TraeX command path: %w", err)
		}
		runtimePath, err := clilaunch.ResolveRuntimeExecutable(commandPath)
		if err != nil {
			return Launch{}, err
		}
		return Launch{
			CommandPath:      commandPath,
			RuntimePath:      runtimePath,
			UnsetEnvironment: codexcommand.ManagedEnvironmentKeys(),
		}, nil
	default:
		return Launch{}, fmt.Errorf("unsupported host kind %q", kind)
	}
}
