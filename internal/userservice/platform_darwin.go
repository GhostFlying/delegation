//go:build darwin

package userservice

import (
	"errors"
	"os"
	"path/filepath"
)

func platformInstall(binaryPath, configPath string) (Result, error) {
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
