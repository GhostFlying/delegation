//go:build linux

package userservice

import (
	"errors"
	"os"
	"path/filepath"
)

func platformInstall(binaryPath, configPath string) (Result, error) {
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
