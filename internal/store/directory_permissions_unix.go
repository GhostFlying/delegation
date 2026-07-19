//go:build !windows

package store

import (
	"errors"
	"fmt"
	"os"
)

func createPrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

func validatePrivateDirectory(_ string, info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("broker state directory must not be accessible by group or other users")
	}
	return nil
}

func validatePrivateStateFile(string, os.FileInfo) error {
	return nil
}

func protectDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect opened broker state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("opened broker state is not a regular file")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("protect broker state: %w", err)
	}
	return nil
}
