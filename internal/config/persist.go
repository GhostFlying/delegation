package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type CommittedError struct {
	Err error
}

func (e *CommittedError) Error() string {
	return fmt.Sprintf("config was committed but final durability work failed: %v", e.Err)
}

func (e *CommittedError) Unwrap() error {
	return e.Err
}

func IsCommitted(err error) bool {
	var committed *CommittedError
	return errors.As(err, &committed)
}

func WriteNew(path string, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect config: %w", err)
	}

	dir := filepath.Dir(path)
	if err := createDirectoriesDurably(dir); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("protect temporary config: %w", err)
	}
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		temp.Close()
		return fmt.Errorf("encode config: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync config: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close config: %w", err)
	}
	installErr := noReplaceRename(tempPath, path)
	if installErr != nil && !IsCommitted(installErr) {
		return fmt.Errorf("install config: %w", installErr)
	}
	if err := syncInstalledConfig(dir); err != nil {
		if installErr != nil {
			err = errors.Join(installErr, err)
		}
		return &CommittedError{Err: err}
	}
	if installErr != nil {
		return installErr
	}
	return nil
}

var syncInstalledConfig = syncParentDirectory

func createDirectoriesDurably(path string) error {
	var missing []string
	current := filepath.Clean(path)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("path component is not a directory: %s", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("no existing parent directory for %s", path)
		}
		current = parent
	}
	// A prior attempt may have created the existing anchor but failed while
	// syncing its parent. Syncing it again makes retries close that durability
	// gap before creating any descendants.
	if err := syncInstalledConfig(filepath.Dir(current)); err != nil {
		return fmt.Errorf("sync parent of existing directory %s: %w", current, err)
	}

	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncInstalledConfig(filepath.Dir(directory)); err != nil {
			return fmt.Errorf("sync parent of new directory %s: %w", directory, err)
		}
	}
	return nil
}
