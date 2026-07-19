package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func nearestExistingPath(path string) (string, error) {
	current := filepath.Clean(path)
	for {
		if _, err := os.Stat(current); err == nil {
			return current, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect broker state filesystem: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("broker state path has no existing filesystem ancestor")
		}
		current = parent
	}
}
