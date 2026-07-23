//go:build !windows

package gitworkspace

import (
	"errors"
	"fmt"
	"os"
)

func syncResultArtifactDirectories(paths ...string) error {
	for _, path := range paths {
		directory, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open result artifact directory for sync: %w", err)
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if syncErr != nil || closeErr != nil {
			return fmt.Errorf("sync result artifact directory: %w", errors.Join(syncErr, closeErr))
		}
	}
	return nil
}
