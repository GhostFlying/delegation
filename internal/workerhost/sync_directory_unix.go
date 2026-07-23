//go:build !windows

package workerhost

import (
	"fmt"
	"os"
)

func syncDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync workspace root: %w", err)
	}
	return nil
}
