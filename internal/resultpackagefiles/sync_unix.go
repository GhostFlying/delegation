//go:build !windows

package resultpackagefiles

import (
	"errors"
	"os"
)

func syncDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
