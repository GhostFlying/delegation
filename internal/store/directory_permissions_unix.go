//go:build !windows

package store

import (
	"errors"
	"os"
)

func validatePrivateDirectory(info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("broker state directory must not be accessible by group or other users")
	}
	return nil
}
