//go:build windows

package store

import (
	"os"

	"github.com/GhostFlying/delegation/internal/winlocalpath"
)

func validateStateDirectoryHandle(file *os.File) error {
	return winlocalpath.ValidateDirectory(file)
}

func validateStateDirectoryLocation(path string) error {
	return winlocalpath.ValidateDirectoryPath(path)
}
