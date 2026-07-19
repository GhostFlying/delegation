//go:build linux || darwin

package store

import "os"

func validateStateDirectoryHandle(*os.File) error {
	return nil
}

func validateStateDirectoryLocation(string) error {
	return nil
}
