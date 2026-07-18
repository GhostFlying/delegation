//go:build windows

package store

import "os"

func validatePrivateDirectory(os.FileInfo) error {
	// Windows permission bits do not describe the directory ACL. The state
	// directory inherits the current user's protected Delegation home ACL.
	return nil
}
