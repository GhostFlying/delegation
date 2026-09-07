//go:build linux || darwin

package releaseverify

import "os"

func validateInstalledRuntimeEntry(_ string, info os.FileInfo, expected os.FileMode) error {
	if info.Mode().Perm() != expected.Perm() {
		return os.ErrPermission
	}
	return nil
}
