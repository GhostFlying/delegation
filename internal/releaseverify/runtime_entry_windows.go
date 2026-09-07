//go:build windows

package releaseverify

import (
	"os"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func validateInstalledRuntimeEntry(path string, _ os.FileInfo, _ os.FileMode) error {
	return delegationconfig.ValidateProtectedFile(path)
}
