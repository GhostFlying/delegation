//go:build !windows

package cli

import (
	"os"
	"syscall"
	"testing"

	"github.com/GhostFlying/delegation/internal/appserver"
)

func TestMain(m *testing.M) {
	if handled, exitCode := appserver.RunDarwinSupervisorIfRequested(); handled {
		os.Exit(exitCode)
	}
	syscall.Umask(0o077)
	os.Exit(m.Run())
}
