package appserver

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if handled, exitCode := RunDarwinSupervisorIfRequested(); handled {
		os.Exit(exitCode)
	}
	if os.Getenv(helperModeEnvironment) != "" {
		os.Exit(runHelperProcess())
	}
	if handled, exitCode := runParentDeathHelperIfRequested(); handled {
		os.Exit(exitCode)
	}
	os.Exit(m.Run())
}
