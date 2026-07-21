package appserver

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv(helperModeEnvironment) != "" {
		os.Exit(runHelperProcess())
	}
	os.Exit(m.Run())
}
