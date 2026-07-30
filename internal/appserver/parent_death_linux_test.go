//go:build linux

package appserver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxOwnerKillsAppServerAfterConnectorHardDeathThroughExecLauncher(t *testing.T) {
	launcher := filepath.Join(t.TempDir(), "exec-launcher")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	testPlatformOwnerKillsAppServerAfterConnectorHardDeath(t, launcher)
}
