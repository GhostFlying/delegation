//go:build windows

package cli

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSetupBrokerRejectsWindowsNetworkStateWithoutSideEffects(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	tokenPath := filepath.Join(dir, "broker.token")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run([]string{
		"setup", "broker",
		"--config", configPath,
		"--state", `\\server\share\delegation\broker.sqlite3`,
		"--token-file", tokenPath,
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("setup accepted Windows network state; stderr = %q", stderr.String())
	}
	for _, path := range []string{configPath, tokenPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("setup created %s after state preflight failure: %v", path, err)
		}
	}
}
