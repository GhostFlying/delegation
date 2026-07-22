//go:build linux || darwin

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestConnectorRejectsManagedDirectoryPermissionDriftBeforeStateOpen(t *testing.T) {
	configPath, cfg := setupConnectorRuntimeTest(
		t,
		runtimeDeviceID,
		"permission-drift",
		"ws://127.0.0.1:1",
	)
	if err := os.Chmod(cfg.Peer.CodexHome, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runConnectorService(ctx, configPath, cfg, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "mode 0700") {
		t.Fatalf("runConnectorService() error = %v", err)
	}
	if _, statErr := os.Lstat(cfg.Peer.StateFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("connector opened state after authority failure: %v", statErr)
	}
}
