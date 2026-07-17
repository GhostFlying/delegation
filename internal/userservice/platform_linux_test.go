//go:build linux

package userservice

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLinuxServiceLifecycleUsesXDGUserDirectory(t *testing.T) {
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	result, err := Install("/opt/delegation/bin/delegation", "/home/test/.delegation/config.json")
	if err != nil || result.State != StatePrepared || result.Kind != KindSystemd {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(configHome, "systemd", "user", SystemdUnitName)
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
	content, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "Prepared by Delegation M0") {
		t.Fatalf("service definition is not explicitly inactive:\n%s", content)
	}
}

func TestLinuxServiceRejectsRelativeXDGConfigHome(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "relative")
	if _, err := Install("/opt/delegation", "/home/test/config.json"); err == nil {
		t.Fatal("Install() accepted relative XDG_CONFIG_HOME")
	}
}
