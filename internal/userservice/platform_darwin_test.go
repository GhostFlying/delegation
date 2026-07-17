//go:build darwin

package userservice

import (
	"path/filepath"
	"testing"
)

func TestDarwinServiceLifecycleUsesLaunchAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	result, err := Install("/opt/delegation/bin/delegation", filepath.Join(home, ".delegation", "config.json"))
	if err != nil || result.State != StatePrepared || result.Kind != KindLaunchAgent {
		t.Fatalf("Install() = %#v, %v", result, err)
	}
	wantPath := filepath.Join(home, "Library", "LaunchAgents", LaunchAgentName+".plist")
	if result.Artifact != wantPath {
		t.Fatalf("artifact = %q, want %q", result.Artifact, wantPath)
	}
}

func TestDarwinServiceRejectsRelativeHome(t *testing.T) {
	t.Setenv("HOME", "relative")
	if _, err := Install("/opt/delegation", "/Users/test/config.json"); err == nil {
		t.Fatal("Install() accepted relative HOME")
	}
}
