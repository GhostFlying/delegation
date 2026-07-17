package cli

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestServiceRunIsExplicitlyUnavailableAfterValidation(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	var setupOutput bytes.Buffer
	var setupError bytes.Buffer
	if code := Run([]string{"setup", "broker", "--config", configPath}, &setupOutput, &setupError); code != 0 {
		t.Fatalf("setup code = %d, want 0; stderr = %q", code, setupError.String())
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "run", "--config", configPath}, &stdout, &stderr)

	if code != exitUnavailable {
		t.Fatalf("service run code = %d, want %d", code, exitUnavailable)
	}
	if got, want := stderr.String(), "delegation: broker service runtime is not available in M0\n"; got != want {
		t.Fatalf("service run stderr = %q, want %q", got, want)
	}
}

func TestServiceRunRejectsInvalidConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "missing.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"service", "run", "--config", configPath}, &stdout, &stderr)

	if code == exitUnavailable || code == 0 {
		t.Fatalf("service run code = %d, want configuration failure", code)
	}
}
