//go:build linux || darwin

package traexauth

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateSandboxPathsRejectsDarwinTemporaryRoots(t *testing.T) {
	safeSource := "/Users/operator/.delegation-auth/auth.json"
	safeManaged := "/Users/operator/.delegation/trae/cli/auth.json"
	for _, root := range darwinSandboxTemporaryRoots {
		for _, test := range []struct {
			name    string
			source  string
			managed string
		}{
			{name: "source", source: filepath.Join(root, "secret", "auth.json"), managed: safeManaged},
			{name: "managed", source: safeSource, managed: filepath.Join(root, "managed", "cli", "auth.json")},
		} {
			t.Run(strings.TrimPrefix(root, "/")+"/"+test.name, func(t *testing.T) {
				err := validateSandboxPaths("darwin", test.source, test.managed)
				if !errors.Is(err, ErrUnsafeSandboxPath) || !strings.Contains(err.Error(), test.name) {
					t.Fatalf("validateSandboxPaths() error = %v", err)
				}
			})
		}
	}
	if err := validateSandboxPaths("darwin", safeSource, safeManaged); err != nil {
		t.Fatalf("validateSandboxPaths() rejected safe paths: %v", err)
	}
	if err := validateSandboxPaths("linux", "/tmp/source/auth.json", "/tmp/managed/auth.json"); err != nil {
		t.Fatalf("validateSandboxPaths() applied macOS rule on Linux: %v", err)
	}
}

func TestValidateSandboxPathsRejectsCanonicalDarwinTemporaryAlias(t *testing.T) {
	root, err := os.MkdirTemp(".", "sandbox-path-alias-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	alias := filepath.Join(root, "safe-looking")
	if err := os.Symlink("/tmp", alias); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	err = validateSandboxPaths(
		"darwin", filepath.Join(alias, "not-created", "auth.json"),
		filepath.Join(root, "managed", "cli", "auth.json"),
	)
	if !errors.Is(err, ErrUnsafeSandboxPath) {
		t.Fatalf("validateSandboxPaths() canonical alias error = %v", err)
	}
}
