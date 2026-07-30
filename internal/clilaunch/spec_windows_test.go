//go:build windows

package clilaunch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveRejectsWindowsScriptLauncher(t *testing.T) {
	for _, extension := range []string{".bat", ".cmd", ".ps1"} {
		t.Run(extension, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "launcher"+extension)
			if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Resolve(Spec{Executable: path}); err == nil ||
				!strings.Contains(err.Error(), "must be a native executable on Windows") {
				t.Fatalf("Resolve() error = %v", err)
			}
		})
	}
}
