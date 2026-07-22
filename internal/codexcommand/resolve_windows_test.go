//go:build windows

package codexcommand

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveOfficialWindowsNPMShims(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name        string
		commandPath string
		packageRoot string
	}{
		{
			name:        "global cmd",
			commandPath: filepath.Join(root, "npm", "codex.cmd"),
			packageRoot: filepath.Join(root, "npm", "node_modules", "@openai", "codex"),
		},
		{
			name:        "project local bat",
			commandPath: filepath.Join(root, "project", "node_modules", ".bin", "codex.bat"),
			packageRoot: filepath.Join(root, "project", "node_modules", "@openai", "codex"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.MkdirAll(filepath.Dir(test.commandPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(test.packageRoot, "bin"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(test.commandPath, []byte("@ECHO off\r\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeJSON(t, filepath.Join(test.packageRoot, "package.json"), map[string]any{
				"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
			})
			nativePath := installNativeFixture(t, test.packageRoot)

			resolved, err := Resolve(test.commandPath)
			if err != nil {
				t.Fatal(err)
			}
			canonicalRoot, err := filepath.EvalSymlinks(test.packageRoot)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.CommandPath != test.commandPath || resolved.NativePath != nativePath ||
				resolved.Environment["CODEX_MANAGED_PACKAGE_ROOT"] != canonicalRoot ||
				resolved.Environment["CODEX_MANAGED_BY_NPM"] != "1" {
				t.Fatalf("resolved npm shim = %#v", resolved)
			}
		})
	}
}

func TestResolveRejectsUnofficialWindowsCommandShim(t *testing.T) {
	commandPath := filepath.Join(t.TempDir(), "not-codex.cmd")
	if err := os.WriteFile(commandPath, []byte("@ECHO off\r\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(commandPath); err == nil {
		t.Fatal("Resolve accepted a Windows command shim not named codex.cmd")
	}
}

func TestResolveOfficialWindowsStoreShims(t *testing.T) {
	for _, test := range []struct {
		name       string
		store      string
		managerKey string
	}{
		{name: "pnpm", store: filepath.Join(".pnpm", "@openai+codex@test", "node_modules"), managerKey: "CODEX_MANAGED_BY_PNPM"},
		{name: "bun", store: filepath.Join(".bun", "install", "global", "node_modules"), managerKey: "CODEX_MANAGED_BY_BUN"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			commandPath := filepath.Join(root, "bin", "codex.cmd")
			packageRoot := filepath.Join(root, test.store, "@openai", "codex")
			if err := os.MkdirAll(filepath.Join(packageRoot, "bin"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeJSON(t, filepath.Join(packageRoot, "package.json"), map[string]any{
				"name": "@openai/codex", "version": "test", "bin": map[string]string{"codex": "bin/codex.js"},
			})
			nativePath := installNativeFixture(t, packageRoot)
			relativeEntrypoint, err := filepath.Rel(
				filepath.Dir(commandPath),
				filepath.Join(packageRoot, "bin", "codex.js"),
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Dir(commandPath), 0o700); err != nil {
				t.Fatal(err)
			}
			shim := `@ECHO off\r\nnode "%dp0%\\` + relativeEntrypoint + `" %*\r\n`
			if err := os.WriteFile(commandPath, []byte(shim), 0o700); err != nil {
				t.Fatal(err)
			}

			resolved, err := Resolve(commandPath)
			if err != nil {
				t.Fatal(err)
			}
			if resolved.NativePath != nativePath || resolved.Environment[test.managerKey] != "1" {
				t.Fatalf("resolved %s command = %#v", test.name, resolved)
			}
		})
	}
}
