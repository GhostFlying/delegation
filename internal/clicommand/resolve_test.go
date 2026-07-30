package clicommand

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/GhostFlying/delegation/internal/hostkind"
)

func TestResolveTraeXCommandDoesNotAddCodexEnvironment(t *testing.T) {
	root := t.TempDir()
	runtimePath := filepath.Join(root, "traex-runtime")
	if err := os.WriteFile(runtimePath, []byte("test"), 0o700); err != nil {
		t.Fatal(err)
	}
	commandPath := filepath.Join(root, "traex")
	if err := os.Symlink(runtimePath, commandPath); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	t.Setenv("PATH", root)

	got, err := Resolve(hostkind.TraeX, "traex")
	if err != nil {
		t.Fatal(err)
	}
	wantRuntime, err := filepath.EvalSymlinks(runtimePath)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommandPath != commandPath || got.RuntimePath != wantRuntime {
		t.Fatalf("Resolve() = %#v", got)
	}
	if got.Environment != nil {
		t.Fatalf("TraeX command added Codex environment metadata: %#v", got)
	}
	wantUnset := []string{
		"CODEX_MANAGED_PACKAGE_ROOT",
		"CODEX_MANAGED_BY_NPM",
		"CODEX_MANAGED_BY_PNPM",
		"CODEX_MANAGED_BY_BUN",
	}
	if !reflect.DeepEqual(got.UnsetEnvironment, wantUnset) {
		t.Fatalf("TraeX unset environment = %#v, want %#v", got.UnsetEnvironment, wantUnset)
	}
}

func TestResolveCodexNativeCommandPreservesManagedEnvironmentUnset(t *testing.T) {
	path, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	got, err := Resolve(hostkind.Codex, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommandPath != path || got.RuntimePath == "" {
		t.Fatalf("Resolve() = %#v", got)
	}
	wantUnset := []string{
		"CODEX_MANAGED_PACKAGE_ROOT",
		"CODEX_MANAGED_BY_NPM",
		"CODEX_MANAGED_BY_PNPM",
		"CODEX_MANAGED_BY_BUN",
	}
	if !reflect.DeepEqual(got.UnsetEnvironment, wantUnset) {
		t.Fatalf("Codex unset environment = %#v, want %#v", got.UnsetEnvironment, wantUnset)
	}
}

func TestResolveRejectsUnsupportedHostKind(t *testing.T) {
	if _, err := Resolve(hostkind.Kind("unsupported"), os.Args[0]); err == nil {
		t.Fatal("Resolve accepted unsupported host kind")
	}
}
