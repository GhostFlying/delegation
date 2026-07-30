//go:build integration && live && linux

package appserver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/clilaunch"
)

func TestTraeXWarmpoolUsesManagedRuntimeHome(t *testing.T) {
	traeXBinary := liveExecutable(t, "TRAE_X_BINARY")
	warmpoolBinary := liveExecutable(t, "WARMPOOL_BINARY")
	ambientCodexHome := emptyDirectory(t)
	ambientTraeHome := emptyDirectory(t)
	ambientTraeCLIHome := emptyDirectory(t)
	t.Setenv("CODEX_HOME", ambientCodexHome)
	t.Setenv("TRAE_HOME", ambientTraeHome)
	t.Setenv("TRAECLI_HOME", ambientTraeCLIHome)

	managedHome := emptyDirectory(t)
	managedCLIHome := filepath.Join(managedHome, "cli")
	if err := os.Mkdir(managedCLIHome, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Start(ctx, Options{
		Launch: clilaunch.Spec{
			Executable: warmpoolBinary,
			PrefixArguments: []string{
				"run", "--", traeXBinary, "-p", "ultra",
			},
		},
		CodexHome: managedHome,
		RuntimeHomeEnvironment: map[string]string{
			"TRAE_HOME":    managedHome,
			"TRAECLI_HOME": managedCLIHome,
		},
		UnsetEnvironment: []string{"CODEX_HOME"},
	})
	if err != nil {
		t.Fatal(err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := client.Close(closeCtx); err != nil {
		t.Fatal(err)
	}

	if empty, err := directoryTreeEmpty(managedCLIHome); err != nil {
		t.Fatal(err)
	} else if empty {
		t.Fatal("TraeX did not write runtime state beneath managed TRAECLI_HOME")
	}
	for name, path := range map[string]string{
		"ambient CODEX_HOME":   ambientCodexHome,
		"ambient TRAE_HOME":    ambientTraeHome,
		"ambient TRAECLI_HOME": ambientTraeCLIHome,
	} {
		if empty, err := directoryTreeEmpty(path); err != nil {
			t.Fatal(err)
		} else if !empty {
			t.Fatalf("%s received TraeX runtime state", name)
		}
	}
}

func liveExecutable(t *testing.T, name string) string {
	t.Helper()
	path := os.Getenv(name)
	if path == "" {
		t.Skipf("%s is not set", name)
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func emptyDirectory(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func directoryTreeEmpty(path string) (bool, error) {
	empty := true
	err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if current != path {
			empty = false
		}
		return nil
	})
	return empty, err
}
