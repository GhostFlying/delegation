//go:build linux || darwin

package workerhost

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/store"
)

func TestHostRejectsManagedDirectoryPermissionDriftBeforeLaunch(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	if err := os.Chmod(paths.codexHome, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(paths.codexHome, 0o700) })
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174442",
		ParentAgentID: testParentID, TaskName: "permission drift", Prompt: "permission drift",
	})
	if err == nil || !strings.Contains(err.Error(), "mode 0700") {
		t.Fatalf("Spawn() error = %v", err)
	}
	if got := application.snapshot(); len(got.starts) != 0 {
		t.Fatalf("app-server started after permission drift: %#v", got)
	}
}

func TestHostCanonicalizesSymlinkedCodexBinaryWithoutGrantingEitherDirectory(t *testing.T) {
	fixtureHost, state, paths := newTestHost(t, 1)
	targetDirectory := t.TempDir()
	target := filepath.Join(targetDirectory, "codex")
	if err := os.WriteFile(target, []byte("test"), 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(filepath.Dir(paths.configPath), "codex-link")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	resolvedTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	resolvedSymlinkDirectory, err := filepath.EvalSymlinks(filepath.Dir(symlink))
	if err != nil {
		t.Fatal(err)
	}

	host, err := New(context.Background(), Options{
		ControllerID:         testControllerID,
		DeviceID:             testDeviceID,
		PeerConfigPath:       paths.configPath,
		DelegationBinary:     paths.delegationBinary,
		CLILaunch:            directWorkerCLILaunch(symlink),
		CLIRuntimeExecutable: symlink,
		GitBinary:            paths.codexBinary,
		CodexHome:            paths.codexHome,
		WorkspaceRoot:        filepath.Join(filepath.Dir(paths.configPath), "workspaces"),
		MaxWorkerSlots:       1,
		Store:                state,
		ResultPackages:       fixtureHost.resultPackages,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close symlink host: %v", err)
		}
	})
	if host.cliRuntimeExecutable != resolvedTarget {
		t.Fatalf(
			"canonical CLI runtime = %q, want %q",
			host.cliRuntimeExecutable,
			resolvedTarget,
		)
	}
	filesystem := managedFilesystemPermissions(t, host.managedConfig(store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174444",
		},
		ParentAgentID: testParentID,
	}))
	for _, directory := range []string{
		filepath.Dir(symlink), resolvedSymlinkDirectory,
		filepath.Dir(target), filepath.Dir(resolvedTarget),
	} {
		if _, found := filesystem[directory]; found {
			t.Fatalf("managed profile grants Codex directory %q: %#v", directory, filesystem)
		}
	}
	if _, found := filesystem[paths.configPath]; found {
		t.Fatalf("managed profile grants the symlink-adjacent peer config: %#v", filesystem)
	}
	assertCodexRuntimeFilesystemPermission(t, filesystem, resolvedTarget)
}

func TestHostRejectsManagedCLIExecutableAliases(t *testing.T) {
	fixtureHost, state, paths := newTestHost(t, 1)
	workspaceRoot := fixtureHost.workspaceRoot.Name()
	tests := map[string]struct {
		targetDirectory string
		mutate          func(*Options, string)
		want            string
	}{
		"launcher inside CODEX_HOME": {
			targetDirectory: paths.codexHome,
			mutate: func(options *Options, alias string) {
				options.CLILaunch = directWorkerCLILaunch(alias)
			},
			want: "CLI launcher must not be inside worker CODEX_HOME",
		},
		"runtime inside workspace root": {
			targetDirectory: workspaceRoot,
			mutate: func(options *Options, alias string) {
				options.CLIRuntimeExecutable = alias
			},
			want: "CLI runtime must not be inside worker workspace root",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			target := filepath.Join(test.targetDirectory, "managed-cli")
			if err := os.WriteFile(target, []byte("test"), 0o700); err != nil {
				t.Fatal(err)
			}
			alias := filepath.Join(t.TempDir(), "cli-alias")
			if err := os.Symlink(target, alias); err != nil {
				t.Fatal(err)
			}
			options := Options{
				ControllerID: testControllerID, DeviceID: testDeviceID,
				PeerConfigPath: paths.configPath, DelegationBinary: paths.delegationBinary,
				CLILaunch:            directWorkerCLILaunch(paths.codexBinary),
				CLIRuntimeExecutable: paths.codexBinary, GitBinary: paths.gitBinary,
				CodexHome: paths.codexHome, WorkspaceRoot: workspaceRoot,
				MaxWorkerSlots: 1, Store: state, ResultPackages: fixtureHost.resultPackages,
			}
			test.mutate(&options, alias)
			if _, err := New(context.Background(), options); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestHostCanonicalizesCodexHomeThroughSymlinkedParent(t *testing.T) {
	fixtureHost, state, paths := newTestHost(t, 1)
	aliasParent := filepath.Join(t.TempDir(), "runtime-link")
	if err := os.Symlink(filepath.Dir(paths.codexHome), aliasParent); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	aliasCodexHome := filepath.Join(aliasParent, filepath.Base(paths.codexHome))
	resolvedCodexHome, err := filepath.EvalSymlinks(aliasCodexHome)
	if err != nil {
		t.Fatal(err)
	}

	host, err := New(context.Background(), Options{
		ControllerID:         testControllerID,
		DeviceID:             testDeviceID,
		PeerConfigPath:       paths.configPath,
		DelegationBinary:     paths.delegationBinary,
		CLILaunch:            directWorkerCLILaunch(paths.codexBinary),
		CLIRuntimeExecutable: paths.codexBinary,
		GitBinary:            paths.gitBinary,
		CodexHome:            aliasCodexHome,
		WorkspaceRoot:        filepath.Join(filepath.Dir(paths.configPath), "workspaces"),
		MaxWorkerSlots:       1,
		Store:                state,
		ResultPackages:       fixtureHost.resultPackages,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close Codex home alias host: %v", err)
		}
	})
	if host.codexHome != resolvedCodexHome {
		t.Fatalf("canonical Codex home = %q, want %q", host.codexHome, resolvedCodexHome)
	}
}
