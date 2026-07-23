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
	_, state, paths := newTestHost(t, 1)
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
		ControllerID:     testControllerID,
		DeviceID:         testDeviceID,
		PeerConfigPath:   paths.configPath,
		DelegationBinary: paths.delegationBinary,
		CodexBinary:      symlink,
		GitBinary:        paths.codexBinary,
		CodexHome:        paths.codexHome,
		WorkspaceRoot:    filepath.Join(filepath.Dir(paths.configPath), "workspaces"),
		MaxWorkerSlots:   1,
		Store:            state,
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
	if host.codexBinary != resolvedTarget {
		t.Fatalf("canonical Codex binary = %q, want %q", host.codexBinary, resolvedTarget)
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
