package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const workspaceSyncID = "123e4567-e89b-42d3-a456-426614174070"

func TestWorkspaceSyncPinsSourceSnapshotAndIsConsumedOnce(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	ctx := context.Background()
	intent := WorkspaceSyncIntent{
		Source: root.Identity(), SyncID: workspaceSyncID,
		TargetDeviceID: agentSpawnTargetID, GitURL: "ssh://git@example.invalid/repository.git",
		SourcePathHash: sha256.Sum256([]byte("/trusted/source")),
	}
	created, err := registry.BeginWorkspaceSync(ctx, intent, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != WorkspaceSyncPending {
		t.Fatalf("created receipt = %#v", created)
	}
	manifest := testWorkspaceManifest(intent.GitURL)
	pinned, err := registry.PinWorkspaceSyncManifest(
		ctx, created.Key, manifest, time.Unix(11, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.Status != WorkspaceSyncInspected || !sameWorkspaceManifest(pinned.Manifest(), manifest) {
		t.Fatalf("pinned receipt = %#v", pinned)
	}
	repeated, err := registry.PinWorkspaceSyncManifest(
		ctx, created.Key, manifest, time.Unix(12, 0),
	)
	if err != nil || !reflect.DeepEqual(repeated, pinned) {
		t.Fatalf("idempotent pin = %#v, %v", repeated, err)
	}
	changed := manifest
	changed.HeadOID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	changed.SourceSnapshotHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, err := registry.PinWorkspaceSyncManifest(
		ctx, created.Key, changed, time.Unix(13, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed source pin = %v, want ErrConflict", err)
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	summary := protocol.WorkspaceSummary{
		WorkspaceID: workspaceSyncID, SourceDeviceID: root.DeviceID,
		TargetDeviceID: agentSpawnTargetID, HeadOID: manifest.HeadOID,
		ObjectFormat: manifest.ObjectFormat, WorkingDirectory: manifest.WorkingDirectory,
		Strategy: protocol.WorkspaceStrategyDirect, ManifestHash: manifestHash,
		Warnings: append([]string(nil), manifest.Warnings...),
	}
	finished, err := registry.FinishWorkspaceSync(ctx, created.Key, summary, time.Unix(14, 0))
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != WorkspaceSyncPrepared || !reflect.DeepEqual(finished.Summary(), summary) {
		t.Fatalf("finished receipt = %#v", finished)
	}

	spawn := AgentSpawnIntent{
		Source: root.Identity(), SpawnID: agentSpawnID, AgentID: agentSpawnAgentID,
		TargetDeviceID: agentSpawnTargetID, TaskName: "workspace_build",
		PromptDigest: sha256.Sum256([]byte("build")), WorkspaceID: workspaceSyncID,
	}
	receipt, err := registry.BeginAgentSpawn(ctx, spawn, time.Unix(15, 0))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Agent.WorkspaceID != workspaceSyncID {
		t.Fatalf("spawn receipt = %#v", receipt)
	}
	retry := spawn
	retry.AgentID = "123e4567-e89b-42d3-a456-426614174071"
	if repeated, err := registry.BeginAgentSpawn(ctx, retry, time.Unix(16, 0)); err != nil ||
		!reflect.DeepEqual(repeated, receipt) {
		t.Fatalf("idempotent spawn = %#v, %v", repeated, err)
	}
	second := spawn
	second.SpawnID = "123e4567-e89b-42d3-a456-426614174072"
	second.AgentID = "123e4567-e89b-42d3-a456-426614174073"
	second.TaskName = "workspace_reuse"
	if _, err := registry.BeginAgentSpawn(ctx, second, time.Unix(17, 0)); !errors.Is(err, ErrConflict) {
		t.Fatalf("workspace reuse = %v, want ErrConflict", err)
	}
}

func TestWorkspaceSyncRejectsResultThatDiffersFromPinnedManifest(t *testing.T) {
	registry, root := prepareAgentSpawnStore(t)
	ctx := context.Background()
	intent := WorkspaceSyncIntent{
		Source: root.Identity(), SyncID: workspaceSyncID,
		TargetDeviceID: agentSpawnTargetID, GitURL: "https://example.invalid/repository.git",
		SourcePathHash: sha256.Sum256([]byte("/trusted/source")),
	}
	created, err := registry.BeginWorkspaceSync(ctx, intent, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifest := testWorkspaceManifest(intent.GitURL)
	pinned, err := registry.PinWorkspaceSyncManifest(ctx, created.Key, manifest, time.Unix(21, 0))
	if err != nil {
		t.Fatal(err)
	}
	bad := protocol.WorkspaceSummary{
		WorkspaceID: workspaceSyncID, SourceDeviceID: root.DeviceID,
		TargetDeviceID: agentSpawnTargetID, HeadOID: manifest.HeadOID,
		ObjectFormat: manifest.ObjectFormat, WorkingDirectory: manifest.WorkingDirectory,
		Strategy:     protocol.WorkspaceStrategyDirect,
		ManifestHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		Warnings:     manifest.Warnings,
	}
	if _, err := registry.FinishWorkspaceSync(ctx, created.Key, bad, time.Unix(22, 0)); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched finish = %v, want ErrConflict; pinned %#v", err, pinned)
	}
}

func TestWorkspaceSyncAcceptsFullFallbackWarningOnlyForFullStrategy(t *testing.T) {
	for _, test := range []struct {
		name     string
		strategy protocol.WorkspaceStrategy
		wantErr  bool
	}{
		{name: "self contained", strategy: protocol.WorkspaceStrategyFull},
		{name: "direct", strategy: protocol.WorkspaceStrategyDirect, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, root := prepareAgentSpawnStore(t)
			ctx := context.Background()
			intent := WorkspaceSyncIntent{
				Source: root.Identity(), SyncID: workspaceSyncID,
				TargetDeviceID: agentSpawnTargetID, GitURL: "https://example.invalid/repository.git",
				SourcePathHash: sha256.Sum256([]byte("/trusted/source")),
			}
			created, err := registry.BeginWorkspaceSync(ctx, intent, time.Unix(30, 0))
			if err != nil {
				t.Fatal(err)
			}
			manifest := testWorkspaceManifest(intent.GitURL)
			pinned, err := registry.PinWorkspaceSyncManifest(ctx, created.Key, manifest, time.Unix(31, 0))
			if err != nil {
				t.Fatal(err)
			}
			warnings := []string{protocol.WorkspaceWarningFullHistoryFallback}
			summary := protocol.WorkspaceSummary{
				WorkspaceID: workspaceSyncID, SourceDeviceID: root.DeviceID,
				TargetDeviceID: agentSpawnTargetID, HeadOID: manifest.HeadOID,
				ObjectFormat: manifest.ObjectFormat, WorkingDirectory: manifest.WorkingDirectory,
				Strategy: test.strategy, ManifestHash: pinned.ManifestHash, Warnings: warnings,
			}
			_, err = registry.FinishWorkspaceSync(ctx, created.Key, summary, time.Unix(32, 0))
			if test.wantErr && !errors.Is(err, ErrConflict) {
				t.Fatalf("FinishWorkspaceSync() = %v, want ErrConflict", err)
			}
			if !test.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testWorkspaceManifest(gitURL string) protocol.WorkspaceManifest {
	return protocol.WorkspaceManifest{
		GitURL: gitURL, HeadOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ObjectFormat: "sha1", WorkingDirectory: "nested", Clean: true,
		SourceSnapshotHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Warnings:           []string{},
	}
}
