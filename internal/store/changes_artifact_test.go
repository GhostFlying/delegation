package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	changesArtifactID  = "123e4567-e89b-42d3-a456-426614174120"
	changesTurnID      = "123e4567-e89b-42d3-a456-426614174121"
	changesSecondID    = "123e4567-e89b-42d3-a456-426614174122"
	changesSecondTurn  = "123e4567-e89b-42d3-a456-426614174123"
	changesWorkspaceID = "123e4567-e89b-42d3-a456-426614174124"
)

func TestChangesArtifactPublishIsMetadataOnlyAndExactlyIdempotent(t *testing.T) {
	registry, root, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)
	params := testChangesArtifactParams(manifest, manifestHash)
	params.ResultWarnings = []string{"submodule_repository_not_transferred"}
	ctx := context.Background()
	created, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, params, time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if created != (protocol.PublishChangesArtifactResult{ArtifactID: params.ArtifactID, Sequence: 1}) {
		t.Fatalf("created result = %#v", created)
	}
	replayed, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, params, time.Unix(30, 0),
	)
	if err != nil || replayed != created {
		t.Fatalf("replayed result = %#v, error %v", replayed, err)
	}
	page, err := registry.ListChangesArtifacts(
		ctx, root.Identity(), ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil || len(page.Artifacts) != 1 || page.NextSequence != 1 || page.Highwater != 1 {
		t.Fatalf("artifact page = %#v, error %v", page, err)
	}
	want := protocol.ChangesArtifactMetadata{
		TreeID: worker.TreeID, ArtifactID: params.ArtifactID, TurnID: params.TurnID,
		WorkspaceID: params.WorkspaceID, Status: params.Status,
		SourceAgentID: worker.AgentID, SourceDeviceID: worker.DeviceID,
		ObjectFormat: manifest.ObjectFormat, BaseHeadOID: params.BaseHeadOID,
		BaseManifestHash: params.BaseManifestHash, BaseSnapshotHash: params.BaseSnapshotHash,
		BaseClean: manifest.Clean, ResultHeadOID: params.ResultHeadOID,
		ResultSnapshotHash: params.ResultSnapshotHash, ResultClean: params.ResultClean,
		Parts: params.Parts, BaseWarnings: params.BaseWarnings,
		ResultWarnings: params.ResultWarnings, FailureCode: params.FailureCode,
		Sequence: 1, ObservedAt: 20,
	}
	if !reflect.DeepEqual(page.Artifacts[0], want) {
		t.Fatalf("stored metadata = %#v, want %#v", page.Artifacts[0], want)
	}
	rows, err := registry.db.QueryContext(ctx, `SELECT name FROM pragma_table_info('changes_artifacts')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(name)
		if strings.Contains(lower, "path") || strings.Contains(lower, "payload") || strings.Contains(lower, "blob") {
			t.Fatalf("broker changes artifact table contains payload-bearing column %q", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestChangesArtifactPublishRejectsArtifactAndTurnConflictsAtomically(t *testing.T) {
	registry, _, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)
	ctx := context.Background()
	params := testChangesArtifactParams(manifest, manifestHash)
	params.ResultWarnings = []string{"lfs_payload_not_transferred"}
	if _, err := registry.PublishChangesArtifact(
		ctx, worker.DeviceID, worker, params, time.Unix(20, 0),
	); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*protocol.PublishChangesArtifactParams)
	}{
		{name: "same artifact changed turn", mutate: func(value *protocol.PublishChangesArtifactParams) {
			value.TurnID = changesSecondTurn
		}},
		{name: "same turn changed artifact", mutate: func(value *protocol.PublishChangesArtifactParams) {
			value.ArtifactID = changesSecondID
		}},
		{name: "same keys changed result", mutate: func(value *protocol.PublishChangesArtifactParams) {
			value.ResultSnapshotHash = strings.Repeat("9", 64)
		}},
		{name: "same keys changed result warnings", mutate: func(value *protocol.PublishChangesArtifactParams) {
			value.ResultWarnings = []string{"submodule_repository_not_transferred"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := params
			test.mutate(&changed)
			if _, err := registry.PublishChangesArtifact(
				ctx, worker.DeviceID, worker, changed, time.Unix(30, 0),
			); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting publish error = %v, want ErrConflict", err)
			}
		})
	}
	var count, sequence int
	if err := registry.db.QueryRowContext(ctx, `SELECT count(*) FROM changes_artifacts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := registry.db.QueryRowContext(ctx, `SELECT last_artifact_sequence FROM trees`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sequence != 1 {
		t.Fatalf("conflicts changed artifact state: count=%d sequence=%d", count, sequence)
	}
}

func TestChangesArtifactPublishEnforcesConnectionPrincipalSpawnAndWorkspaceAuthority(t *testing.T) {
	tests := []struct {
		name      string
		started   bool
		mutate    func(*string, *control.PrincipalIdentity, *protocol.PublishChangesArtifactParams)
		wantError error
	}{
		{name: "wrong connection device", started: true, mutate: func(deviceID *string, _ *control.PrincipalIdentity, _ *protocol.PublishChangesArtifactParams) {
			*deviceID = testDeviceID
		}, wantError: ErrAuthorizationDenied},
		{name: "forged principal device", started: true, mutate: func(_ *string, source *control.PrincipalIdentity, _ *protocol.PublishChangesArtifactParams) {
			source.DeviceID = testDeviceID
		}, wantError: ErrAuthorizationDenied},
		{name: "unstarted spawn", mutate: func(_ *string, _ *control.PrincipalIdentity, _ *protocol.PublishChangesArtifactParams) {}, wantError: ErrAuthorizationDenied},
		{name: "wrong workspace", started: true, mutate: func(_ *string, _ *control.PrincipalIdentity, params *protocol.PublishChangesArtifactParams) {
			params.WorkspaceID = workspaceSyncID
		}, wantError: ErrAuthorizationDenied},
		{name: "changed base head", started: true, mutate: func(_ *string, _ *control.PrincipalIdentity, params *protocol.PublishChangesArtifactParams) {
			params.BaseHeadOID = strings.Repeat("9", 40)
		}, wantError: ErrConflict},
		{name: "changed base manifest", started: true, mutate: func(_ *string, _ *control.PrincipalIdentity, params *protocol.PublishChangesArtifactParams) {
			params.BaseManifestHash = strings.Repeat("9", 64)
		}, wantError: ErrConflict},
		{name: "changed base snapshot", started: true, mutate: func(_ *string, _ *control.PrincipalIdentity, params *protocol.PublishChangesArtifactParams) {
			params.BaseSnapshotHash = strings.Repeat("9", 64)
		}, wantError: ErrConflict},
		{name: "changed workspace warnings", started: true, mutate: func(_ *string, _ *control.PrincipalIdentity, params *protocol.PublishChangesArtifactParams) {
			params.BaseWarnings = []string{"submodule_repository_not_transferred"}
		}, wantError: ErrConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, _, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, test.started)
			params := testChangesArtifactParams(manifest, manifestHash)
			deviceID := worker.DeviceID
			source := worker
			test.mutate(&deviceID, &source, &params)
			if _, err := registry.PublishChangesArtifact(
				context.Background(), deviceID, source, params, time.Unix(20, 0),
			); !errors.Is(err, test.wantError) {
				t.Fatalf("publish error = %v, want %v", err, test.wantError)
			}
			var count int
			if err := registry.db.QueryRowContext(
				context.Background(), `SELECT count(*) FROM changes_artifacts`,
			).Scan(&count); err != nil || count != 0 {
				t.Fatalf("denied publish count = %d, error %v", count, err)
			}
		})
	}

	registry, root, _, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)
	if _, err := registry.PublishChangesArtifact(
		context.Background(), root.DeviceID, root.Identity(),
		testChangesArtifactParams(manifest, manifestHash), time.Unix(20, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("root publish error = %v, want authorization denial", err)
	}
}

func TestChangesArtifactSupportsDirtyUnchangedAndRejectsCleanPayloadFreeReset(t *testing.T) {
	registry, root, worker, manifest, manifestHash := prepareChangesArtifactStore(t, false, true)
	params := testChangesArtifactParams(manifest, manifestHash)
	params.Status = protocol.ChangesArtifactUnchanged
	params.ResultHeadOID = params.BaseHeadOID
	params.ResultSnapshotHash = params.BaseSnapshotHash
	params.ResultClean = false
	params.Parts = []protocol.WorkspaceArtifactDescriptor{}
	if _, err := registry.PublishChangesArtifact(
		context.Background(), worker.DeviceID, worker, params, time.Unix(20, 0),
	); err != nil {
		t.Fatal(err)
	}
	page, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil || len(page.Artifacts) != 1 || page.Artifacts[0].BaseClean || page.Artifacts[0].ResultClean {
		t.Fatalf("dirty unchanged page = %#v, error %v", page, err)
	}

	cleanRegistry, _, cleanWorker, cleanManifest, cleanManifestHash := prepareChangesArtifactStore(t, true, true)
	reset := testChangesArtifactParams(cleanManifest, cleanManifestHash)
	reset.ResultHeadOID = reset.BaseHeadOID
	reset.ResultSnapshotHash = strings.Repeat("9", 64)
	reset.Parts = []protocol.WorkspaceArtifactDescriptor{}
	if err := reset.Validate(); err != nil {
		t.Fatalf("protocol rejected base-relative reset before derived cleanliness: %v", err)
	}
	if _, err := cleanRegistry.PublishChangesArtifact(
		context.Background(), cleanWorker.DeviceID, cleanWorker, reset, time.Unix(20, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("clean reset error = %v, want ErrConflict", err)
	}
}

func TestChangesArtifactCaptureFailurePublishesBoundedDiagnosticMetadata(t *testing.T) {
	registry, root, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)
	params := testChangesArtifactParams(manifest, manifestHash)
	params.Status = protocol.ChangesArtifactCaptureFailed
	params.ResultHeadOID = ""
	params.ResultSnapshotHash = ""
	params.ResultClean = false
	params.Parts = []protocol.WorkspaceArtifactDescriptor{}
	params.ResultWarnings = []string{}
	params.FailureCode = "changes_capture_failed"
	if _, err := registry.PublishChangesArtifact(
		context.Background(), worker.DeviceID, worker, params, time.Unix(20, 0),
	); err != nil {
		t.Fatal(err)
	}
	page, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil || len(page.Artifacts) != 1 ||
		page.Artifacts[0].Status != protocol.ChangesArtifactCaptureFailed ||
		page.Artifacts[0].FailureCode != params.FailureCode || len(page.Artifacts[0].Parts) != 0 ||
		!reflect.DeepEqual(page.Artifacts[0].BaseWarnings, manifest.Warnings) ||
		len(page.Artifacts[0].ResultWarnings) != 0 {
		t.Fatalf("capture failure page = %#v, error %v", page, err)
	}
}

func TestChangesArtifactPaginationIsIndependentBoundedAndTreeScoped(t *testing.T) {
	registry, root, worker, manifest, manifestHash := prepareChangesArtifactStore(t, true, true)
	first := testChangesArtifactParams(manifest, manifestHash)
	second := first
	second.ArtifactID = changesSecondID
	second.TurnID = changesSecondTurn
	for index, params := range []protocol.PublishChangesArtifactParams{first, second} {
		if _, err := registry.PublishChangesArtifact(
			context.Background(), worker.DeviceID, worker, params, time.Unix(int64(20+index), 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	overfetch, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{Limit: 2},
	)
	if err != nil || len(overfetch.Artifacts) != 2 || overfetch.NextSequence != 2 ||
		overfetch.Highwater != 2 {
		t.Fatalf("overfetch page = %#v, error %v", overfetch, err)
	}
	_, otherRoot, err := registry.EnsureRootTree(
		context.Background(), testControllerID, treeSecondThreadID, testDeviceID, time.Unix(30, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	otherPage, err := registry.ListChangesArtifacts(
		context.Background(), otherRoot.Identity(), ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil || len(otherPage.Artifacts) != 0 || otherPage.Highwater != 0 {
		t.Fatalf("other tree artifact page = %#v, error %v", otherPage, err)
	}
	page, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{Limit: 1},
	)
	if err != nil || len(page.Artifacts) != 1 || page.NextSequence != 1 || page.Highwater != 2 {
		t.Fatalf("first page = %#v, error %v", page, err)
	}
	page, err = registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{AfterSequence: 1, Limit: 1},
	)
	if err != nil || len(page.Artifacts) != 1 || page.Artifacts[0].ArtifactID != changesSecondID ||
		page.NextSequence != 2 || page.Highwater != 2 {
		t.Fatalf("second page = %#v, error %v", page, err)
	}
	if _, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{AfterSequence: 3, Limit: 1},
	); !errors.Is(err, ErrChangesArtifactCursorAhead) {
		t.Fatalf("ahead cursor error = %v", err)
	}
	if _, err := registry.ListChangesArtifacts(
		context.Background(), root.Identity(), ChangesArtifactPageRequest{Limit: 3},
	); err == nil {
		t.Fatal("artifact page accepted a limit above the overfetch bound")
	}
}

func prepareChangesArtifactStore(
	t *testing.T,
	baseClean, started bool,
) (*Store, control.Principal, control.PrincipalIdentity, protocol.WorkspaceManifest, string) {
	t.Helper()
	registry, root := prepareAgentSpawnStore(t)
	ctx := context.Background()
	manifest := testWorkspaceManifest("ssh://git@example.invalid/changes.git")
	manifest.Clean = baseClean
	manifest.Warnings = []string{"lfs_payload_not_transferred"}
	intent := WorkspaceSyncIntent{
		Source: root.Identity(), SyncID: changesWorkspaceID,
		TargetDeviceID: agentSpawnTargetID, GitURL: manifest.GitURL,
		SourcePathHash: sha256.Sum256([]byte("/trusted/changes-source")),
	}
	receipt, err := registry.BeginWorkspaceSync(ctx, intent, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = registry.PinWorkspaceSyncManifest(ctx, receipt.Key, manifest, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.FinishWorkspaceSync(ctx, receipt.Key, protocol.WorkspaceSummary{
		WorkspaceID: changesWorkspaceID, SourceDeviceID: root.DeviceID,
		TargetDeviceID: agentSpawnTargetID, HeadOID: manifest.HeadOID,
		ObjectFormat: manifest.ObjectFormat, WorkingDirectory: manifest.WorkingDirectory,
		Strategy: protocol.WorkspaceStrategyDirect, ManifestHash: manifestHash,
		Warnings: manifest.Warnings,
	}, time.Unix(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	spawn, err := registry.BeginAgentSpawn(ctx, AgentSpawnIntent{
		Source: root.Identity(), SpawnID: agentSpawnID, AgentID: agentSpawnAgentID,
		TargetDeviceID: agentSpawnTargetID, TaskName: "changes_worker",
		PromptDigest: sha256.Sum256([]byte("capture changes")), WorkspaceID: changesWorkspaceID,
	}, time.Unix(6, 0))
	if err != nil {
		t.Fatal(err)
	}
	if started {
		if _, err := registry.MarkAgentSpawnStarted(ctx, keyForReceipt(spawn), time.Unix(7, 0)); err != nil {
			t.Fatal(err)
		}
	}
	return registry, root, spawn.Agent.Principal, manifest, manifestHash
}

func testChangesArtifactParams(
	manifest protocol.WorkspaceManifest,
	manifestHash string,
) protocol.PublishChangesArtifactParams {
	return protocol.PublishChangesArtifactParams{
		ArtifactID: changesArtifactID, TurnID: changesTurnID, WorkspaceID: changesWorkspaceID,
		Status: protocol.ChangesArtifactAvailable, BaseHeadOID: manifest.HeadOID,
		BaseManifestHash: manifestHash, BaseSnapshotHash: manifest.SourceSnapshotHash,
		ResultHeadOID:      strings.Repeat("d", len(manifest.HeadOID)),
		ResultSnapshotHash: strings.Repeat("e", 64), ResultClean: true,
		Parts: []protocol.WorkspaceArtifactDescriptor{{
			Kind: protocol.WorkspaceArtifactBundle, Size: 64, SHA256: strings.Repeat("f", 64),
		}},
		BaseWarnings: append([]string{}, manifest.Warnings...), ResultWarnings: []string{},
	}
}
