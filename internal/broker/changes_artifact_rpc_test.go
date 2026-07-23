package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/coder/websocket"
)

const (
	changesRPCWorkspaceID      = "123e4567-e89b-42d3-a456-426614174470"
	changesRPCSpawnID          = "123e4567-e89b-42d3-a456-426614174471"
	changesRPCWorkerID         = "123e4567-e89b-42d3-a456-426614174472"
	changesRPCArtifactID       = "123e4567-e89b-42d3-a456-426614174473"
	changesRPCTurnID           = "123e4567-e89b-42d3-a456-426614174474"
	changesRPCOtherWorkspaceID = "123e4567-e89b-42d3-a456-426614174475"
	changesRPCForgedWorkerID   = "123e4567-e89b-42d3-a456-426614174476"
)

type brokerChangesFixture struct {
	worker       control.Principal
	manifest     protocol.WorkspaceManifest
	manifestHash string
	params       protocol.PublishChangesArtifactParams
}

func TestPublishChangesArtifactWakesRootAndReplaysExactly(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)

	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	fixture := prepareBrokerChangesFixture(t, harness.registry, root, true)

	waitRequest := principalRequest(t, protocol.MethodWaitAgent, protocol.WaitAgentParams{
		TimeoutMillis: 2_000, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1,
	}, root)
	writeEnvelope(t, rootConnection, waitRequest)
	waitForPendingAgentWait(
		t, harness.server, activeBrokerSession(t, harness.server, brokerTestDeviceID),
		root, waitRequest.RequestID,
	)

	published := writeAndRead(t, workerConnection, principalRequest(
		t, protocol.MethodPublishChangesArtifact, fixture.params, fixture.worker,
	))
	if published.Error != nil {
		t.Fatalf("publish changes artifact = %#v", published.Error)
	}
	publishResult := decodeResult[protocol.PublishChangesArtifactResult](t, published)
	if publishResult != (protocol.PublishChangesArtifactResult{
		ArtifactID: changesRPCArtifactID, Sequence: 1,
	}) {
		t.Fatalf("publish result = %#v", publishResult)
	}

	waitResponse := readBrokerResponse(t, rootConnection)
	if waitResponse.ReplyTo != waitRequest.RequestID || waitResponse.Error != nil {
		t.Fatalf("artifact wait response = %#v", waitResponse)
	}
	waitResult := decodeResult[protocol.WaitAgentResult](t, waitResponse)
	if len(waitResult.Messages) != 0 || len(waitResult.Activities) != 0 ||
		len(waitResult.Artifacts) != 1 || waitResult.NextMailboxCursor != 0 ||
		waitResult.NextLifecycleCursor != 0 || waitResult.NextArtifactCursor != 1 ||
		waitResult.MoreMessages || waitResult.MoreActivities || waitResult.MoreArtifacts {
		t.Fatalf("artifact wait result = %#v", waitResult)
	}
	artifact := waitResult.Artifacts[0]
	if err := artifact.Validate(); err != nil {
		t.Fatalf("published metadata is invalid: %v", err)
	}
	wantArtifact := protocol.ChangesArtifactMetadata{
		TreeID: root.TreeID, ArtifactID: fixture.params.ArtifactID,
		TurnID: fixture.params.TurnID, WorkspaceID: fixture.params.WorkspaceID,
		Status: fixture.params.Status, SourceAgentID: fixture.worker.AgentID,
		SourceDeviceID: fixture.worker.DeviceID, ObjectFormat: fixture.manifest.ObjectFormat,
		BaseHeadOID: fixture.params.BaseHeadOID, BaseManifestHash: fixture.manifestHash,
		BaseSnapshotHash: fixture.params.BaseSnapshotHash, BaseClean: fixture.manifest.Clean,
		ResultHeadOID:      fixture.params.ResultHeadOID,
		ResultSnapshotHash: fixture.params.ResultSnapshotHash, ResultClean: fixture.params.ResultClean,
		Parts: fixture.params.Parts, Warnings: fixture.params.Warnings,
		FailureCode: fixture.params.FailureCode, Sequence: 1, ObservedAt: artifact.ObservedAt,
	}
	if artifact.ObservedAt <= 0 || !reflect.DeepEqual(artifact, wantArtifact) {
		t.Fatalf("published artifact metadata = %#v, want %#v", artifact, wantArtifact)
	}
	metadataJSON, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte(`"path"`), []byte(`"name"`), []byte(`"payload"`), []byte(`"blob"`)} {
		if bytes.Contains(metadataJSON, forbidden) {
			t.Fatalf("broker exposed payload-bearing artifact metadata %q: %s", forbidden, metadataJSON)
		}
	}

	replayed := writeAndRead(t, workerConnection, principalRequest(
		t, protocol.MethodPublishChangesArtifact, fixture.params, fixture.worker,
	))
	if replayed.Error != nil ||
		decodeResult[protocol.PublishChangesArtifactResult](t, replayed) != publishResult {
		t.Fatalf("exact replay = %#v", replayed)
	}
	changed := fixture.params
	changed.ResultSnapshotHash = strings.Repeat("9", 64)
	conflict := writeAndRead(t, workerConnection, principalRequest(
		t, protocol.MethodPublishChangesArtifact, changed, fixture.worker,
	))
	if conflict.Error == nil || conflict.Error.Code != protocol.ErrorConflict {
		t.Fatalf("changed replay = %#v", conflict)
	}

	drained := writeAndRead(t, rootConnection, principalRequest(
		t, protocol.MethodWaitAgent,
		protocol.WaitAgentParams{
			ArtifactCursor: 1, MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1,
		},
		root,
	))
	drainedResult := decodeResult[protocol.WaitAgentResult](t, drained)
	if drained.Error != nil || len(drainedResult.Artifacts) != 0 ||
		drainedResult.NextArtifactCursor != 1 {
		t.Fatalf("artifact replay changed registry = %#v, error %#v", drainedResult, drained.Error)
	}
}

func TestPublishChangesArtifactEnforcesWorkerWorkspaceAuthority(t *testing.T) {
	tests := []struct {
		name              string
		consume           bool
		useRootConnection bool
		useRootSource     bool
		mutate            func(*control.Principal, *protocol.PublishChangesArtifactParams)
		wantCode          int
	}{
		{
			name: "wrong device", consume: true, useRootConnection: true,
			mutate:   func(_ *control.Principal, _ *protocol.PublishChangesArtifactParams) {},
			wantCode: protocol.ErrorForbidden,
		},
		{
			name: "root principal", consume: true, useRootConnection: true, useRootSource: true,
			mutate:   func(_ *control.Principal, _ *protocol.PublishChangesArtifactParams) {},
			wantCode: protocol.ErrorForbidden,
		},
		{
			name: "forged worker", consume: true,
			mutate: func(source *control.Principal, _ *protocol.PublishChangesArtifactParams) {
				source.AgentID = changesRPCForgedWorkerID
			},
			wantCode: protocol.ErrorForbidden,
		},
		{
			name:     "unconsumed workspace",
			mutate:   func(_ *control.Principal, _ *protocol.PublishChangesArtifactParams) {},
			wantCode: protocol.ErrorForbidden,
		},
		{
			name: "mismatched workspace", consume: true,
			mutate: func(_ *control.Principal, params *protocol.PublishChangesArtifactParams) {
				params.WorkspaceID = changesRPCOtherWorkspaceID
			},
			wantCode: protocol.ErrorForbidden,
		},
		{
			name: "mismatched base head", consume: true,
			mutate: func(_ *control.Principal, params *protocol.PublishChangesArtifactParams) {
				params.BaseHeadOID = strings.Repeat("9", 40)
			},
			wantCode: protocol.ErrorConflict,
		},
		{
			name: "mismatched base manifest", consume: true,
			mutate: func(_ *control.Principal, params *protocol.PublishChangesArtifactParams) {
				params.BaseManifestHash = strings.Repeat("9", 64)
			},
			wantCode: protocol.ErrorConflict,
		},
		{
			name: "mismatched base snapshot", consume: true,
			mutate: func(_ *control.Principal, params *protocol.PublishChangesArtifactParams) {
				params.BaseSnapshotHash = strings.Repeat("9", 64)
			},
			wantCode: protocol.ErrorConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
			rootConnection, _, err := dialBroker(harness, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rootConnection.Close(websocket.StatusNormalClosure, "done")
			sendHello(t, rootConnection)
			root := ensureRootPrincipal(t, rootConnection)
			workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
			defer workerConnection.Close(websocket.StatusNormalClosure, "done")
			fixture := prepareBrokerChangesFixture(t, harness.registry, root, test.consume)
			source := fixture.worker
			if test.useRootSource {
				source = root
			}
			params := fixture.params
			test.mutate(&source, &params)
			connection := workerConnection
			if test.useRootConnection {
				connection = rootConnection
			}
			response := writeAndRead(t, connection, principalRequest(
				t, protocol.MethodPublishChangesArtifact, params, source,
			))
			if response.Error == nil || response.Error.Code != test.wantCode {
				t.Fatalf("publish response = %#v, want code %d", response, test.wantCode)
			}
			page, err := harness.registry.ListChangesArtifacts(
				context.Background(), root.Identity(), store.ChangesArtifactPageRequest{Limit: 1},
			)
			if err != nil || len(page.Artifacts) != 0 || page.Highwater != 0 {
				t.Fatalf("denied publish changed artifact state: %#v, error %v", page, err)
			}
		})
	}
}

func TestPublishChangesArtifactRejectsInvalidPayload(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootConnection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rootConnection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, rootConnection)
	root := ensureRootPrincipal(t, rootConnection)
	workerConnection := connectBrokerMailboxPeer(t, harness, brokerTestSecondDeviceID)
	defer workerConnection.Close(websocket.StatusNormalClosure, "done")
	fixture := prepareBrokerChangesFixture(t, harness.registry, root, true)

	invalid := identityRequest(
		t, protocol.MethodPublishChangesArtifact, map[string]any{"artifactId": "not-an-id"},
		fixture.worker.Identity(),
	)
	response := writeAndRead(t, workerConnection, invalid)
	if response.Error == nil || response.Error.Code != protocol.ErrorInvalidParams {
		t.Fatalf("invalid artifact payload = %#v", response)
	}
}

func prepareBrokerChangesFixture(
	t *testing.T,
	registry *store.Store,
	root control.Principal,
	consume bool,
) brokerChangesFixture {
	t.Helper()
	ctx := context.Background()
	manifest := protocol.WorkspaceManifest{
		GitURL:  "ssh://git@example.invalid/changes.git",
		HeadOID: strings.Repeat("a", 40), ObjectFormat: "sha1",
		WorkingDirectory: "nested", Clean: true,
		SourceSnapshotHash: strings.Repeat("b", 64), Warnings: []string{},
	}
	receipt, err := registry.BeginWorkspaceSync(ctx, store.WorkspaceSyncIntent{
		Source: root.Identity(), SyncID: changesRPCWorkspaceID,
		TargetDeviceID: brokerTestSecondDeviceID, GitURL: manifest.GitURL,
		SourcePathHash: sha256.Sum256([]byte("/trusted/changes-source")),
	}, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = registry.PinWorkspaceSyncManifest(ctx, receipt.Key, manifest, time.Unix(11, 0))
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.FinishWorkspaceSync(ctx, receipt.Key, protocol.WorkspaceSummary{
		WorkspaceID: changesRPCWorkspaceID, SourceDeviceID: root.DeviceID,
		TargetDeviceID: brokerTestSecondDeviceID, HeadOID: manifest.HeadOID,
		ObjectFormat: manifest.ObjectFormat, WorkingDirectory: manifest.WorkingDirectory,
		Strategy: protocol.WorkspaceStrategyDirect, ManifestHash: manifestHash,
		Warnings: manifest.Warnings,
	}, time.Unix(12, 0)); err != nil {
		t.Fatal(err)
	}
	workspaceID := ""
	if consume {
		workspaceID = changesRPCWorkspaceID
	}
	_, err = registry.BeginAgentSpawn(ctx, store.AgentSpawnIntent{
		Source: root.Identity(), SpawnID: changesRPCSpawnID, AgentID: changesRPCWorkerID,
		TargetDeviceID: brokerTestSecondDeviceID, TaskName: "changes_worker",
		PromptDigest: sha256.Sum256([]byte("capture worker changes")), WorkspaceID: workspaceID,
	}, time.Unix(13, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.MarkAgentSpawnStarted(ctx, store.AgentSpawnKey{
		ControllerID: root.ControllerID, TreeID: root.TreeID,
		SourceAgentID: root.AgentID, SpawnID: changesRPCSpawnID,
	}, time.Unix(14, 0)); err != nil {
		t.Fatal(err)
	}
	params := protocol.PublishChangesArtifactParams{
		ArtifactID: changesRPCArtifactID, TurnID: changesRPCTurnID,
		WorkspaceID: changesRPCWorkspaceID, Status: protocol.ChangesArtifactAvailable,
		BaseHeadOID: manifest.HeadOID, BaseManifestHash: manifestHash,
		BaseSnapshotHash: manifest.SourceSnapshotHash,
		ResultHeadOID:    strings.Repeat("d", 40), ResultSnapshotHash: strings.Repeat("e", 64),
		ResultClean: true,
		Parts: []protocol.WorkspaceArtifactDescriptor{{
			Kind: protocol.WorkspaceArtifactBundle, Size: 64, SHA256: strings.Repeat("f", 64),
		}},
		Warnings: []string{},
	}
	return brokerChangesFixture{
		worker: control.NewWorkerPrincipal(
			root.ControllerID, root.TreeID, changesRPCWorkerID, root.AgentID, brokerTestSecondDeviceID,
		),
		manifest: manifest, manifestHash: manifestHash, params: params,
	}
}
