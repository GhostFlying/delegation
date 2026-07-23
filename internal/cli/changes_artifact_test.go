package cli

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	cliChangesAgentID    = "123e4567-e89b-42d3-a456-426614174220"
	cliChangesParentID   = "123e4567-e89b-42d3-a456-426614174221"
	cliChangesArtifactID = "123e4567-e89b-42d3-a456-426614174222"
	cliChangesTurnID     = "123e4567-e89b-42d3-a456-426614174223"
	cliChangesWorkspace  = "123e4567-e89b-42d3-a456-426614174224"
)

type changesArtifactHostStub struct {
	mu        sync.Mutex
	changes   chan struct{}
	artifacts []store.ChangesArtifact
	result    store.WorkerFinalization
	ackKey    store.WorkerKey
	ackID     string
	ackSeq    uint64
	ackCalls  int
}

func (h *changesArtifactHostStub) ArtifactChanges() <-chan struct{} { return h.changes }

func (h *changesArtifactHostStub) ListPendingChangesPublications(
	context.Context,
) ([]store.ChangesArtifact, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]store.ChangesArtifact(nil), h.artifacts...), nil
}

func (h *changesArtifactHostStub) AcknowledgeChangesArtifact(
	_ context.Context,
	key store.WorkerKey,
	artifactID string,
	sequence uint64,
) (store.WorkerFinalization, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ackKey = key
	h.ackID = artifactID
	h.ackSeq = sequence
	h.ackCalls++
	return h.result, nil
}

func TestManagedChangesArtifactSourceMapsAndAcknowledgesDurableOutbox(t *testing.T) {
	worker, artifact := validCLIChangesArtifactState()
	terminalWorker := worker
	terminalWorker.Status = store.WorkerIdle
	terminalWorker.ActiveTurnID = ""
	terminalWorker.FinalTarget = ""
	terminalWorker.Revision++
	terminalArtifact := artifact
	terminalArtifact.State = store.ChangesPublished
	terminalArtifact.BrokerSequence = 41
	host := &changesArtifactHostStub{
		changes: make(chan struct{}, 1), artifacts: []store.ChangesArtifact{artifact},
		result: store.WorkerFinalization{Worker: terminalWorker, Artifact: terminalArtifact},
	}
	source := managedChangesArtifactSource{
		host:         host,
		workers:      lifecycleHostStub{workers: []store.WorkerReservation{worker}},
		controllerID: runtimeControllerID, deviceID: runtimeDeviceID,
	}
	if source.ArtifactChanges() != host.changes {
		t.Fatal("managed changes source did not expose the dedicated notification channel")
	}

	publications, err := source.ListPendingChangesPublications(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(publications) != 1 {
		t.Fatalf("changes artifact publications = %#v", publications)
	}
	publication := publications[0]
	wantSource := connector.ChangesArtifactPublication{
		Source: protocolChangesWorkerIdentity(worker),
		Params: protocol.PublishChangesArtifactParams{
			ArtifactID: artifact.ArtifactID, TurnID: artifact.TurnID,
			WorkspaceID: artifact.WorkspaceID, Status: protocol.ChangesArtifactAvailable,
			BaseHeadOID: artifact.BaseHeadOID, BaseManifestHash: artifact.BaseManifestHash,
			BaseSnapshotHash: artifact.BaseSnapshotHash, ResultHeadOID: artifact.ResultHeadOID,
			ResultSnapshotHash: artifact.ResultSnapshotHash, ResultClean: artifact.ResultClean,
			Parts: []protocol.WorkspaceArtifactDescriptor{
				{Kind: protocol.WorkspaceArtifactBundle, Size: 10, SHA256: strings.Repeat("e", 64)},
				{Kind: protocol.WorkspaceArtifactOverlay, Size: 20, SHA256: strings.Repeat("f", 64)},
			},
			BaseWarnings:   []string{protocol.WorkspaceWarningFullHistoryFallback},
			ResultWarnings: []string{protocol.WorkspaceWarningLFSPayloadNotTransferred},
		},
	}
	if publication.Source != wantSource.Source ||
		!protocol.SameChangesArtifactParams(publication.Params, wantSource.Params) {
		t.Fatalf("changes artifact publication = %#v, want %#v", publication, wantSource)
	}
	if err := source.AcknowledgeChangesArtifact(context.Background(), publication, 41); err != nil {
		t.Fatal(err)
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.ackCalls != 1 || host.ackKey != artifact.WorkerKey ||
		host.ackID != artifact.ArtifactID || host.ackSeq != 41 {
		t.Fatalf("changes artifact host acknowledgement = key %#v, id %q, seq %d, calls %d",
			host.ackKey, host.ackID, host.ackSeq, host.ackCalls)
	}
}

func protocolChangesWorkerIdentity(worker store.WorkerReservation) control.PrincipalIdentity {
	return control.PrincipalIdentity{
		ControllerID: worker.ControllerID, TreeID: worker.TreeID, AgentID: worker.AgentID,
		ParentAgentID: worker.ParentAgentID, DeviceID: worker.DeviceID,
	}
}

func validCLIChangesArtifactState() (store.WorkerReservation, store.ChangesArtifact) {
	workspacePath := "/tmp/delegation-managed-worker"
	if runtime.GOOS == "windows" {
		workspacePath = `C:\delegation-managed-worker`
	}
	key := store.WorkerKey{
		ControllerID: runtimeControllerID, TreeID: runtimeThreadID, AgentID: cliChangesAgentID,
	}
	worker := store.WorkerReservation{
		WorkerKey: key, ParentAgentID: cliChangesParentID, DeviceID: runtimeDeviceID,
		TaskName: "capture changes", PromptDigest: strings.Repeat("1", 64),
		WorkspaceID: cliChangesWorkspace, WorkspacePath: workspacePath,
		CodexThreadID: runtimeManagedThreadID, ProfileVersion: 1,
		Status: store.WorkerFinalizing, ActiveTurnID: cliChangesTurnID,
		FinalTarget: store.WorkerIdle, Revision: 7, CreatedAt: 1, UpdatedAt: 2,
	}
	artifact := store.ChangesArtifact{
		WorkerKey: key, ArtifactID: cliChangesArtifactID, TurnID: cliChangesTurnID,
		WorkspaceID: cliChangesWorkspace, CompletionTarget: store.WorkerIdle,
		State: store.ChangesPublishPending, Status: store.ChangesAvailable,
		ObjectFormat: "sha1", BaseHeadOID: strings.Repeat("a", 40), BaseClean: true,
		BaseManifestHash: strings.Repeat("b", 64), BaseSnapshotHash: strings.Repeat("c", 64),
		ResultHeadOID: strings.Repeat("d", 40), ResultSnapshotHash: strings.Repeat("4", 64),
		ResultClean: false,
		Parts: []store.ChangesArtifactPart{
			{Kind: store.ChangesArtifactOverlay, Name: store.ChangesOverlayPartName, SizeBytes: 20, SHA256: strings.Repeat("f", 64)},
			{Kind: store.ChangesArtifactBundle, Name: store.ChangesBundlePartName, SizeBytes: 10, SHA256: strings.Repeat("e", 64)},
		},
		BaseWarnings:      []string{protocol.WorkspaceWarningFullHistoryFallback},
		ResultWarnings:    []string{protocol.WorkspaceWarningLFSPayloadNotTransferred},
		RetentionReserved: true, ReservedBytes: 30, PayloadBytes: 30,
		CreatedAt: 1, UpdatedAt: 2,
	}
	return worker, artifact
}
