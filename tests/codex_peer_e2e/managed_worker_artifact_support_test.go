//go:build integration

package codex_peer_e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

type artifactWorkerMock struct {
	mu         sync.Mutex
	calls      int
	errors     []string
	testCase   string
	leaveDirty bool
}

func (m *artifactWorkerMock) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/responses" {
		m.fail(writer, fmt.Errorf("unexpected model request %s %s", request.Method, request.URL.Path))
		return
	}
	body, err := decodeManagedRequest(request)
	if err != nil {
		m.fail(writer, err)
		return
	}
	encoded, _ := json.Marshal(body)
	if !bytes.Contains(encoded, []byte("managed-worker-case="+m.testCase)) {
		m.fail(writer, errors.New("managed model request has no artifact test case"))
		return
	}
	m.mu.Lock()
	call := m.calls
	m.calls++
	m.mu.Unlock()
	command := artifactE2EWorkerCommand(m.leaveDirty)
	shellTool, shellArguments := managedShellTool(command)
	callID := "call-artifact-cross-platform"
	switch call {
	case 0:
		tools, _ := json.Marshal(body["tools"])
		if !bytes.Contains(tools, []byte(`"name":"`+shellTool+`"`)) {
			m.record(fmt.Errorf("managed worker exposes no %s tool: %s", shellTool, tools))
		}
		arguments, _ := json.Marshal(shellArguments)
		writeManagedSSE(writer,
			map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-artifact-shell"}},
			map[string]any{"type": "response.output_item.done", "item": map[string]any{
				"type": "function_call", "call_id": callID,
				"name": shellTool, "arguments": string(arguments),
			}},
			managedCompletedEvent("resp-artifact-shell"),
		)
	case 1:
		output := functionCallOutput(body["input"], callID)
		if !strings.Contains(output, artifactEESuccessMarker) {
			m.fail(writer, fmt.Errorf("workspace mutation failed: %s", output))
			return
		}
		writeManagedSSE(writer,
			map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-artifact-final"}},
			map[string]any{"type": "response.output_item.done", "item": map[string]any{
				"type": "message", "role": "assistant", "id": "msg-artifact-final",
				"content": []map[string]any{{"type": "output_text", "text": "artifact-ok"}},
			}},
			managedCompletedEvent("resp-artifact-final"),
		)
	case 2:
		writeManagedSSE(writer,
			map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-artifact-followup"}},
			map[string]any{"type": "response.output_item.done", "item": map[string]any{
				"type": "message", "role": "assistant", "id": "msg-artifact-followup",
				"content": []map[string]any{{"type": "output_text", "text": "result-ready"}},
			}},
			managedCompletedEvent("resp-artifact-followup"),
		)
	default:
		m.fail(writer, fmt.Errorf("artifact model received unexpected call %d", call+1))
	}
}

func artifactE2EWorkerCommand(leaveDirty bool) string {
	dirtyWindows := ""
	dirtyPOSIX := ""
	if leaveDirty {
		dirtyWindows = "[System.IO.File]::WriteAllText((Join-Path (Get-Location) 'dirty-worker.txt'), 'dirty-worker', $utf8)\n"
		dirtyPOSIX = "printf '%s\\n' dirty-worker > dirty-worker.txt\n"
	}
	if runtime.GOOS == "windows" {
		return `$ErrorActionPreference = 'Stop'
	if ((Get-Content -Raw tracked.txt).Trim() -ne 'tracked-base') { throw 'unexpected tracked base' }
	if ((Get-Content -Raw rename-source.txt).Trim() -ne 'rename-base') { throw 'unexpected rename base' }
$utf8 = [System.Text.UTF8Encoding]::new($false)
[System.IO.File]::WriteAllText((Join-Path (Get-Location) 'tracked.txt'), 'tracked-worker', $utf8)
if ((Get-Content -Raw tracked.txt).Trim() -ne 'tracked-worker') { throw 'tracked write failed' }
git mv -- rename-source.txt renamed-worker.txt
if ($LASTEXITCODE -ne 0) { throw 'git mv failed' }
git add -- tracked.txt renamed-worker.txt
if ($LASTEXITCODE -ne 0) { throw 'git add failed' }
	git -c "user.name=Delegation Worker Test" -c "user.email=worker@example.invalid" commit -m "worker artifact commit"
	if ($LASTEXITCODE -ne 0) { throw 'git commit failed' }
	` + dirtyWindows + `Write-Output '` + artifactEESuccessMarker + `'`
	}
	return `set -eu
test "$(cat tracked.txt)" = tracked-base
test "$(cat rename-source.txt)" = rename-base
printf '%s\n' tracked-worker > tracked.txt
	git mv -- rename-source.txt renamed-worker.txt
	git add -- tracked.txt renamed-worker.txt
	git -c user.name='Delegation Worker Test' -c user.email=worker@example.invalid commit -m 'worker artifact commit'
	` + dirtyPOSIX + `printf '%s\n' ` + artifactEESuccessMarker
}

func (m *artifactWorkerMock) fail(writer http.ResponseWriter, err error) {
	m.record(err)
	http.Error(writer, err.Error(), http.StatusInternalServerError)
}

func (m *artifactWorkerMock) record(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err.Error())
}

func (m *artifactWorkerMock) verify(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls != 3 || len(m.errors) != 0 {
		t.Fatalf("artifact model calls = %d, errors = %v", m.calls, m.errors)
	}
}

func (m *artifactWorkerMock) diagnostics() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("calls=%d errors=%v", m.calls, m.errors)
}

type artifactE2ESource struct {
	host  *workerhost.Host
	state *store.PeerStore
}

func (s *artifactE2ESource) ArtifactChanges() <-chan struct{} {
	return s.host.ArtifactChanges()
}

func (s *artifactE2ESource) ListPendingChangesPublications(
	ctx context.Context,
) ([]connector.ChangesArtifactPublication, error) {
	artifacts, err := s.host.ListPendingChangesPublications(ctx)
	if err != nil {
		return nil, err
	}
	publications := make([]connector.ChangesArtifactPublication, 0, len(artifacts))
	for _, artifact := range artifacts {
		worker, err := s.state.GetWorker(ctx, artifact.WorkerKey)
		if err != nil {
			return nil, err
		}
		params, err := artifactE2EPublicationParams(artifact)
		if err != nil {
			return nil, err
		}
		publications = append(publications, connector.ChangesArtifactPublication{
			Source: control.PrincipalIdentity{
				ControllerID: artifact.ControllerID,
				TreeID:       artifact.TreeID, AgentID: artifact.AgentID,
				ParentAgentID: worker.ParentAgentID, DeviceID: worker.DeviceID,
			},
			Params: params,
		})
	}
	return publications, nil
}

func (s *artifactE2ESource) AcknowledgeChangesArtifact(
	ctx context.Context,
	publication connector.ChangesArtifactPublication,
	sequence uint64,
) error {
	finalization, err := s.host.AcknowledgeChangesArtifact(
		ctx,
		store.WorkerKey{
			ControllerID: publication.Source.ControllerID,
			TreeID:       publication.Source.TreeID, AgentID: publication.Source.AgentID,
		},
		publication.Params.ArtifactID,
		sequence,
	)
	if err != nil {
		return err
	}
	if finalization.Artifact.State != store.ChangesPublished ||
		finalization.Artifact.BrokerSequence != sequence {
		return errors.New("worker host returned a mismatched changes artifact acknowledgement")
	}
	return nil
}

func artifactE2EPublicationParams(
	artifact store.ChangesArtifact,
) (protocol.PublishChangesArtifactParams, error) {
	if artifact.Status != store.ChangesAvailable {
		return protocol.PublishChangesArtifactParams{}, fmt.Errorf("unexpected artifact status %q", artifact.Status)
	}
	parts := make([]protocol.WorkspaceArtifactDescriptor, 0, len(artifact.Parts))
	for _, part := range artifact.Parts {
		var kind protocol.WorkspaceArtifactKind
		switch part.Kind {
		case store.ChangesArtifactBundle:
			kind = protocol.WorkspaceArtifactBundle
		case store.ChangesArtifactOverlay:
			kind = protocol.WorkspaceArtifactOverlay
		default:
			return protocol.PublishChangesArtifactParams{}, fmt.Errorf("unexpected artifact part %q", part.Kind)
		}
		parts = append(parts, protocol.WorkspaceArtifactDescriptor{
			Kind: kind, Size: part.SizeBytes, SHA256: part.SHA256,
		})
	}
	slices.SortFunc(parts, func(left, right protocol.WorkspaceArtifactDescriptor) int {
		return strings.Compare(string(left.Kind), string(right.Kind))
	})
	params := protocol.PublishChangesArtifactParams{
		ArtifactID: artifact.ArtifactID, TurnID: artifact.TurnID,
		WorkspaceID: artifact.WorkspaceID, Status: protocol.ChangesArtifactAvailable,
		WorkspaceSourceDeviceID: artifact.WorkspaceSourceDeviceID,
		WorkspaceTargetDeviceID: artifact.WorkspaceTargetDeviceID,
		BaseHeadOID:             artifact.BaseHeadOID, BaseManifestHash: artifact.BaseManifestHash,
		BaseSnapshotHash: artifact.BaseSnapshotHash, ResultHeadOID: artifact.ResultHeadOID,
		ResultSnapshotHash: artifact.ResultSnapshotHash, ResultClean: artifact.ResultClean,
		Parts: parts, BaseWarnings: slices.Clone(artifact.BaseWarnings),
		ResultWarnings: slices.Clone(artifact.ResultWarnings), FailureCode: artifact.FailureCode,
	}
	if err := params.Validate(); err != nil {
		return protocol.PublishChangesArtifactParams{}, err
	}
	return params, nil
}

type artifactE2ENoopPeer struct{}

func (artifactE2ENoopPeer) SpawnWorker(
	context.Context, connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	return protocol.SpawnWorkerResult{}, errors.New("unexpected worker spawn")
}

func (artifactE2ENoopPeer) SendWorker(
	context.Context, connector.WorkerSendRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{}, errors.New("unexpected worker send")
}

func (artifactE2ENoopPeer) FollowupWorker(
	context.Context, connector.WorkerFollowupRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{}, errors.New("unexpected worker followup")
}

func (artifactE2ENoopPeer) InterruptWorker(
	context.Context, connector.WorkerInterruptRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{}, errors.New("unexpected worker interrupt")
}

func (artifactE2ENoopPeer) WorkerRevision() uint64 { return 0 }

func (artifactE2ENoopPeer) WorkerLifecycleChanges() <-chan struct{} { return nil }

func (artifactE2ENoopPeer) ListWorkerLifecycles(
	context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	return []protocol.WorkerLifecycleSnapshot{}, nil
}

func (artifactE2ENoopPeer) InspectWorkspace(
	context.Context, connector.WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	return protocol.InspectWorkspaceResult{}, errors.New("unexpected workspace inspection")
}

func (artifactE2ENoopPeer) PrepareWorkspace(
	context.Context, connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	return protocol.PrepareWorkspaceResult{}, errors.New("unexpected workspace preparation")
}

func (artifactE2ENoopPeer) CreateWorkspaceTransfer(
	context.Context, connector.WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	return protocol.CreateWorkspaceTransferResult{}, errors.New("unexpected workspace transfer creation")
}

func (artifactE2ENoopPeer) ReadWorkspaceArtifact(
	context.Context, connector.WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	return protocol.ReadWorkspaceArtifactResult{}, errors.New("unexpected workspace artifact read")
}

func (artifactE2ENoopPeer) BeginWorkspaceTransfer(
	context.Context, connector.WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	return protocol.BeginWorkspaceTransferResult{}, errors.New("unexpected workspace transfer begin")
}

func (artifactE2ENoopPeer) WriteWorkspaceArtifact(
	context.Context, connector.WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	return protocol.WriteWorkspaceArtifactResult{}, errors.New("unexpected workspace artifact write")
}

func (artifactE2ENoopPeer) FinishWorkspaceTransfer(
	context.Context, connector.WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	return protocol.FinishWorkspaceTransferResult{}, errors.New("unexpected workspace transfer finish")
}

func (artifactE2ENoopPeer) CancelWorkspaceTransfer(
	context.Context, connector.WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	return protocol.CancelWorkspaceTransferResult{}, errors.New("unexpected workspace transfer cancel")
}

func (artifactE2ENoopPeer) CleanupWorkspaceTransfers(context.Context) error { return nil }

func drainArtifactE2EErrors(errorsChannel <-chan error) []error {
	var errorsList []error
	for {
		select {
		case err := <-errorsChannel:
			errorsList = append(errorsList, err)
		default:
			return errorsList
		}
	}
}
