package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const workspaceTransferRPCSyncID = "123e4567-e89b-42d3-a456-426614174143"

type transferWorkspacePeer struct {
	recordingWorkspacePeer

	requireTransfer bool
	artifact        []byte
	transfer        protocol.WorkspaceTransferManifest
	received        []byte
	readRequests    []connector.WorkspaceReadArtifactRequest
	writeRequests   []connector.WorkspaceWriteArtifactRequest
	cancelCount     int
	cancelRequests  []connector.WorkspaceTransferControlRequest
	cancelObserved  chan connector.WorkspaceTransferControlRequest
	cancelErr       error
	createErr       error
	beginErr        error
	shortReads      bool
}

func (p *transferWorkspacePeer) PrepareWorkspace(
	_ context.Context,
	request connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	if !p.requireTransfer {
		return p.recordingWorkspacePeer.PrepareWorkspace(context.Background(), request)
	}
	hash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	p.mu.Lock()
	p.preparations = append(p.preparations, request)
	p.mu.Unlock()
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID,
		Outcome:     protocol.WorkspacePrepareTransferRequired, ManifestHash: hash,
		Warnings:  append([]string(nil), request.Params.Manifest.Warnings...),
		BasisOIDs: []string{"1111111111111111111111111111111111111111"}, BundleRequired: true,
	}, nil
}

func (p *transferWorkspacePeer) CreateWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	if len(p.artifact) == 0 {
		return protocol.CreateWorkspaceTransferResult{}, errors.New("source artifact is unavailable")
	}
	digest := sha256.Sum256(p.artifact)
	manifestHash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	transfer := protocol.WorkspaceTransferManifest{
		TransferID: request.Params.TransferID, WorkspaceID: request.Params.WorkspaceID,
		Strategy: protocol.WorkspaceStrategyThin, ManifestHash: manifestHash,
		Artifacts: []protocol.WorkspaceArtifactDescriptor{{
			Kind: protocol.WorkspaceArtifactBundle, Size: int64(len(p.artifact)),
			SHA256: hex.EncodeToString(digest[:]),
		}},
		Warnings: append([]string(nil), request.Params.Manifest.Warnings...),
	}
	p.mu.Lock()
	p.transfer = transfer
	createErr := p.createErr
	p.mu.Unlock()
	if createErr != nil {
		return protocol.CreateWorkspaceTransferResult{}, createErr
	}
	return protocol.CreateWorkspaceTransferResult{Transfer: transfer}, nil
}

func (p *transferWorkspacePeer) ReadWorkspaceArtifact(
	_ context.Context,
	request connector.WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readRequests = append(p.readRequests, request)
	if request.Params.TransferID != p.transfer.TransferID || request.Params.Kind != protocol.WorkspaceArtifactBundle ||
		request.Params.Offset < 0 || request.Params.Offset >= int64(len(p.artifact)) {
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("invalid source artifact read")
	}
	end := min(request.Params.Offset+int64(request.Params.Limit), int64(len(p.artifact)))
	if p.shortReads {
		end = request.Params.Offset + 1
	}
	data := append([]byte(nil), p.artifact[request.Params.Offset:end]...)
	return protocol.ReadWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, Kind: request.Params.Kind,
		Offset: request.Params.Offset, Data: data, NextOffset: end,
	}, nil
}

func (p *transferWorkspacePeer) BeginWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transfer = request.Params.Transfer
	p.received = nil
	if p.beginErr != nil {
		return protocol.BeginWorkspaceTransferResult{}, p.beginErr
	}
	return protocol.BeginWorkspaceTransferResult{TransferID: request.Params.Transfer.TransferID}, nil
}

func (p *transferWorkspacePeer) WriteWorkspaceArtifact(
	_ context.Context,
	request connector.WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeRequests = append(p.writeRequests, request)
	if request.Params.TransferID != p.transfer.TransferID ||
		request.Params.Offset != int64(len(p.received)) {
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("out-of-sequence target artifact write")
	}
	p.received = append(p.received, request.Params.Data...)
	return protocol.WriteWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, NextOffset: int64(len(p.received)),
	}, nil
}

func (p *transferWorkspacePeer) FinishWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.transfer.Artifacts) != 1 {
		return protocol.FinishWorkspaceTransferResult{}, errors.New("target transfer descriptor is unavailable")
	}
	digest := sha256.Sum256(p.received)
	if int64(len(p.received)) != p.transfer.Artifacts[0].Size ||
		hex.EncodeToString(digest[:]) != p.transfer.Artifacts[0].SHA256 {
		return protocol.FinishWorkspaceTransferResult{}, errors.New("target transfer digest mismatch")
	}
	return protocol.FinishWorkspaceTransferResult{Workspace: protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID, Outcome: protocol.WorkspacePrepareReady,
		Strategy: p.transfer.Strategy, ManifestHash: p.transfer.ManifestHash,
		Warnings: append([]string(nil), p.transfer.Warnings...),
	}}, nil
}

func (p *transferWorkspacePeer) CancelWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	p.mu.Lock()
	p.cancelCount++
	cancelCount := p.cancelCount
	cancelErr := p.cancelErr
	p.cancelRequests = append(p.cancelRequests, request)
	cancelObserved := p.cancelObserved
	p.mu.Unlock()
	if cancelObserved != nil {
		cancelObserved <- request
	}
	if cancelCount == 1 && cancelErr != nil {
		return protocol.CancelWorkspaceTransferResult{}, cancelErr
	}
	return protocol.CancelWorkspaceTransferResult{TransferID: request.Params.TransferID}, nil
}

func TestWorkspaceRPCRelaysBoundedArtifactWithoutExposingIntermediateState(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	artifact := bytes.Repeat([]byte("bounded-transfer-payload"), 20_000)
	sourceManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{
			deviceID: brokerTestDeviceID, manifest: manifest,
		},
		artifact: artifact,
	}
	targetManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
		requireTransfer:        true,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	params := protocol.SyncWorkspaceParams{
		SyncID: workspaceTransferRPCSyncID, TargetDeviceID: agentRPCTargetID,
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
	}
	var synchronized protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &synchronized,
	); err != nil {
		t.Fatal(err)
	}
	if synchronized.Outcome != protocol.WorkspacePrepareReady || synchronized.Workspace == nil ||
		synchronized.Workspace.Strategy != protocol.WorkspaceStrategyThin {
		t.Fatalf("workspace sync = %#v", synchronized)
	}
	targetManager.mu.Lock()
	received := append([]byte(nil), targetManager.received...)
	writes := append([]connector.WorkspaceWriteArtifactRequest(nil), targetManager.writeRequests...)
	targetCancels := targetManager.cancelCount
	targetManager.mu.Unlock()
	sourceManager.mu.Lock()
	reads := append([]connector.WorkspaceReadArtifactRequest(nil), sourceManager.readRequests...)
	sourceCancels := sourceManager.cancelCount
	sourceManager.mu.Unlock()
	if !slices.Equal(received, artifact) || len(reads) < 2 || len(reads) != len(writes) {
		t.Fatalf("relay = %d bytes, %d reads, %d writes", len(received), len(reads), len(writes))
	}
	for index := range reads {
		if reads[index].Params.Limit > protocol.WorkspaceArtifactChunkBytes ||
			len(writes[index].Params.Data) > protocol.WorkspaceArtifactChunkBytes {
			t.Fatalf("relay chunk %d exceeded limit", index)
		}
	}
	if sourceCancels != 1 || targetCancels != 0 {
		t.Fatalf("cleanup calls = source %d, target %d", sourceCancels, targetCancels)
	}
	var repeated protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &repeated,
	); err != nil || repeated.Workspace == nil ||
		repeated.Workspace.ManifestHash != synchronized.Workspace.ManifestHash {
		t.Fatalf("idempotent transferred workspace = %#v, %v", repeated, err)
	}
	sourceManager.mu.Lock()
	repeatedReads := len(sourceManager.readRequests)
	sourceManager.mu.Unlock()
	if repeatedReads != len(reads) {
		t.Fatalf("idempotent sync relayed artifact again: %d reads, want %d", repeatedReads, len(reads))
	}
}

func TestWorkspaceRPCTransferFailureCleansCreatedState(t *testing.T) {
	tests := []struct {
		name                      string
		configureSource           func(*transferWorkspacePeer)
		configureTarget           func(*transferWorkspacePeer)
		wantReads                 int
		wantSourceCancels         int
		wantTransferTargetCancels int
		wantCleanupError          bool
	}{
		{
			name: "source create acknowledgement is lost",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.createErr = errors.New("lost source create acknowledgement")
			},
			wantSourceCancels: 1,
		},
		{
			name: "target begin acknowledgement is lost",
			configureTarget: func(peer *transferWorkspacePeer) {
				peer.beginErr = errors.New("lost target begin acknowledgement")
			},
			wantSourceCancels:         1,
			wantTransferTargetCancels: 1,
		},
		{
			name: "source returns a short chunk",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.shortReads = true
			},
			wantReads:                 1,
			wantSourceCancels:         1,
			wantTransferTargetCancels: 1,
		},
		{
			name: "target cleanup failure does not skip source cleanup",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.shortReads = true
			},
			configureTarget: func(peer *transferWorkspacePeer) {
				peer.cancelErr = errors.New("target cleanup failed")
			},
			wantReads:                 1,
			wantSourceCancels:         1,
			wantTransferTargetCancels: 1,
			wantCleanupError:          true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
			gitURL := "ssh://git@example.invalid/repository.git"
			manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			sourceManager := &transferWorkspacePeer{
				recordingWorkspacePeer: recordingWorkspacePeer{
					deviceID: brokerTestDeviceID, manifest: manifest,
				},
				artifact: []byte("bounded-transfer-payload"),
			}
			targetManager := &transferWorkspacePeer{
				recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
				requireTransfer:        true,
				cancelObserved:         make(chan connector.WorkspaceTransferControlRequest, 2),
			}
			if test.configureSource != nil {
				test.configureSource(sourceManager)
			}
			if test.configureTarget != nil {
				test.configureTarget(targetManager)
			}
			sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
			startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var root protocol.EnsureRootTreeResult
			if err := sourceClient.Call(
				ctx, protocol.MethodEnsureRootTree, "", nil,
				protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID}, &root,
			); err != nil {
				t.Fatal(err)
			}
			source := root.Principal.Identity()
			var synchronized protocol.SyncWorkspaceResult
			err := sourceClient.Call(
				ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source,
				protocol.SyncWorkspaceParams{
					SyncID: workspaceTransferRPCSyncID, TargetDeviceID: agentRPCTargetID,
					GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
				},
				&synchronized,
			)
			if err == nil {
				t.Fatalf("workspace transfer unexpectedly succeeded: %#v", synchronized)
			}
			if test.wantCleanupError {
				select {
				case reported := <-harness.reported:
					if !strings.Contains(reported.Error(), "cancel target workspace transfer") {
						t.Fatalf("reported cleanup error = %v", reported)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("target cleanup error was not reported")
				}
			}
			wantTargetCancels := test.wantTransferTargetCancels + 1
			var provisionalCleanup connector.WorkspaceTransferControlRequest
			provisionalCleanupCount := 0
			for range wantTargetCancels {
				select {
				case canceled := <-targetManager.cancelObserved:
					if canceled.Params.TransferID == workspaceTransferRPCSyncID {
						provisionalCleanup = canceled
						provisionalCleanupCount++
					}
				case <-time.After(2 * time.Second):
					t.Fatal("target workspace cleanup did not finish")
				}
			}
			if provisionalCleanupCount != 1 ||
				provisionalCleanup.Params.WorkspaceID != workspaceTransferRPCSyncID ||
				provisionalCleanup.Params.TransferID != workspaceTransferRPCSyncID {
				t.Fatalf(
					"provisional target cleanup = %#v, observed %d exact calls",
					provisionalCleanup, provisionalCleanupCount,
				)
			}
			sourceManager.mu.Lock()
			reads := len(sourceManager.readRequests)
			sourceCancels := sourceManager.cancelCount
			sourceManager.mu.Unlock()
			targetManager.mu.Lock()
			targetCancels := targetManager.cancelCount
			targetManager.mu.Unlock()
			if reads != test.wantReads || sourceCancels != test.wantSourceCancels ||
				targetCancels != wantTargetCancels {
				t.Fatalf(
					"failure cleanup = %d reads, %d source cancels, %d target cancels; want %d/%d/%d",
					reads, sourceCancels, targetCancels,
					test.wantReads, test.wantSourceCancels, wantTargetCancels,
				)
			}
			waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
		})
	}
}
