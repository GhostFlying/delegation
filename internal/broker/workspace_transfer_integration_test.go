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
	requireOverlay  bool
	artifact        []byte
	transfer        protocol.WorkspaceTransferManifest
	received        []byte
	readRequests    []connector.WorkspaceReadArtifactRequest
	writeRequests   []connector.WorkspaceWriteArtifactRequest
	cancelCount     int
	cancelRequests  []connector.WorkspaceTransferControlRequest
	cancelObserved  chan connector.WorkspaceTransferControlRequest
	cancelErr       error
	cleanupCount    int
	createErr       error
	beginErr        error
	writeApplied    chan struct{}
	writeRelease    chan struct{}
	finishPublished chan struct{}
	finishRelease   chan struct{}
	published       *protocol.PrepareWorkspaceResult
	shortReads      bool
}

func (p *transferWorkspacePeer) PrepareWorkspace(
	_ context.Context,
	request connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	if !p.requireTransfer {
		return p.recordingWorkspacePeer.PrepareWorkspace(context.Background(), request)
	}
	p.mu.Lock()
	p.preparations = append(p.preparations, request)
	if p.published != nil {
		published := *p.published
		p.mu.Unlock()
		return published, nil
	}
	p.mu.Unlock()
	hash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	basisOIDs := []string{"1111111111111111111111111111111111111111"}
	if p.requireOverlay {
		basisOIDs = nil
	}
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID,
		Outcome:     protocol.WorkspacePrepareTransferRequired, ManifestHash: hash,
		Warnings:       append([]string(nil), request.Params.Manifest.Warnings...),
		BasisOIDs:      basisOIDs,
		BundleRequired: !p.requireOverlay, OverlayRequired: p.requireOverlay,
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
	p.writeRequests = append(p.writeRequests, request)
	if request.Params.TransferID != p.transfer.TransferID ||
		request.Params.Offset != int64(len(p.received)) {
		p.mu.Unlock()
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("out-of-sequence target artifact write")
	}
	p.received = append(p.received, request.Params.Data...)
	nextOffset := int64(len(p.received))
	writeApplied := p.writeApplied
	writeRelease := p.writeRelease
	p.writeApplied = nil
	p.writeRelease = nil
	p.mu.Unlock()
	if writeApplied != nil {
		close(writeApplied)
		<-writeRelease
	}
	return protocol.WriteWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, NextOffset: nextOffset,
	}, nil
}

func (p *transferWorkspacePeer) FinishWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	p.mu.Lock()
	if len(p.transfer.Artifacts) != 1 {
		p.mu.Unlock()
		return protocol.FinishWorkspaceTransferResult{}, errors.New("target transfer descriptor is unavailable")
	}
	digest := sha256.Sum256(p.received)
	if int64(len(p.received)) != p.transfer.Artifacts[0].Size ||
		hex.EncodeToString(digest[:]) != p.transfer.Artifacts[0].SHA256 {
		p.mu.Unlock()
		return protocol.FinishWorkspaceTransferResult{}, errors.New("target transfer digest mismatch")
	}
	workspace := protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID, Outcome: protocol.WorkspacePrepareReady,
		Strategy: p.transfer.Strategy, ManifestHash: p.transfer.ManifestHash,
		Warnings: append([]string(nil), p.transfer.Warnings...),
	}
	p.published = &workspace
	finishPublished := p.finishPublished
	finishRelease := p.finishRelease
	p.finishPublished = nil
	p.finishRelease = nil
	p.mu.Unlock()
	if finishPublished != nil {
		close(finishPublished)
		<-finishRelease
	}
	return protocol.FinishWorkspaceTransferResult{Workspace: workspace}, nil
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
	p.mu.Lock()
	if p.transfer.TransferID == request.Params.TransferID {
		p.transfer = protocol.WorkspaceTransferManifest{}
		p.received = nil
	}
	p.mu.Unlock()
	return protocol.CancelWorkspaceTransferResult{TransferID: request.Params.TransferID}, nil
}

func (p *transferWorkspacePeer) CleanupWorkspaceTransfers(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupCount++
	p.transfer = protocol.WorkspaceTransferManifest{}
	p.received = nil
	return nil
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

func TestWorkspaceRPCPostSideEffectAcknowledgementLossRetriesSameSync(t *testing.T) {
	tests := []struct {
		name              string
		dropAfter         string
		wantRepeatedRelay bool
	}{
		{
			name:              "artifact write acknowledgement",
			dropAfter:         "write",
			wantRepeatedRelay: true,
		},
		{
			name:      "finish acknowledgement",
			dropAfter: "finish",
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
				artifact: []byte("post-side-effect-acknowledgement-loss"),
			}
			sideEffect := make(chan struct{})
			release := make(chan struct{})
			targetManager := &transferWorkspacePeer{
				recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
				requireTransfer:        true,
			}
			if test.dropAfter == "write" {
				targetManager.writeApplied = sideEffect
				targetManager.writeRelease = release
			} else {
				targetManager.finishPublished = sideEffect
				targetManager.finishRelease = release
			}
			sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
			targetClient := startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
			initialTarget := harness.server.connection(agentRPCTargetID)
			if initialTarget == nil {
				t.Fatal("target connector was not registered")
			}
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
			callDone := make(chan error, 1)
			go func() {
				callDone <- sourceClient.Call(
					ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params,
					&protocol.SyncWorkspaceResult{},
				)
			}()
			select {
			case <-sideEffect:
			case <-time.After(2 * time.Second):
				t.Fatalf("target did not apply the %s side effect", test.dropAfter)
			}
			_ = initialTarget.connection.CloseNow()
			close(release)
			select {
			case err := <-callDone:
				if err == nil {
					t.Fatal("workspace sync with lost wire acknowledgement unexpectedly succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("workspace wire acknowledgement loss did not return")
			}
			select {
			case reported := <-harness.reported:
				if !strings.Contains(reported.Error(), "cancel target workspace transfer") {
					t.Fatalf("wire acknowledgement loss cleanup error = %v", reported)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("wire acknowledgement loss did not report target cleanup fencing")
			}
			waitForWorkspaceSessionReplacement(t, harness.server, agentRPCTargetID, initialTarget)
			if err := targetClient.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
			sourceManager.mu.Lock()
			readsBeforeRetry := len(sourceManager.readRequests)
			sourceManager.mu.Unlock()
			var retried protocol.SyncWorkspaceResult
			if err := sourceClient.Call(
				ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &retried,
			); err != nil || retried.Workspace == nil || retried.Workspace.Strategy != protocol.WorkspaceStrategyThin {
				t.Fatalf("same-sync acknowledgement-loss retry = %#v, %v", retried, err)
			}
			sourceManager.mu.Lock()
			readsAfterRetry := len(sourceManager.readRequests)
			sourceManager.mu.Unlock()
			if test.wantRepeatedRelay != (readsAfterRetry > readsBeforeRetry) {
				t.Fatalf(
					"relay reads before/after retry = %d/%d, repeated=%t",
					readsBeforeRetry, readsAfterRetry, test.wantRepeatedRelay,
				)
			}
			targetManager.mu.Lock()
			published := targetManager.published != nil
			cleanupCount := targetManager.cleanupCount
			targetManager.mu.Unlock()
			if !published || cleanupCount == 0 {
				t.Fatalf("target retry publication/cleanup = %t/%d", published, cleanupCount)
			}
		})
	}
}

func TestWorkspaceRPCSuccessfulTransferSurvivesSourceCleanupFailure(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	sourceManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{
			deviceID: brokerTestDeviceID, manifest: manifest,
		},
		artifact:  []byte("successful-transfer-with-source-cleanup-failure"),
		cancelErr: errors.New("source cleanup failed after target publication"),
	}
	targetManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
		requireTransfer:        true,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
	initialSource := harness.server.connection(brokerTestDeviceID)
	if initialSource == nil {
		t.Fatal("source connector was not registered")
	}
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
	); err != nil || synchronized.Workspace == nil ||
		synchronized.Workspace.Strategy != protocol.WorkspaceStrategyThin {
		t.Fatalf("successful sync with source cleanup failure = %#v, %v", synchronized, err)
	}
	select {
	case reported := <-harness.reported:
		if !strings.Contains(reported.Error(), "clean source workspace transfer") {
			t.Fatalf("source cleanup error = %v", reported)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source cleanup failure was not reported")
	}
	waitForWorkspaceSessionReplacement(t, harness.server, brokerTestDeviceID, initialSource)
	if err := sourceClient.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	sourceManager.mu.Lock()
	sourceCleanupCount := sourceManager.cleanupCount
	sourceCancelCount := sourceManager.cancelCount
	sourceManager.mu.Unlock()
	targetManager.mu.Lock()
	published := targetManager.published != nil
	targetManager.mu.Unlock()
	if sourceCleanupCount == 0 || sourceCancelCount != 1 || !published {
		t.Fatalf(
			"successful cleanup fence = source cleanup %d/cancel %d, target published %t",
			sourceCleanupCount, sourceCancelCount, published,
		)
	}
	var repeated protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &repeated,
	); err != nil || repeated.Workspace == nil ||
		repeated.Workspace.ManifestHash != synchronized.Workspace.ManifestHash {
		t.Fatalf("durable successful sync retry = %#v, %v", repeated, err)
	}
}

func TestWorkspaceRPCDirtyOverlayRejectionCleansBothPeers(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	manifest.Clean = false
	sourceManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{
			deviceID: brokerTestDeviceID, manifest: manifest,
		},
		artifact:  []byte("unused-overlay-placeholder"),
		createErr: errors.New("dirty workspace overlay transport is not implemented"),
	}
	targetManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
		requireTransfer:        true, requireOverlay: true,
		cancelObserved: make(chan connector.WorkspaceTransferControlRequest, 1),
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
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "dirty-source"),
	}
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params,
		&protocol.SyncWorkspaceResult{},
	); err == nil {
		t.Fatal("dirty workspace synchronization unexpectedly succeeded")
	}
	select {
	case canceled := <-targetManager.cancelObserved:
		if canceled.Params.WorkspaceID != params.SyncID || canceled.Params.TransferID != params.SyncID {
			t.Fatalf("dirty target provisional cleanup = %#v", canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dirty target provisional workspace was not cleaned")
	}
	sourceManager.mu.Lock()
	sourceCancels := sourceManager.cancelCount
	sourceManager.mu.Unlock()
	targetManager.mu.Lock()
	targetCancels := targetManager.cancelCount
	targetManager.mu.Unlock()
	if sourceCancels != 1 || targetCancels != 1 {
		t.Fatalf("dirty rejection cleanup = source %d target %d, want 1/1", sourceCancels, targetCancels)
	}
	waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
}

func TestWorkspaceRPCTransferFailureCleansCreatedState(t *testing.T) {
	tests := []struct {
		name                   string
		configureSource        func(*transferWorkspacePeer)
		configureTarget        func(*transferWorkspacePeer)
		wantReads              int
		wantSourceCancels      int
		wantTargetCancels      int
		wantProvisionalCleanup bool
		wantSourceCleanupError bool
		wantTargetCleanupError bool
	}{
		{
			name: "source create acknowledgement is lost",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.createErr = errors.New("lost source create acknowledgement")
			},
			wantSourceCancels:      1,
			wantTargetCancels:      1,
			wantProvisionalCleanup: true,
		},
		{
			name: "target begin acknowledgement is lost",
			configureTarget: func(peer *transferWorkspacePeer) {
				peer.beginErr = errors.New("lost target begin acknowledgement")
			},
			wantSourceCancels:      1,
			wantTargetCancels:      2,
			wantProvisionalCleanup: true,
		},
		{
			name: "source returns a short chunk",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.shortReads = true
			},
			wantReads:              1,
			wantSourceCancels:      1,
			wantTargetCancels:      2,
			wantProvisionalCleanup: true,
		},
		{
			name: "target cleanup failure does not skip source cleanup",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.shortReads = true
			},
			configureTarget: func(peer *transferWorkspacePeer) {
				peer.cancelErr = errors.New("target cleanup failed")
			},
			wantReads:              1,
			wantSourceCancels:      1,
			wantTargetCancels:      1,
			wantTargetCleanupError: true,
		},
		{
			name: "source cleanup failure fences and retries",
			configureSource: func(peer *transferWorkspacePeer) {
				peer.cancelErr = errors.New("source cleanup failed")
			},
			configureTarget: func(peer *transferWorkspacePeer) {
				peer.beginErr = errors.New("lost target begin acknowledgement")
			},
			wantSourceCancels:      1,
			wantTargetCancels:      2,
			wantProvisionalCleanup: true,
			wantSourceCleanupError: true,
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
			initialSourceSession := harness.server.connection(brokerTestDeviceID)
			initialTargetSession := harness.server.connection(agentRPCTargetID)
			if initialSourceSession == nil || initialTargetSession == nil {
				t.Fatal("workspace connectors were not registered")
			}
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
			err := sourceClient.Call(
				ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source,
				params,
				&synchronized,
			)
			if err == nil {
				t.Fatalf("workspace transfer unexpectedly succeeded: %#v", synchronized)
			}
			if test.wantSourceCleanupError || test.wantTargetCleanupError {
				operation := "cancel target workspace transfer"
				if test.wantSourceCleanupError {
					operation = "clean source workspace transfer"
				}
				select {
				case reported := <-harness.reported:
					if !strings.Contains(reported.Error(), operation) {
						t.Fatalf("reported cleanup error = %v", reported)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("target cleanup error was not reported")
				}
			}
			var provisionalCleanup connector.WorkspaceTransferControlRequest
			provisionalCleanupCount := 0
			for range test.wantTargetCancels {
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
			if test.wantProvisionalCleanup && (provisionalCleanupCount != 1 ||
				provisionalCleanup.Params.WorkspaceID != workspaceTransferRPCSyncID ||
				provisionalCleanup.Params.TransferID != workspaceTransferRPCSyncID) {
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
				targetCancels != test.wantTargetCancels {
				t.Fatalf(
					"failure cleanup = %d reads, %d source cancels, %d target cancels; want %d/%d/%d",
					reads, sourceCancels, targetCancels,
					test.wantReads, test.wantSourceCancels, test.wantTargetCancels,
				)
			}
			if test.wantTargetCleanupError {
				waitForWorkspaceTargetReplacement(
					t, harness.server, agentRPCTargetID, initialTargetSession, targetManager,
				)
				waitForWorkspaceTargetReplacement(
					t, harness.server, brokerTestDeviceID, initialSourceSession, sourceManager,
				)
				sourceManager.mu.Lock()
				sourceManager.shortReads = false
				sourceManager.mu.Unlock()
				targetManager.mu.Lock()
				targetManager.cancelErr = nil
				targetManager.mu.Unlock()
			}
			if test.wantSourceCleanupError {
				waitForWorkspaceTargetReplacement(
					t, harness.server, brokerTestDeviceID, initialSourceSession, sourceManager,
				)
				sourceManager.mu.Lock()
				sourceManager.cancelErr = nil
				sourceManager.mu.Unlock()
				targetManager.mu.Lock()
				targetManager.beginErr = nil
				targetManager.mu.Unlock()
			}
			if test.wantSourceCleanupError || test.wantTargetCleanupError {
				if err := sourceClient.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				var retried protocol.SyncWorkspaceResult
				if err := sourceClient.Call(
					ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &retried,
				); err != nil || retried.Workspace == nil ||
					retried.Workspace.Strategy != protocol.WorkspaceStrategyThin {
					t.Fatalf("same-sync retry after cleanup fencing = %#v, %v", retried, err)
				}
			}
			waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
		})
	}
}

func waitForWorkspaceSessionReplacement(t *testing.T, server *Server, deviceID string, initial *session) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current := server.connection(deviceID)
		if current != nil && current != initial {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workspace connector did not replace session %p: current=%p", initial, current)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForWorkspaceTargetReplacement(
	t *testing.T,
	server *Server,
	deviceID string,
	initial *session,
	manager *transferWorkspacePeer,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.Lock()
		cleanupCount := manager.cleanupCount
		manager.mu.Unlock()
		current := server.connection(deviceID)
		if cleanupCount > 0 && current != nil && current != initial {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("target was not cleaned and reconnected: cleanup=%d current=%p initial=%p", cleanupCount, current, initial)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
