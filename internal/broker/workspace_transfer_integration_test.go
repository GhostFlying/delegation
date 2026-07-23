package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	requireBundle   bool
	requireOverlay  bool
	fullFallback    bool
	artifact        []byte
	artifacts       map[protocol.WorkspaceArtifactKind][]byte
	transfer        protocol.WorkspaceTransferManifest
	received        map[protocol.WorkspaceArtifactKind][]byte
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
	writeBlockKind  protocol.WorkspaceArtifactKind
	readObserved    chan struct{}
	readRelease     chan struct{}
	readBlockKind   protocol.WorkspaceArtifactKind
	readBlockOffset int64
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
	bundleRequired := !p.requireOverlay || p.requireBundle
	basisOIDs := []string{"1111111111111111111111111111111111111111"}
	if !bundleRequired || p.fullFallback {
		basisOIDs = nil
	}
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID,
		Outcome:     protocol.WorkspacePrepareTransferRequired, ManifestHash: hash,
		Warnings:       append([]string(nil), request.Params.Manifest.Warnings...),
		BasisOIDs:      basisOIDs,
		BundleRequired: bundleRequired, OverlayRequired: p.requireOverlay,
	}, nil
}

func (p *transferWorkspacePeer) CreateWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	manifestHash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	strategy := protocol.WorkspaceStrategyDirect
	if request.Params.BundleRequired {
		strategy = protocol.WorkspaceStrategyFull
		if len(request.Params.BasisOIDs) != 0 {
			strategy = protocol.WorkspaceStrategyThin
		}
	}
	warnings, err := protocol.WorkspaceWarningsForStrategy(request.Params.Manifest.Warnings, strategy)
	if err != nil {
		return protocol.CreateWorkspaceTransferResult{}, err
	}
	descriptors := make([]protocol.WorkspaceArtifactDescriptor, 0, 2)
	for _, kind := range []protocol.WorkspaceArtifactKind{
		protocol.WorkspaceArtifactBundle, protocol.WorkspaceArtifactOverlay,
	} {
		required := kind == protocol.WorkspaceArtifactBundle && request.Params.BundleRequired ||
			kind == protocol.WorkspaceArtifactOverlay && request.Params.OverlayRequired
		if !required {
			continue
		}
		data := p.artifacts[kind]
		if len(data) == 0 && len(p.artifact) != 0 &&
			(!request.Params.BundleRequired || kind == protocol.WorkspaceArtifactBundle) {
			data = p.artifact
		}
		if len(data) == 0 {
			return protocol.CreateWorkspaceTransferResult{}, fmt.Errorf("source %s artifact is unavailable", kind)
		}
		digest := sha256.Sum256(data)
		descriptors = append(descriptors, protocol.WorkspaceArtifactDescriptor{
			Kind: kind, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]),
		})
	}
	transfer := protocol.WorkspaceTransferManifest{
		TransferID: request.Params.TransferID, WorkspaceID: request.Params.WorkspaceID,
		Strategy: strategy, ManifestHash: manifestHash, Artifacts: descriptors, Warnings: warnings,
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
	p.readRequests = append(p.readRequests, request)
	data := p.artifacts[request.Params.Kind]
	if len(data) == 0 && len(p.artifact) != 0 && len(p.transfer.Artifacts) != 0 &&
		request.Params.Kind == p.transfer.Artifacts[0].Kind {
		data = p.artifact
	}
	if request.Params.TransferID != p.transfer.TransferID ||
		request.Params.Offset < 0 || request.Params.Offset >= int64(len(data)) {
		p.mu.Unlock()
		return protocol.ReadWorkspaceArtifactResult{}, errors.New("invalid source artifact read")
	}
	end := min(request.Params.Offset+int64(request.Params.Limit), int64(len(data)))
	if p.shortReads {
		end = request.Params.Offset + 1
	}
	chunk := append([]byte(nil), data[request.Params.Offset:end]...)
	readObserved := p.readObserved
	readRelease := p.readRelease
	if request.Params.Kind != p.readBlockKind || request.Params.Offset != p.readBlockOffset {
		readObserved = nil
		readRelease = nil
	} else {
		p.readObserved = nil
		p.readRelease = nil
	}
	p.mu.Unlock()
	if readObserved != nil {
		close(readObserved)
		<-readRelease
	}
	return protocol.ReadWorkspaceArtifactResult{
		TransferID: request.Params.TransferID, Kind: request.Params.Kind,
		Offset: request.Params.Offset, Data: chunk, NextOffset: end,
	}, nil
}

func (p *transferWorkspacePeer) BeginWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transfer = request.Params.Transfer
	p.received = make(map[protocol.WorkspaceArtifactKind][]byte)
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
		request.Params.Offset != int64(len(p.received[request.Params.Kind])) {
		p.mu.Unlock()
		return protocol.WriteWorkspaceArtifactResult{}, errors.New("out-of-sequence target artifact write")
	}
	p.received[request.Params.Kind] = append(p.received[request.Params.Kind], request.Params.Data...)
	nextOffset := int64(len(p.received[request.Params.Kind]))
	writeApplied := p.writeApplied
	writeRelease := p.writeRelease
	if p.writeBlockKind != "" && request.Params.Kind != p.writeBlockKind {
		writeApplied = nil
		writeRelease = nil
	} else {
		p.writeApplied = nil
		p.writeRelease = nil
	}
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
	for _, descriptor := range p.transfer.Artifacts {
		data := p.received[descriptor.Kind]
		digest := sha256.Sum256(data)
		if int64(len(data)) != descriptor.Size || hex.EncodeToString(digest[:]) != descriptor.SHA256 {
			p.mu.Unlock()
			return protocol.FinishWorkspaceTransferResult{}, fmt.Errorf("target %s transfer digest mismatch", descriptor.Kind)
		}
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
	received := append([]byte(nil), targetManager.received[protocol.WorkspaceArtifactBundle]...)
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
			initialTarget := waitForWorkspaceInitialSession(t, harness.server, agentRPCTargetID)
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

func TestWorkspaceRPCDirtyBundleOverlayReconnectsAtArtifactBoundaries(t *testing.T) {
	for _, test := range []struct {
		name              string
		blockSourceRead   bool
		wantOverlayBefore int
		fullFallback      bool
		strategy          protocol.WorkspaceStrategy
	}{
		{name: "thin bundle to overlay boundary", blockSourceRead: true, strategy: protocol.WorkspaceStrategyThin},
		{
			name: "full bundle overlay write acknowledgement", fullFallback: true,
			wantOverlayBefore: protocol.WorkspaceArtifactChunkBytes, strategy: protocol.WorkspaceStrategyFull,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
			gitURL := "ssh://git@example.invalid/repository.git"
			manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			manifest.Clean = false
			bundle := bytes.Repeat([]byte("b"), protocol.WorkspaceArtifactChunkBytes+17)
			overlay := bytes.Repeat([]byte("o"), protocol.WorkspaceArtifactChunkBytes+31)
			sourceManager := &transferWorkspacePeer{
				recordingWorkspacePeer: recordingWorkspacePeer{
					deviceID: brokerTestDeviceID, manifest: manifest,
				},
				artifacts: map[protocol.WorkspaceArtifactKind][]byte{
					protocol.WorkspaceArtifactBundle: bundle, protocol.WorkspaceArtifactOverlay: overlay,
				},
			}
			targetManager := &transferWorkspacePeer{
				recordingWorkspacePeer: recordingWorkspacePeer{deviceID: agentRPCTargetID},
				requireTransfer:        true, requireBundle: true, requireOverlay: true,
				fullFallback: test.fullFallback,
			}
			observed := make(chan struct{})
			release := make(chan struct{})
			if test.blockSourceRead {
				sourceManager.readObserved = observed
				sourceManager.readRelease = release
				sourceManager.readBlockKind = protocol.WorkspaceArtifactOverlay
			} else {
				targetManager.writeApplied = observed
				targetManager.writeRelease = release
				targetManager.writeBlockKind = protocol.WorkspaceArtifactOverlay
			}
			sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
			targetClient := startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
			initialTarget := waitForWorkspaceInitialSession(t, harness.server, agentRPCTargetID)
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
			callDone := make(chan error, 1)
			go func() {
				callDone <- sourceClient.Call(
					ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params,
					&protocol.SyncWorkspaceResult{},
				)
			}()
			select {
			case <-observed:
			case <-time.After(2 * time.Second):
				t.Fatal("dirty transfer did not reach the requested artifact boundary")
			}
			targetManager.mu.Lock()
			receivedBundle := append([]byte(nil), targetManager.received[protocol.WorkspaceArtifactBundle]...)
			receivedOverlay := len(targetManager.received[protocol.WorkspaceArtifactOverlay])
			targetManager.mu.Unlock()
			if !bytes.Equal(receivedBundle, bundle) || receivedOverlay != test.wantOverlayBefore {
				t.Fatalf(
					"boundary receipt = bundle %d/%d, overlay %d/%d",
					len(receivedBundle), len(bundle), receivedOverlay, test.wantOverlayBefore,
				)
			}
			_ = initialTarget.connection.CloseNow()
			close(release)
			select {
			case err := <-callDone:
				if err == nil {
					t.Fatal("dirty transfer with a lost connection unexpectedly succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("dirty transfer connection loss did not return")
			}
			select {
			case <-harness.reported:
			case <-time.After(2 * time.Second):
				t.Fatal("dirty transfer connection loss did not report cleanup fencing")
			}
			waitForWorkspaceSessionReplacement(t, harness.server, agentRPCTargetID, initialTarget)
			if err := targetClient.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
			var retried protocol.SyncWorkspaceResult
			if err := sourceClient.Call(
				ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &retried,
			); err != nil || retried.Workspace == nil || retried.Workspace.Strategy != test.strategy {
				t.Fatalf("dirty transfer retry = %#v, %v", retried, err)
			}
			wantFullWarning := test.strategy == protocol.WorkspaceStrategyFull
			if slices.Contains(retried.Workspace.Warnings, protocol.WorkspaceWarningFullHistoryFallback) != wantFullWarning {
				t.Fatalf("dirty transfer warnings = %v", retried.Workspace.Warnings)
			}
			targetManager.mu.Lock()
			gotBundle := append([]byte(nil), targetManager.received[protocol.WorkspaceArtifactBundle]...)
			gotOverlay := append([]byte(nil), targetManager.received[protocol.WorkspaceArtifactOverlay]...)
			targetManager.mu.Unlock()
			if !bytes.Equal(gotBundle, bundle) || !bytes.Equal(gotOverlay, overlay) {
				t.Fatalf("retried dirty transfer received bundle/overlay = %d/%d", len(gotBundle), len(gotOverlay))
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
	initialSource := waitForWorkspaceInitialSession(t, harness.server, brokerTestDeviceID)
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

func TestWorkspaceRPCOverlayCreationFailureCleansBothPeers(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	manifest.Clean = false
	sourceManager := &transferWorkspacePeer{
		recordingWorkspacePeer: recordingWorkspacePeer{
			deviceID: brokerTestDeviceID, manifest: manifest,
		},
		artifact:  []byte("unused-overlay-placeholder"),
		createErr: errors.New("simulated workspace overlay creation failure"),
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
		t.Fatal("workspace synchronization unexpectedly succeeded after overlay creation failure")
	}
	select {
	case canceled := <-targetManager.cancelObserved:
		if canceled.Params.WorkspaceID != params.SyncID || canceled.Params.TransferID != params.SyncID {
			t.Fatalf("overlay target provisional cleanup = %#v", canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("overlay target provisional workspace was not cleaned")
	}
	sourceManager.mu.Lock()
	sourceCancels := sourceManager.cancelCount
	sourceManager.mu.Unlock()
	targetManager.mu.Lock()
	targetCancels := targetManager.cancelCount
	targetManager.mu.Unlock()
	if sourceCancels != 1 || targetCancels != 1 {
		t.Fatalf("overlay failure cleanup = source %d target %d, want 1/1", sourceCancels, targetCancels)
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
			initialSourceSession := waitForWorkspaceInitialSession(t, harness.server, brokerTestDeviceID)
			initialTargetSession := waitForWorkspaceInitialSession(t, harness.server, agentRPCTargetID)
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

func waitForWorkspaceInitialSession(t *testing.T, server *Server, deviceID string) *session {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if current := server.connection(deviceID); current != nil {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("workspace connector %s was not registered", deviceID)
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
