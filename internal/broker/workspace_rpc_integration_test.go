package broker

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	workspaceRPCSyncID       = "123e4567-e89b-42d3-a456-426614174140"
	workspaceRPCFailedSyncID = "123e4567-e89b-42d3-a456-426614174141"
	workspaceRPCSpawnID      = "123e4567-e89b-42d3-a456-426614174142"
)

type recordingWorkspacePeer struct {
	mu sync.Mutex

	deviceID         string
	manifest         protocol.WorkspaceManifest
	prepareErr       error
	prepareStarted   chan struct{}
	prepareCanceled  chan struct{}
	preparePublished chan struct{}
	prepareRelease   chan struct{}
	cancelObserved   chan connector.WorkspaceTransferControlRequest
	prepared         *protocol.PrepareWorkspaceResult
	publishCount     int

	inspections           []connector.WorkspaceInspectRequest
	preparations          []connector.WorkspacePrepareRequest
	transferCancellations []connector.WorkspaceTransferControlRequest
	spawns                []connector.WorkerSpawnRequest
}

func (p *recordingWorkspacePeer) InspectWorkspace(
	_ context.Context,
	request connector.WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inspections = append(p.inspections, request)
	return protocol.InspectWorkspaceResult{
		SyncID: request.Params.SyncID, Manifest: p.manifest,
	}, nil
}

func (p *recordingWorkspacePeer) PrepareWorkspace(
	ctx context.Context,
	request connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	p.mu.Lock()
	p.preparations = append(p.preparations, request)
	prepareErr := p.prepareErr
	prepareStarted := p.prepareStarted
	prepareCanceled := p.prepareCanceled
	if p.prepared != nil {
		prepared := *p.prepared
		p.mu.Unlock()
		return prepared, nil
	}
	p.mu.Unlock()
	if prepareStarted != nil {
		prepareStarted <- struct{}{}
		<-ctx.Done()
		if prepareCanceled != nil {
			prepareCanceled <- struct{}{}
		}
		return protocol.PrepareWorkspaceResult{}, ctx.Err()
	}
	if prepareErr != nil {
		return protocol.PrepareWorkspaceResult{}, prepareErr
	}
	hash, err := protocol.WorkspaceManifestHash(request.Params.Manifest)
	if err != nil {
		return protocol.PrepareWorkspaceResult{}, err
	}
	result := protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID,
		Outcome:     protocol.WorkspacePrepareReady, Strategy: protocol.WorkspaceStrategyDirect,
		ManifestHash: hash, Warnings: append([]string{}, request.Params.Manifest.Warnings...),
	}
	p.mu.Lock()
	p.prepared = &result
	p.publishCount++
	preparePublished := p.preparePublished
	prepareRelease := p.prepareRelease
	p.preparePublished = nil
	p.prepareRelease = nil
	p.mu.Unlock()
	if preparePublished != nil {
		close(preparePublished)
		<-prepareRelease
	}
	return result, nil
}

func (p *recordingWorkspacePeer) CreateWorkspaceTransfer(
	context.Context,
	connector.WorkspaceCreateTransferRequest,
) (protocol.CreateWorkspaceTransferResult, error) {
	return protocol.CreateWorkspaceTransferResult{}, errors.New("not used")
}

func (p *recordingWorkspacePeer) ReadWorkspaceArtifact(
	context.Context,
	connector.WorkspaceReadArtifactRequest,
) (protocol.ReadWorkspaceArtifactResult, error) {
	return protocol.ReadWorkspaceArtifactResult{}, errors.New("not used")
}

func (p *recordingWorkspacePeer) BeginWorkspaceTransfer(
	context.Context,
	connector.WorkspaceBeginTransferRequest,
) (protocol.BeginWorkspaceTransferResult, error) {
	return protocol.BeginWorkspaceTransferResult{}, errors.New("not used")
}

func (p *recordingWorkspacePeer) WriteWorkspaceArtifact(
	context.Context,
	connector.WorkspaceWriteArtifactRequest,
) (protocol.WriteWorkspaceArtifactResult, error) {
	return protocol.WriteWorkspaceArtifactResult{}, errors.New("not used")
}

func (p *recordingWorkspacePeer) FinishWorkspaceTransfer(
	context.Context,
	connector.WorkspaceTransferControlRequest,
) (protocol.FinishWorkspaceTransferResult, error) {
	return protocol.FinishWorkspaceTransferResult{}, errors.New("not used")
}

func (p *recordingWorkspacePeer) CancelWorkspaceTransfer(
	_ context.Context,
	request connector.WorkspaceTransferControlRequest,
) (protocol.CancelWorkspaceTransferResult, error) {
	p.mu.Lock()
	p.transferCancellations = append(p.transferCancellations, request)
	cancelObserved := p.cancelObserved
	p.mu.Unlock()
	if cancelObserved != nil {
		cancelObserved <- request
	}
	return protocol.CancelWorkspaceTransferResult{TransferID: request.Params.TransferID}, nil
}

func (p *recordingWorkspacePeer) CleanupWorkspaceTransfers(context.Context) error {
	return nil
}

func TestWorkspaceRPCPrepareFailureCancelsExactProvisionalTarget(t *testing.T) {
	tests := []struct {
		name          string
		prepareErr    error
		cancelRequest bool
	}{
		{name: "target returns error", prepareErr: errors.New("target preparation failed")},
		{name: "root request is canceled", cancelRequest: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
			gitURL := "ssh://git@example.invalid/repository.git"
			sourceManager := &recordingWorkspacePeer{
				deviceID: brokerTestDeviceID,
				manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			}
			cancelObserved := make(chan connector.WorkspaceTransferControlRequest, 1)
			targetManager := &recordingWorkspacePeer{
				deviceID: agentRPCTargetID, prepareErr: test.prepareErr,
				cancelObserved: cancelObserved,
			}
			if test.cancelRequest {
				targetManager.prepareStarted = make(chan struct{}, 1)
				targetManager.prepareCanceled = make(chan struct{}, 1)
			}
			sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
			startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var root protocol.EnsureRootTreeResult
			if err := sourceClient.Call(
				ctx, protocol.MethodEnsureRootTree, "", nil,
				protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID}, &root,
			); err != nil {
				t.Fatal(err)
			}
			source := root.Principal.Identity()
			callContext, cancelCall := context.WithCancel(ctx)
			defer cancelCall()
			params := protocol.SyncWorkspaceParams{
				SyncID: workspaceRPCFailedSyncID, TargetDeviceID: agentRPCTargetID,
				GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
			}
			callDone := make(chan error, 1)
			go func() {
				var result protocol.SyncWorkspaceResult
				callDone <- sourceClient.Call(
					callContext, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &result,
				)
			}()
			if test.cancelRequest {
				select {
				case <-targetManager.prepareStarted:
				case <-time.After(2 * time.Second):
					t.Fatal("target workspace preparation did not start")
				}
				cancelCall()
			}
			select {
			case err := <-callDone:
				if err == nil {
					t.Fatal("workspace sync unexpectedly succeeded")
				}
				if test.cancelRequest && !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled workspace sync = %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("failed workspace sync did not return")
			}

			var cleanup connector.WorkspaceTransferControlRequest
			select {
			case cleanup = <-cancelObserved:
			case <-time.After(2 * time.Second):
				t.Fatal("broker did not cancel the provisional target workspace")
			}
			wantCleanup := connector.WorkspaceTransferControlRequest{
				TreeID: root.Tree.TreeID, Source: source,
				Params: protocol.WorkspaceTransferControlParams{
					WorkspaceID: params.SyncID, TransferID: params.SyncID,
					SourceAgentID: source.AgentID, SourceDeviceID: source.DeviceID,
				},
			}
			if !reflect.DeepEqual(cleanup, wantCleanup) {
				t.Fatalf("provisional target cleanup = %#v, want %#v", cleanup, wantCleanup)
			}
			targetManager.mu.Lock()
			cancellations := append(
				[]connector.WorkspaceTransferControlRequest(nil),
				targetManager.transferCancellations...,
			)
			targetManager.mu.Unlock()
			if !reflect.DeepEqual(cancellations, []connector.WorkspaceTransferControlRequest{wantCleanup}) {
				t.Fatalf("target cleanup calls = %#v, want exactly the provisional cleanup", cancellations)
			}
			waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
		})
	}
}

func TestWorkspaceRPCDirectPrepareAcknowledgementLossRetriesSameSync(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	sourceManager := &recordingWorkspacePeer{
		deviceID: brokerTestDeviceID,
		manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	preparePublished := make(chan struct{})
	prepareRelease := make(chan struct{})
	targetManager := &recordingWorkspacePeer{
		deviceID: agentRPCTargetID, preparePublished: preparePublished, prepareRelease: prepareRelease,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	targetClient := startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)
	initialTarget := waitForWorkspaceInitialSession(t, harness.server, agentRPCTargetID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	params := protocol.SyncWorkspaceParams{
		SyncID: workspaceRPCFailedSyncID, TargetDeviceID: agentRPCTargetID,
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
	case <-preparePublished:
	case <-time.After(2 * time.Second):
		t.Fatal("target did not publish the direct workspace")
	}
	_ = initialTarget.connection.CloseNow()
	close(prepareRelease)
	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("direct prepare with lost wire acknowledgement unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("direct prepare acknowledgement loss did not return")
	}
	waitForWorkspaceSessionReplacement(t, harness.server, agentRPCTargetID, initialTarget)
	if err := targetClient.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	var retried protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &retried,
	); err != nil || retried.Workspace == nil || retried.Workspace.Strategy != protocol.WorkspaceStrategyDirect {
		t.Fatalf("same-sync direct retry = %#v, %v", retried, err)
	}
	targetManager.mu.Lock()
	preparations := len(targetManager.preparations)
	publishCount := targetManager.publishCount
	cancelCount := len(targetManager.transferCancellations)
	targetManager.mu.Unlock()
	if preparations != 2 || publishCount != 1 || cancelCount != 0 {
		t.Fatalf(
			"direct retry = %d prepare calls, %d publications, %d cancellations; want 2/1/0",
			preparations, publishCount, cancelCount,
		)
	}
}

func TestWorkspaceRPCCancellationStopsTargetPeerOperation(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	sourceManager := &recordingWorkspacePeer{
		deviceID: brokerTestDeviceID,
		manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	prepareStarted := make(chan struct{}, 1)
	prepareCanceled := make(chan struct{}, 1)
	cancelObserved := make(chan connector.WorkspaceTransferControlRequest, 1)
	targetManager := &recordingWorkspacePeer{
		deviceID: agentRPCTargetID, prepareStarted: prepareStarted, prepareCanceled: prepareCanceled,
		cancelObserved: cancelObserved,
	}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	callContext, cancelCall := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	sourcePath := filepath.Join(t.TempDir(), "trusted", "source")
	go func() {
		var result protocol.SyncWorkspaceResult
		callDone <- sourceClient.Call(
			callContext, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source,
			protocol.SyncWorkspaceParams{
				SyncID: workspaceRPCFailedSyncID, TargetDeviceID: agentRPCTargetID,
				GitURL: gitURL, SourcePath: sourcePath,
			},
			&result,
		)
	}()
	select {
	case <-prepareStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("target workspace preparation did not start")
	}
	cancelCall()
	select {
	case err := <-callDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled workspace sync = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled workspace sync did not return")
	}
	select {
	case <-prepareCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("target peer operation was not canceled")
	}
	select {
	case <-cancelObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not finish provisional target cleanup")
	}
	waitForWorkspaceCleanupDrain(t, harness.server, brokerTestDeviceID, agentRPCTargetID)
}

func waitForWorkspaceCleanupDrain(t *testing.T, server *Server, sourceDeviceID, targetDeviceID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		sourceAsync := 0
		if source := server.connection(sourceDeviceID); source != nil {
			source.asyncMu.Lock()
			sourceAsync = len(source.asyncCancels)
			source.asyncMu.Unlock()
		}
		targetPending := 0
		if target := server.connection(targetDeviceID); target != nil {
			target.pendingMu.Lock()
			targetPending = len(target.pending)
			target.pendingMu.Unlock()
		}
		if sourceAsync == 0 && targetPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"workspace cleanup did not drain: source async=%d, target pending=%d",
				sourceAsync, targetPending,
			)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *recordingWorkspacePeer) SpawnWorker(
	_ context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spawns = append(p.spawns, request)
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID,
		Principal: control.NewWorkerPrincipal(
			brokerTestControllerID, request.TreeID, request.Params.AgentID,
			request.Source.AgentID, p.deviceID,
		).Identity(),
		Outcome: protocol.AgentSpawnOutcomeStarted,
	}, nil
}

func (p *recordingWorkspacePeer) snapshot() (
	[]connector.WorkspaceInspectRequest,
	[]connector.WorkspacePrepareRequest,
	[]connector.WorkerSpawnRequest,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]connector.WorkspaceInspectRequest(nil), p.inspections...),
		append([]connector.WorkspacePrepareRequest(nil), p.preparations...),
		append([]connector.WorkerSpawnRequest(nil), p.spawns...)
}

func TestWorkspaceRPCRoutesPinnedDirectWorkspaceAndSpawn(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "ssh://git@example.invalid/repository.git"
	manifest := workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	sourceManager := &recordingWorkspacePeer{deviceID: brokerTestDeviceID, manifest: manifest}
	targetManager := &recordingWorkspacePeer{deviceID: agentRPCTargetID}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceManager)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetManager)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	params := protocol.SyncWorkspaceParams{
		SyncID: workspaceRPCSyncID, TargetDeviceID: agentRPCTargetID,
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
	}
	var synchronized protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &synchronized,
	); err != nil {
		t.Fatal(err)
	}
	if synchronized.Outcome != protocol.WorkspacePrepareReady || synchronized.Workspace == nil ||
		synchronized.Workspace.WorkspaceID != workspaceRPCSyncID {
		t.Fatalf("workspace sync = %#v", synchronized)
	}
	var repeated protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &repeated,
	); err != nil || !reflect.DeepEqual(repeated, synchronized) {
		t.Fatalf("idempotent workspace sync = %#v, %v", repeated, err)
	}

	spawn := protocol.SpawnAgentParams{
		SpawnID: workspaceRPCSpawnID, TargetDeviceID: agentRPCTargetID,
		TaskName: "workspace_build", Message: "validate the synchronized source",
		WorkspaceID: workspaceRPCSyncID,
	}
	var spawned protocol.SpawnAgentResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, spawn, &spawned,
	); err != nil {
		t.Fatal(err)
	}
	if spawned.Agent.WorkspaceID != workspaceRPCSyncID || spawned.Outcome != protocol.AgentSpawnOutcomeStarted {
		t.Fatalf("workspace spawn = %#v", spawned)
	}

	sourceInspections, sourcePreparations, sourceSpawns := sourceManager.snapshot()
	targetInspections, targetPreparations, targetSpawns := targetManager.snapshot()
	if len(sourceInspections) != 1 || len(sourcePreparations) != 0 || len(sourceSpawns) != 0 ||
		len(targetInspections) != 0 || len(targetPreparations) != 1 || len(targetSpawns) != 1 {
		t.Fatalf(
			"source calls = inspect %d prepare %d spawn %d; target calls = inspect %d prepare %d spawn %d",
			len(sourceInspections), len(sourcePreparations), len(sourceSpawns),
			len(targetInspections), len(targetPreparations), len(targetSpawns),
		)
	}
	if sourceInspections[0].Source != source || sourceInspections[0].Params.SourcePath != params.SourcePath ||
		targetPreparations[0].Source != source || !reflect.DeepEqual(targetPreparations[0].Params.Manifest, manifest) ||
		targetSpawns[0].Params.WorkspaceID != workspaceRPCSyncID {
		t.Fatalf("routed calls = %#v, %#v, %#v", sourceInspections[0], targetPreparations[0], targetSpawns[0])
	}
}

func TestWorkspaceRPCRetryRejectsChangedPinnedSource(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	gitURL := "https://example.invalid/repository.git"
	sourceManager := &recordingWorkspacePeer{
		deviceID: brokerTestDeviceID,
		manifest: workspaceRPCManifest(gitURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	targetManager := &recordingWorkspacePeer{
		deviceID: agentRPCTargetID, prepareErr: errors.New("target failed before preparation"),
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
		SyncID: workspaceRPCFailedSyncID, TargetDeviceID: agentRPCTargetID,
		GitURL: gitURL, SourcePath: filepath.Join(t.TempDir(), "trusted", "source"),
	}
	var result protocol.SyncWorkspaceResult
	if err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &result,
	); err == nil {
		t.Fatal("first sync unexpectedly succeeded")
	}
	sourceManager.mu.Lock()
	sourceManager.manifest = workspaceRPCManifest(gitURL, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	sourceManager.mu.Unlock()
	err := sourceClient.Call(
		ctx, protocol.MethodSyncWorkspace, root.Tree.TreeID, &source, params, &result,
	)
	var rpcErr *connector.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != protocol.ErrorConflict {
		t.Fatalf("changed retry = %v, want conflict", err)
	}
	inspections, _, _ := sourceManager.snapshot()
	_, preparations, _ := targetManager.snapshot()
	if len(inspections) != 2 || len(preparations) != 1 {
		t.Fatalf("retry calls = %d inspections, %d preparations", len(inspections), len(preparations))
	}
}

func workspaceRPCManifest(gitURL, head string) protocol.WorkspaceManifest {
	return protocol.WorkspaceManifest{
		GitURL: gitURL, HeadOID: head, ObjectFormat: "sha1",
		WorkingDirectory: "nested", Clean: true,
		SourceSnapshotHash: head + "000000000000000000000000",
		Warnings:           []string{},
	}
}
