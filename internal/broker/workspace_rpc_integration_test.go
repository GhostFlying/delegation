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

	deviceID        string
	manifest        protocol.WorkspaceManifest
	prepareErr      error
	prepareStarted  chan struct{}
	prepareCanceled chan struct{}

	inspections  []connector.WorkspaceInspectRequest
	preparations []connector.WorkspacePrepareRequest
	spawns       []connector.WorkerSpawnRequest
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
	return protocol.PrepareWorkspaceResult{
		WorkspaceID: request.Params.WorkspaceID,
		Outcome:     protocol.WorkspacePrepareDirect, Strategy: protocol.WorkspaceStrategyDirect,
		ManifestHash: hash, Warnings: append([]string{}, request.Params.Manifest.Warnings...),
	}, nil
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
	targetManager := &recordingWorkspacePeer{
		deviceID: agentRPCTargetID, prepareStarted: prepareStarted, prepareCanceled: prepareCanceled,
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
	if synchronized.Outcome != protocol.WorkspacePrepareDirect || synchronized.Workspace == nil ||
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
