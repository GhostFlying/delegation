package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type publicationDuringSpawn struct {
	worker control.PrincipalIdentity
	params protocol.PublishResultPackageParams
	err    error
}

type publishingSpawnPeer struct {
	client  *connector.Client
	attempt chan publicationDuringSpawn
}

func (p *publishingSpawnPeer) SpawnWorker(
	ctx context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	worker := control.NewWorkerPrincipal(
		brokerTestControllerID, request.TreeID, request.Params.AgentID,
		request.Source.AgentID, agentRPCTargetID,
	).Identity()
	// Finish and publish before returning SpawnWorker's successful reply. The
	// broker cannot have committed MarkAgentSpawnStarted at this point.
	var synced protocol.SyncWorkerLifecycleResult
	if err := p.client.Call(ctx, protocol.MethodSyncWorkerLifecycle, "", nil,
		protocol.SyncWorkerLifecycleParams{
			BaseRevision: 0, ThroughRevision: 1, Complete: true,
			Workers: []protocol.WorkerLifecycleSnapshot{{
				TreeID: worker.TreeID, AgentID: worker.AgentID, Revision: 1,
				Phase:         protocol.WorkerLifecycleFinalizing,
				CodexThreadID: resultRelayThreadID, ActiveTurnID: resultRelayTurnID,
			}},
		}, &synced,
	); err != nil {
		return protocol.SpawnWorkerResult{}, err
	}
	manifest := protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: resultRelayPackageID,
		ControllerID: worker.ControllerID, TreeID: worker.TreeID,
		SourceAgentID: worker.AgentID, SourceDeviceID: worker.DeviceID,
		ManagedThreadID: resultRelayThreadID, TurnID: resultRelayTurnID,
		LifecycleRevision: 1, CapturedAt: 30,
		Terminal: protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutCaptureFailed, FailureCode: "rollout_unavailable",
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status: protocol.ResultWorkspaceNotManaged, BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{},
	}
	encoded, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		return protocol.SpawnWorkerResult{}, err
	}
	params := protocol.PublishResultPackageParams{Metadata: protocol.ResultPackageMetadata{
		Manifest: encoded, ManifestDescriptor: descriptor,
	}}
	var published protocol.PublishResultPackageResult
	err = p.client.Call(ctx, protocol.MethodPublishResultPackage, worker.TreeID, &worker, params, &published)
	p.attempt <- publicationDuringSpawn{worker: worker, params: params, err: err}
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID, Principal: worker, Outcome: protocol.AgentSpawnOutcomeStarted,
	}, nil
}

func TestResultPackagePublicationBeforeSpawnReplyIsRetryable(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	rootClient := startAgentRPCConnector(t, harness, brokerTestDeviceID,
		&basicDispatchSpawner{deviceID: brokerTestDeviceID})
	peer := &publishingSpawnPeer{attempt: make(chan publicationDuringSpawn, 1)}
	peer.client = startAgentRPCConnector(t, harness, agentRPCTargetID, peer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := rootClient.Call(ctx, protocol.MethodEnsureRootTree, "", nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID}, &root,
	); err != nil {
		t.Fatal(err)
	}
	principal := root.Principal.Identity()
	var spawned protocol.SpawnAgentResult
	if err := rootClient.Call(ctx, protocol.MethodSpawnAgent, root.Tree.TreeID, &principal,
		protocol.SpawnAgentParams{
			SpawnID: agentRPCSpawnID, TargetDeviceID: agentRPCTargetID,
			TaskName: "fast_result", Message: "complete the initial turn",
		}, &spawned,
	); err != nil || spawned.Outcome != protocol.AgentSpawnOutcomeStarted {
		t.Fatalf("spawn = %#v, error %v", spawned, err)
	}
	var attempt publicationDuringSpawn
	select {
	case attempt = <-peer.attempt:
	case <-ctx.Done():
		t.Fatal("target did not publish before its spawn reply")
	}
	var rpcError *connector.RPCError
	if !errors.As(attempt.err, &rpcError) || rpcError.Code != protocol.ErrorUnavailable ||
		rpcError.Message != "lifecycle_not_ready" {
		t.Fatalf("early publication = %v, want retryable lifecycle_not_ready", attempt.err)
	}
	if _, err := harness.registry.GetResultPackageForDelivery(
		ctx, attempt.worker.DeviceID, attempt.worker, resultRelayPackageID,
	); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pending spawn inserted result metadata: %v", err)
	}
	// The broker has now consumed the successful spawn reply. The unchanged
	// metadata is accepted only with both started and finalizing authority.
	if _, err := harness.registry.PublishResultPackage(
		ctx, attempt.worker.DeviceID, attempt.worker, attempt.params, time.Unix(40, 0),
	); err != nil {
		t.Fatalf("publication after spawn reply: %v", err)
	}
}
