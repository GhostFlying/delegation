package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

const (
	agentRPCThreadID       = "123e4567-e89b-42d3-a456-426614174130"
	agentRPCSpawnID        = "123e4567-e89b-42d3-a456-426614174131"
	agentRPCTargetID       = "123e4567-e89b-42d3-a456-426614174132"
	agentRPCRemoteThreadID = "123e4567-e89b-42d3-a456-426614174133"
	agentRPCRemoteSpawnID  = "123e4567-e89b-42d3-a456-426614174134"
	agentRPCMessageID      = "123e4567-e89b-42d3-a456-426614174135"
	agentRPCBusyThreadID   = "123e4567-e89b-42d3-a456-426614174136"
	agentRPCBusySpawnID    = "123e4567-e89b-42d3-a456-426614174137"
)

type selfDispatchSpawner struct {
	client *connector.Client
	calls  atomic.Int32
}

type basicDispatchSpawner struct {
	deviceID string
	outcome  protocol.AgentSpawnOutcome
	calls    atomic.Int32
}

type busyThenStartedSpawner struct {
	mu        sync.Mutex
	deviceID  string
	agentIDs  []string
	callCount int
}

type agentRPCWorkerController struct{}

type agentRPCLifecycleSource struct{}

func (agentRPCLifecycleSource) WorkerRevision() uint64 { return 0 }

func (agentRPCLifecycleSource) WorkerLifecycleChanges() <-chan struct{} { return nil }

func (agentRPCLifecycleSource) ListWorkerLifecycles(
	context.Context,
) ([]protocol.WorkerLifecycleSnapshot, error) {
	return []protocol.WorkerLifecycleSnapshot{}, nil
}

func (agentRPCWorkerController) InspectWorkspace(
	context.Context,
	connector.WorkspaceInspectRequest,
) (protocol.InspectWorkspaceResult, error) {
	return protocol.InspectWorkspaceResult{}, errors.New("not used")
}

func (agentRPCWorkerController) PrepareWorkspace(
	context.Context,
	connector.WorkspacePrepareRequest,
) (protocol.PrepareWorkspaceResult, error) {
	return protocol.PrepareWorkspaceResult{}, errors.New("not used")
}

func (agentRPCWorkerController) SendWorker(
	_ context.Context,
	request connector.WorkerSendRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.MessageID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationSend,
		Outcome:     protocol.AgentOperationOutcomeQueued,
	}, nil
}

func (agentRPCWorkerController) FollowupWorker(
	_ context.Context,
	request connector.WorkerFollowupRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.OperationID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationFollowup,
		Outcome:     protocol.AgentOperationOutcomeStarted,
	}, nil
}

func (agentRPCWorkerController) InterruptWorker(
	_ context.Context,
	request connector.WorkerInterruptRequest,
) (protocol.WorkerOperationResult, error) {
	return protocol.WorkerOperationResult{
		OperationID: request.Params.OperationID,
		AgentID:     request.Params.AgentID,
		Action:      protocol.AgentOperationInterrupt,
		Outcome:     protocol.AgentOperationOutcomeInterrupted,
	}, nil
}

func (s *basicDispatchSpawner) SpawnWorker(
	_ context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	s.calls.Add(1)
	outcome := s.outcome
	if outcome == "" {
		outcome = protocol.AgentSpawnOutcomeStarted
	}
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID,
		Principal: control.NewWorkerPrincipal(
			brokerTestControllerID,
			request.TreeID,
			request.Params.AgentID,
			request.Source.AgentID,
			s.deviceID,
		).Identity(),
		Outcome: outcome,
	}, nil
}

func (s *busyThenStartedSpawner) SpawnWorker(
	_ context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCount++
	s.agentIDs = append(s.agentIDs, request.Params.AgentID)
	outcome := protocol.AgentSpawnOutcomeStarted
	if s.callCount == 1 {
		outcome = protocol.AgentSpawnOutcomeBusy
	}
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID,
		Principal: control.NewWorkerPrincipal(
			brokerTestControllerID,
			request.TreeID,
			request.Params.AgentID,
			request.Source.AgentID,
			s.deviceID,
		).Identity(),
		Outcome: outcome,
	}, nil
}

func (s *busyThenStartedSpawner) snapshot() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callCount, append([]string(nil), s.agentIDs...)
}

func (s *selfDispatchSpawner) SpawnWorker(
	ctx context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	s.calls.Add(1)
	heartbeatContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var heartbeat protocol.HeartbeatResult
	if err := s.client.Call(
		heartbeatContext, protocol.MethodHeartbeat, "", nil, protocol.Heartbeat{}, &heartbeat,
	); err != nil {
		return protocol.SpawnWorkerResult{}, err
	}
	return protocol.SpawnWorkerResult{
		SpawnID: request.Params.SpawnID,
		Principal: control.NewWorkerPrincipal(
			brokerTestControllerID,
			request.TreeID,
			request.Params.AgentID,
			request.Source.AgentID,
			brokerTestDeviceID,
		).Identity(),
		Outcome: protocol.AgentSpawnOutcomeStarted,
	}, nil
}

func TestAgentRPCSelfDispatchIsDurableIdempotentAndNonBlocking(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	spawner := &selfDispatchSpawner{}
	client, err := connector.New(connector.Options{
		BrokerURL:             strings.Replace(harness.httpServer.URL, "http://", "ws://", 1) + ConnectPath,
		ControllerID:          brokerTestControllerID,
		DeviceID:              brokerTestDeviceID,
		DeviceName:            "self-dispatch-peer",
		AuthMode:              config.AuthModeNone,
		RuntimeVersion:        "agent-rpc-test",
		OperatingSystem:       "linux",
		Architecture:          "amd64",
		ReconnectMin:          5 * time.Millisecond,
		ReconnectMax:          10 * time.Millisecond,
		WorkerSpawner:         spawner,
		WorkerController:      agentRPCWorkerController{},
		WorkerLifecycleSource: agentRPCLifecycleSource{},
		WorkspaceManager:      agentRPCWorkerController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	spawner.client = client
	runContext, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(runContext) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("connector run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("connector did not stop")
		}
	})
	readyContext, cancelReady := context.WithTimeout(context.Background(), 2*time.Second)
	if err := client.WaitReady(readyContext); err != nil {
		cancelReady()
		t.Fatal(err)
	}
	cancelReady()

	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := client.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	params := protocol.SpawnAgentParams{
		SpawnID:        agentRPCSpawnID,
		TargetDeviceID: brokerTestDeviceID,
		TaskName:       "self_build",
		Message:        "run a self-targeted isolated build",
	}
	source := root.Principal.Identity()
	var spawned protocol.SpawnAgentResult
	if err := client.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &spawned,
	); err != nil {
		t.Fatal(err)
	}
	if spawned.Agent.Status != protocol.AgentSpawnStarted ||
		spawned.Outcome != protocol.AgentSpawnOutcomeStarted ||
		spawned.Agent.Principal.ParentAgentID != root.Principal.AgentID ||
		spawned.Agent.Principal.DeviceID != brokerTestDeviceID || spawner.calls.Load() != 1 {
		t.Fatalf("self-target spawn = %#v, target calls %d", spawned, spawner.calls.Load())
	}
	var repeated protocol.SpawnAgentResult
	if err := client.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &repeated,
	); err != nil || repeated != spawned || spawner.calls.Load() != 1 {
		t.Fatalf("repeated spawn = %#v, target calls %d, error %v", repeated, spawner.calls.Load(), err)
	}
	var agents protocol.ListAgentsResult
	if err := client.Call(
		callContext,
		protocol.MethodListAgents,
		root.Tree.TreeID,
		&source,
		protocol.ListAgentsParams{Limit: protocol.MaximumAgentPage},
		&agents,
	); err != nil {
		t.Fatal(err)
	}
	if len(agents.Agents) != 1 || agents.Agents[0] != spawned.Agent {
		t.Fatalf("agent list = %#v", agents)
	}
	changed := params
	changed.Message = "changed semantic input"
	err = client.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, changed, &protocol.SpawnAgentResult{},
	)
	var rpcError *connector.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != protocol.ErrorConflict {
		t.Fatalf("changed spawn error = %v, want conflict", err)
	}
	worker := spawned.Agent.Principal
	err = client.Call(
		callContext,
		protocol.MethodListAgents,
		root.Tree.TreeID,
		&worker,
		protocol.ListAgentsParams{Limit: 1},
		&protocol.ListAgentsResult{},
	)
	if !errors.As(err, &rpcError) || rpcError.Code != protocol.ErrorForbidden {
		t.Fatalf("worker list error = %v, want forbidden", err)
	}
}

func TestAgentRPCRoutesToExplicitRemotePeer(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	sourceSpawner := &basicDispatchSpawner{deviceID: brokerTestDeviceID}
	targetSpawner := &basicDispatchSpawner{deviceID: agentRPCTargetID}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceSpawner)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetSpawner)

	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	var spawned protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodSpawnAgent,
		root.Tree.TreeID,
		&source,
		protocol.SpawnAgentParams{
			SpawnID:        agentRPCRemoteSpawnID,
			TargetDeviceID: agentRPCTargetID,
			TaskName:       "remote_build",
			Message:        "run the remote platform build",
		},
		&spawned,
	); err != nil {
		t.Fatal(err)
	}
	if spawned.Agent.Status != protocol.AgentSpawnStarted ||
		spawned.Outcome != protocol.AgentSpawnOutcomeStarted ||
		spawned.Agent.Principal.DeviceID != agentRPCTargetID ||
		targetSpawner.calls.Load() != 1 || sourceSpawner.calls.Load() != 0 {
		t.Fatalf(
			"remote spawn = %#v, source calls %d, target calls %d",
			spawned,
			sourceSpawner.calls.Load(),
			targetSpawner.calls.Load(),
		)
	}
}

func TestAgentRPCBusyAttemptRetriesOneDurablePrincipal(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	sourceSpawner := &basicDispatchSpawner{deviceID: brokerTestDeviceID}
	targetSpawner := &busyThenStartedSpawner{deviceID: agentRPCTargetID}
	sourceClient := startAgentRPCConnector(t, harness, brokerTestDeviceID, sourceSpawner)
	startAgentRPCConnector(t, harness, agentRPCTargetID, targetSpawner)

	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCBusyThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	params := protocol.SpawnAgentParams{
		SpawnID: agentRPCBusySpawnID, TargetDeviceID: agentRPCTargetID,
		TaskName: "busy_retry", Message: "retry this exact dispatch after capacity becomes available",
	}
	source := root.Principal.Identity()
	var busy protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &busy,
	); err != nil {
		t.Fatal(err)
	}
	if err := busy.Validate(); err != nil || busy.Outcome != protocol.AgentSpawnOutcomeBusy ||
		busy.Agent.Status != protocol.AgentSpawnPending {
		t.Fatalf("busy spawn = %#v, error %v", busy, err)
	}

	var started protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &started,
	); err != nil {
		t.Fatal(err)
	}
	if err := started.Validate(); err != nil || started.Outcome != protocol.AgentSpawnOutcomeStarted ||
		started.Agent.Status != protocol.AgentSpawnStarted ||
		started.Agent.Principal != busy.Agent.Principal || started.Agent.Sequence != busy.Agent.Sequence {
		t.Fatalf("started retry = %#v, busy %#v, error %v", started, busy, err)
	}

	var replayed protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &replayed,
	); err != nil || replayed != started {
		t.Fatalf("terminal spawn replay = %#v, error %v", replayed, err)
	}
	calls, agentIDs := targetSpawner.snapshot()
	if calls != 2 || len(agentIDs) != 2 || agentIDs[0] != busy.Agent.Principal.AgentID ||
		agentIDs[1] != busy.Agent.Principal.AgentID || sourceSpawner.calls.Load() != 0 {
		t.Fatalf(
			"busy target calls = %d, agent IDs %v, source calls %d",
			calls,
			agentIDs,
			sourceSpawner.calls.Load(),
		)
	}
	var agents protocol.ListAgentsResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodListAgents,
		root.Tree.TreeID,
		&source,
		protocol.ListAgentsParams{Limit: protocol.MaximumAgentPage},
		&agents,
	); err != nil {
		t.Fatal(err)
	}
	if len(agents.Agents) != 1 || agents.Agents[0] != started.Agent {
		t.Fatalf("busy retry durable agents = %#v", agents)
	}
}

func TestAgentRPCOfflineTargetReturnsIndeterminateReceipt(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	sourceClient := startAgentRPCConnector(
		t, harness, brokerTestDeviceID, &basicDispatchSpawner{deviceID: brokerTestDeviceID},
	)
	targetDescriptor := hello().Descriptor()
	targetDescriptor.DeviceID = agentRPCTargetID
	targetDescriptor.Name = "offline-target"
	if _, err := harness.registry.RegisterTrustedDevice(
		context.Background(), targetDescriptor, time.Unix(10, 0),
	); err != nil {
		t.Fatal(err)
	}

	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	params := protocol.SpawnAgentParams{
		SpawnID: agentRPCRemoteSpawnID, TargetDeviceID: agentRPCTargetID,
		TaskName: "offline_spawn", Message: "retain one receipt until the target reconnects",
	}
	source := root.Principal.Identity()
	var first protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &first,
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); err != nil || first.Outcome != protocol.AgentSpawnOutcomeIndeterminate ||
		first.Agent.Status != protocol.AgentSpawnPending {
		t.Fatalf("offline spawn = %#v, error %v", first, err)
	}
	var repeated protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &repeated,
	); err != nil || repeated != first {
		t.Fatalf("offline spawn replay = %#v, first %#v, error %v", repeated, first, err)
	}
	var agents protocol.ListAgentsResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodListAgents,
		root.Tree.TreeID,
		&source,
		protocol.ListAgentsParams{Limit: protocol.MaximumAgentPage},
		&agents,
	); err != nil {
		t.Fatal(err)
	}
	if len(agents.Agents) != 1 || agents.Agents[0] != first.Agent {
		t.Fatalf("offline durable agents = %#v", agents)
	}
}

func TestAgentRPCQueuesOfflineSendExactlyOnce(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	sourceClient := startAgentRPCConnector(
		t, harness, brokerTestDeviceID, &basicDispatchSpawner{deviceID: brokerTestDeviceID},
	)
	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	targetDescriptor := hello().Descriptor()
	targetDescriptor.DeviceID = agentRPCTargetID
	targetDescriptor.Name = "offline-target"
	if _, err := harness.registry.RegisterTrustedDevice(
		context.Background(), targetDescriptor, time.Unix(10, 0),
	); err != nil {
		t.Fatal(err)
	}
	receipt, err := harness.registry.BeginAgentSpawn(
		context.Background(),
		store.AgentSpawnIntent{
			Source:         root.Principal.Identity(),
			SpawnID:        agentRPCRemoteSpawnID,
			AgentID:        agentRPCRemoteSpawnID,
			TargetDeviceID: agentRPCTargetID,
			TaskName:       "offline_worker",
			PromptDigest:   sha256.Sum256([]byte("offline worker prompt")),
		},
		time.Unix(20, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = harness.registry.MarkAgentSpawnStarted(
		context.Background(),
		store.AgentSpawnKey{
			ControllerID:  receipt.Agent.Principal.ControllerID,
			TreeID:        receipt.Agent.Principal.TreeID,
			SourceAgentID: receipt.Agent.Principal.ParentAgentID,
			SpawnID:       agentRPCRemoteSpawnID,
		},
		time.Unix(21, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	params := protocol.SendAgentParams{
		AgentID:   receipt.Agent.Principal.AgentID,
		MessageID: agentRPCMessageID,
		Message:   "message for an offline managed worker",
	}
	source := root.Principal.Identity()
	var first protocol.AgentOperationResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSendAgent, root.Tree.TreeID, &source, params, &first,
	); err != nil {
		t.Fatal(err)
	}
	if first.Outcome != protocol.AgentOperationOutcomeQueued ||
		first.OperationID != params.MessageID || first.AgentID != params.AgentID {
		t.Fatalf("offline send result = %#v", first)
	}
	var replay protocol.AgentOperationResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSendAgent, root.Tree.TreeID, &source, params, &replay,
	); err != nil || replay != first {
		t.Fatalf("offline send replay = %#v, error %v", replay, err)
	}
	worker := control.NewWorkerPrincipal(
		receipt.Agent.Principal.ControllerID,
		receipt.Agent.Principal.TreeID,
		receipt.Agent.Principal.AgentID,
		receipt.Agent.Principal.ParentAgentID,
		receipt.Agent.Principal.DeviceID,
	)
	mailbox, err := harness.registry.ReadMailbox(context.Background(), worker, 0, 2)
	if err != nil || len(mailbox.Messages) != 1 ||
		mailbox.Messages[0].MessageID != params.MessageID ||
		mailbox.Messages[0].Message != params.Message {
		t.Fatalf("offline worker mailbox = %#v, error %v", mailbox, err)
	}
	changed := params
	changed.Message = "changed message"
	err = sourceClient.Call(
		callContext,
		protocol.MethodSendAgent,
		root.Tree.TreeID,
		&source,
		changed,
		&protocol.AgentOperationResult{},
	)
	var rpcError *connector.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != protocol.ErrorConflict {
		t.Fatalf("changed offline send error = %v, want conflict", err)
	}
}

func TestAgentRPCReauthenticatesTargetBeforeDispatch(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeToken, time.Hour)
	targetToken := tokenfile.Token{3}
	if err := harness.registry.CreateCredential(context.Background(), store.NewCredential(
		brokerTestControllerID,
		agentRPCTargetID,
		credential.MAC(harness.masterToken, targetToken),
		time.Unix(1, 0),
	)); err != nil {
		t.Fatal(err)
	}
	sourceSpawner := &basicDispatchSpawner{deviceID: brokerTestDeviceID}
	targetSpawner := &basicDispatchSpawner{
		deviceID: agentRPCTargetID,
		outcome:  protocol.AgentSpawnOutcomeIndeterminate,
	}
	sourceClient := startAuthenticatedAgentRPCConnector(
		t, harness, brokerTestDeviceID, sourceSpawner, harness.deviceToken,
	)
	startAuthenticatedAgentRPCConnector(t, harness, agentRPCTargetID, targetSpawner, targetToken)

	callContext, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	var root protocol.EnsureRootTreeResult
	if err := sourceClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: agentRPCRemoteThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	params := protocol.SpawnAgentParams{
		SpawnID:        agentRPCRemoteSpawnID,
		TargetDeviceID: agentRPCTargetID,
		TaskName:       "revocation_probe",
		Message:        "this prompt must not cross a revoked target session",
	}
	source := root.Principal.Identity()
	var initial protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &initial,
	); err != nil {
		t.Fatal(err)
	}
	if initial.Agent.Status != protocol.AgentSpawnPending ||
		initial.Outcome != protocol.AgentSpawnOutcomeIndeterminate || targetSpawner.calls.Load() != 1 {
		t.Fatalf("initial pending spawn = %#v, target calls %d", initial, targetSpawner.calls.Load())
	}
	if err := harness.registry.DisableCredential(
		context.Background(), brokerTestControllerID, agentRPCTargetID,
	); err != nil {
		t.Fatal(err)
	}
	if harness.server.connection(agentRPCTargetID) == nil {
		t.Fatal("target session disappeared before outbound reauthentication")
	}
	var repeated protocol.SpawnAgentResult
	if err := sourceClient.Call(
		callContext, protocol.MethodSpawnAgent, root.Tree.TreeID, &source, params, &repeated,
	); err != nil {
		t.Fatal(err)
	}
	if repeated.Agent != initial.Agent || targetSpawner.calls.Load() != 1 {
		t.Fatalf(
			"revoked target retry = %#v, target calls %d; want unchanged pending receipt",
			repeated,
			targetSpawner.calls.Load(),
		)
	}
}

func startAgentRPCConnector(
	t *testing.T,
	harness brokerHarness,
	deviceID string,
	spawner connector.WorkerSpawner,
) *connector.Client {
	return startAgentRPCConnectorWithAuth(
		t, harness, deviceID, spawner, config.AuthModeNone, nil,
	)
}

func startAuthenticatedAgentRPCConnector(
	t *testing.T,
	harness brokerHarness,
	deviceID string,
	spawner connector.WorkerSpawner,
	token tokenfile.Token,
) *connector.Client {
	return startAgentRPCConnectorWithAuth(
		t, harness, deviceID, spawner, config.AuthModeToken, &token,
	)
}

func startAgentRPCConnectorWithAuth(
	t *testing.T,
	harness brokerHarness,
	deviceID string,
	spawner connector.WorkerSpawner,
	authMode config.AuthMode,
	token *tokenfile.Token,
) *connector.Client {
	t.Helper()
	workspaceManager := connector.WorkspaceManager(agentRPCWorkerController{})
	if manager, ok := spawner.(connector.WorkspaceManager); ok {
		workspaceManager = manager
	}
	client, err := connector.New(connector.Options{
		BrokerURL:             strings.Replace(harness.httpServer.URL, "http://", "ws://", 1) + ConnectPath,
		ControllerID:          brokerTestControllerID,
		DeviceID:              deviceID,
		DeviceName:            "agent-rpc-peer",
		AuthMode:              authMode,
		Token:                 token,
		RuntimeVersion:        "agent-rpc-test",
		OperatingSystem:       "linux",
		Architecture:          "amd64",
		ReconnectMin:          5 * time.Millisecond,
		ReconnectMax:          10 * time.Millisecond,
		WorkerSpawner:         spawner,
		WorkerController:      agentRPCWorkerController{},
		WorkerLifecycleSource: agentRPCLifecycleSource{},
		WorkspaceManager:      workspaceManager,
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(runContext) }()
	t.Cleanup(func() {
		cancelRun()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("connector run: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("connector did not stop")
		}
	})
	readyContext, cancelReady := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelReady()
	if err := client.WaitReady(readyContext); err != nil {
		t.Fatal(err)
	}
	return client
}
