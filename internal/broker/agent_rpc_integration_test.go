package broker

import (
	"context"
	"errors"
	"strings"
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
)

type selfDispatchSpawner struct {
	client *connector.Client
	calls  atomic.Int32
}

type basicDispatchSpawner struct {
	deviceID string
	status   protocol.AgentSpawnStatus
	calls    atomic.Int32
}

func (s *basicDispatchSpawner) SpawnWorker(
	_ context.Context,
	request connector.WorkerSpawnRequest,
) (protocol.SpawnWorkerResult, error) {
	s.calls.Add(1)
	status := s.status
	if status == "" {
		status = protocol.AgentSpawnStarted
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
		Status: status,
	}, nil
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
		Status: protocol.AgentSpawnStarted,
	}, nil
}

func TestAgentRPCSelfDispatchIsDurableIdempotentAndNonBlocking(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	spawner := &selfDispatchSpawner{}
	client, err := connector.New(connector.Options{
		BrokerURL:       strings.Replace(harness.httpServer.URL, "http://", "ws://", 1) + ConnectPath,
		ControllerID:    brokerTestControllerID,
		DeviceID:        brokerTestDeviceID,
		DeviceName:      "self-dispatch-peer",
		AuthMode:        config.AuthModeNone,
		RuntimeVersion:  "agent-rpc-test",
		OperatingSystem: "linux",
		Architecture:    "amd64",
		ReconnectMin:    5 * time.Millisecond,
		ReconnectMax:    10 * time.Millisecond,
		WorkerSpawner:   spawner,
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
		status:   protocol.AgentSpawnPending,
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
	if initial.Agent.Status != protocol.AgentSpawnPending || targetSpawner.calls.Load() != 1 {
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
	client, err := connector.New(connector.Options{
		BrokerURL:       strings.Replace(harness.httpServer.URL, "http://", "ws://", 1) + ConnectPath,
		ControllerID:    brokerTestControllerID,
		DeviceID:        deviceID,
		DeviceName:      "agent-rpc-peer",
		AuthMode:        authMode,
		Token:           token,
		RuntimeVersion:  "agent-rpc-test",
		OperatingSystem: "linux",
		Architecture:    "amd64",
		ReconnectMin:    5 * time.Millisecond,
		ReconnectMax:    10 * time.Millisecond,
		WorkerSpawner:   spawner,
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
