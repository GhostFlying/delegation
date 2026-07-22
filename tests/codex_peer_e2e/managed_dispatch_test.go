//go:build integration && linux

package codex_peer_e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	managedDispatchSpawnID = "123e4567-e89b-42d3-a456-426614174910"
	managedDispatchTask    = "managed_dispatch"
	workerDispatchCase     = "worker-dispatch"
)

func testManagedDispatch(
	t *testing.T,
	source peer,
	target peer,
	brokerStatePath string,
	externalThreadID string,
) {
	t.Helper()
	t.Setenv("HOME", source.home)
	endpoint, err := localbridge.Endpoint(networkID, deviceIDs[source.label])
	if err != nil {
		t.Fatal(err)
	}
	client, err := localbridge.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var root protocol.EnsureRootTreeResult
	if err := client.Call(
		ctx,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: externalThreadID},
		&root,
	); err != nil {
		t.Fatal(err)
	}
	sourcePrincipal := root.Principal.Identity()
	params := protocol.SpawnAgentParams{
		SpawnID:        managedDispatchSpawnID,
		TargetDeviceID: deviceIDs[target.label],
		TaskName:       managedDispatchTask,
		Message:        "delegation-worker-case=" + workerDispatchCase + " Return worker-dispatch-ok.",
	}
	var spawned protocol.SpawnAgentResult
	if err := client.Call(
		ctx,
		protocol.MethodSpawnAgent,
		root.Tree.TreeID,
		&sourcePrincipal,
		params,
		&spawned,
	); err != nil {
		t.Fatal(err)
	}
	if spawned.Agent.Status != protocol.AgentSpawnStarted ||
		spawned.Agent.Principal.ParentAgentID != root.Principal.AgentID ||
		spawned.Agent.Principal.DeviceID != deviceIDs[target.label] ||
		spawned.Agent.TaskName != managedDispatchTask {
		t.Fatalf("managed dispatch result = %#v", spawned)
	}
	var repeated protocol.SpawnAgentResult
	if err := client.Call(
		ctx,
		protocol.MethodSpawnAgent,
		root.Tree.TreeID,
		&sourcePrincipal,
		params,
		&repeated,
	); err != nil || repeated != spawned {
		t.Fatalf("managed dispatch retry = %#v, error %v", repeated, err)
	}
	var agents protocol.ListAgentsResult
	if err := client.Call(
		ctx,
		protocol.MethodListAgents,
		root.Tree.TreeID,
		&sourcePrincipal,
		protocol.ListAgentsParams{Limit: protocol.MaximumAgentPage},
		&agents,
	); err != nil {
		t.Fatal(err)
	}
	if len(agents.Agents) != 1 || agents.Agents[0] != spawned.Agent || agents.NextSequence != 0 {
		t.Fatalf("managed agent list = %#v", agents)
	}

	brokerState := openDatabase(t, brokerStatePath)
	defer brokerState.Close()
	var receiptStatus, receiptTarget, receiptAgent string
	if err := brokerState.QueryRow(`
SELECT status, target_device_id, agent_id
FROM agent_spawn_receipts
WHERE controller_id = ? AND tree_id = ? AND source_agent_id = ? AND spawn_id = ?
`, networkID, root.Tree.TreeID, root.Principal.AgentID, managedDispatchSpawnID).Scan(
		&receiptStatus,
		&receiptTarget,
		&receiptAgent,
	); err != nil {
		t.Fatal(err)
	}
	if receiptStatus != string(protocol.AgentSpawnStarted) ||
		receiptTarget != deviceIDs[target.label] ||
		receiptAgent != spawned.Agent.Principal.AgentID {
		t.Fatalf(
			"managed dispatch receipt = status %q, target %q, agent %q",
			receiptStatus,
			receiptTarget,
			receiptAgent,
		)
	}

	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		root.Tree.TreeID,
		spawned.Agent.Principal.AgentID,
		root.Principal.AgentID,
		deviceIDs[target.label],
	)
}

func waitForManagedWorkerIdle(
	t *testing.T,
	statePath, treeID, agentID, parentAgentID, targetDeviceID string,
) {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var status, threadID, parent, device, taskName string
		err := database.QueryRow(`
SELECT status, codex_thread_id, parent_agent_id, device_id, task_name
FROM worker_reservations
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, networkID, treeID, agentID).Scan(&status, &threadID, &parent, &device, &taskName)
		if err == nil && status == "idle" && threadID != "" && parent == parentAgentID &&
			device == targetDeviceID && taskName == managedDispatchTask {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(fmt.Errorf("managed worker %s did not become idle on target peer", agentID))
}
