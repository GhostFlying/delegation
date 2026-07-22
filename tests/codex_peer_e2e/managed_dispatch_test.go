//go:build integration && linux

package codex_peer_e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	managedAdmissionSpawnA     = "123e4567-e89b-42d3-a456-426614174910"
	managedAdmissionSpawnB     = "123e4567-e89b-42d3-a456-426614174911"
	managedAdmissionSpawnRetry = "123e4567-e89b-42d3-a456-426614174912"
	managedAdmissionTaskA      = "admission_a"
	managedAdmissionTaskB      = "admission_b"
	managedAdmissionTaskRetry  = "admission_retry"
	workerAdmissionA           = "worker-admission-a"
	workerAdmissionB           = "worker-admission-b"
	workerAdmissionRetry       = "worker-admission-retry"
	managedRootMCPSpawn        = "123e4567-e89b-42d3-a456-426614174913"
	managedQueuedMessageID     = "123e4567-e89b-42d3-a456-426614174915"
	managedFollowupOperationID = "123e4567-e89b-42d3-a456-426614174916"
	managedFollowupReplyID     = "123e4567-e89b-42d3-a456-426614174918"
	managedRootMCPTask         = "root_mcp_worker"
	workerRootMCPInitial       = "worker-root-mcp-initial"
	workerRootMCPFollowup      = "worker-root-mcp-followup"
	rootMCPSpawn               = "root-mcp-spawn"
	rootMCPQueue               = "root-mcp-queue"
	rootMCPFollowup            = "root-mcp-followup"
	rootMCPWaitFollowup        = "root-mcp-wait-followup"
	managedQueuedMessage       = "This queued root message must reach the resumed worker."
	managedFollowupReply       = "The resumed worker received the queued root message."
	managedCollaborationSpawn  = "123e4567-e89b-42d3-a456-426614174920"
	managedSteerMessageID      = "123e4567-e89b-42d3-a456-426614174921"
	managedRecoveryMessageID   = "123e4567-e89b-42d3-a456-426614174922"
	managedRecoveryOperationID = "123e4567-e89b-42d3-a456-426614174923"
	managedInitialReplyID      = "123e4567-e89b-42d3-a456-426614174924"
	managedRecoveryReplyID     = "123e4567-e89b-42d3-a456-426614174925"
	managedSelfSpawn           = "123e4567-e89b-42d3-a456-426614174926"
	managedCollaborationTask   = "collaboration_worker"
	managedSelfTask            = "self_target_worker"
	workerCollaborationInitial = "worker-collaboration-initial"
	workerCollaborationResume  = "worker-collaboration-resume"
	workerSelfTarget           = "worker-self-target"
	managedSteerMessage        = "Continue the active turn and report the collaboration result."
	managedRecoveryMessage     = "This queued root message must survive the peer restart."
	managedInitialReply        = "The running worker received the steered task."
	managedRecoveryReply       = "The resumed worker received the queued root message."

	managedAdmissionHelperEnvironment = "DELEGATION_ADMISSION_HELPER"
	managedAdmissionSourceEnvironment = "DELEGATION_ADMISSION_SOURCE"
	managedAdmissionThreadEnvironment = "DELEGATION_ADMISSION_THREAD"
	managedAdmissionParamsEnvironment = "DELEGATION_ADMISSION_PARAMS"
	managedAdmissionResultEnvironment = "DELEGATION_ADMISSION_RESULT"
)

type managedDispatchRoot struct {
	client *localbridge.Client
	root   protocol.EnsureRootTreeResult
}

type managedSpawnResult struct {
	root    protocol.EnsureRootTreeResult
	spawned protocol.SpawnAgentResult
	err     error
}

type managedSpawnReport struct {
	Root    protocol.EnsureRootTreeResult `json:"root"`
	Spawned protocol.SpawnAgentResult     `json:"spawned"`
}

type managedWaitCursor struct {
	mailbox   uint64
	lifecycle uint64
}

func testManagedAdmission(
	t *testing.T,
	sourceA peer,
	sourceB peer,
	target peer,
	externalThreadA string,
	externalThreadB string,
	mock *mockResponses,
	concurrentDispatch func(),
) {
	t.Helper()
	startedA, releaseA := mock.blockWorker(workerAdmissionA)
	startedB, releaseB := mock.blockWorker(workerAdmissionB)
	defer releaseA()
	defer releaseB()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	paramsA := protocol.SpawnAgentParams{
		SpawnID: managedAdmissionSpawnA, TargetDeviceID: deviceIDs[target.label],
		TaskName: managedAdmissionTaskA,
		Message:  "delegation-worker-case=" + workerAdmissionA + " Hold the first worker turn.",
	}
	paramsB := protocol.SpawnAgentParams{
		SpawnID: managedAdmissionSpawnB, TargetDeviceID: deviceIDs[target.label],
		TaskName: managedAdmissionTaskB,
		Message:  "delegation-worker-case=" + workerAdmissionB + " Hold the second worker turn.",
	}
	resultA := startManagedSpawnHelper(t, sourceA, externalThreadA, paramsA)
	resultB := startManagedSpawnHelper(t, sourceB, externalThreadB, paramsB)
	first := receiveManagedSpawn(t, resultA, managedAdmissionTaskA)
	second := receiveManagedSpawn(t, resultB, managedAdmissionTaskB)
	for _, dispatched := range []struct {
		taskName string
		source   peer
		result   managedSpawnResult
	}{
		{taskName: managedAdmissionTaskA, source: sourceA, result: first},
		{taskName: managedAdmissionTaskB, source: sourceB, result: second},
	} {
		spawned := dispatched.result.spawned
		root := dispatched.result.root
		if err := spawned.Validate(); err != nil ||
			spawned.Agent.Status != protocol.AgentSpawnStarted ||
			spawned.Outcome != protocol.AgentSpawnOutcomeStarted ||
			spawned.Agent.TaskName != dispatched.taskName ||
			spawned.Agent.Principal.DeviceID != deviceIDs[target.label] ||
			spawned.Agent.Principal.TreeID != root.Tree.TreeID ||
			spawned.Agent.Principal.ParentAgentID != root.Principal.AgentID ||
			root.Principal.DeviceID != deviceIDs[dispatched.source.label] {
			t.Fatalf("concurrent managed spawn %s = %#v, root %#v, error %v", dispatched.taskName, spawned, root, err)
		}
	}
	if first.root.Tree.TreeID == second.root.Tree.TreeID ||
		first.root.Principal.AgentID == second.root.Principal.AgentID {
		t.Fatalf("concurrent sources reused one root: A %#v, B %#v", first.root, second.root)
	}
	waitForWorkerModelRequest(t, startedA, managedAdmissionTaskA)
	waitForWorkerModelRequest(t, startedB, managedAdmissionTaskB)
	concurrentDispatch()
	rootA := prepareManagedDispatchRoot(t, sourceA, externalThreadA)

	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}
	waitForCount(t, targetConfig.Peer.StateFile, `
SELECT count(*) FROM worker_reservations
WHERE status IN ('reserved', 'starting', 'preflight', 'ready', 'running')
`, 2)
	busyParams := protocol.SpawnAgentParams{
		SpawnID: managedAdmissionSpawnRetry, TargetDeviceID: deviceIDs[target.label],
		TaskName: managedAdmissionTaskRetry,
		Message:  "delegation-worker-case=" + workerAdmissionRetry + " Start after a slot is released.",
	}
	busy, err := spawnManagedAgent(ctx, rootA, busyParams)
	if err != nil || busy.Outcome != protocol.AgentSpawnOutcomeBusy ||
		busy.Agent.Status != protocol.AgentSpawnPending {
		t.Fatalf("capacity-limited spawn = %#v, error %v", busy, err)
	}
	assertWorkerReservationCount(t, targetConfig.Peer.StateFile, busy.Agent, 0)

	releaseA()
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		first.spawned.Agent.Principal.TreeID,
		first.spawned.Agent.Principal.AgentID,
		first.spawned.Agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedAdmissionTaskA,
	)
	startedRetry, err := spawnManagedAgent(ctx, rootA, busyParams)
	if err != nil || startedRetry.Outcome != protocol.AgentSpawnOutcomeStarted ||
		startedRetry.Agent.Status != protocol.AgentSpawnStarted ||
		startedRetry.Agent.Principal != busy.Agent.Principal ||
		startedRetry.Agent.Sequence != busy.Agent.Sequence {
		t.Fatalf("capacity retry = %#v, busy %#v, error %v", startedRetry, busy, err)
	}
	assertWorkerReservationCount(t, targetConfig.Peer.StateFile, startedRetry.Agent, 1)
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		startedRetry.Agent.Principal.TreeID,
		startedRetry.Agent.Principal.AgentID,
		startedRetry.Agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedAdmissionTaskRetry,
	)
	releaseB()
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		second.spawned.Agent.Principal.TreeID,
		second.spawned.Agent.Principal.AgentID,
		second.spawned.Agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedAdmissionTaskB,
	)
	replayed, err := spawnManagedAgent(ctx, rootA, busyParams)
	if err != nil || replayed != startedRetry {
		t.Fatalf("terminal admission retry = %#v, error %v", replayed, err)
	}
}

func testManagedRootMCPFlow(
	t *testing.T,
	source peer,
	target peer,
	externalSourceThread string,
	codexBinary string,
	delegationBinary string,
	repositoryRoot string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}

	runRootAgentCase := func(testCase string) {
		result := runCodex(
			t, source, codexBinary, delegationBinary, repositoryRoot, testCase, externalSourceThread,
		)
		if result.threadID != externalSourceThread {
			t.Fatalf("root MCP case %s resumed thread %q, want %q", testCase, result.threadID, externalSourceThread)
		}
	}
	runRootAgentCase(rootMCPSpawn)
	root := prepareManagedDispatchRoot(t, source, externalSourceThread)
	agent := findManagedAgent(t, ctx, root, managedRootMCPTask)
	if agent.Status != protocol.AgentSpawnStarted ||
		agent.Principal.ParentAgentID != root.root.Principal.AgentID ||
		agent.Principal.DeviceID != deviceIDs[target.label] {
		t.Fatalf("root MCP managed agent = %#v", agent)
	}
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		agent.Principal.TreeID,
		agent.Principal.AgentID,
		agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedRootMCPTask,
	)
	managedThreadID := managedWorkerThreadID(t, targetConfig.Peer.StateFile, agent)
	runRootAgentCase(rootMCPQueue)
	runRootAgentCase(rootMCPFollowup)
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		agent.Principal.TreeID,
		agent.Principal.AgentID,
		agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedRootMCPTask,
	)
	if afterFollowup := managedWorkerThreadID(t, targetConfig.Peer.StateFile, agent); afterFollowup != managedThreadID {
		t.Fatalf("managed thread after cold follow-up = %q, want %q", afterFollowup, managedThreadID)
	}
	runRootAgentCase(rootMCPWaitFollowup)
}

func testManagedCollaborationAndRecovery(
	t *testing.T,
	source peer,
	selfSource peer,
	target peer,
	externalSourceThread string,
	externalSelfThread string,
	mock *mockResponses,
	restartTarget func(),
) {
	t.Helper()
	started, release := mock.blockWorker(workerCollaborationInitial)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	targetConfig, err := delegationconfig.Read(target.configPath)
	if err != nil {
		t.Fatal(err)
	}

	root := prepareManagedDispatchRoot(t, source, externalSourceThread)
	spawned, err := spawnManagedAgent(ctx, root, protocol.SpawnAgentParams{
		SpawnID: managedCollaborationSpawn, TargetDeviceID: deviceIDs[target.label],
		TaskName: managedCollaborationTask,
		Message:  "delegation-worker-case=" + workerCollaborationInitial + " Wait for a root steer, then report to the parent.",
	})
	if err != nil || spawned.Outcome != protocol.AgentSpawnOutcomeStarted ||
		spawned.Agent.Status != protocol.AgentSpawnStarted ||
		spawned.Agent.Principal.ParentAgentID != root.root.Principal.AgentID ||
		spawned.Agent.Principal.DeviceID != deviceIDs[target.label] {
		t.Fatalf("collaboration spawn = %#v, error %v", spawned, err)
	}
	waitForWorkerModelRequest(t, started, managedCollaborationTask)
	steered, err := runManagedAgentOperation(ctx, root, protocol.MethodSendAgent, protocol.SendAgentParams{
		AgentID: spawned.Agent.Principal.AgentID, MessageID: managedSteerMessageID,
		Message: managedSteerMessage,
	})
	assertManagedAgentOperation(
		t, steered, err, protocol.AgentOperationSend, protocol.AgentOperationOutcomeSteered,
		managedSteerMessageID, spawned.Agent.Principal.AgentID,
	)
	release()
	cursor := &managedWaitCursor{}
	waitForManagedRootMessage(
		t, ctx, root, cursor, managedInitialReplyID, managedInitialReply, spawned.Agent.Principal,
	)
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		spawned.Agent.Principal.TreeID,
		spawned.Agent.Principal.AgentID,
		spawned.Agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedCollaborationTask,
	)
	managedThreadID := managedWorkerThreadID(t, targetConfig.Peer.StateFile, spawned.Agent)
	queued, err := runManagedAgentOperation(ctx, root, protocol.MethodSendAgent, protocol.SendAgentParams{
		AgentID: spawned.Agent.Principal.AgentID, MessageID: managedRecoveryMessageID,
		Message: managedRecoveryMessage,
	})
	assertManagedAgentOperation(
		t, queued, err, protocol.AgentOperationSend, protocol.AgentOperationOutcomeQueued,
		managedRecoveryMessageID, spawned.Agent.Principal.AgentID,
	)

	restartTarget()
	if afterRestart := managedWorkerThreadID(t, targetConfig.Peer.StateFile, spawned.Agent); afterRestart != managedThreadID {
		t.Fatalf("managed thread after peer restart = %q, want %q", afterRestart, managedThreadID)
	}
	followedUp, err := runManagedAgentOperation(ctx, root, protocol.MethodFollowupAgent, protocol.FollowupAgentParams{
		OperationID: managedRecoveryOperationID,
		AgentID:     spawned.Agent.Principal.AgentID,
		Message: "delegation-worker-case=" + workerCollaborationResume +
			" Read the queued root message and acknowledge it.",
	})
	assertManagedAgentOperation(
		t, followedUp, err, protocol.AgentOperationFollowup, protocol.AgentOperationOutcomeStarted,
		managedRecoveryOperationID, spawned.Agent.Principal.AgentID,
	)
	waitForManagedRootMessage(
		t, ctx, root, cursor, managedRecoveryReplyID, managedRecoveryReply, spawned.Agent.Principal,
	)
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		spawned.Agent.Principal.TreeID,
		spawned.Agent.Principal.AgentID,
		spawned.Agent.Principal.ParentAgentID,
		deviceIDs[target.label],
		managedCollaborationTask,
	)
	if afterFollowup := managedWorkerThreadID(t, targetConfig.Peer.StateFile, spawned.Agent); afterFollowup != managedThreadID {
		t.Fatalf("managed thread after cold follow-up = %q, want %q", afterFollowup, managedThreadID)
	}

	selfRoot := prepareManagedDispatchRoot(t, selfSource, externalSelfThread)
	selfSpawned, err := spawnManagedAgent(ctx, selfRoot, protocol.SpawnAgentParams{
		SpawnID: managedSelfSpawn, TargetDeviceID: deviceIDs[selfSource.label],
		TaskName: managedSelfTask,
		Message:  "delegation-worker-case=" + workerSelfTarget + " Complete the isolated self-target task.",
	})
	if err != nil || selfSpawned.Outcome != protocol.AgentSpawnOutcomeStarted ||
		selfSpawned.Agent.Principal.AgentID == selfRoot.root.Principal.AgentID ||
		selfSpawned.Agent.Principal.ParentAgentID != selfRoot.root.Principal.AgentID ||
		selfSpawned.Agent.Principal.DeviceID != selfRoot.root.Principal.DeviceID ||
		selfSpawned.Agent.Principal.TreeID != selfRoot.root.Tree.TreeID {
		t.Fatalf("self-target spawn = %#v, root %#v, error %v", selfSpawned, selfRoot.root, err)
	}
	waitForManagedWorkerIdle(
		t,
		targetConfig.Peer.StateFile,
		selfSpawned.Agent.Principal.TreeID,
		selfSpawned.Agent.Principal.AgentID,
		selfSpawned.Agent.Principal.ParentAgentID,
		deviceIDs[selfSource.label],
		managedSelfTask,
	)
	if selfThreadID := managedWorkerThreadID(t, targetConfig.Peer.StateFile, selfSpawned.Agent); selfThreadID == externalSelfThread {
		t.Fatalf("self-target managed worker reused user thread %s", selfThreadID)
	}
}

func runManagedAgentOperation(
	ctx context.Context,
	root managedDispatchRoot,
	method string,
	params any,
) (protocol.AgentOperationResult, error) {
	source := root.root.Principal.Identity()
	var result protocol.AgentOperationResult
	err := root.client.Call(ctx, method, root.root.Tree.TreeID, &source, params, &result)
	return result, err
}

func assertManagedAgentOperation(
	t *testing.T,
	result protocol.AgentOperationResult,
	err error,
	action protocol.AgentOperationAction,
	outcome protocol.AgentOperationOutcome,
	operationID string,
	agentID string,
) {
	t.Helper()
	if err != nil || result.Validate() != nil || result.Action != action || result.Outcome != outcome ||
		result.OperationID != operationID || result.AgentID != agentID {
		t.Fatalf("managed agent operation = %#v, error %v; want %s/%s", result, err, action, outcome)
	}
}

func waitForManagedRootMessage(
	t *testing.T,
	ctx context.Context,
	root managedDispatchRoot,
	cursor *managedWaitCursor,
	messageID string,
	message string,
	source control.PrincipalIdentity,
) protocol.MailboxMessage {
	t.Helper()
	for {
		var result protocol.WaitAgentResult
		rootSource := root.root.Principal.Identity()
		err := root.client.Call(ctx, protocol.MethodWaitAgent, root.root.Tree.TreeID, &rootSource, protocol.WaitAgentParams{
			MailboxCursor: cursor.mailbox, LifecycleCursor: cursor.lifecycle,
			TimeoutMillis: 10_000, MessageLimit: protocol.MaximumAgentWaitMessages,
			ActivityLimit: protocol.MaximumAgentWaitActivities,
		}, &result)
		if err != nil {
			t.Fatalf("wait for managed root message %s: %v", messageID, err)
		}
		if result.NextMailboxCursor < cursor.mailbox || result.NextLifecycleCursor < cursor.lifecycle {
			t.Fatalf("managed root wait cursors regressed: %#v", result)
		}
		cursor.mailbox = result.NextMailboxCursor
		cursor.lifecycle = result.NextLifecycleCursor
		for _, received := range result.Messages {
			if received.MessageID != messageID {
				continue
			}
			if received.Message != message || received.Source != source {
				t.Fatalf("managed root message = %#v, want %q from %#v", received, message, source)
			}
			return received
		}
	}
}

func findManagedAgent(
	t *testing.T,
	ctx context.Context,
	root managedDispatchRoot,
	taskName string,
) protocol.AgentSummary {
	t.Helper()
	source := root.root.Principal.Identity()
	var result protocol.ListAgentsResult
	if err := root.client.Call(ctx, protocol.MethodListAgents, root.root.Tree.TreeID, &source, protocol.ListAgentsParams{
		Limit: protocol.MaximumAgentPage,
	}, &result); err != nil {
		t.Fatal(err)
	}
	for _, agent := range result.Agents {
		if agent.TaskName == taskName {
			return agent
		}
	}
	t.Fatalf("managed agent %s was not listed: %#v", taskName, result)
	return protocol.AgentSummary{}
}

func runManagedAdmissionHelper(t *testing.T) {
	t.Helper()
	label := os.Getenv(managedAdmissionSourceEnvironment)
	externalThreadID := os.Getenv(managedAdmissionThreadEnvironment)
	encodedParams := os.Getenv(managedAdmissionParamsEnvironment)
	resultPath := os.Getenv(managedAdmissionResultEnvironment)
	if deviceIDs[label] == "" || externalThreadID == "" || encodedParams == "" || resultPath == "" {
		t.Fatal("managed admission helper environment is incomplete")
	}
	data, err := base64.RawStdEncoding.DecodeString(encodedParams)
	if err != nil {
		t.Fatal(err)
	}
	var params protocol.SpawnAgentParams
	if err := json.Unmarshal(data, &params); err != nil {
		t.Fatal(err)
	}
	endpoint, err := localbridge.Endpoint(networkID, deviceIDs[label])
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
	source := root.Principal.Identity()
	var spawned protocol.SpawnAgentResult
	if err := client.Call(
		ctx,
		protocol.MethodSpawnAgent,
		root.Tree.TreeID,
		&source,
		params,
		&spawned,
	); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(managedSpawnReport{Root: root, Spawned: spawned})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func startManagedSpawnHelper(
	t *testing.T,
	source peer,
	externalThreadID string,
	params protocol.SpawnAgentParams,
) <-chan managedSpawnResult {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	resultPath := t.TempDir() + string(os.PathSeparator) + "spawn-result.json"
	command := exec.Command(executable, "-test.run=^TestCodexPeerTopology$")
	command.Env = append(commandEnv(source),
		managedAdmissionHelperEnvironment+"=1",
		managedAdmissionSourceEnvironment+"="+source.label,
		managedAdmissionThreadEnvironment+"="+externalThreadID,
		managedAdmissionParamsEnvironment+"="+base64.RawStdEncoding.EncodeToString(data),
		managedAdmissionResultEnvironment+"="+resultPath,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill() })
	done := make(chan managedSpawnResult, 1)
	go func() {
		if err := command.Wait(); err != nil {
			done <- managedSpawnResult{err: fmt.Errorf(
				"admission helper failed: %w; stdout: %s; stderr: %s",
				err,
				stdout.String(),
				stderr.String(),
			)}
			return
		}
		data, err := os.ReadFile(resultPath)
		if err != nil {
			done <- managedSpawnResult{err: err}
			return
		}
		var report managedSpawnReport
		if err := json.Unmarshal(data, &report); err != nil {
			done <- managedSpawnResult{err: err}
			return
		}
		done <- managedSpawnResult{root: report.Root, spawned: report.Spawned}
	}()
	return done
}

func prepareManagedDispatchRoot(
	t *testing.T,
	source peer,
	externalThreadID string,
) managedDispatchRoot {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	return managedDispatchRoot{client: client, root: root}
}

func spawnManagedAgent(
	ctx context.Context,
	root managedDispatchRoot,
	params protocol.SpawnAgentParams,
) (protocol.SpawnAgentResult, error) {
	sourcePrincipal := root.root.Principal.Identity()
	var spawned protocol.SpawnAgentResult
	err := root.client.Call(
		ctx,
		protocol.MethodSpawnAgent,
		root.root.Tree.TreeID,
		&sourcePrincipal,
		params,
		&spawned,
	)
	return spawned, err
}

func waitForWorkerModelRequest(t *testing.T, started <-chan struct{}, taskName string) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatalf("managed worker %s did not reach the mock provider", taskName)
	}
}

func receiveManagedSpawn(
	t *testing.T,
	result <-chan managedSpawnResult,
	taskName string,
) managedSpawnResult {
	t.Helper()
	select {
	case received := <-result:
		if received.err != nil {
			t.Fatalf("managed worker %s spawn: %v", taskName, received.err)
		}
		return received
	case <-time.After(30 * time.Second):
		t.Fatalf("managed worker %s spawn did not return", taskName)
		return managedSpawnResult{}
	}
}

func waitForManagedWorkerIdle(
	t *testing.T,
	statePath, treeID, agentID, parentAgentID, targetDeviceID, taskName string,
) {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var status, threadID, parent, device, storedTaskName string
		err := database.QueryRow(`
SELECT status, codex_thread_id, parent_agent_id, device_id, task_name
FROM worker_reservations
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, networkID, treeID, agentID).Scan(&status, &threadID, &parent, &device, &storedTaskName)
		if err == nil && status == "idle" && threadID != "" && parent == parentAgentID &&
			device == targetDeviceID && storedTaskName == taskName {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(fmt.Errorf("managed worker %s did not become idle on target peer", agentID))
}

func managedWorkerThreadID(t *testing.T, statePath string, agent protocol.AgentSummary) string {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	var threadID string
	if err := database.QueryRow(`
SELECT codex_thread_id
FROM worker_reservations
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, networkID, agent.Principal.TreeID, agent.Principal.AgentID).Scan(&threadID); err != nil {
		t.Fatal(err)
	}
	if threadID == "" {
		t.Fatalf("managed worker %s does not have a Codex thread", agent.Principal.AgentID)
	}
	return threadID
}

func assertWorkerReservationCount(
	t *testing.T,
	statePath string,
	agent protocol.AgentSummary,
	want int,
) {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	var count int
	if err := database.QueryRow(`
SELECT count(*) FROM worker_reservations
WHERE controller_id = ? AND tree_id = ? AND agent_id = ?
`, networkID, agent.Principal.TreeID, agent.Principal.AgentID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("worker reservation count for %s = %d, want %d", agent.Principal.AgentID, count, want)
	}
}

func assertAgentReceiptCount(t *testing.T, statePath string, want int) {
	t.Helper()
	count := agentReceiptCount(t, statePath)
	if count != want {
		t.Fatalf("managed agent receipt count = %d, want %d", count, want)
	}
}

func agentReceiptCount(t *testing.T, statePath string) int {
	t.Helper()
	database := openDatabase(t, statePath)
	defer database.Close()
	var count int
	if err := database.QueryRow("SELECT count(*) FROM agent_spawn_receipts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
