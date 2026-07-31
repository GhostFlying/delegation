package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	testControllerID = "123e4567-e89b-42d3-a456-426614174400"
	testDeviceID     = "123e4567-e89b-42d3-a456-426614174401"
	testTreeID       = "123e4567-e89b-42d3-a456-426614174402"
	testParentID     = "123e4567-e89b-42d3-a456-426614174403"
)

type testErrorRecorder struct {
	mu     sync.Mutex
	errors []error
}

func (r *testErrorRecorder) report(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err)
}

func (r *testErrorRecorder) snapshot() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errors)
}

func TestHostUsesOneAppServerAndEnforcesWorkerSlots(t *testing.T) {
	application := newFakeApplication()
	host, state, paths := newTestHost(t, 2, application)
	first := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174410", "first")
	second := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174411", "second")
	if first.Worker.Status != store.WorkerRunning || second.Worker.Status != store.WorkerRunning {
		t.Fatalf("spawned workers = %#v / %#v", first.Worker, second.Worker)
	}
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174412",
		ParentAgentID: testParentID, TaskName: "third", Prompt: "third prompt",
	})
	if !errors.Is(err, store.ErrWorkerBusy) {
		t.Fatalf("third Spawn() error = %v, want ErrWorkerBusy", err)
	}

	record := application.snapshot()
	if len(record.starts) != 2 || len(record.turns) != 2 || record.preflights != 2 {
		t.Fatalf("app-server calls = %#v", record)
	}
	assertManagedProfile(t, record.starts[0].Config, paths, first.Worker)
	if _, err := os.Stat(first.Worker.WorkspacePath); err != nil {
		t.Fatal(err)
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 2 {
		t.Fatalf("stored workers = %#v", workers)
	}
}

func TestHostPassesStructuredCLILaunchToAppServer(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	launcher := filepath.Join(t.TempDir(), "warmpool")
	runtimeExecutable := filepath.Join(t.TempDir(), "traex")
	for _, path := range []string{launcher, runtimeExecutable} {
		if err := os.WriteFile(path, []byte("test"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	resolved, err := clilaunch.Resolve(clilaunch.Spec{
		Executable:      launcher,
		PrefixArguments: []string{"run", "--", "traex", "-p", "ultra"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolvedRuntime, err := clilaunch.ResolveRuntimeExecutable(runtimeExecutable)
	if err != nil {
		t.Fatal(err)
	}
	host.cliLaunch = resolved
	host.cliRuntimeExecutable = resolvedRuntime

	spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174413", "structured launch")

	if !reflect.DeepEqual(paths.launchOptions.Launch, resolved) {
		t.Fatalf("app-server launch = %#v, want %#v", paths.launchOptions.Launch, resolved)
	}
	config := host.managedConfig(store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174414",
		},
		ParentAgentID: testParentID,
		WorkspacePath: filepath.Join(filepath.Dir(paths.codexHome), "managed-worker"),
	})
	if runtime.GOOS == "linux" {
		filesystem := managedFilesystemPermissions(t, config)
		if filesystem[resolvedRuntime] != "read" {
			t.Fatalf("managed profile omits CLI runtime: %#v", filesystem)
		}
		if _, found := filesystem[resolved.Executable]; found {
			t.Fatalf("managed profile grants launcher wrapper to worker shell: %#v", filesystem)
		}
	}
}

func TestHostIsolatesRuntimeHomeForHostKind(t *testing.T) {
	for _, test := range []struct {
		name          string
		hostKind      hostkind.Kind
		wantHomes     func(string) map[string]string
		wantUnset     []string
		wantShellHome []string
	}{
		{
			name:     "codex",
			hostKind: hostkind.Codex,
			wantHomes: func(home string) map[string]string {
				return map[string]string{"CODEX_HOME": home}
			},
			wantUnset:     []string{"TRAE_HOME", "TRAECLI_HOME"},
			wantShellHome: []string{"CODEX_HOME", "TRAE_HOME", "TRAECLI_HOME"},
		},
		{
			name:     "traex",
			hostKind: hostkind.TraeX,
			wantHomes: func(home string) map[string]string {
				return map[string]string{
					"TRAE_HOME":    home,
					"TRAECLI_HOME": filepath.Join(home, "cli"),
				}
			},
			wantUnset:     []string{"CODEX_HOME"},
			wantShellHome: []string{"CODEX_HOME", "TRAE_HOME", "TRAECLI_HOME"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newFakeApplication()
			host, _, paths := newTestHostForKind(t, test.hostKind, 1, application)
			spawnTestWorker(
				t,
				host,
				"123e4567-e89b-42d3-a456-426614174415",
				test.name+" runtime home",
			)

			wantHomes := test.wantHomes(host.codexHome)
			if !reflect.DeepEqual(paths.launchOptions.RuntimeHomeEnvironment, wantHomes) {
				t.Fatalf(
					"runtime home environment = %#v, want %#v",
					paths.launchOptions.RuntimeHomeEnvironment,
					wantHomes,
				)
			}
			wantRolloutHome := host.codexHome
			if test.hostKind == hostkind.TraeX {
				wantRolloutHome = wantHomes["TRAECLI_HOME"]
			}
			if host.rolloutHome != wantRolloutHome {
				t.Fatalf("rollout home = %q, want %q", host.rolloutHome, wantRolloutHome)
			}
			for _, name := range test.wantUnset {
				if !slices.Contains(paths.launchOptions.UnsetEnvironment, name) {
					t.Fatalf(
						"app-server does not unset ambient %s: %#v",
						name,
						paths.launchOptions.UnsetEnvironment,
					)
				}
			}
			for _, name := range []string{"CODEX_HOME", "TRAE_HOME", "TRAECLI_HOME"} {
				if _, found := paths.launchOptions.Environment[name]; found {
					t.Fatalf(
						"managed app-server environment retains untrusted %s: %#v",
						name,
						paths.launchOptions.Environment,
					)
				}
			}
			if test.hostKind == hostkind.TraeX {
				cliHome := wantHomes["TRAECLI_HOME"]
				info, err := os.Stat(cliHome)
				if err != nil {
					t.Fatal(err)
				}
				if !info.IsDir() {
					t.Fatalf("managed TRAECLI_HOME mode = %v", info.Mode())
				}
				if err := config.ValidatePrivateDirectory(cliHome); err != nil {
					t.Fatalf("managed TRAECLI_HOME is not private: %v", err)
				}
				if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
					t.Fatalf("managed TRAECLI_HOME mode = %v", info.Mode())
				}
			}
			config := application.snapshot().starts[0].Config
			policy := config["shell_environment_policy"].(map[string]any)
			excluded := policy["exclude"].([]string)
			for _, name := range test.wantShellHome {
				if !slices.Contains(excluded, name) {
					t.Fatalf("managed worker shell does not exclude %s: %#v", name, excluded)
				}
			}
		})
	}
}

func TestHostAdaptsThreadStartProtocolByHostKind(t *testing.T) {
	for _, test := range []struct {
		name       string
		hostKind   hostkind.Kind
		wantSource string
		omitRoots  bool
	}{
		{name: "codex", hostKind: hostkind.Codex, wantSource: codexWorkerSource},
		{name: "traex", hostKind: hostkind.TraeX, wantSource: traeXWorkerSource, omitRoots: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newFakeApplication()
			if test.omitRoots {
				application.threadStartResultHook = func(result *threadResult) {
					result.RuntimeWorkspaceRoots = nil
				}
			}
			host, _, _ := newTestHostForKind(t, test.hostKind, 1, application)
			spawnTestWorker(t, host, newTestID(), test.name+" thread protocol")
			record := application.snapshot()
			if len(record.starts) != 1 || record.starts[0].ThreadSource != test.wantSource {
				t.Fatalf("thread starts = %#v, want source %q", record.starts, test.wantSource)
			}
			if len(record.starts[0].RuntimeWorkspaceRoots) != 1 {
				t.Fatalf("thread request omitted runtime workspace roots: %#v", record.starts[0])
			}
		})
	}
}

func TestTraeXWorkerMCPPreflightProbesOnlyRequiredTools(t *testing.T) {
	application := newFakeApplication()
	host, _, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "TraeX MCP preflight")

	record := application.snapshot()
	want := []mcpToolCallParams{
		{
			ThreadID: started.Worker.CodexThreadID,
			Server:   workerServerName,
			Tool:     "send_upstream_message",
			Arguments: map[string]any{
				"messageId": "invalid",
				"message":   "MCP availability probe",
			},
		},
		{
			ThreadID: started.Worker.CodexThreadID,
			Server:   workerServerName,
			Tool:     "wait_for_upstream_message",
			Arguments: map[string]any{
				"timeoutSeconds": -1,
			},
		},
	}
	if record.preflights != 0 || !reflect.DeepEqual(record.mcpTools, want) {
		t.Fatalf("TraeX MCP preflight = %#v, inventory calls = %d", record.mcpTools, record.preflights)
	}
}

func TestTraeXWorkerMCPPreflightFailsClosed(t *testing.T) {
	falseValue := false
	tests := map[string]func(*fakeApplication){
		"missing tool": func(application *fakeApplication) {
			application.mcpToolErrors["wait_for_upstream_message"] = &appserver.RPCError{
				Code: -32602, Message: "unknown tool",
			}
		},
		"successful execution": func(application *fakeApplication) {
			application.mcpToolResults["send_upstream_message"] = mcpToolCallResult{
				Content: []json.RawMessage{json.RawMessage(`{"type":"text","text":"unexpected"}`)},
				IsError: &falseValue,
			}
		},
		"empty error": func(application *fakeApplication) {
			trueValue := true
			application.mcpToolResults["send_upstream_message"] = mcpToolCallResult{
				IsError: &trueValue,
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			application := newFakeApplication()
			mutate(application)
			host, state, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
			started, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
				TaskName: name, Prompt: name,
			})
			if !errors.Is(err, ErrMCPInjectionBlocked) {
				t.Fatalf("Spawn() error = %v, want ErrMCPInjectionBlocked", err)
			}
			failed, stateErr := state.GetWorker(context.Background(), started.Worker.WorkerKey)
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			if failed.Status != store.WorkerFailed || failed.FailureCode != "mcp_injection_blocked" {
				t.Fatalf("failed worker = %#v", failed)
			}
			if len(application.snapshot().turns) != 0 {
				t.Fatal("TraeX worker started a turn after failed MCP preflight")
			}
		})
	}
}

func TestTraeXWorkerMCPPreflightPreservesUnsentRequest(t *testing.T) {
	application := newFakeApplication()
	application.mcpToolErrors["send_upstream_message"] = errors.Join(
		appserver.ErrRequestNotWritten,
		context.Canceled,
	)
	host, state, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "unsent TraeX MCP preflight", Prompt: "unsent TraeX MCP preflight",
	})
	if !errors.Is(err, appserver.ErrRequestNotWritten) ||
		errors.Is(err, ErrMCPInjectionBlocked) {
		t.Fatalf("Spawn() error = %v", err)
	}
	restored, stateErr := state.GetWorker(context.Background(), started.Worker.WorkerKey)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if restored.Status != store.WorkerPending || restored.FailureCode != "" {
		t.Fatalf("restored worker = %#v", restored)
	}
}

func TestTraeXRejectsMismatchedReturnedWorkspaceRoot(t *testing.T) {
	application := newFakeApplication()
	application.threadStartResultHook = func(result *threadResult) {
		result.RuntimeWorkspaceRoots = []string{t.TempDir()}
	}
	host, _, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "mismatched TraeX root", Prompt: "mismatched TraeX root",
	})
	if err == nil || !strings.Contains(err.Error(), "runtime workspace roots") {
		t.Fatalf("Spawn() error = %v", err)
	}
}

func TestTraeXIdleCompletesTurnBeforeStartResponse(t *testing.T) {
	application := newFakeApplication()
	application.idleBeforeReturn = true
	application.idleReadStarted = make(chan struct{})
	host, state, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "TraeX early idle")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	record := application.snapshot()
	if len(record.reads) == 0 || !record.reads[len(record.reads)-1].IncludeTurns {
		t.Fatalf("TraeX idle reads = %#v", record.reads)
	}
	reads := len(record.reads)
	payload, err := json.Marshal(map[string]any{
		"threadId": started.Worker.CodexThreadID,
		"status":   map[string]string{"type": "idle"},
	})
	if err != nil {
		t.Fatal(err)
	}
	application.notifications <- appserver.Notification{
		Method: "thread/status/changed", Params: payload,
	}
	time.Sleep(20 * time.Millisecond)
	if got := len(application.snapshot().reads); got != reads {
		t.Fatalf("duplicate TraeX idle triggered %d reads, want %d", got, reads)
	}
}

func TestTraeXIdleResponseLossPreservesInitialRollout(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	application.idleBeforeReturn = true
	application.idleReadStarted = make(chan struct{})
	application.turnStartErr = context.DeadlineExceeded
	application.turnStartResponseLost = true
	application.completeThenLose = true
	host, state, _ := newTestHostForKind(t, hostkind.TraeX, 1, application)
	rolloutPath := filepath.Join(
		host.rolloutHome,
		"sessions",
		"2026",
		"07",
		"31",
		"rollout-2026-07-31T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	var (
		rolloutSegment string
		materialize    sync.Once
		retries        int
	)
	application.threadReadHook = func(read threadReadParams, result *threadResult) {
		if !read.IncludeTurns || len(result.Thread.Turns) != 1 {
			return
		}
		rolloutSegment = testManagedRolloutLine("task_started", result.Thread.Turns[0].ID) +
			testManagedRolloutLine("task_complete", result.Thread.Turns[0].ID)
		result.Thread.Path = &rolloutPath
	}
	host.waitForRolloutFlush = func(_ context.Context, _ time.Duration) error {
		retries++
		materialize.Do(func() {
			if err := os.WriteFile(rolloutPath, []byte(rolloutSegment), 0o600); err != nil {
				t.Error(err)
			}
		})
		return nil
	}
	agentID := newTestID()
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "TraeX early idle", Prompt: "TraeX early idle prompt",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early-idle response loss error = %v", err)
	}
	key := store.WorkerKey{
		ControllerID: testControllerID, TreeID: testTreeID, AgentID: agentID,
	}
	outbox := waitResultPublication(t, state, key)
	if retries != 1 {
		t.Fatalf("early-idle rollout retries = %d, want 1", retries)
	}
	if outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable {
		t.Fatalf("early-idle result rollout = %#v", outbox.Manifest.Rollout)
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), key, outbox.Manifest.TurnID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Rollout.Status != store.WorkerRolloutAvailable ||
		intent.Rollout.Path != rolloutPath || intent.Rollout.Offset != 0 {
		t.Fatalf("early-idle intent rollout = %#v", intent.Rollout)
	}
	if _, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	); err != nil {
		t.Fatal(err)
	}
	waitWorkerStatus(t, state, key, store.WorkerIdle)
	if started.Worker.WorkerKey != key {
		t.Fatalf("response-loss worker key = %#v, want %#v", started.Worker.WorkerKey, key)
	}
}

func TestCodexIgnoresThreadIdleAsCompletion(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "Codex idle diagnostic")
	reads := len(application.snapshot().reads)
	payload, err := json.Marshal(map[string]any{
		"threadId": started.Worker.CodexThreadID,
		"status":   map[string]string{"type": "idle"},
	})
	if err != nil {
		t.Fatal(err)
	}
	application.notifications <- appserver.Notification{
		Method: "thread/status/changed", Params: payload,
	}
	time.Sleep(20 * time.Millisecond)
	worker, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != store.WorkerRunning || len(application.snapshot().reads) != reads {
		t.Fatalf("Codex idle changed worker = %#v, reads = %#v", worker, application.snapshot().reads)
	}
}

func TestHostSpawnsFromPreparedWorkspaceWithRepositoryRuntimeBoundary(t *testing.T) {
	application := newFakeApplication()
	workspaceID := "123e4567-e89b-42d3-a456-42661417440a"
	agentID := "123e4567-e89b-42d3-a456-42661417440b"
	var workspacePath string
	host, state, _ := newTestHostWithStateSetup(t, 2, "", func(state *store.PeerStore, root string) {
		workspacePath = filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		head := initializeTestRepository(t, workspacePath)
		recordPreparedWorkspace(t, state, workspaceID, workspacePath, head)
	}, application)
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "prepared", Prompt: "inspect and modify the synchronized repository",
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.Worker.WorkspaceID != workspaceID || started.Worker.WorkspacePath != workspacePath ||
		started.Worker.WorkingDirectory != "nested" || started.Worker.Status != store.WorkerRunning {
		t.Fatalf("prepared worker = %#v", started.Worker)
	}
	record := application.snapshot()
	if len(record.starts) != 1 || record.starts[0].CWD != filepath.Join(workspacePath, "nested") ||
		!reflect.DeepEqual(record.starts[0].RuntimeWorkspaceRoots, []string{workspacePath}) {
		t.Fatalf("prepared app-server start = %#v", record.starts)
	}
	prepared, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Status != store.PreparedWorkspaceClaimed || prepared.ClaimedAgentID != agentID {
		t.Fatalf("claimed prepared workspace = %#v", prepared)
	}
	_, err = host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417440c",
		ParentAgentID: testParentID, TaskName: "reuse", Prompt: "reuse workspace",
		WorkspaceID: workspaceID,
	})
	if !errors.Is(err, store.ErrWorkerReservationConflict) {
		t.Fatalf("second workspace claim = %v, want conflict", err)
	}
}

func TestHostRejectsPreparedWorkingDirectorySymlinkEscape(t *testing.T) {
	application := newFakeApplication()
	workspaceID := "123e4567-e89b-42d3-a456-42661417440d"
	var workspacePath string
	host, _, _ := newTestHostWithStateSetup(t, 1, "", func(state *store.PeerStore, root string) {
		workspacePath = filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		head := initializeTestRepository(t, workspacePath)
		recordPreparedWorkspace(t, state, workspaceID, workspacePath, head)
	}, application)
	if err := os.RemoveAll(filepath.Join(workspacePath, "nested")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(workspacePath, "nested")); err != nil {
		t.Skipf("creating a directory symlink is unavailable: %v", err)
	}
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417440e",
		ParentAgentID: testParentID, TaskName: "escape", Prompt: "must not escape",
		WorkspaceID: workspaceID,
	})
	if err == nil || !strings.Contains(err.Error(), "working directory") {
		t.Fatalf("symlink escape Spawn() = %v", err)
	}
	if got := application.snapshot(); len(got.starts) != 0 {
		t.Fatalf("app-server started after symlink escape: %#v", got)
	}
}

func TestHostDirectPreparationRecoversOrphanAndRevalidatesReadyWorkspace(t *testing.T) {
	host, state, _ := newTestHost(t, 1)
	gitURL, sourcePath := createHostedTestRepository(t)
	source := control.NewRootPrincipal(
		testControllerID, testTreeID, testParentID, testDeviceID,
	).Identity()
	workspaceID := "123e4567-e89b-42d3-a456-42661417440f"
	inspected, err := host.InspectWorkspace(context.Background(), WorkspaceInspectRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.InspectWorkspaceParams{
			SyncID: workspaceID, GitURL: gitURL,
			SourcePath: filepath.Join(sourcePath, "nested"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(host.workspaceRoot.Name(), workspaceSyncName(testTreeID, workspaceID))
	if err := os.Mkdir(orphanPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanPath, "orphan"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := WorkspacePrepareRequest{
		TreeID: testTreeID, Source: source,
		Params: protocol.PrepareWorkspaceParams{
			WorkspaceID: workspaceID, SourceAgentID: source.AgentID,
			SourceDeviceID: source.DeviceID, Manifest: inspected.Manifest,
		},
	}
	prepared, err := host.PrepareWorkspace(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Outcome != protocol.WorkspacePrepareReady ||
		prepared.Strategy != protocol.WorkspaceStrategyDirect {
		t.Fatalf("prepared result = %#v", prepared)
	}
	if _, err := os.Stat(filepath.Join(orphanPath, "orphan")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan marker survived preparation: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(orphanPath, "nested", "source.txt")); err != nil ||
		string(data) != "source\n" {
		t.Fatalf("prepared source = %q, %v", data, err)
	}
	repeated, err := host.PrepareWorkspace(context.Background(), request)
	if err != nil || !reflect.DeepEqual(repeated, prepared) {
		t.Fatalf("idempotent direct preparation = %#v, %v", repeated, err)
	}
	if err := os.WriteFile(filepath.Join(orphanPath, "nested", "source.txt"), []byte("tampered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := host.PrepareWorkspace(context.Background(), request); err == nil ||
		!strings.Contains(err.Error(), "no longer clean") {
		t.Fatalf("tampered prepared workspace retry = %v", err)
	}
	stored, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
		ControllerID: testControllerID, TreeID: testTreeID, WorkspaceID: workspaceID,
	})
	if err != nil || stored.Status != store.PreparedWorkspaceReady {
		t.Fatalf("stored direct workspace = %#v, %v", stored, err)
	}
}

func TestHostScopesPreparedWorkspacePathsByTree(t *testing.T) {
	host, state, _ := newTestHost(t, 1)
	gitURL, sourcePath := createHostedTestRepository(t)
	workspaceID := "123e4567-e89b-42d3-a456-4266141744a0"
	trees := []struct {
		treeID  string
		agentID string
	}{
		{treeID: testTreeID, agentID: "123e4567-e89b-42d3-a456-4266141744a1"},
		{treeID: "123e4567-e89b-42d3-a456-4266141744a2", agentID: "123e4567-e89b-42d3-a456-4266141744a3"},
	}
	paths := make([]string, 0, len(trees))
	for _, current := range trees {
		source := control.NewRootPrincipal(
			testControllerID, current.treeID, current.agentID, testDeviceID,
		).Identity()
		inspected, err := host.InspectWorkspace(context.Background(), WorkspaceInspectRequest{
			TreeID: current.treeID, Source: source,
			Params: protocol.InspectWorkspaceParams{
				SyncID: workspaceID, GitURL: gitURL,
				SourcePath: filepath.Join(sourcePath, "nested"),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := host.PrepareWorkspace(context.Background(), WorkspacePrepareRequest{
			TreeID: current.treeID, Source: source,
			Params: protocol.PrepareWorkspaceParams{
				WorkspaceID: workspaceID, SourceAgentID: source.AgentID,
				SourceDeviceID: source.DeviceID, Manifest: inspected.Manifest,
			},
		}); err != nil {
			t.Fatal(err)
		}
		stored, err := state.GetPreparedWorkspace(context.Background(), store.PreparedWorkspaceKey{
			ControllerID: testControllerID, TreeID: current.treeID, WorkspaceID: workspaceID,
		})
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, stored.WorkspacePath)
	}
	if paths[0] == paths[1] {
		t.Fatalf("two trees shared prepared workspace path %q", paths[0])
	}
	for _, path := range paths {
		if data, err := os.ReadFile(filepath.Join(path, "nested", "source.txt")); err != nil ||
			string(data) != "source\n" {
			t.Fatalf("tree-scoped workspace %q = %q, %v", path, data, err)
		}
	}
}

func TestHostStartupToleratesClaimedWorkspaceContentChanges(t *testing.T) {
	workspaceID := "123e4567-e89b-42d3-a456-4266141744a4"
	agentID := "123e4567-e89b-42d3-a456-4266141744a5"
	var reservation store.WorkerReservation
	host, _, _ := newTestHostWithStateSetup(t, 2, "", func(state *store.PeerStore, root string) {
		workspacePath := filepath.Join(root, workspaceSyncName(testTreeID, workspaceID))
		head := initializeTestRepository(t, workspacePath)
		recordPreparedWorkspace(t, state, workspaceID, workspacePath, head)
		reservation = store.WorkerReservation{
			WorkerKey: store.WorkerKey{
				ControllerID: testControllerID, TreeID: testTreeID, AgentID: agentID,
			},
			ParentAgentID: testParentID, DeviceID: testDeviceID, TaskName: "claimed",
			PromptDigest: strings.Repeat("a", 64), WorkspaceID: workspaceID,
			WorkspacePath: workspacePath, WorkingDirectory: "nested",
			ProfileVersion: workerProfileVersion,
		}
		if _, err := state.ReserveWorkerStartWithWorkspace(
			context.Background(), reservation, 2, time.Unix(1_700_000_001, 0),
		); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(workspacePath, "nested")); err != nil {
			t.Fatal(err)
		}
	}, newFakeApplication())
	if err := host.prepareWorkerWorkspace(context.Background(), reservation); err == nil ||
		!strings.Contains(err.Error(), "working directory") {
		t.Fatalf("changed claimed workspace validation = %v", err)
	}
	if started := spawnTestWorker(
		t, host, "123e4567-e89b-42d3-a456-4266141744a6", "unrelated",
	); started.Worker.Status != store.WorkerRunning {
		t.Fatalf("unrelated worker after claimed workspace drift = %#v", started.Worker)
	}
}

func TestHostRejectsSpawnRetryWithDifferentPrompt(t *testing.T) {
	application := newFakeApplication()
	host, _, _ := newTestHost(t, 1, application)
	agentID := "123e4567-e89b-42d3-a456-426614174413"
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "digest", Prompt: "first prompt",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: "digest", Prompt: "different prompt",
	}); !errors.Is(err, store.ErrWorkerReservationConflict) {
		t.Fatalf("changed prompt Spawn() error = %v, want reservation conflict", err)
	}
}

func TestHostBoundsSpawnAndFollowupPromptItems(t *testing.T) {
	host, _, _ := newTestHost(t, 1, newFakeApplication())
	oversized := strings.Repeat("x", maximumPromptBytes+1)
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174414",
		ParentAgentID: testParentID, TaskName: "oversized", Prompt: oversized,
	}); err == nil {
		t.Fatal("Spawn accepted an oversized model-visible item")
	}
	if _, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(),
		Key: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174414",
		},
		Message: oversized,
	}); err == nil {
		t.Fatal("Followup accepted an oversized model-visible item")
	}
}

func TestHostCallerCancellationDoesNotRetireSharedAppServer(t *testing.T) {
	application := newFakeApplication()
	application.turnStartGate = make(chan struct{})
	application.turnStartStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		started StartedTurn
		err     error
	}, 1)
	go func() {
		started, err := host.Spawn(ctx, SpawnRequest{
			TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174419",
			ParentAgentID: testParentID, TaskName: "detached", Prompt: "detached prompt",
		})
		done <- struct {
			started StartedTurn
			err     error
		}{started: started, err: err}
	}()
	select {
	case <-application.turnStartStarted:
	case <-time.After(time.Second):
		t.Fatal("turn/start did not begin")
	}
	cancel()
	select {
	case result := <-done:
		t.Fatalf("Spawn returned on caller cancellation: %#v, %v", result.started, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("caller cancellation retired shared app-server %d times", got)
	}
	close(application.turnStartGate)
	select {
	case result := <-done:
		if result.err != nil || result.started.Worker.Status != store.WorkerRunning {
			t.Fatalf("detached Spawn() = %#v, %v", result.started, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("detached Spawn did not finish")
	}
}

func TestHostSerializesEarlyCompletionAndColdResumesAfterCrash(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174420", "fast")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	firstApplication.crash(errors.New("test app-server crash"))
	waitForClientRetirement(t, host, firstApplication)
	followupRequest := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "follow-up",
	}
	followup, err := host.Followup(context.Background(), followupRequest)
	if err != nil {
		t.Fatal(err)
	}
	if followup.Worker.Status != store.WorkerRunning ||
		followup.Worker.CodexThreadID != started.Worker.CodexThreadID ||
		followup.Receipt.Outcome != store.WorkerOutcomeStarted {
		t.Fatalf("follow-up worker = %#v", followup.Worker)
	}
	replayed, err := host.Followup(context.Background(), followupRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != followup {
		t.Fatalf("follow-up replay = %#v, want %#v", replayed, followup)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || record.resumes[0].ThreadID != started.Worker.CodexThreadID ||
		record.resumes[0].Path != "" || !record.resumes[0].ExcludeTurns ||
		record.preflights != 1 || len(record.turns) != 1 {
		t.Fatalf("cold-resume calls = %#v", record)
	}
	assertManagedProfile(t, record.resumes[0].Config, paths, followup.Worker)
	if _, err := os.Stat(filepath.Join(paths.codexHome, "config.toml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed Codex config.toml exists: %v", err)
	}
}

func TestTraeXColdResumeUsesManagedRolloutPath(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadID = newTestID()
	secondApplication := newFakeApplication()
	host, state, _ := newTestHostForKind(
		t, hostkind.TraeX, 1, firstApplication, secondApplication,
	)
	rolloutPath := writeTestManagedRollout(t, host.rolloutHome, firstApplication.threadID)
	firstApplication.threadPath = rolloutPath
	secondApplication.threadPath = rolloutPath

	started := spawnTestWorker(t, host, newTestID(), "TraeX cold resume")
	firstApplication.notifyCompletion(
		started.Worker.CodexThreadID, started.Worker.ActiveTurnID, "completed",
	)
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.crash(errors.New("force TraeX cold resume"))
	waitForClientRetirement(t, host, firstApplication)

	followup, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "resume from the managed TraeX rollout",
	})
	if err != nil || followup.Worker.Status != store.WorkerRunning {
		t.Fatalf("TraeX cold Followup() = %#v, %v", followup, err)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || record.resumes[0].Path != rolloutPath {
		t.Fatalf("TraeX cold-resume calls = %#v, want path %q", record.resumes, rolloutPath)
	}
}

func TestTraeXPreparedIntentReconciliationUsesManagedRolloutPath(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadID = newTestID()
	firstApplication.turnStartErr = context.DeadlineExceeded
	secondApplication := newFakeApplication()
	host, state, paths := newTestHostForKind(
		t, hostkind.TraeX, 1, firstApplication, secondApplication,
	)
	paths.allowCloseError.Store(true)
	rolloutPath := writeTestManagedRollout(t, host.rolloutHome, firstApplication.threadID)
	firstApplication.threadPath = rolloutPath
	secondApplication.threadPath = rolloutPath

	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "TraeX prepared reconciliation", Prompt: "prepare then lose the response",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ambiguous TraeX Spawn() error = %v", err)
	}
	intent, err := state.GetPreparedWorkerTurnStartIntent(
		context.Background(), started.Worker.WorkerKey,
	)
	if err != nil || intent.Rollout.Path != rolloutPath {
		t.Fatalf("prepared TraeX intent = %#v, %v", intent, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(secondApplication.snapshot().resumes) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || record.resumes[0].Path != rolloutPath {
		t.Fatalf(
			"TraeX prepared reconciliation resumes = %#v, want path %q",
			record.resumes,
			rolloutPath,
		)
	}
}

func TestTraeXBoundIntentRecoveryUsesManagedRolloutPath(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadID = newTestID()
	host, state, _ := newTestHostForKind(t, hostkind.TraeX, 1, firstApplication)
	rolloutPath := writeTestManagedRollout(t, host.rolloutHome, firstApplication.threadID)
	firstApplication.threadPath = rolloutPath
	started := spawnTestWorker(t, host, newTestID(), "TraeX bound recovery")
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil || intent.Rollout.Path != rolloutPath {
		t.Fatalf("bound TraeX intent = %#v, %v", intent, err)
	}

	replacement := newFakeApplication()
	replacement.threadPath = rolloutPath
	replacement.threadReadHook = mirrorFakeThreadTurns(firstApplication, nil)
	if err := host.recoverBoundTurnIntent(
		context.Background(), replacement, intent, started.Worker,
	); err != nil {
		t.Fatal(err)
	}
	record := replacement.snapshot()
	if len(record.resumes) != 1 || record.resumes[0].Path != rolloutPath {
		t.Fatalf("TraeX bound recovery resumes = %#v, want path %q", record.resumes, rolloutPath)
	}
}

func TestHostDrainsCompletionBeforeRecoveringClosedClient(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.crashAfterComplete = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174423", "drain-crash")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
}

func TestHostCompletionFencePrecedesRecovery(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	firstApplication.crashAfterComplete = true
	firstApplication.closeGate = make(chan struct{})
	firstApplication.closeStarted = make(chan struct{})
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	completionStarted := make(chan struct{})
	releaseCompletion := make(chan struct{})
	realApply := host.applyCompletion
	host.applyCompletion = func(completed turnCompletedNotification) error {
		close(completionStarted)
		<-releaseCompletion
		return realApply(completed)
	}
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174424", "fenced")
	select {
	case <-completionStarted:
	case <-time.After(time.Second):
		t.Fatal("completion processing did not start")
	}
	select {
	case <-firstApplication.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("client retirement did not reach Close")
	}
	close(releaseCompletion)
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	blockedContext, blockedCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, blockedErr := host.Followup(blockedContext, FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "while recovery is fenced",
	})
	blockedCancel()
	if !errors.Is(blockedErr, context.DeadlineExceeded) {
		t.Fatalf("Followup while recovery was fenced = %v, want deadline exceeded", blockedErr)
	}
	if got := secondApplication.snapshot(); len(got.resumes) != 0 {
		t.Fatalf("replacement app-server started before recovery was released: %#v", got)
	}
	close(firstApplication.closeGate)
	waitForClientRetirement(t, host, firstApplication)
	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "after fence",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerRunning)
}

func TestHostSpawnReturnsOnCallerCancellationWhileRecoveryContinues(t *testing.T) {
	application := newFakeApplication()
	transportErr := errors.New("injected app-server transport failure")
	application.threadStartErr = transportErr
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := host.Spawn(ctx, SpawnRequest{
			TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174425",
			ParentAgentID: testParentID, TaskName: "caller-cancel", Prompt: "caller cancel prompt",
		})
		result <- err
	}()
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server recovery did not start")
	}
	cancel()
	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("Spawn did not return after caller cancellation")
	}
	if !errors.Is(err, transportErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Spawn() error = %v, want transport error and caller cancellation", err)
	}
	close(application.closeGate)
	waitForClientRetirement(t, host, application)
}

func TestHostRetriesDeferredCompletionWithoutConsumerDeadlock(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	firstAttempt := make(chan struct{})
	retryStarted := make(chan struct{})
	releaseRetry := make(chan struct{})
	realApply := host.applyCompletion
	var attempts atomic.Int32
	host.applyCompletion = func(completed turnCompletedNotification) error {
		switch attempts.Add(1) {
		case 1:
			close(firstAttempt)
			return errors.New("injected completion write failure")
		case 2:
			close(retryStarted)
			<-releaseRetry
			return realApply(completed)
		default:
			return realApply(completed)
		}
	}
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174425", "retry")
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("first completion attempt did not run")
	}
	select {
	case <-retryStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("deferred completion retry did not run")
	}
	followupDone := make(chan error, 1)
	go func() {
		_, err := host.Followup(context.Background(), FollowupRequest{
			OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "after retry",
		})
		followupDone <- err
	}()
	select {
	case err := <-followupDone:
		t.Fatalf("Followup returned while completion retry was blocked: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseRetry)
	select {
	case err := <-followupDone:
		if !errors.Is(err, ErrWorkerNotIdle) {
			t.Fatalf("Followup after capture fence error = %v, want ErrWorkerNotIdle", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Followup remained blocked after completion retry")
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("completion attempts = %d, want at least 2", got)
	}
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if _, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	); err != nil {
		t.Fatal(err)
	}
	followup, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "after publication",
	})
	if err != nil || followup.Worker.Status != store.WorkerRunning {
		t.Fatalf("Followup after result ACK = %#v, %v", followup, err)
	}
}

func TestHostFailsClosedWhenAppServerExitIsUnconfirmed(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.closeErr = errors.Join(
		appserver.ErrCloseTimeout,
		appserver.ErrProcessExitUnconfirmed,
	)
	secondApplication := newFakeApplication()
	host, _, paths := newTestHost(t, 1, firstApplication, secondApplication)
	paths.allowCloseError.Store(true)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174426", "unconfirmed")
	firstApplication.crash(errors.New("test app-server crash"))
	select {
	case <-host.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("host did not fail after an unconfirmed app-server exit")
	}
	if !errors.Is(host.Err(), appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("host error = %v, want ErrProcessExitUnconfirmed", host.Err())
	}
	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "must not restart",
	})
	if !errors.Is(err, appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("Followup error = %v, want ErrProcessExitUnconfirmed", err)
	}
	if got := secondApplication.snapshot(); len(got.resumes) != 0 || len(got.starts) != 0 {
		t.Fatalf("replacement app-server started after unconfirmed exit: %#v", got)
	}
}

func TestHostCloseDrainsAcceptedCompletion(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174421", "drain")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatal(err)
	}
	worker, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status != store.WorkerFinalizing || worker.FinalTarget != store.WorkerIdle {
		t.Fatalf("worker after drained Close() = %#v", worker)
	}
}

func TestHostCloseContinuesAfterCallerTimeout(t *testing.T) {
	application := newFakeApplication()
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, _, _ := newTestHost(t, 1, application)
	spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174422", "close-timeout")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := host.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v, want deadline exceeded", err)
	}
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("managed app-server Close did not start")
	}
	close(application.closeGate)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestHostFailsClosedWhenWorkerMCPInventoryIsWrong(t *testing.T) {
	tests := map[string]func(*fakeApplication){
		"unexpected tool": func(application *fakeApplication) {
			application.tools = []string{
				"send_message",
				"send_upstream_message",
				"wait_agent",
				"wait_for_upstream_message",
			}
		},
		"extra server": func(application *fakeApplication) {
			application.extraServers = []mcpServerStatus{{Name: "delegation"}}
		},
		"wrong auth": func(application *fakeApplication) {
			application.authStatus = "authenticated"
		},
		"resource": func(application *fakeApplication) {
			application.resources = []json.RawMessage{json.RawMessage(`{"uri":"file:///unexpected"}`)}
		},
		"resource template": func(application *fakeApplication) {
			application.resourceTemplates = []json.RawMessage{json.RawMessage(`{"uriTemplate":"file:///{path}"}`)}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			blockedApplication := newFakeApplication()
			mutate(blockedApplication)
			cleanApplication := newFakeApplication()
			host, state, _ := newTestHost(t, 1, blockedApplication, cleanApplication)
			started, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174430",
				ParentAgentID: testParentID, TaskName: "blocked", Prompt: "blocked prompt",
			})
			if !errors.Is(err, ErrMCPInjectionBlocked) {
				t.Fatalf("blocked Spawn() error = %v, want ErrMCPInjectionBlocked", err)
			}
			failed, err := state.GetWorker(context.Background(), started.Worker.WorkerKey)
			if err != nil {
				t.Fatal(err)
			}
			if failed.Status != store.WorkerFailed || failed.FailureCode != "mcp_injection_blocked" {
				t.Fatalf("failed worker = %#v", failed)
			}
			if got := blockedApplication.closeCount(); got != 1 {
				t.Fatalf("blocked app-server Close calls = %d, want 1", got)
			}
			if _, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174431",
				ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement prompt",
			}); err != nil {
				t.Fatalf("clean replacement Spawn() error = %v", err)
			}
			if got := cleanApplication.snapshot(); len(got.starts) != 1 || got.preflights != 1 || len(got.turns) != 1 {
				t.Fatalf("clean replacement calls = %#v", got)
			}
		})
	}
}

func TestHostFailsClosedAfterAmbiguousThreadStart(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadStartErr = context.DeadlineExceeded
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "ambiguous thread", Prompt: "must run once",
	}
	started, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Spawn() error = %v, want deadline exceeded", err)
	}
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "thread_start_ambiguous" || failed.CodexThreadID != "" {
		t.Fatalf("ambiguous thread worker = %#v", failed)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, ErrWorkerFailed) {
		t.Fatalf("retry Spawn() error = %v, want ErrWorkerFailed", err)
	}
	if got := len(firstApplication.snapshot().starts); got != 1 {
		t.Fatalf("first app-server thread/start calls = %d, want 1", got)
	}
	if got := len(secondApplication.snapshot().starts); got != 0 {
		t.Fatalf("replacement app-server thread/start calls = %d, want 0", got)
	}
}

func TestHostPersistsFailedTurnOutcome(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.completionStatus = "failed"
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174436", "failed-turn")
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "turn_failed" {
		t.Fatalf("failed worker = %#v", failed)
	}
}

func TestHostContainsUnknownTurnStatusToWorker(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.completionStatus = "future-status"
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174437", "unknown-status")
	failed := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerFailed)
	if failed.FailureCode != "unsupported_turn_status" {
		t.Fatalf("failed worker = %#v", failed)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unknown turn status retired shared app-server %d times", got)
	}
	application.completeBeforeReturn = false
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174438",
		ParentAgentID: testParentID, TaskName: "after unknown status", Prompt: "after unknown status",
	}); err != nil {
		t.Fatalf("Spawn after unknown status error = %v", err)
	}
}

func TestHostDoesNotRetireSharedAppServerForUnsentRequest(t *testing.T) {
	application := newFakeApplication()
	application.threadStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174439",
		ParentAgentID: testParentID, TaskName: "canceled", Prompt: "canceled",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Spawn() error = %v", err)
	}
	restored := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if restored.CodexThreadID != "" || restored.FailureCode != "" {
		t.Fatalf("restored worker = %#v", restored)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent request retired shared app-server %d times", got)
	}
	application.threadStartErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry Spawn after unsent request = %#v, %v", retried, err)
	}
	if got := len(application.snapshot().starts); got != 2 {
		t.Fatalf("thread/start calls = %d, want 2", got)
	}
}

func TestHostUnsentThreadStartDoesNotRetainSlot(t *testing.T) {
	application := newFakeApplication()
	application.threadStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	first, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174446",
		ParentAgentID: testParentID, TaskName: "abandoned start", Prompt: "abandoned start",
	})
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	application.threadStartErr = nil
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174447",
		ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement",
	}); err != nil {
		t.Fatalf("replacement Spawn() while first worker is pending = %v", err)
	}
}

func TestHostRetriesInitialTurnWhenRequestWasNotWritten(t *testing.T) {
	application := newFakeApplication()
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417443c",
		ParentAgentID: testParentID, TaskName: "unsent initial turn", Prompt: "unsent initial turn",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("initial Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.turnStartErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry initial Spawn() = %#v, %v", retried, err)
	}
	record := application.snapshot()
	if len(record.starts) != 1 || len(record.turns) != 2 || record.preflights != 2 {
		t.Fatalf("retried initial calls = %#v", record)
	}
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent initial turn retired shared app-server %d times", got)
	}
}

func TestHostReadsThreadPathBeforePreparingInitialTurn(t *testing.T) {
	application := newFakeApplication()
	application.threadStartPath = filepath.Join(t.TempDir(), "untrusted-start-response.jsonl")
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "read rollout path")
	record := application.snapshot()
	if len(record.reads) != 1 || record.reads[0].ThreadID != started.Worker.CodexThreadID ||
		record.reads[0].IncludeTurns {
		t.Fatalf("thread/read calls before initial turn = %#v", record.reads)
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Rollout.Status != store.WorkerRolloutUnavailable || intent.Rollout.Path != "" {
		t.Fatalf("intent trusted thread/start path without thread/read = %#v", intent.Rollout)
	}
}

func TestHostBindsInitialRolloutAfterFirstTurnMaterializesPath(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome,
		"sessions",
		"2026",
		"07",
		"27",
		"rollout-2026-07-27T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("thread metadata\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.threadReadHook = func(_ threadReadParams, result *threadResult) {
		if len(application.snapshot().turns) != 0 {
			result.Thread.Path = &rolloutPath
		}
	}
	host.initialRolloutWait = 100 * time.Millisecond

	started := spawnTestWorker(t, host, newTestID(), "materialize initial rollout")
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil {
		t.Fatal(err)
	}
	resolvedRolloutPath, err := filepath.EvalSymlinks(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	want := store.WorkerRolloutLocator{
		Status: store.WorkerRolloutAvailable, CodexHome: host.codexHome,
		Path: resolvedRolloutPath, Offset: 0,
	}
	if intent.Rollout != want {
		t.Fatalf("materialized initial rollout = %#v, want %#v", intent.Rollout, want)
	}
	record := application.snapshot()
	if len(record.reads) != 2 || record.reads[0].IncludeTurns || record.reads[1].IncludeTurns {
		t.Fatalf("initial rollout thread/read calls = %#v", record.reads)
	}
}

func TestHostBoundsBlockedInitialRolloutReadWithoutRetiringClient(t *testing.T) {
	application := newFakeApplication()
	application.readAfterTurnGate = make(chan struct{})
	host, state, _ := newTestHost(t, 1, application)
	host.initialRolloutWait = 50 * time.Millisecond

	startedAt := time.Now()
	started := spawnTestWorker(t, host, newTestID(), "bound initial rollout read")
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("blocked initial rollout read took %s", elapsed)
	}
	if started.Worker.Status != store.WorkerRunning || application.closeCount() != 0 {
		t.Fatalf("blocked initial rollout read = %#v, closes=%d", started, application.closeCount())
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil || intent.Rollout.Status != store.WorkerRolloutUnavailable {
		t.Fatalf("blocked initial rollout intent = %#v, %v", intent, err)
	}
}

func TestHostPreservesLearnedInitialRolloutPathAtWaitDeadline(t *testing.T) {
	for _, test := range []struct {
		name            string
		path            func(testHostPaths, string) string
		create          bool
		wantDiagnostics int
	}{
		{
			name: "lazy file",
			path: func(paths testHostPaths, threadID string) string {
				return filepath.Join(
					paths.codexHome,
					"sessions",
					"rollout-2026-07-27T00-00-00-"+threadID+".jsonl",
				)
			},
		},
		{
			name: "invalid outside path",
			path: func(_ testHostPaths, threadID string) string {
				return filepath.Join(
					t.TempDir(), "rollout-2026-07-27T00-00-00-"+threadID+".jsonl",
				)
			},
			create: true, wantDiagnostics: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newFakeApplication()
			application.threadID = newTestID()
			host, state, paths := newTestHost(t, 1, application)
			rolloutPath := test.path(paths, application.threadID)
			if test.create {
				if err := os.WriteFile(rolloutPath, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			application.threadReadHook = func(_ threadReadParams, result *threadResult) {
				if len(application.snapshot().turns) != 0 {
					result.Thread.Path = &rolloutPath
				}
			}
			host.initialRolloutWait = 30 * time.Millisecond
			reported := &testErrorRecorder{}
			host.reportError = reported.report

			started := spawnTestWorker(t, host, newTestID(), "preserve initial rollout path")
			host.clientMu.Lock()
			loaded := host.loaded[started.Worker.WorkerKey]
			host.clientMu.Unlock()
			diagnostics := reported.snapshot()
			if loaded.Path != rolloutPath || len(diagnostics) != test.wantDiagnostics {
				t.Fatalf("loaded initial rollout = %#v, reported=%#v", loaded, diagnostics)
			}
			intent, err := state.GetWorkerTurnStartIntentByTurn(
				context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
			)
			if err != nil || intent.Rollout.Status != store.WorkerRolloutUnavailable {
				t.Fatalf("deadline initial rollout intent = %#v, %v", intent, err)
			}
		})
	}
}

func TestHostDegradesNonRetirableInitialRolloutReadError(t *testing.T) {
	application := newFakeApplication()
	application.readAfterTurnErr = &appserver.RPCError{Code: -32601, Message: "thread/read unavailable"}
	host, state, _ := newTestHost(t, 1, application)
	host.initialRolloutWait = 100 * time.Millisecond
	reported := &testErrorRecorder{}
	host.reportError = reported.report

	started := spawnTestWorker(t, host, newTestID(), "degraded initial rollout read")
	if started.Worker.Status != store.WorkerRunning || application.closeCount() != 0 {
		t.Fatalf("degraded initial rollout read = %#v, closes=%d", started, application.closeCount())
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil || intent.Rollout.Status != store.WorkerRolloutUnavailable {
		t.Fatalf("degraded initial rollout intent = %#v, %v", intent, err)
	}
	diagnostics := reported.snapshot()
	if len(diagnostics) != 1 || !strings.Contains(diagnostics[0].Error(), "thread/read unavailable") {
		t.Fatalf("reported initial rollout errors = %#v", diagnostics)
	}
}

func TestHostRetriesTransientInitialRolloutReadError(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	application.readAfterTurnErrors = []error{
		&appserver.RPCError{Code: -32603, Message: "rollout is temporarily empty"},
	}
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome,
		"sessions",
		"rollout-2026-07-27T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("thread metadata\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.threadReadHook = func(_ threadReadParams, result *threadResult) {
		if len(application.snapshot().turns) != 0 {
			result.Thread.Path = &rolloutPath
		}
	}
	host.initialRolloutWait = 200 * time.Millisecond
	reported := &testErrorRecorder{}
	host.reportError = reported.report

	started := spawnTestWorker(t, host, newTestID(), "retry transient initial rollout read")
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, started.Worker.ActiveTurnID,
	)
	if err != nil || intent.Rollout.Status != store.WorkerRolloutAvailable {
		t.Fatalf("retried initial rollout intent = %#v, %v", intent, err)
	}
	if diagnostics := reported.snapshot(); len(diagnostics) != 0 {
		t.Fatalf("transient initial rollout diagnostics = %#v", diagnostics)
	}
}

func TestHostReportsMissingManagedHomeButNotLazyInitialRollout(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	reported := &testErrorRecorder{}
	host.reportError = reported.report
	client, err := host.ensureClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	worker := store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID, TreeID: testTreeID, AgentID: newTestID(),
		},
		CodexThreadID: newTestID(),
	}
	rolloutPath := filepath.Join(
		paths.codexHome,
		"sessions",
		"rollout-2026-07-27T00-00-00-"+worker.CodexThreadID+".jsonl",
	)
	host.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, &rolloutPath)
	if rollout := host.rolloutLocator(client, worker); rollout.Status != store.WorkerRolloutUnavailable || len(reported.snapshot()) != 0 {
		t.Fatalf("lazy initial rollout = %#v, reported=%#v", rollout, reported.snapshot())
	}
	if err := os.Remove(paths.codexHome); err != nil {
		t.Fatal(err)
	}
	if rollout := host.rolloutLocator(client, worker); rollout.Status != store.WorkerRolloutUnavailable || len(reported.snapshot()) != 1 {
		t.Fatalf("missing managed home rollout = %#v, reported=%#v", rollout, reported.snapshot())
	}
}

func TestHostRefreshesThreadPathBeforeLoadedFollowupTurn(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "refresh followup rollout path")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	rolloutPath := filepath.Join(
		paths.codexHome, "sessions", "2026", "07", "27",
		"rollout-2026-07-27T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.mu.Lock()
	application.threadPath = rolloutPath
	application.completeBeforeReturn = false
	application.mu.Unlock()

	followup, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "capture the loaded follow-up rollout",
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), followup.Worker.WorkerKey, followup.Worker.ActiveTurnID,
	)
	if err != nil {
		t.Fatal(err)
	}
	resolvedRolloutPath, err := filepath.EvalSymlinks(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Rollout.Status != store.WorkerRolloutAvailable ||
		intent.Rollout.Path != resolvedRolloutPath || intent.Rollout.Offset != 3 {
		t.Fatalf("loaded follow-up rollout locator = %#v", intent.Rollout)
	}
	record := application.snapshot()
	if len(record.reads) != 2 || record.reads[1].ThreadID != application.threadID ||
		record.reads[1].IncludeTurns {
		t.Fatalf("thread/read calls before loaded follow-up = %#v", record.reads)
	}
	segment := testManagedRolloutLine("task_started", followup.Worker.ActiveTurnID) +
		testManagedRolloutLine("task_complete", followup.Worker.ActiveTurnID)
	if err := appendSyncedFile(rolloutPath, segment); err != nil {
		t.Fatal(err)
	}
	application.notifyCompletion(
		followup.Worker.CodexThreadID, followup.Worker.ActiveTurnID, "completed",
	)
	waitWorkerStatus(t, state, followup.Worker.WorkerKey, store.WorkerIdle)
}

func TestHostRestoresPendingWhenResultBacklogRejectsInitialTurn(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	fillResultCapacity(t, host, state, 4, protocol.MaximumResultPackageBytes)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "result_backlog", Prompt: "do not dispatch without result capacity",
	}
	for attempt := range 2 {
		started, err := host.Spawn(context.Background(), request)
		if !errors.Is(err, store.ErrResultPackageQuota) ||
			started.Worker.Status != store.WorkerPending {
			t.Fatalf("backlogged Spawn attempt %d = %#v, %v", attempt, started, err)
		}
		if host.Err() != nil {
			t.Fatalf("result backlog failed host: %v", host.Err())
		}
	}
	if got := len(application.snapshot().turns); got != 0 {
		t.Fatalf("result backlog wrote %d turn/start requests", got)
	}
}

func TestHostRestoresIdleAndFailsFollowupReceiptOnResultBacklog(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, newTestID(), "followup result backlog")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	noWorkspaceReservation := int64(
		protocol.MaximumResultManifestBytes + protocol.MaximumResultRolloutBytes,
	)
	fillResultCapacity(t, host, state, 31, noWorkspaceReservation)
	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "retry after result backlog drains",
	}
	result, err := host.Followup(context.Background(), request)
	if !errors.Is(err, store.ErrResultPackageQuota) ||
		result.Worker.Status != store.WorkerIdle ||
		result.Receipt.Status != store.WorkerOperationFailed ||
		result.Receipt.Outcome != store.WorkerOutcomeFailed ||
		result.Receipt.FailureCode != operationFailureResultBacklog {
		t.Fatalf("backlogged Followup = %#v, %v", result, err)
	}
	if got := len(application.snapshot().turns); got != 1 {
		t.Fatalf("backlogged follow-up wrote another turn/start: %d calls", got)
	}
	replayed, err := host.Followup(context.Background(), request)
	if err != nil || replayed != result || len(application.snapshot().turns) != 1 {
		t.Fatalf("backlogged follow-up replay = %#v, %v; want %#v", replayed, err, result)
	}
}

func TestAmbiguousTurnStartWithoutObservedTurnRetainsPreparedIntent(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.turnStartErr = context.DeadlineExceeded
	secondApplication := newFakeApplication()
	host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
	paths.allowCloseError.Store(true)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "ambiguous turn", Prompt: "ambiguous turn prompt",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ambiguous initial Spawn() error = %v", err)
	}
	if first.Worker.Status != store.WorkerReady {
		first.Worker = waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerReady)
	}
	_, err = host.Spawn(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "outcome remains ambiguous") {
		t.Fatalf("ambiguous Spawn() reconciliation error = %v", err)
	}
	intent, err := state.GetPreparedWorkerTurnStartIntent(
		context.Background(), first.Worker.WorkerKey,
	)
	if err != nil || intent.State != store.WorkerTurnStartPrepared {
		t.Fatalf("retained turn intent = %#v, %v", intent, err)
	}
	captures, err := state.ListPendingResultCaptures(
		context.Background(), testControllerID, testDeviceID, 10,
	)
	if err != nil || len(captures) != 1 || captures[0].PackageID != intent.PackageID {
		t.Fatalf("retained result reservation = %#v, %v", captures, err)
	}
	if !errors.Is(host.Err(), errTurnStartAmbiguous) {
		t.Fatalf("host fatal error = %v, want errTurnStartAmbiguous", host.Err())
	}
	_, err = host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "blocked after ambiguity", Prompt: "must not start",
	})
	if !errors.Is(err, errTurnStartAmbiguous) {
		t.Fatalf("Spawn() after ambiguous recovery error = %v", err)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || len(record.reads) != 1 || !record.reads[0].IncludeTurns ||
		len(record.turns) != 0 {
		t.Fatalf("ambiguous reconciliation calls = %#v", record)
	}
}

func TestAmbiguousTurnStartConflictingEvidenceFailsHost(t *testing.T) {
	tests := map[string]func(*fakeApplication, *fakeApplication){
		"multiple turns": func(first, replacement *fakeApplication) {
			replacement.threadReadHook = mirrorFakeThreadTurns(first, func(turns []turn) []turn {
				return append(turns, turn{ID: newTestID(), Status: "inProgress"})
			})
		},
		"invalid turn ID": func(first, replacement *fakeApplication) {
			replacement.threadReadHook = mirrorFakeThreadTurns(first, func(turns []turn) []turn {
				turns[0].ID = "invalid"
				return turns
			})
		},
		"unsupported turn status": func(first, replacement *fakeApplication) {
			replacement.threadReadHook = mirrorFakeThreadTurns(first, func(turns []turn) []turn {
				turns[0].Status = "paused"
				return turns
			})
		},
		"workspace mismatch": func(_, application *fakeApplication) {
			application.resumeResultHook = func(result *threadResult) {
				result.CWD = filepath.Join(result.CWD, "unexpected")
			}
		},
		"blocked worker MCP": func(_, application *fakeApplication) {
			application.tools = []string{
				"send_message",
				"send_upstream_message",
				"wait_agent",
				"wait_for_upstream_message",
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			firstApplication := newFakeApplication()
			firstApplication.turnStartErr = context.DeadlineExceeded
			firstApplication.turnStartResponseLost = true
			secondApplication := newFakeApplication()
			mutate(firstApplication, secondApplication)
			host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
			paths.allowCloseError.Store(true)
			request := SpawnRequest{
				TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
				TaskName: "conflicting turn evidence", Prompt: "execute exactly once",
			}
			first, err := host.Spawn(context.Background(), request)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("response-loss Spawn() error = %v", err)
			}
			if !errors.Is(host.Err(), errTurnStartAmbiguous) {
				t.Fatalf("host error after automatic reconciliation = %v", host.Err())
			}
			intent, err := state.GetPreparedWorkerTurnStartIntent(
				context.Background(), first.Worker.WorkerKey,
			)
			if err != nil || intent.State != store.WorkerTurnStartPrepared {
				t.Fatalf("retained turn intent = %#v, %v", intent, err)
			}
			captures, err := state.ListPendingResultCaptures(
				context.Background(), testControllerID, testDeviceID, 10,
			)
			if err != nil || len(captures) != 1 || captures[0].PackageID != intent.PackageID {
				t.Fatalf("retained result reservation = %#v, %v", captures, err)
			}
		})
	}
}

func TestAmbiguousFollowupMissingPreviousBoundaryFailsHost(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	secondApplication.threadReadHook = mirrorFakeThreadTurns(
		firstApplication,
		func(turns []turn) []turn { return turns[1:] },
	)
	host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
	paths.allowCloseError.Store(true)
	started := spawnTestWorker(t, host, newTestID(), "missing previous boundary")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	firstApplication.mu.Lock()
	firstApplication.turnStartErr = context.DeadlineExceeded
	firstApplication.turnStartResponseLost = true
	firstApplication.mu.Unlock()
	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "execute this follow-up exactly once",
	}
	if _, err := host.Followup(context.Background(), request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("response-loss Followup() error = %v", err)
	}
	firstApplication.mu.Lock()
	turns := append([]turn(nil), firstApplication.threadTurns[started.Worker.CodexThreadID]...)
	firstApplication.mu.Unlock()
	if len(turns) != 2 {
		t.Fatalf("written follow-up history = %#v", turns)
	}
	if !errors.Is(host.Err(), errTurnStartAmbiguous) {
		t.Fatalf("host fatal error = %v, want errTurnStartAmbiguous", host.Err())
	}
	intent, err := state.GetPreparedWorkerTurnStartIntentByOperation(
		context.Background(), testControllerID, request.OperationID,
	)
	if err != nil || intent.State != store.WorkerTurnStartPrepared ||
		intent.PreviousTurnID != turns[0].ID {
		t.Fatalf("retained follow-up intent = %#v, %v", intent, err)
	}
}

func TestAmbiguousTurnStartBindsObservedTurnWithoutRedispatch(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadID = newTestID()
	firstApplication.turnStartErr = context.DeadlineExceeded
	firstApplication.turnStartResponseLost = true
	secondApplication := newFakeApplication()
	host, state, paths := newTestHost(t, 1, firstApplication, secondApplication)
	rolloutPath := filepath.Join(
		paths.codexHome,
		"sessions",
		"2026",
		"07",
		"27",
		"rollout-2026-07-27T00-00-00-"+firstApplication.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("thread metadata\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mirrorTurns := mirrorFakeThreadTurns(firstApplication, nil)
	secondApplication.threadReadHook = func(params threadReadParams, result *threadResult) {
		mirrorTurns(params, result)
		result.Thread.Path = &rolloutPath
	}
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "lost response", Prompt: "execute exactly once",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("response-loss Spawn() error = %v", err)
	}
	firstApplication.mu.Lock()
	turns := append([]turn(nil), firstApplication.threadTurns[first.Worker.CodexThreadID]...)
	firstApplication.mu.Unlock()
	if len(turns) != 1 {
		t.Fatalf("written turn history = %#v", turns)
	}
	reconciled := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerRunning)
	if reconciled.ActiveTurnID != turns[0].ID {
		t.Fatalf("automatically reconciled worker = %#v", reconciled)
	}
	if got := len(secondApplication.snapshot().turns); got != 0 {
		t.Fatalf("response-loss reconciliation redispatched %d turns", got)
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), reconciled.WorkerKey, turns[0].ID,
	)
	if err != nil || intent.State != store.WorkerTurnStartBound {
		t.Fatalf("reconciled intent = %#v, %v", intent, err)
	}
	resolvedRolloutPath, err := filepath.EvalSymlinks(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	wantRollout := store.WorkerRolloutLocator{
		Status: store.WorkerRolloutAvailable, CodexHome: host.codexHome,
		Path: resolvedRolloutPath, Offset: 0,
	}
	if intent.Rollout != wantRollout {
		t.Fatalf("reconciled initial rollout = %#v, want %#v", intent.Rollout, wantRollout)
	}
	sent, err := host.Send(context.Background(), SendRequest{
		Key: reconciled.WorkerKey, MessageID: newTestID(), Message: "after nil-path recovery",
	})
	if err != nil || sent.Receipt.Outcome != store.WorkerOutcomeSteered {
		t.Fatalf("Send() after nil-path recovery = %#v, %v", sent, err)
	}
	interrupted, err := host.Interrupt(context.Background(), InterruptRequest{
		OperationID: newTestID(), Key: reconciled.WorkerKey,
	})
	if err != nil || interrupted.Receipt.Outcome != store.WorkerOutcomeInterrupted {
		t.Fatalf("Interrupt() after nil-path recovery = %#v, %v", interrupted, err)
	}
	record := secondApplication.snapshot()
	if len(record.steers) != 1 || len(record.interrupts) != 1 {
		t.Fatalf("nil-path recovery operations = %#v", record)
	}
}

func TestCompletionBeforeLostInitialTurnResponseFinalizesWithoutRecovery(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.threadID = newTestID()
	firstApplication.turnStartErr = context.DeadlineExceeded
	firstApplication.turnStartResponseLost = true
	firstApplication.completeThenLose = true
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: newTestID(), ParentAgentID: testParentID,
		TaskName: "completed response loss", Prompt: "complete exactly once",
	}
	started, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("response-loss Spawn() error = %v", err)
	}
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
		outbox.Manifest.Rollout.Status != protocol.ResultRolloutCaptureFailed ||
		outbox.Manifest.Rollout.FailureCode != rolloutLocatorFailureCode {
		t.Fatalf("durable initial result = %#v", outbox.Manifest)
	}
	intent, err := state.GetWorkerTurnStartIntentByTurn(
		context.Background(), started.Worker.WorkerKey, outbox.Manifest.TurnID,
	)
	if err != nil || intent.Rollout.Status != store.WorkerRolloutUnavailable ||
		intent.Rollout.FailureCode != rolloutLocatorFailureCode {
		t.Fatalf("durable initial intent = %#v, %v", intent, err)
	}
	if got := secondApplication.snapshot(); len(got.starts) != 0 || len(got.resumes) != 0 ||
		len(got.reads) != 0 || len(got.turns) != 0 {
		t.Fatalf("completion fallback started replacement app-server: %#v", got)
	}
	if _, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	); err != nil {
		t.Fatal(err)
	}
}

func TestHostRetriesInitialPreflightWhenRequestWasNotWritten(t *testing.T) {
	application := newFakeApplication()
	application.mcpStatusErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417444b",
		ParentAgentID: testParentID, TaskName: "unsent preflight", Prompt: "unsent preflight",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("initial Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.mcpStatusErr = nil
	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry initial Spawn() = %#v, %v", retried, err)
	}
	record := application.snapshot()
	if len(record.starts) != 1 || len(record.resumes) != 1 ||
		!record.resumes[0].ExcludeTurns || record.preflights != 2 || len(record.turns) != 1 {
		t.Fatalf("retried preflight calls = %#v", record)
	}
}

func TestHostUnsentInitialTurnDoesNotRetainSlot(t *testing.T) {
	application := newFakeApplication()
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, application)
	first, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "abandoned turn", Prompt: "abandoned turn",
	})
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	pending := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	if pending.CodexThreadID == "" {
		t.Fatalf("pending worker = %#v", pending)
	}
	application.turnStartErr = nil
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174449",
		ParentAgentID: testParentID, TaskName: "replacement", Prompt: "replacement",
	}); err != nil {
		t.Fatalf("replacement Spawn() while first worker is pending = %v", err)
	}
}

func TestHostColdResumesPendingInitialTurnAfterAppServerRestart(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-42661417444a",
		ParentAgentID: testParentID, TaskName: "cold pending", Prompt: "cold pending",
	}
	first, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("first Spawn() error = %v", err)
	}
	waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerPending)
	firstApplication.crash(errors.New("force cold pending retry"))
	waitForClientRetirement(t, host, firstApplication)

	retried, err := host.Spawn(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("cold pending retry = %#v, %v", retried, err)
	}
	record := secondApplication.snapshot()
	if len(record.resumes) != 1 || len(record.starts) != 0 || record.preflights != 1 ||
		len(record.reads) != 1 || record.reads[0].IncludeTurns || len(record.turns) != 1 {
		t.Fatalf("cold pending retry calls = %#v", record)
	}
}

func TestHostRecoversRunningWorkerWhenAppServerDies(t *testing.T) {
	firstApplication := newFakeApplication()
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174440", "running")
	firstApplication.crash(errors.New("lost process"))
	interrupted := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID != "" || interrupted.LastBoundTurnID == "" ||
		interrupted.FailureCode != "app_server_lost" {
		t.Fatalf("interrupted worker = %#v", interrupted)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: started.Worker.AgentID, ParentAgentID: testParentID,
		TaskName: "running", Prompt: "running prompt",
	}); !errors.Is(err, ErrWorkerInterrupted) {
		t.Fatalf("interrupted Spawn retry error = %v, want ErrWorkerInterrupted", err)
	}
	resumed, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "resume after loss",
	})
	if err != nil || resumed.Worker.Status != store.WorkerRunning {
		t.Fatalf("Followup() = %#v, %v", resumed, err)
	}
}

func TestHostRecoversPersistedCompletionWhenNotificationIsLost(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome, "sessions", "2026", "07", "26",
		"rollout-2026-07-26T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.threadPath = rolloutPath
	started := spawnTestWorker(t, host, newTestID(), "persisted completion")
	segment := testManagedRolloutLine("task_started", started.Worker.ActiveTurnID) +
		testManagedRolloutLine("task_complete", started.Worker.ActiveTurnID)
	if err := appendSyncedFile(rolloutPath, segment); err != nil {
		t.Fatal(err)
	}
	application.crash(errors.New("lost completion notification"))
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
		outbox.Manifest.Terminal.FailureCode != "" ||
		outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable {
		t.Fatalf("persisted completion result = %#v", outbox.Manifest)
	}
	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil || finalization.Worker.Status != store.WorkerIdle ||
		finalization.Worker.FailureCode != "" {
		t.Fatalf("persisted completion ACK = %#v, %v", finalization, err)
	}
}

func TestHostRecoversPersistedFailureWhenNotificationIsLost(t *testing.T) {
	application := newFakeApplication()
	application.threadID = newTestID()
	host, state, paths := newTestHost(t, 1, application)
	rolloutPath := filepath.Join(
		paths.codexHome, "sessions", "2026", "07", "26",
		"rollout-2026-07-26T00-00-00-"+application.threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application.threadPath = rolloutPath
	started := spawnTestWorker(t, host, newTestID(), "persisted failure")
	segment := testManagedRolloutLine("task_started", started.Worker.ActiveTurnID) +
		testManagedFailedRolloutLine(started.Worker.ActiveTurnID)
	if err := appendSyncedFile(rolloutPath, segment); err != nil {
		t.Fatal(err)
	}
	application.crash(errors.New("lost failure notification"))
	outbox := waitResultPublication(t, state, started.Worker.WorkerKey)
	if outbox.Manifest.Terminal.Outcome != protocol.ResultTerminalFailed ||
		outbox.Manifest.Terminal.FailureCode != "turn_failed" ||
		outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable {
		t.Fatalf("persisted failure result = %#v", outbox.Manifest)
	}
	finalization, err := resultManager(host).AcknowledgeResultPackageMetadata(
		context.Background(), outbox.ResultOutboxKey, outbox.Metadata,
	)
	if err != nil || finalization.Worker.Status != store.WorkerFailed ||
		finalization.Worker.FailureCode != "turn_failed" {
		t.Fatalf("persisted failure ACK = %#v, %v", finalization, err)
	}
}

func TestHostKeepsColdResumeRetryableAfterInterruption(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	interruptedResume := newFakeApplication()
	interruptedResume.resumeErr = context.DeadlineExceeded
	retryApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, interruptedResume, retryApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174442", "resume")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.crash(errors.New("force cold resume"))
	waitForClientRetirement(t, host, firstApplication)

	_, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "interrupted follow-up",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupted Followup() error = %v, want deadline exceeded", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)

	retried, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry follow-up",
	})
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry Followup() = %#v, %v", retried, err)
	}
	if got := len(retryApplication.snapshot().resumes); got != 1 {
		t.Fatalf("retry resume calls = %d, want 1", got)
	}
}

func TestHostRetriesUnsentColdResumeOnSameAppServer(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	secondApplication.resumeErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417443a", "unsent-resume")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.crash(errors.New("force cold resume"))
	waitForClientRetirement(t, host, firstApplication)

	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry cold resume",
	}
	if _, err := host.Followup(context.Background(), request); !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("unsent cold Followup() error = %v", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	if got := secondApplication.closeCount(); got != 0 {
		t.Fatalf("unsent cold resume retired shared app-server %d times", got)
	}
	secondApplication.resumeErr = nil
	request.OperationID = newTestID()
	retried, err := host.Followup(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry cold Followup() = %#v, %v", retried, err)
	}
	if got := len(secondApplication.snapshot().resumes); got != 2 {
		t.Fatalf("thread/resume calls = %d, want 2", got)
	}
}

func TestHostRetriesUnsentLoadedTurnOnSameAppServer(t *testing.T) {
	application := newFakeApplication()
	application.completeBeforeReturn = true
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417443b", "unsent-turn")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	application.turnStartErr = errors.Join(appserver.ErrRequestNotWritten, context.Canceled)

	request := FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey, Message: "retry loaded turn",
	}
	if _, err := host.Followup(context.Background(), request); !errors.Is(err, appserver.ErrRequestNotWritten) {
		t.Fatalf("unsent loaded Followup() error = %v", err)
	}
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	if got := application.closeCount(); got != 0 {
		t.Fatalf("unsent loaded turn retired shared app-server %d times", got)
	}
	application.turnStartErr = nil
	request.OperationID = newTestID()
	retried, err := host.Followup(context.Background(), request)
	if err != nil || retried.Worker.Status != store.WorkerRunning {
		t.Fatalf("retry loaded Followup() = %#v, %v", retried, err)
	}
	if got := len(application.snapshot().turns); got != 3 {
		t.Fatalf("turn/start calls = %d, want 3", got)
	}
}

func TestHostRecoversRunningWorkerWhenThreadCloses(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, application)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174443", "thread-closed")
	application.notifyThreadClosed(started.Worker.CodexThreadID)
	interrupted := waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID != "" || interrupted.LastBoundTurnID == "" ||
		interrupted.FailureCode != "app_server_lost" {
		t.Fatalf("thread-closed worker = %#v", interrupted)
	}
}

func TestHostPreservesOtherWorkerCompletionWhileRetiringClient(t *testing.T) {
	application := newFakeApplication()
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, state, _ := newTestHost(t, 2, application)
	first := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417444a", "closed-thread")
	second := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-42661417444b", "completed-thread")

	application.notifyThreadClosed(first.Worker.CodexThreadID)
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("client retirement did not start")
	}
	application.notifyCompletion(
		second.Worker.CodexThreadID,
		second.Worker.ActiveTurnID,
		"completed",
	)
	close(application.closeGate)

	interrupted := waitWorkerStatus(t, state, first.Worker.WorkerKey, store.WorkerInterrupted)
	if interrupted.ActiveTurnID != "" || interrupted.LastBoundTurnID == "" ||
		interrupted.FailureCode != "app_server_lost" {
		t.Fatalf("thread-closed worker = %#v", interrupted)
	}
	completed := waitWorkerStatus(t, state, second.Worker.WorkerKey, store.WorkerIdle)
	if completed.ActiveTurnID != "" || completed.FailureCode != "" {
		t.Fatalf("completed worker = %#v", completed)
	}
}

func TestHostColdResumesIdleWorkerWhenThreadCloses(t *testing.T) {
	firstApplication := newFakeApplication()
	firstApplication.completeBeforeReturn = true
	secondApplication := newFakeApplication()
	host, state, _ := newTestHost(t, 1, firstApplication, secondApplication)
	started := spawnTestWorker(t, host, "123e4567-e89b-42d3-a456-426614174444", "idle-thread-closed")
	waitWorkerStatus(t, state, started.Worker.WorkerKey, store.WorkerIdle)
	firstApplication.notifyThreadClosed(started.Worker.CodexThreadID)
	waitForClientRetirement(t, host, firstApplication)

	resumed, err := host.Followup(context.Background(), FollowupRequest{
		OperationID: newTestID(), Key: started.Worker.WorkerKey,
		Message: "cold resume closed idle thread",
	})
	if err != nil || resumed.Worker.Status != store.WorkerRunning {
		t.Fatalf("Followup() = %#v, %v", resumed, err)
	}
	if got := len(secondApplication.snapshot().resumes); got != 1 {
		t.Fatalf("cold resume count = %d, want 1", got)
	}
}

func TestHostStartFailureDoesNotReserveWorkerSlot(t *testing.T) {
	application := newFakeApplication()
	host, state, _ := newTestHost(t, 1, nil, application)
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174445",
		ParentAgentID: testParentID, TaskName: "start-failure", Prompt: "start failure",
	}); err == nil {
		t.Fatal("first Spawn() unexpectedly succeeded")
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 0 {
		t.Fatalf("app-server start failure reserved workers: %#v", workers)
	}
	if _, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174446",
		ParentAgentID: testParentID, TaskName: "start-retry", Prompt: "start retry",
	}); err != nil {
		t.Fatalf("second Spawn() error = %v", err)
	}
}

func TestHostFailsClosedWhenWorkerFailureCannotBePersisted(t *testing.T) {
	application := newFakeApplication()
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174447",
		ParentAgentID: testParentID, TaskName: "persistence-failure", Prompt: "persistence failure",
	}
	key := store.WorkerKey{
		ControllerID: testControllerID,
		TreeID:       request.TreeID,
		AgentID:      request.AgentID,
	}
	host, state, paths := newTestHost(t, 1, application)
	paths.allowCloseError.Store(true)
	application.threadStartHook = func() error {
		_, err := state.FailWorker(context.Background(), key, "injected_failure", time.Now())
		return err
	}
	rpcFailure := &appserver.RPCError{Code: -32000, Message: "test thread failure"}
	application.threadStartErr = rpcFailure

	_, err := host.Spawn(context.Background(), request)
	if !errors.Is(err, rpcFailure) || !errors.Is(err, store.ErrWorkerTransition) ||
		!strings.Contains(err.Error(), "record worker failure") {
		t.Fatalf("Spawn() error = %v, want RPC and persistence failures", err)
	}
	select {
	case <-host.Done():
	case <-time.After(time.Second):
		t.Fatal("host did not fail closed after losing authoritative worker state")
	}
	if fatal := host.Err(); !errors.Is(fatal, rpcFailure) || !errors.Is(fatal, store.ErrWorkerTransition) {
		t.Fatalf("host Err() = %v, want combined terminal error", fatal)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, rpcFailure) ||
		!errors.Is(err, store.ErrWorkerTransition) {
		t.Fatalf("second Spawn() error = %v, want terminal host error", err)
	}
	if got := len(application.snapshot().starts); got != 1 {
		t.Fatalf("thread/start calls = %d, want 1", got)
	}
}

func TestHostCloseOverlappingAppServerStartLeavesNoReservation(t *testing.T) {
	application := newFakeApplication()
	application.startGate = make(chan struct{})
	application.startStarted = make(chan struct{})
	application.closeGate = make(chan struct{})
	application.closeStarted = make(chan struct{})
	host, state, _ := newTestHost(t, 1, application)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174448",
		ParentAgentID: testParentID, TaskName: "closing", Prompt: "closing prompt",
	}
	spawnDone := make(chan error, 1)
	go func() {
		_, err := host.Spawn(context.Background(), request)
		spawnDone <- err
	}()
	select {
	case <-application.startStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server start did not begin")
	}
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeDone <- host.Close(ctx)
	}()
	waitHostClosed(t, host)
	close(application.startGate)
	select {
	case <-application.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("unclaimed app-server cleanup did not start")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Host Close returned before unclaimed app-server cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case err := <-spawnDone:
		t.Fatalf("Spawn returned before unclaimed app-server cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(application.closeGate)
	select {
	case err := <-spawnDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("overlapping Spawn() error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping Spawn did not return")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	workers, err := state.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 0 {
		t.Fatalf("overlapping Close reserved workers: %#v", workers)
	}
	if got := application.closeCount(); got != 1 {
		t.Fatalf("app-server Close calls = %d, want 1", got)
	}
	if _, err := host.Spawn(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Spawn() error = %v, want ErrClosed", err)
	}
}

func TestHostCloseOverlappingAppServerStartReportsUnconfirmedExit(t *testing.T) {
	application := newFakeApplication()
	application.startGate = make(chan struct{})
	application.startStarted = make(chan struct{})
	application.closeErr = errors.Join(
		appserver.ErrCloseTimeout,
		appserver.ErrProcessExitUnconfirmed,
	)
	host, _, paths := newTestHost(t, 1, application)
	paths.allowCloseError.Store(true)
	request := SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174449",
		ParentAgentID: testParentID, TaskName: "unconfirmed-start", Prompt: "unconfirmed start",
	}
	spawnDone := make(chan error, 1)
	go func() {
		_, err := host.Spawn(context.Background(), request)
		spawnDone <- err
	}()
	select {
	case <-application.startStarted:
	case <-time.After(time.Second):
		t.Fatal("app-server start did not begin")
	}
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closeDone <- host.Close(ctx)
	}()
	waitHostClosed(t, host)
	close(application.startGate)
	select {
	case err := <-spawnDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("overlapping Spawn() error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping Spawn did not return")
	}
	select {
	case err := <-closeDone:
		if !errors.Is(err, appserver.ErrProcessExitUnconfirmed) {
			t.Fatalf("Close() error = %v, want ErrProcessExitUnconfirmed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	if !errors.Is(host.Err(), appserver.ErrProcessExitUnconfirmed) {
		t.Fatalf("host error = %v, want ErrProcessExitUnconfirmed", host.Err())
	}
}

func TestHostRejectsWorkspaceRootAlias(t *testing.T) {
	root := t.TempDir()
	actualRoot := filepath.Join(root, "actual")
	if err := os.Mkdir(actualRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasRoot := filepath.Join(root, "alias")
	if err := os.Symlink(actualRoot, aliasRoot); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}

	if err := config.ValidatePrivateDirectory(aliasRoot); err == nil {
		t.Fatal("private directory validation accepted a symbolic-link workspace root")
	}
}

func TestHostRejectsStoredWorkerAuthorityDrift(t *testing.T) {
	tests := map[string]func(*store.WorkerReservation){
		"workspace root": func(worker *store.WorkerReservation) {
			worker.WorkspacePath = filepath.Join(t.TempDir(), "stale-workspace")
		},
		"profile version": func(worker *store.WorkerReservation) {
			worker.ProfileVersion = workerProfileVersion + 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			workspaceRoot := filepath.Join(root, "workspaces")
			if err := os.Mkdir(workspaceRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			anchored, err := os.OpenRoot(workspaceRoot)
			if err != nil {
				t.Fatal(err)
			}
			defer anchored.Close()
			state, err := store.OpenPeer(ctx, filepath.Join(root, "state", "peer.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			worker := store.WorkerReservation{
				WorkerKey: store.WorkerKey{
					ControllerID: testControllerID,
					TreeID:       testTreeID,
					AgentID:      "123e4567-e89b-42d3-a456-42661417443d",
				},
				ParentAgentID: testParentID,
				DeviceID:      testDeviceID,
				TaskName:      "stored authority",
				PromptDigest:  promptDigest("stored authority"),
				WorkspacePath: filepath.Join(
					workspaceRoot,
					testTreeID+"-123e4567-e89b-42d3-a456-42661417443d",
				),
				ProfileVersion: workerProfileVersion,
			}
			mutate(&worker)
			if _, err := state.ReserveWorker(ctx, worker, 1, time.Unix(1_700_000_000, 0)); err != nil {
				t.Fatal(err)
			}
			host := &Host{
				controllerID:  testControllerID,
				deviceID:      testDeviceID,
				workspaceRoot: anchored,
				state:         state,
			}
			if err := host.validateStoredAuthority(ctx); err == nil {
				t.Fatal("validateStoredAuthority accepted drifted worker state")
			}
		})
	}
}

func TestHostRejectsManagedCodexConfigurationBeforeLaunch(t *testing.T) {
	application := newFakeApplication()
	host, _, paths := newTestHost(t, 1, application)
	configPath := filepath.Join(paths.codexHome, "config.toml")
	if err := os.WriteFile(configPath, []byte("model = \"ambient\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(configPath) })
	_, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174441",
		ParentAgentID: testParentID, TaskName: "blocked config", Prompt: "blocked config",
	})
	if err == nil || !strings.Contains(err.Error(), "config.toml") {
		t.Fatalf("Spawn() error = %v", err)
	}
	if got := application.snapshot(); len(got.starts) != 0 {
		t.Fatalf("app-server started with managed config present: %#v", got)
	}
}

func TestHostRejectsTraeXRuntimeConfigurationBeforeLaunch(t *testing.T) {
	for _, test := range []struct {
		name     string
		relative string
		want     string
	}{
		{name: "instructions", relative: "AGENTS.md", want: "AGENTS.md"},
		{name: "CLI authentication", relative: filepath.Join("cli", "auth.json"), want: "auth.json"},
		{name: "CLI hooks", relative: filepath.Join("cli", "hooks.json"), want: "hooks.json"},
		{name: "CLI plugins", relative: filepath.Join("cli", "plugins"), want: "plugins"},
		{name: "CLI rules", relative: filepath.Join("cli", "rules"), want: "rules"},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newFakeApplication()
			host, _, paths := newTestHostForKind(t, hostkind.TraeX, 1, application)
			artifact := filepath.Join(paths.codexHome, test.relative)
			if err := os.MkdirAll(filepath.Dir(artifact), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(artifact, []byte("managed"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := host.Spawn(context.Background(), SpawnRequest{
				TreeID: testTreeID, AgentID: "123e4567-e89b-42d3-a456-426614174417",
				ParentAgentID: testParentID, TaskName: "TraeX managed configuration",
				Prompt: "TraeX managed configuration",
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Spawn() error = %v", err)
			}
			if got := application.snapshot(); len(got.starts) != 0 {
				t.Fatalf("app-server started with managed TraeX config present: %#v", got)
			}
		})
	}
}

func TestManagedProfileUsesPlatformPermissionBoundary(t *testing.T) {
	host, _, paths := newTestHost(t, 1)
	worker := store.WorkerReservation{
		WorkerKey: store.WorkerKey{
			ControllerID: testControllerID,
			TreeID:       testTreeID,
			AgentID:      "123e4567-e89b-42d3-a456-426614174443",
		},
		ParentAgentID: testParentID,
		WorkspacePath: filepath.Join(filepath.Dir(paths.codexHome), "managed-worker"),
	}
	config := host.managedConfig(worker)
	if runtime.GOOS == "windows" {
		if config["default_permissions"] != windowsWorkerProfile {
			t.Fatalf("managed Windows permissions = %#v", config["default_permissions"])
		}
		projects, ok := config["projects"].(map[string]any)
		if !ok {
			t.Fatalf("managed Windows projects = %#v", config["projects"])
		}
		project, ok := projects[worker.WorkspacePath].(map[string]any)
		if !ok || project["trust_level"] != "untrusted" {
			t.Fatalf("managed Windows workspace trust = %#v", projects)
		}
		if _, found := config["permissions."+workerPermissionProfile]; found {
			t.Fatalf("managed Windows config retains an unenforceable restricted profile: %#v", config)
		}
		return
	}
	filesystem := managedFilesystemPermissions(t, config)
	workspacePermissions, ok := filesystem[":workspace_roots"].(map[string]any)
	if !ok || workspacePermissions["."] != "write" || workspacePermissions[".git"] != "write" {
		t.Fatalf("managed workspace permissions = %#v", filesystem[":workspace_roots"])
	}
	for _, protected := range []string{".agents", ".codex"} {
		if _, found := workspacePermissions[protected]; found {
			t.Fatalf("managed profile grants protected workspace metadata %q: %#v", protected, workspacePermissions)
		}
	}
	if filepath.Dir(paths.configPath) != filepath.Dir(paths.codexBinary) {
		t.Fatal("test fixture does not co-locate the peer config and Codex binary")
	}
	for _, directory := range []string{
		filepath.Dir(paths.codexBinary),
		filepath.Dir(host.cliRuntimeExecutable),
	} {
		if _, found := filesystem[directory]; found {
			t.Fatalf("managed profile grants the Codex binary directory %q: %#v", directory, filesystem)
		}
	}
	if _, found := filesystem[paths.configPath]; found {
		t.Fatalf("managed profile grants the co-located peer config: %#v", filesystem)
	}
	assertCodexRuntimeFilesystemPermission(t, filesystem, host.cliRuntimeExecutable)
	if filesystem[host.workerGitBinary] != "read" {
		t.Fatalf("managed profile does not grant the exact Git executable: %#v", filesystem)
	}
	if host.workerGitBinary != host.git.Binary {
		if _, found := filesystem[host.git.Binary]; found {
			t.Fatalf("managed profile grants the configured host Git executable: %#v", filesystem)
		}
	}
}

type testHostPaths struct {
	configPath              string
	delegationBinary        string
	codexBinary             string
	gitBinary               string
	codexHome               string
	providerEnvironmentFile string
	launchOptions           *appserver.Options
	allowCloseError         *atomic.Bool
}

func directWorkerCLILaunch(executable string) clilaunch.Spec {
	return clilaunch.Spec{Executable: executable}
}

func newTestHost(
	t *testing.T,
	maxSlots int,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostForKind(t, hostkind.Codex, maxSlots, applications...)
}

func newTestHostForKind(
	t *testing.T,
	hostKind hostkind.Kind,
	maxSlots int,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithStateSetupAndResultPublisherForKind(
		t, hostKind, maxSlots, "", nil, nil, applications...,
	)
}

func newTestHostWithWorkspaceRoot(
	t *testing.T,
	maxSlots int,
	workspaceRoot string,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithStateSetup(t, maxSlots, workspaceRoot, nil, applications...)
}

func newTestHostWithStateSetup(
	t *testing.T,
	maxSlots int,
	workspaceRoot string,
	setup func(*store.PeerStore, string),
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithStateSetupAndResultPublisher(
		t, maxSlots, workspaceRoot, setup, nil, applications...,
	)
}

func newTestHostWithStateSetupAndResultPublisher(
	t *testing.T,
	maxSlots int,
	workspaceRoot string,
	setup func(*store.PeerStore, string),
	publisherFactory func(*resultpackagefiles.Manager) resultPackagePublisher,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	return newTestHostWithStateSetupAndResultPublisherForKind(
		t, hostkind.Codex, maxSlots, workspaceRoot, setup, publisherFactory, applications...,
	)
}

func newTestHostWithStateSetupAndResultPublisherForKind(
	t *testing.T,
	hostKind hostkind.Kind,
	maxSlots int,
	workspaceRoot string,
	setup func(*store.PeerStore, string),
	publisherFactory func(*resultpackagefiles.Manager) resultPackagePublisher,
	applications ...*fakeApplication,
) (*Host, *store.PeerStore, testHostPaths) {
	t.Helper()
	root := t.TempDir()
	gitBinary, err := exec.LookPath("git")
	if err != nil {
		t.Skip("Git is unavailable")
	}
	gitBinary, err = filepath.Abs(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	gitBinary, err = filepath.EvalSymlinks(gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	paths := testHostPaths{
		configPath: filepath.Join(root, "peer.json"), delegationBinary: filepath.Join(root, "delegation"),
		codexBinary: filepath.Join(root, "codex"), gitBinary: gitBinary,
		codexHome:               filepath.Join(root, "codex-home"),
		providerEnvironmentFile: filepath.Join(root, "peer.env"),
		launchOptions:           &appserver.Options{}, allowCloseError: &atomic.Bool{},
	}
	for _, file := range []struct {
		path string
		mode os.FileMode
	}{
		{path: paths.configPath, mode: 0o600},
		{path: paths.delegationBinary, mode: 0o600},
		{path: paths.codexBinary, mode: 0o700},
		{path: paths.providerEnvironmentFile, mode: 0o600},
	} {
		if err := os.WriteFile(file.path, []byte("test"), file.mode); err != nil {
			t.Fatal(err)
		}
	}
	if workspaceRoot == "" {
		workspaceRoot = filepath.Join(root, "workspaces")
	}
	for _, path := range []string{paths.codexHome, workspaceRoot} {
		if err := config.PreparePrivateDirectory(path); err != nil {
			t.Fatal(err)
		}
	}
	workspaceRoot, err = filepath.EvalSymlinks(workspaceRoot)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.OpenPeer(context.Background(), filepath.Join(root, "state", "peer.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(state, workspaceRoot)
	}
	resultPackages, err := resultpackagefiles.New(context.Background(), resultpackagefiles.Options{
		ControllerID: testControllerID, DeviceID: testDeviceID,
		WorkspaceRoot: workspaceRoot, Store: state,
	})
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	var resultPublisher resultPackagePublisher = resultPackages
	if publisherFactory != nil {
		resultPublisher = publisherFactory(resultPackages)
	}
	var factoryMu sync.Mutex
	applicationIndex := 0
	host, err := New(context.Background(), Options{
		ControllerID: testControllerID, DeviceID: testDeviceID, HostKind: hostKind,
		PeerConfigPath: paths.configPath, DelegationBinary: paths.delegationBinary,
		CLILaunch:            directWorkerCLILaunch(paths.codexBinary),
		CLIRuntimeExecutable: paths.codexBinary, GitBinary: paths.gitBinary,
		CodexHome: paths.codexHome,
		CodexEnvironment: map[string]string{
			"CODEX_ACCESS_TOKEN":  "host-auth",
			"CODEX_API_KEY":       "ambient-codex-auth",
			"CODEX_HOME":          filepath.Join(root, "ambient-codex-home"),
			"OPENAI_API_KEY":      "ambient-openai-auth",
			"CODEX_SQLITE_HOME":   filepath.Join(root, "ambient-sqlite"),
			"TEST_PROVIDER_VALUE": "provider-auth",
			"TRAECLI_HOME":        filepath.Join(root, "ambient-trae-cli-home"),
			"TRAE_HOME":           filepath.Join(root, "ambient-trae-home"),
		},
		CodexUnsetEnvironment:   []string{"CODEX_MANAGED_BY_NPM"},
		ProviderEnvironmentFile: paths.providerEnvironmentFile,
		WorkspaceRoot:           workspaceRoot, MaxWorkerSlots: maxSlots,
		CodexConfig: map[string]any{
			"model":          "test-model",
			"model_provider": "test",
			"model_providers.test": map[string]any{
				"name": "Test provider", "base_url": "https://example.test/v1",
				"env_key": "TEST_PROVIDER_VALUE", "requires_openai_auth": false,
			},
		},
		Store: state, ResultPackages: resultPublisher,
		startApplication: func(ctx context.Context, options appserver.Options) (application, error) {
			factoryMu.Lock()
			defer factoryMu.Unlock()
			*paths.launchOptions = options
			if applicationIndex >= len(applications) {
				return nil, errors.New("unexpected app-server restart")
			}
			application := applications[applicationIndex]
			applicationIndex++
			if application == nil {
				return nil, errors.New("test app-server start failure")
			}
			if err := application.awaitStart(ctx); err != nil {
				return nil, err
			}
			return application, nil
		},
	})
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	host.initialRolloutWait = 0
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := host.Close(ctx); err != nil && !paths.allowCloseError.Load() {
			t.Errorf("close host: %v", err)
		}
		cancel()
		if err := resultPackages.Close(); err != nil {
			t.Errorf("close result packages: %v", err)
		}
		if err := state.Close(); err != nil {
			t.Errorf("close peer state: %v", err)
		}
	})
	return host, state, paths
}

func spawnTestWorker(t *testing.T, host *Host, agentID, name string) StartedTurn {
	t.Helper()
	started, err := host.Spawn(context.Background(), SpawnRequest{
		TreeID: testTreeID, AgentID: agentID, ParentAgentID: testParentID,
		TaskName: name, Prompt: name + " prompt",
	})
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func fillResultCapacity(
	t *testing.T,
	host *Host,
	state *store.PeerStore,
	count int,
	reservationBytes int64,
) {
	t.Helper()
	key := store.WorkerKey{
		ControllerID: testControllerID, TreeID: testTreeID, AgentID: newTestID(),
	}
	workspacePath := filepath.Join(host.workspaceRoot.Name(), "quota-"+key.AgentID)
	if err := os.Mkdir(workspacePath, 0o700); err != nil {
		t.Fatal(err)
	}
	worker, err := state.ReserveWorkerStart(
		context.Background(),
		store.WorkerReservation{
			WorkerKey: key, ParentAgentID: testParentID, DeviceID: testDeviceID,
			TaskName: "quota_filler", PromptDigest: promptDigest("quota filler"),
			WorkspacePath: workspacePath, ProfileVersion: workerProfileVersion,
		},
		1,
		time.Now(),
	)
	if err == nil {
		worker, err = state.FailWorker(
			context.Background(), worker.WorkerKey, "quota_filler", time.Now(),
		)
	}
	if err != nil {
		t.Fatal(err)
	}
	for index := range count {
		_, err := state.ReserveResultOutbox(
			context.Background(),
			store.ResultOutboxKey{
				WorkerKey: worker.WorkerKey, SourceDeviceID: testDeviceID, PackageID: newTestID(),
			},
			reservationBytes,
			time.Now().Add(time.Duration(index)*time.Second),
		)
		if err != nil {
			t.Fatalf("fill result capacity %d: %v", index, err)
		}
	}
}

func assertManagedProfile(
	t *testing.T,
	config map[string]any,
	paths testHostPaths,
	worker store.WorkerReservation,
) {
	t.Helper()
	for _, key := range []string{
		"features.plugins", "features.multi_agent", "features.multi_agent_v2", "features.enable_fanout",
		rootPluginEnabledConfig,
	} {
		if config[key] != false {
			t.Fatalf("managed config %s = %#v", key, config[key])
		}
	}
	if config["model"] != "test-model" {
		t.Fatalf("provider config = %#v", config)
	}
	filesystem := map[string]any{
		":minimal": "read",
		":workspace_roots": map[string]any{
			".":    "write",
			".git": "write",
		},
	}
	resolvedProviderEnvironmentFile, err := filepath.EvalSymlinks(paths.providerEnvironmentFile)
	if err != nil {
		t.Fatal(err)
	}
	filesystem[resolvedProviderEnvironmentFile] = "deny"
	resolvedCodexBinary, err := filepath.EvalSymlinks(paths.codexBinary)
	if err != nil {
		t.Fatal(err)
	}
	addCLIRuntimeFilesystemPermission(filesystem, resolvedCodexBinary)
	workerGitBinary, err := resolveWorkerGitBinary(context.Background(), paths.gitBinary)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		filesystem[workerGitBinary] = "read"
	}
	if runtime.GOOS == "windows" {
		if config["default_permissions"] != windowsWorkerProfile {
			t.Fatalf("default permissions = %#v", config["default_permissions"])
		}
		if _, found := config["permissions."+workerPermissionProfile]; found {
			t.Fatalf("managed Windows config retains a restricted profile: %#v", config)
		}
	} else {
		if config["default_permissions"] != workerPermissionProfile {
			t.Fatalf("default permissions = %#v", config["default_permissions"])
		}
		wantPermissions := map[string]any{
			"filesystem": filesystem,
		}
		if !reflect.DeepEqual(config["permissions."+workerPermissionProfile], wantPermissions) {
			t.Fatalf("worker permissions = %#v, want %#v", config["permissions."+workerPermissionProfile], wantPermissions)
		}
	}
	wantShellSet := map[string]string{
		"GIT_ATTR_NOSYSTEM":   "1",
		"GIT_CONFIG_GLOBAL":   os.DevNull,
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_TERMINAL_PROMPT": "0",
		"GCM_INTERACTIVE":     "Never",
	}
	if runtime.GOOS != "windows" {
		wantShellSet["PATH"] = prependExecutableDirectory(
			os.Getenv("PATH"), filepath.Dir(workerGitBinary),
		)
	}
	if runtime.GOOS == "darwin" {
		wantShellSet["TMPDIR"] = "/tmp"
	}
	wantShellEnvironment := map[string]any{
		"inherit": "core", "ignore_default_excludes": false,
		"exclude": []string{
			"CODEX_ACCESS_TOKEN", "CODEX_API_KEY", "CODEX_HOME",
			"DELEGATION_CODEX_CONFIG_JSON", "OPENAI_API_KEY", "TEST_PROVIDER_VALUE",
			"TRAECLI_HOME", "TRAE_HOME",
		},
		"set": wantShellSet,
	}
	if !reflect.DeepEqual(config["shell_environment_policy"], wantShellEnvironment) {
		t.Fatalf("shell environment policy = %#v, want %#v", config["shell_environment_policy"], wantShellEnvironment)
	}
	wantMCP := map[string]any{
		"command": paths.delegationBinary,
		"args": []string{
			"mcp", "worker", "--config", paths.configPath,
			"--tree-id", worker.TreeID, "--agent-id", worker.AgentID,
			"--parent-agent-id", worker.ParentAgentID,
		},
		"required": true, "startup_timeout_sec": workerMCPTimeout,
	}
	if _, found := config["mcp_servers."+workerServerName+".command"]; found {
		for key, value := range wantMCP {
			if !reflect.DeepEqual(config["mcp_servers."+workerServerName+"."+key], value) {
				t.Fatalf(
					"worker MCP config %s = %#v, want %#v",
					key,
					config["mcp_servers."+workerServerName+"."+key],
					value,
				)
			}
		}
	} else if !reflect.DeepEqual(config["mcp_servers."+workerServerName], wantMCP) {
		t.Fatalf(
			"worker MCP config = %#v, want %#v",
			config["mcp_servers."+workerServerName],
			wantMCP,
		)
	}
	if paths.launchOptions.Environment["CODEX_ACCESS_TOKEN"] != "host-auth" ||
		paths.launchOptions.Environment["CODEX_API_KEY"] != "ambient-codex-auth" ||
		paths.launchOptions.Environment["OPENAI_API_KEY"] != "ambient-openai-auth" ||
		paths.launchOptions.Environment["TEST_PROVIDER_VALUE"] != "provider-auth" {
		t.Fatalf("managed app-server environment = %#v", paths.launchOptions.Environment)
	}
	if paths.launchOptions.SupervisorBinary != paths.delegationBinary {
		t.Fatalf("managed app-server supervisor = %q, want %q", paths.launchOptions.SupervisorBinary, paths.delegationBinary)
	}
	if _, found := paths.launchOptions.Environment["CODEX_SQLITE_HOME"]; found {
		t.Fatalf("managed app-server inherited CODEX_SQLITE_HOME: %#v", paths.launchOptions.Environment)
	}
	if !slices.Contains(paths.launchOptions.UnsetEnvironment, codexconfig.EnvironmentVariable) ||
		!slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_SQLITE_HOME") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_ACCESS_TOKEN") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "CODEX_API_KEY") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "OPENAI_API_KEY") ||
		slices.Contains(paths.launchOptions.UnsetEnvironment, "TEST_PROVIDER_VALUE") {
		t.Fatalf("managed app-server unset environment = %#v", paths.launchOptions.UnsetEnvironment)
	}
}

func managedFilesystemPermissions(t *testing.T, config map[string]any) map[string]any {
	t.Helper()
	permissions, ok := config["permissions."+workerPermissionProfile].(map[string]any)
	if !ok {
		t.Fatalf("managed permissions = %#v", config["permissions."+workerPermissionProfile])
	}
	filesystem, ok := permissions["filesystem"].(map[string]any)
	if !ok {
		t.Fatalf("managed filesystem permissions = %#v", permissions["filesystem"])
	}
	return filesystem
}

func waitWorkerStatus(
	t *testing.T,
	state *store.PeerStore,
	key store.WorkerKey,
	status store.WorkerStatus,
) store.WorkerReservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		worker, err := state.GetWorker(context.Background(), key)
		if err == nil && worker.Status == status {
			return worker
		}
		if err == nil && worker.Status == store.WorkerFinalizing && worker.FinalTarget == status {
			outboxes, listErr := state.ListPendingResultPublications(
				context.Background(), key.ControllerID, worker.DeviceID, 10,
			)
			if listErr == nil {
				for _, outbox := range outboxes {
					if outbox.WorkerKey == key {
						_, _ = state.AcknowledgeResultOutboxMetadata(
							context.Background(), outbox.ResultOutboxKey, outbox.Metadata, time.Now(),
						)
					}
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	worker, err := state.GetWorker(context.Background(), key)
	t.Fatalf("worker status = %#v, %v; want %s", worker, err, status)
	return store.WorkerReservation{}
}

func waitForClientRetirement(t *testing.T, host *Host, retired application) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		host.clientMu.Lock()
		current := host.client
		recovering := host.recovering
		host.clientMu.Unlock()
		if current != retired && recovering == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for app-server retirement")
}

func waitHostClosed(t *testing.T, host *Host) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		host.clientMu.Lock()
		closed := host.closed
		host.clientMu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("host did not enter closed state")
}

type fakeRecord struct {
	starts     []threadStartParams
	resumes    []threadResumeParams
	reads      []threadReadParams
	mcpTools   []mcpToolCallParams
	turns      []turnStartParams
	steers     []turnSteerParams
	interrupts []turnInterruptParams
	preflights int
}

type fakeApplication struct {
	mu                    sync.Mutex
	record                fakeRecord
	tools                 []string
	resources             []json.RawMessage
	resourceTemplates     []json.RawMessage
	extraServers          []mcpServerStatus
	authStatus            string
	mcpToolErrors         map[string]error
	mcpToolResults        map[string]mcpToolCallResult
	threadStartHook       func() error
	threadStartResultHook func(*threadResult)
	threadStartErr        error
	threadID              string
	threadStartPath       string
	threadReadErr         error
	threadReadHook        func(threadReadParams, *threadResult)
	threadPath            string
	threadTurns           map[string][]turn
	readAfterTurnErr      error
	readAfterTurnErrors   []error
	readAfterTurnGate     chan struct{}
	resumeErr             error
	resumeResultHook      func(*threadResult)
	mcpStatusErr          error
	turnStartErr          error
	turnStartResponseLost bool
	completeThenLose      bool
	turnSteerErr          error
	turnInterruptErr      error
	turnSteerHook         func(turnSteerParams)
	steerResponseTurnID   string
	completeBeforeReturn  bool
	idleBeforeReturn      bool
	idleReadStarted       chan struct{}
	idleReadStartedOnce   sync.Once
	completionStatus      string
	crashAfterComplete    bool
	notifications         chan appserver.Notification
	done                  chan struct{}
	closeOnce             sync.Once
	closeStartedOnce      sync.Once
	startStartedOnce      sync.Once
	turnStartStartedOnce  sync.Once
	startGate             chan struct{}
	startStarted          chan struct{}
	turnStartGate         chan struct{}
	turnStartStarted      chan struct{}
	closeGate             chan struct{}
	closeStarted          chan struct{}
	closeCalls            int
	closeErr              error
	err                   error
}

func newFakeApplication() *fakeApplication {
	return &fakeApplication{
		tools:          []string{"send_upstream_message", "wait_for_upstream_message"},
		authStatus:     "unsupported",
		mcpToolErrors:  make(map[string]error),
		mcpToolResults: make(map[string]mcpToolCallResult),
		threadTurns:    make(map[string][]turn),
		notifications:  make(chan appserver.Notification, 16), done: make(chan struct{}),
	}
}

func (a *fakeApplication) ThreadStart(_ context.Context, params, result any) error {
	start := params.(threadStartParams)
	a.mu.Lock()
	a.record.starts = append(a.record.starts, start)
	hook := a.threadStartHook
	threadStartErr := a.threadStartErr
	threadID := a.threadID
	a.mu.Unlock()
	if hook != nil {
		if err := hook(); err != nil {
			return err
		}
	}
	if threadStartErr != nil {
		return threadStartErr
	}
	if threadID == "" {
		threadID = newTestID()
	}
	setFakeThreadResult(result.(*threadResult), threadID, start.CWD, start.RuntimeWorkspaceRoots)
	if a.threadStartResultHook != nil {
		a.threadStartResultHook(result.(*threadResult))
	}
	if a.threadStartPath != "" {
		path := a.threadStartPath
		result.(*threadResult).Thread.Path = &path
	}
	return nil
}

func (a *fakeApplication) awaitStart(ctx context.Context) error {
	if a.startStarted != nil {
		a.startStartedOnce.Do(func() { close(a.startStarted) })
	}
	if a.startGate == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.startGate:
		return nil
	}
}

func (a *fakeApplication) ThreadResume(_ context.Context, params, result any) error {
	resume := params.(threadResumeParams)
	a.mu.Lock()
	a.record.resumes = append(a.record.resumes, resume)
	resumeErr := a.resumeErr
	resumeResultHook := a.resumeResultHook
	a.mu.Unlock()
	if resumeErr != nil {
		return resumeErr
	}
	setFakeThreadResult(result.(*threadResult), resume.ThreadID, resume.CWD, resume.RuntimeWorkspaceRoots)
	if resumeResultHook != nil {
		resumeResultHook(result.(*threadResult))
	}
	return nil
}

func (a *fakeApplication) ThreadRead(ctx context.Context, params, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	read := params.(threadReadParams)
	a.mu.Lock()
	a.record.reads = append(a.record.reads, read)
	readErr := a.threadReadErr
	readHook := a.threadReadHook
	path := a.threadPath
	turns := append([]turn(nil), a.threadTurns[read.ThreadID]...)
	readAfterTurnErr := a.readAfterTurnErr
	if len(turns) != 0 && len(a.readAfterTurnErrors) != 0 {
		readAfterTurnErr = a.readAfterTurnErrors[0]
		a.readAfterTurnErrors = a.readAfterTurnErrors[1:]
	}
	readAfterTurnGate := a.readAfterTurnGate
	idleReadStarted := a.idleReadStarted
	a.mu.Unlock()
	if readErr != nil {
		return readErr
	}
	if len(turns) != 0 && readAfterTurnGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-readAfterTurnGate:
		}
	}
	if len(turns) != 0 && readAfterTurnErr != nil {
		return readAfterTurnErr
	}
	response := result.(*threadResult)
	response.Thread.ID = read.ThreadID
	if path != "" {
		response.Thread.Path = &path
	}
	if read.IncludeTurns {
		response.Thread.Turns = turns
	}
	if readHook != nil {
		readHook(read, response)
	}
	if read.IncludeTurns && idleReadStarted != nil {
		a.idleReadStartedOnce.Do(func() { close(idleReadStarted) })
	}
	return nil
}

func mirrorFakeThreadTurns(
	source *fakeApplication,
	mutate func([]turn) []turn,
) func(threadReadParams, *threadResult) {
	return func(read threadReadParams, result *threadResult) {
		source.mu.Lock()
		turns := append([]turn(nil), source.threadTurns[read.ThreadID]...)
		source.mu.Unlock()
		if mutate != nil {
			turns = mutate(turns)
		}
		result.Thread.Turns = turns
	}
}

func setFakeThreadResult(result *threadResult, threadID, cwd string, roots []string) {
	result.Thread.ID = threadID
	result.CWD = cwd
	result.RuntimeWorkspaceRoots = append([]string(nil), roots...)
	profile := workerPermissionProfile
	if runtime.GOOS == "windows" {
		profile = windowsWorkerProfile
	}
	result.ActivePermissionProfile = &struct {
		ID string `json:"id"`
	}{ID: profile}
}

func (a *fakeApplication) MCPServerStatusList(_ context.Context, params, result any) error {
	a.mu.Lock()
	a.record.preflights++
	tools := append([]string(nil), a.tools...)
	resources := append([]json.RawMessage(nil), a.resources...)
	resourceTemplates := append([]json.RawMessage(nil), a.resourceTemplates...)
	extraServers := append([]mcpServerStatus(nil), a.extraServers...)
	authStatus := a.authStatus
	mcpStatusErr := a.mcpStatusErr
	a.mu.Unlock()
	if mcpStatusErr != nil {
		return mcpStatusErr
	}
	if params.(mcpStatusParams).Detail != "full" {
		return errors.New("managed MCP preflight did not request full inventory")
	}
	toolMap := make(map[string]json.RawMessage, len(tools))
	for _, tool := range tools {
		toolMap[tool] = json.RawMessage(`{}`)
	}
	result.(*mcpStatusPage).Data = append([]mcpServerStatus{{
		Name: workerServerName, Tools: toolMap, Resources: resources,
		ResourceTemplates: resourceTemplates, AuthStatus: authStatus,
	}}, extraServers...)
	return nil
}

func (a *fakeApplication) MCPServerToolCall(_ context.Context, params, result any) error {
	call := params.(mcpToolCallParams)
	a.mu.Lock()
	a.record.mcpTools = append(a.record.mcpTools, call)
	callErr := a.mcpToolErrors[call.Tool]
	callResult, configured := a.mcpToolResults[call.Tool]
	a.mu.Unlock()
	if callErr != nil {
		return callErr
	}
	if !configured {
		isError := true
		field := "messageId"
		if call.Tool == "wait_for_upstream_message" {
			field = "timeoutSeconds"
		}
		callResult = mcpToolCallResult{
			Content: []json.RawMessage{
				json.RawMessage(
					fmt.Sprintf(
						`{"type":"text","text":"validating \"arguments\": %s is invalid"}`,
						field,
					),
				),
			},
			IsError: &isError,
		}
	}
	*result.(*mcpToolCallResult) = callResult
	return nil
}

func (a *fakeApplication) TurnStart(ctx context.Context, params, result any) error {
	turnParams := params.(turnStartParams)
	turnID := newTestID()
	a.mu.Lock()
	a.record.turns = append(a.record.turns, turnParams)
	complete := a.completeBeforeReturn
	idleBeforeReturn := a.idleBeforeReturn
	idleReadStarted := a.idleReadStarted
	completeThenLose := a.completeThenLose
	completionStatus := a.completionStatus
	crashAfterComplete := a.crashAfterComplete
	turnStartErr := a.turnStartErr
	turnStartResponseLost := a.turnStartResponseLost
	turnStartGate := a.turnStartGate
	turnStartStarted := a.turnStartStarted
	a.mu.Unlock()
	complete = complete || turnStartResponseLost && completeThenLose
	if turnStartStarted != nil {
		a.turnStartStartedOnce.Do(func() { close(turnStartStarted) })
	}
	if turnStartGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-turnStartGate:
		}
	}
	if turnStartErr != nil && !turnStartResponseLost {
		return turnStartErr
	}
	started := turn{ID: turnID, Status: "inProgress"}
	result.(*turnStartResult).Turn = started
	a.mu.Lock()
	a.threadTurns[turnParams.ThreadID] = append(a.threadTurns[turnParams.ThreadID], started)
	a.mu.Unlock()
	if turnStartResponseLost && !completeThenLose {
		return turnStartErr
	}
	if complete || idleBeforeReturn {
		if completionStatus == "" {
			completionStatus = "completed"
		}
		a.mu.Lock()
		a.threadTurns[turnParams.ThreadID][len(a.threadTurns[turnParams.ThreadID])-1].Status = completionStatus
		a.mu.Unlock()
		if idleBeforeReturn {
			payload, _ := json.Marshal(map[string]any{
				"threadId": turnParams.ThreadID,
				"status":   map[string]string{"type": "idle"},
			})
			a.notifications <- appserver.Notification{
				Method: "thread/status/changed", Params: payload,
			}
			if idleReadStarted != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-idleReadStarted:
				}
			}
		} else {
			payload, _ := json.Marshal(turnCompletedNotification{
				ThreadID: turnParams.ThreadID,
				Turn:     turn{ID: turnID, Status: completionStatus, Error: json.RawMessage("null")},
			})
			a.notifications <- appserver.Notification{Method: "turn/completed", Params: payload}
		}
		if crashAfterComplete {
			a.crash(errors.New("test crash after buffered completion"))
		}
	}
	if turnStartResponseLost {
		return turnStartErr
	}
	return nil
}

func (a *fakeApplication) TurnSteer(_ context.Context, params, result any) error {
	steer := params.(turnSteerParams)
	a.mu.Lock()
	a.record.steers = append(a.record.steers, steer)
	hook := a.turnSteerHook
	turnSteerErr := a.turnSteerErr
	responseTurnID := a.steerResponseTurnID
	a.mu.Unlock()
	if hook != nil {
		hook(steer)
	}
	if turnSteerErr != nil {
		return turnSteerErr
	}
	if responseTurnID == "" {
		responseTurnID = steer.ExpectedTurnID
	}
	result.(*turnSteerResult).TurnID = responseTurnID
	return nil
}

func (a *fakeApplication) TurnInterrupt(_ context.Context, params, _ any) error {
	interrupt := params.(turnInterruptParams)
	a.mu.Lock()
	a.record.interrupts = append(a.record.interrupts, interrupt)
	turnInterruptErr := a.turnInterruptErr
	a.mu.Unlock()
	return turnInterruptErr
}

func (a *fakeApplication) Notifications() <-chan appserver.Notification {
	return a.notifications
}

func (a *fakeApplication) Done() <-chan struct{} {
	return a.done
}

func (a *fakeApplication) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

func (a *fakeApplication) Close(ctx context.Context) error {
	a.mu.Lock()
	a.closeCalls++
	closeErr := a.closeErr
	a.mu.Unlock()
	if a.closeStarted != nil {
		a.closeStartedOnce.Do(func() { close(a.closeStarted) })
	}
	if a.closeGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closeGate:
		}
	}
	a.finish(nil)
	return closeErr
}

func (a *fakeApplication) closeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closeCalls
}

func (a *fakeApplication) crash(err error) {
	a.finish(err)
}

func (a *fakeApplication) finish(err error) {
	a.mu.Lock()
	if err != nil {
		a.err = err
	}
	a.mu.Unlock()
	a.closeOnce.Do(func() {
		close(a.notifications)
		close(a.done)
	})
}

func (a *fakeApplication) notifyThreadClosed(threadID string) {
	payload, _ := json.Marshal(map[string]string{"threadId": threadID})
	a.notifications <- appserver.Notification{Method: "thread/closed", Params: payload}
}

func (a *fakeApplication) notifyCompletion(threadID, turnID, status string) {
	a.mu.Lock()
	for index := range a.threadTurns[threadID] {
		if a.threadTurns[threadID][index].ID == turnID {
			a.threadTurns[threadID][index].Status = status
		}
	}
	a.mu.Unlock()
	payload, _ := json.Marshal(turnCompletedNotification{
		ThreadID: threadID,
		Turn:     turn{ID: turnID, Status: status, Error: json.RawMessage("null")},
	})
	a.notifications <- appserver.Notification{Method: "turn/completed", Params: payload}
}

func (a *fakeApplication) snapshot() fakeRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return fakeRecord{
		starts:     append([]threadStartParams(nil), a.record.starts...),
		resumes:    append([]threadResumeParams(nil), a.record.resumes...),
		reads:      append([]threadReadParams(nil), a.record.reads...),
		mcpTools:   append([]mcpToolCallParams(nil), a.record.mcpTools...),
		turns:      append([]turnStartParams(nil), a.record.turns...),
		steers:     append([]turnSteerParams(nil), a.record.steers...),
		interrupts: append([]turnInterruptParams(nil), a.record.interrupts...),
		preflights: a.record.preflights,
	}
}

func newTestID() string {
	id, err := identity.NewID()
	if err != nil {
		panic(err)
	}
	return id
}

func writeTestManagedRollout(t *testing.T, home, threadID string) string {
	t.Helper()
	path := filepath.Join(
		home,
		"sessions",
		"2026",
		"07",
		"31",
		"rollout-2026-07-31T00-00-00-"+threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("thread metadata\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func initializeTestRepository(t *testing.T, repositoryPath string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(repositoryPath, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repositoryPath, "nested", "source.txt"), []byte("source\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, filepath.Dir(repositoryPath), "init", repositoryPath)
	hooksPath := "/dev/null"
	if runtime.GOOS == "windows" {
		hooksPath = "NUL"
	}
	for _, setting := range [][2]string{
		{"core.hooksPath", hooksPath},
		{"core.autocrlf", "false"},
		{"core.eol", "lf"},
		{"core.symlinks", "true"},
		{"core.excludesFile", ""},
		{"core.attributesFile", ""},
		{"maintenance.auto", "false"},
		{"gc.auto", "0"},
	} {
		runTestGit(t, repositoryPath, "config", setting[0], setting[1])
	}
	if runtime.GOOS != "windows" {
		runTestGit(t, repositoryPath, "config", "core.fileMode", "true")
	}
	runTestGit(t, repositoryPath, "add", "nested/source.txt")
	runTestGit(
		t, repositoryPath, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "initial",
	)
	return outputTestGit(t, repositoryPath, "rev-parse", "HEAD^{commit}")
}

func recordPreparedWorkspace(
	t *testing.T,
	state *store.PeerStore,
	workspaceID, workspacePath, head string,
) {
	recordPreparedWorkspaceForTree(t, state, testTreeID, workspaceID, workspacePath, head)
}

func recordPreparedWorkspaceForTree(
	t *testing.T,
	state *store.PeerStore,
	treeID, workspaceID, workspacePath, head string,
) {
	t.Helper()
	manifest := protocol.WorkspaceManifest{
		GitURL: "ssh://git@example.invalid/repository.git", HeadOID: head,
		ObjectFormat: "sha1", WorkingDirectory: "nested", Clean: true,
		SourceSnapshotHash: strings.Repeat("a", 64), Warnings: []string{},
	}
	hash, err := protocol.WorkspaceManifestHash(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.RecordPreparedWorkspace(context.Background(), store.PreparedWorkspace{
		PreparedWorkspaceKey: store.PreparedWorkspaceKey{
			ControllerID: testControllerID, TreeID: treeID, WorkspaceID: workspaceID,
		},
		SourceAgentID: testParentID, SourceDeviceID: testParentID,
		TargetDeviceID: testDeviceID, GitURL: manifest.GitURL,
		HeadOID: head, ObjectFormat: "sha1", WorkingDirectory: "nested",
		Clean: manifest.Clean, SourceSnapshotHash: manifest.SourceSnapshotHash,
		WorkspacePath: workspacePath, Strategy: protocol.WorkspaceStrategyDirect,
		ManifestHash: hash, SourceWarnings: []string{}, Warnings: []string{},
	}, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
}

func runTestGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		args = append([]string{"-c", "core.longpaths=true"}, args...)
	}
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

func outputTestGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		args = append([]string{"-c", "core.longpaths=true"}, args...)
	}
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(output))
}

func createHostedTestRepository(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	runTestGit(t, root, "init", "--bare", remote)
	initializeTestRepository(t, source)
	runTestGit(t, source, "remote", "add", "origin", remote)
	runTestGit(t, source, "push", "origin", "HEAD:refs/heads/main")
	runTestGit(t, root, "--git-dir="+remote, "update-server-info")
	server := httptest.NewTLSServer(http.FileServer(http.Dir(root)))
	t.Cleanup(server.Close)
	gitHome := filepath.Join(root, "git-home")
	if err := os.Mkdir(gitHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(gitHome, ".gitconfig"), []byte("[http]\n\tsslVerify = false\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", gitHome)
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	return server.URL + "/" + filepath.Base(remote), source
}
