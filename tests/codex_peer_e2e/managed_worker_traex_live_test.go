//go:build integration && live && linux

package codex_peer_e2e

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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func TestManagedWorkerTraeXWarmpoolLiveSmoke(t *testing.T) {
	delegationBinary := optionalLiveExecutable(t, "DELEGATION_E2E_BINARY")
	traeXBinary := optionalLiveExecutable(t, "TRAE_X_BINARY")
	warmpoolBinary := optionalLiveExecutable(t, "WARMPOOL_BINARY")

	root, err := os.MkdirTemp("", "delegation-traex-live-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}

	isolatedHome := liveEmptyDirectory(t, root, "home")
	ambientCodexHome := liveEmptyDirectory(t, root, "ambient-codex")
	ambientTraeHome := liveEmptyDirectory(t, root, "ambient-trae")
	ambientTraeCLIHome := liveEmptyDirectory(t, root, "ambient-trae-cli")
	t.Setenv("HOME", isolatedHome)
	t.Setenv("CODEX_HOME", ambientCodexHome)
	t.Setenv("TRAE_HOME", ambientTraeHome)
	t.Setenv("TRAECLI_HOME", ambientTraeCLIHome)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	provider := &traeXLiveResponses{}
	modelServer := httptest.NewServer(provider)
	t.Cleanup(modelServer.Close)
	providerConfig := map[string]any{
		"model":          "delegation-traex-live",
		"model_provider": "delegation_live",
		"model_providers.delegation_live": map[string]any{
			"name":                 "Delegation TraeX live smoke",
			"base_url":             modelServer.URL + "/v1",
			"wire_api":             "responses",
			"requires_openai_auth": false,
		},
	}

	controllerID := newIdentity(t)
	deviceID := newIdentity(t)
	treeID := newIdentity(t)
	parentAgentID := newIdentity(t)
	agentID := newIdentity(t)
	delegationHome := filepath.Join(root, "delegation")
	configPath := filepath.Join(delegationHome, "peer.json")
	managedTraeHome := filepath.Join(root, "managed-trae")
	managedTraeCLIHome := filepath.Join(managedTraeHome, "cli")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	run(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", configPath,
		"--host-kind", "traex",
		"--controller-id", controllerID, "--device-id", deviceID,
		"--device-name", "managed-worker-traex-live", "--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none",
		"--cli-command", traeXBinary,
		"--cli-argument=-p", "--cli-argument=ultra",
		"--cli-launcher", warmpoolBinary,
		"--cli-launcher-prefix-argument=run",
		"--cli-launcher-prefix-argument=--",
		"--codex-home", managedTraeHome, "--workspace-root", workspaceRoot,
		"--state", statePath, "--max-worker-slots", "1", "--json",
	)

	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Errorf("close TraeX live peer state: %v", err)
		}
	})
	resultPackages, err := resultpackagefiles.New(context.Background(), resultpackagefiles.Options{
		ControllerID:  controllerID,
		DeviceID:      deviceID,
		WorkspaceRoot: workspaceRoot,
		Store:         state,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resultPackages.Close(); err != nil {
			t.Errorf("close TraeX live result packages: %v", err)
		}
	})

	var reportMu sync.Mutex
	var reportedErrors []error
	reportChanges := make(chan struct{}, 1)
	reportError := func(err error) {
		reportMu.Lock()
		reportedErrors = append(reportedErrors, err)
		reportMu.Unlock()
		select {
		case reportChanges <- struct{}{}:
		default:
		}
	}
	loadReportedErrors := func() error {
		reportMu.Lock()
		defer reportMu.Unlock()
		return errors.Join(reportedErrors...)
	}
	reportedErrorCount := func() int {
		reportMu.Lock()
		defer reportMu.Unlock()
		return len(reportedErrors)
	}
	host, err := workerhost.New(context.Background(), workerhost.Options{
		ControllerID:     controllerID,
		DeviceID:         deviceID,
		HostKind:         hostkind.TraeX,
		PeerConfigPath:   configPath,
		DelegationBinary: delegationBinary,
		CLILaunch: clilaunch.Spec{
			Executable: warmpoolBinary,
			PrefixArguments: []string{
				"run", "--", traeXBinary, "-p", "ultra",
			},
		},
		CLIRuntimeExecutable: traeXBinary,
		GitBinary:            resolveLiveExecutable(t, "git"),
		CodexHome:            managedTraeHome,
		WorkspaceRoot:        workspaceRoot,
		MaxWorkerSlots:       1,
		CodexConfig:          providerConfig,
		Store:                state,
		ResultPackages:       resultPackages,
		ReportError:          reportError,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("close TraeX live worker host: %v", err)
		}
	})

	started, err := host.Spawn(context.Background(), workerhost.SpawnRequest{
		TreeID: treeID, AgentID: agentID, ParentAgentID: parentAgentID,
		TaskName: "TraeX warmpool live smoke",
		Prompt:   "Complete this smoke turn without calling tools.",
	})
	if err != nil {
		t.Fatal(errors.Join(err, loadReportedErrors()))
	}
	worker, published, acknowledged := waitForTraeXLiveResult(
		t,
		state,
		resultPackages,
		started.Worker.WorkerKey,
		started.Worker.ActiveTurnID,
		loadReportedErrors,
	)
	if !published || !acknowledged {
		t.Fatalf("result package published = %t, acknowledged = %t", published, acknowledged)
	}
	calls, workerCalls, initialAuxiliaryCalls, providerErr := provider.result()
	if calls != workerCalls+initialAuxiliaryCalls ||
		workerCalls != 1 || initialAuxiliaryCalls > 1 || providerErr != nil {
		t.Fatalf(
			"initial loopback provider calls = %d (worker %d, auxiliary %d), want 1 worker and at most 1 auxiliary; error = %v",
			calls,
			workerCalls,
			initialAuxiliaryCalls,
			providerErr,
		)
	}
	rolloutPath := assertTraeXLiveRollout(t, managedTraeCLIHome, worker.CodexThreadID)
	rolloutBefore, err := os.Stat(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	firstAppServerPID := waitForTraeXLiveAppServerPID(t, managedTraeCLIHome, 0)
	errorsBeforeReplacement := reportedErrorCount()
	killTraeXLiveAppServer(t, firstAppServerPID)
	waitForTraeXLiveAppServerExit(t, firstAppServerPID)
	waitForTraeXLiveReplacementRecovery(
		t,
		reportChanges,
		reportedErrorCount,
		errorsBeforeReplacement,
	)

	followup, err := host.Followup(context.Background(), workerhost.FollowupRequest{
		OperationID: newIdentity(t),
		Key:         started.Worker.WorkerKey,
		Message:     "Complete this cold-resume follow-up turn without calling tools.",
	})
	if err != nil {
		t.Fatal(errors.Join(err, loadReportedErrors()))
	}
	if followup.Worker.CodexThreadID != worker.CodexThreadID {
		t.Fatalf(
			"cold-resumed thread = %q, want %q",
			followup.Worker.CodexThreadID,
			worker.CodexThreadID,
		)
	}
	if followup.Worker.ActiveTurnID == "" ||
		followup.Worker.ActiveTurnID == started.Worker.ActiveTurnID {
		t.Fatalf(
			"cold-resumed active turn = %q, initial turn = %q",
			followup.Worker.ActiveTurnID,
			started.Worker.ActiveTurnID,
		)
	}
	replacementAppServerPID := waitForTraeXLiveAppServerPID(
		t,
		managedTraeCLIHome,
		firstAppServerPID,
	)
	resumedWorker, published, acknowledged := waitForTraeXLiveResult(
		t,
		state,
		resultPackages,
		followup.Worker.WorkerKey,
		followup.Worker.ActiveTurnID,
		loadReportedErrors,
	)
	if !published || !acknowledged {
		t.Fatalf(
			"cold-resume result package published = %t, acknowledged = %t",
			published,
			acknowledged,
		)
	}
	if resumedWorker.CodexThreadID != worker.CodexThreadID {
		t.Fatalf(
			"acknowledged cold-resume thread = %q, want %q",
			resumedWorker.CodexThreadID,
			worker.CodexThreadID,
		)
	}
	calls, workerCalls, auxiliaryCalls, providerErr := provider.result()
	if calls != workerCalls+auxiliaryCalls || workerCalls != 2 ||
		auxiliaryCalls < initialAuxiliaryCalls ||
		auxiliaryCalls > initialAuxiliaryCalls+1 || providerErr != nil {
		t.Fatalf(
			"cold-resume loopback provider calls = %d (worker %d, auxiliary %d), want 2 workers and at most 1 new auxiliary; error = %v",
			calls,
			workerCalls,
			auxiliaryCalls,
			providerErr,
		)
	}
	if replacementAppServerPID == firstAppServerPID {
		t.Fatalf("TraeX app-server PID was not replaced: %d", firstAppServerPID)
	}
	if resumedRolloutPath := assertTraeXLiveRollout(
		t,
		managedTraeCLIHome,
		resumedWorker.CodexThreadID,
	); resumedRolloutPath != rolloutPath {
		t.Fatalf("cold-resumed rollout path = %q, want %q", resumedRolloutPath, rolloutPath)
	}
	rolloutAfter, err := os.Stat(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	if rolloutAfter.Size() <= rolloutBefore.Size() {
		t.Fatalf(
			"cold-resumed rollout size = %d, want greater than initial size %d",
			rolloutAfter.Size(),
			rolloutBefore.Size(),
		)
	}
	for name, path := range map[string]string{
		"ambient CODEX_HOME":   ambientCodexHome,
		"ambient TRAE_HOME":    ambientTraeHome,
		"ambient TRAECLI_HOME": ambientTraeCLIHome,
	} {
		if empty, err := liveDirectoryTreeEmpty(path); err != nil {
			t.Fatal(err)
		} else if !empty {
			t.Fatalf("%s received managed runtime state", name)
		}
	}
}

type traeXLiveResponses struct {
	mu             sync.Mutex
	calls          int
	workerCalls    int
	auxiliaryCalls int
	err            error
}

func (m *traeXLiveResponses) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, decodeErr := decodeRequest(request)
	m.mu.Lock()
	call := m.calls
	m.calls++
	if request.Method != http.MethodPost || request.URL.Path != "/v1/responses" {
		m.err = errors.Join(
			m.err,
			fmt.Errorf("unexpected provider request %s %s", request.Method, request.URL.Path),
		)
	}
	if decodeErr != nil {
		m.err = errors.Join(m.err, decodeErr)
	}
	workerCall := -1
	auxiliaryCall := -1
	if decodeErr == nil && containsManagedWorkerMCPNamespace(body["tools"]) {
		workerCall = m.workerCalls
		m.workerCalls++
	} else if decodeErr == nil {
		auxiliaryCall = m.auxiliaryCalls
		m.auxiliaryCalls++
	}
	m.mu.Unlock()
	if decodeErr != nil {
		writeFinalResponse(writer, "traex-live-decode-error")
		return
	}
	if workerCall > 1 {
		m.mu.Lock()
		m.err = errors.Join(
			m.err,
			fmt.Errorf("unexpected TraeX worker provider call %d", workerCall+1),
		)
		m.mu.Unlock()
	}
	if workerCall >= 0 {
		if err := validateTraeXLiveWorkerTools(body["tools"]); err != nil {
			m.mu.Lock()
			m.err = errors.Join(m.err, err)
			m.mu.Unlock()
		}
		expectedPrompt := "Complete this smoke turn without calling tools."
		if workerCall == 1 {
			expectedPrompt = "Complete this cold-resume follow-up turn without calling tools."
		}
		encoded, _ := json.Marshal(body)
		if !strings.Contains(string(encoded), expectedPrompt) {
			m.mu.Lock()
			m.err = errors.Join(
				m.err,
				fmt.Errorf(
					"TraeX worker provider call %d omitted prompt %q",
					workerCall+1,
					expectedPrompt,
				),
			)
			m.mu.Unlock()
		}
	}
	if auxiliaryCall > 1 {
		m.mu.Lock()
		m.err = errors.Join(
			m.err,
			fmt.Errorf("unexpected TraeX auxiliary provider call %d", auxiliaryCall+1),
		)
		m.mu.Unlock()
	}
	writeFinalResponse(writer, fmt.Sprintf("traex-live-%d", call+1))
}

func (m *traeXLiveResponses) result() (int, int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.workerCalls, m.auxiliaryCalls, m.err
}

func validateTraeXLiveWorkerTools(raw any) error {
	declarations, ok := raw.([]any)
	if !ok {
		return errors.New("TraeX model request tools are not an array")
	}
	namespaces := make(map[string][]string)
	for _, declaration := range declarations {
		tool, ok := declaration.(map[string]any)
		if !ok {
			return fmt.Errorf("TraeX model request contains an invalid tool declaration: %#v", declaration)
		}
		name, _ := tool["name"].(string)
		if !strings.HasPrefix(name, "mcp__") {
			continue
		}
		if tool["type"] != "namespace" {
			return fmt.Errorf("TraeX model request exposes MCP tool outside a namespace: %#v", tool)
		}
		children, ok := tool["tools"].([]any)
		if !ok {
			return fmt.Errorf("TraeX MCP namespace %q tools are not an array", name)
		}
		names := make([]string, 0, len(children))
		for _, child := range children {
			function, ok := child.(map[string]any)
			if !ok || function["type"] != "function" {
				return fmt.Errorf("TraeX MCP namespace %q contains an invalid tool: %#v", name, child)
			}
			childName, _ := function["name"].(string)
			if childName == "" {
				return fmt.Errorf("TraeX MCP namespace %q contains an unnamed tool", name)
			}
			names = append(names, childName)
		}
		if _, duplicate := namespaces[name]; duplicate {
			return fmt.Errorf("TraeX model request repeats MCP namespace %q", name)
		}
		namespaces[name] = names
	}
	tools, found := namespaces["mcp__delegation_worker__"]
	if len(namespaces) != 1 || !found {
		return fmt.Errorf(
			"TraeX model request MCP namespaces = %v, want only mcp__delegation_worker__",
			namespaces,
		)
	}
	want := map[string]bool{
		"send_upstream_message":     false,
		"wait_for_upstream_message": false,
	}
	for _, tool := range tools {
		if _, allowed := want[tool]; !allowed {
			return fmt.Errorf("TraeX model request exposed unexpected worker MCP tool %q", tool)
		}
		want[tool] = true
	}
	for tool, found := range want {
		if !found {
			return fmt.Errorf("TraeX model request omitted worker MCP tool %q: %v", tool, tools)
		}
	}
	if len(tools) != len(want) {
		return fmt.Errorf(
			"TraeX model request exposed %d worker MCP tools, want %d: %v",
			len(tools),
			len(want),
			tools,
		)
	}
	return nil
}

func containsManagedWorkerMCPNamespace(raw any) bool {
	declarations, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, declaration := range declarations {
		tool, ok := declaration.(map[string]any)
		if !ok {
			continue
		}
		name, _ := tool["name"].(string)
		if name == "mcp__delegation_worker__" {
			return true
		}
	}
	return false
}

func waitForTraeXLiveResult(
	t *testing.T,
	state *store.PeerStore,
	resultPackages *resultpackagefiles.Manager,
	key store.WorkerKey,
	expectedTurnID string,
	reportedErrors func() error,
) (store.WorkerReservation, bool, bool) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	published := false
	acknowledged := false
	for time.Now().Before(deadline) {
		worker, workerErr := state.GetWorker(context.Background(), key)
		if workerErr == nil {
			switch worker.Status {
			case store.WorkerIdle:
				return worker, published, acknowledged
			case store.WorkerFailed:
				t.Fatalf(
					"TraeX live worker failed with %s: %v",
					worker.FailureCode,
					reportedErrors(),
				)
			}
		}
		pending, err := resultPackages.ListPendingResultPublications(context.Background())
		if err != nil {
			t.Fatal(errors.Join(err, reportedErrors()))
		}
		for _, outbox := range pending {
			if outbox.WorkerKey != key {
				continue
			}
			published = true
			if outbox.Manifest.TurnID != expectedTurnID ||
				outbox.Manifest.Terminal.Outcome != protocol.ResultTerminalCompleted ||
				outbox.Manifest.Rollout.Status != protocol.ResultRolloutAvailable {
				t.Fatalf(
					"TraeX live result package for turn %s = %#v; background errors: %v",
					expectedTurnID,
					outbox,
					reportedErrors(),
				)
			}
			finalization, err := resultPackages.AcknowledgeResultPackageMetadata(
				context.Background(),
				outbox.ResultOutboxKey,
				outbox.Metadata,
			)
			if err != nil {
				t.Fatalf(
					"acknowledge TraeX live result package: %v",
					errors.Join(err, reportedErrors()),
				)
			}
			if finalization.Outbox.State != store.ResultOutboxDeliveryPending {
				t.Fatalf("acknowledged TraeX live outbox = %#v", finalization.Outbox)
			}
			acknowledged = true
		}
		time.Sleep(50 * time.Millisecond)
	}
	worker, err := state.GetWorker(context.Background(), key)
	t.Fatalf(
		"TraeX live worker did not become idle: %#v, %v; published = %t; acknowledged = %t; background errors: %v",
		worker,
		err,
		published,
		acknowledged,
		reportedErrors(),
	)
	return store.WorkerReservation{}, published, acknowledged
}

func assertTraeXLiveRollout(t *testing.T, cliHome, threadID string) string {
	t.Helper()
	sessionsRoot := filepath.Join(cliHome, "sessions")
	var matches []string
	err := filepath.WalkDir(sessionsRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() &&
			strings.Contains(entry.Name(), threadID) &&
			strings.HasSuffix(entry.Name(), ".jsonl") {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil || len(matches) != 1 {
		t.Fatalf(
			"TraeX rollout for thread %s beneath %s = %v, error %v",
			threadID,
			sessionsRoot,
			matches,
			err,
		)
	}
	return matches[0]
}

func waitForTraeXLiveAppServerPID(t *testing.T, cliHome string, previous int) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		pids, err := traeXLiveAppServerPIDs(cliHome, previous)
		if err == nil && len(pids) == 1 {
			return pids[0]
		}
		if err == nil {
			err = fmt.Errorf("found %d matching app-server processes: %v", len(pids), pids)
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf(
		"TraeX app-server beneath TRAECLI_HOME %s did not change from %d: %v",
		cliHome,
		previous,
		lastErr,
	)
	return 0
}

func traeXLiveAppServerPIDs(cliHome string, excluded int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == excluded {
			continue
		}
		processRoot := filepath.Join("/proc", entry.Name())
		environment, err := os.ReadFile(filepath.Join(processRoot, "environ"))
		if err != nil || !containsProcValue(environment, "TRAECLI_HOME", cliHome) {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(processRoot, "cmdline"))
		if err != nil || !containsProcArguments(
			cmdline,
			"app-server",
			"--listen",
			"stdio://",
		) {
			continue
		}
		signalErr := syscall.Kill(pid, 0)
		if signalErr == nil || errors.Is(signalErr, syscall.EPERM) {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

func containsProcValue(environment []byte, name, value string) bool {
	want := name + "=" + value
	for _, entry := range strings.Split(string(environment), "\x00") {
		if entry == want {
			return true
		}
	}
	return false
}

func containsProcArguments(cmdline []byte, want ...string) bool {
	arguments := strings.Split(strings.TrimSuffix(string(cmdline), "\x00"), "\x00")
	for index := 0; index+len(want) <= len(arguments); index++ {
		matches := true
		for offset := range want {
			if arguments[index+offset] != want[offset] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func killTraeXLiveAppServer(t *testing.T, pid int) {
	t.Helper()
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatalf("kill TraeX app-server %d: %v", pid, err)
	}
}

func waitForTraeXLiveAppServerExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("TraeX app-server %d did not exit after SIGKILL", pid)
}

func waitForTraeXLiveReplacementRecovery(
	t *testing.T,
	changes <-chan struct{},
	count func() int,
	previous int,
) {
	t.Helper()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for count() <= previous {
		select {
		case <-changes:
		case <-timer.C:
			t.Fatalf("managed host did not observe TraeX app-server replacement")
		}
	}
}

func optionalLiveExecutable(t *testing.T, variable string) string {
	t.Helper()
	path := os.Getenv(variable)
	if path == "" {
		t.Skipf("%s is not set", variable)
	}
	return resolveLiveExecutable(t, path)
}

func resolveLiveExecutable(t *testing.T, path string) string {
	t.Helper()
	resolved, err := exec.LookPath(path)
	if err != nil {
		t.Fatalf("resolve executable %s: %v", path, err)
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func liveEmptyDirectory(t *testing.T, root, name string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func liveDirectoryTreeEmpty(path string) (bool, error) {
	empty := true
	err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if current != path {
			empty = false
		}
		return nil
	})
	return empty, err
}
