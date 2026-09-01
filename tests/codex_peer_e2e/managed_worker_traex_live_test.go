//go:build integration && live && (linux || darwin)

package codex_peer_e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/traexauth"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func TestManagedWorkerTraeXWarmpoolLiveSmoke(t *testing.T) {
	delegationBinary := optionalLiveExecutable(t, "DELEGATION_E2E_BINARY")
	traeXBinary := optionalLiveExecutable(t, "TRAE_X_BINARY")
	warmpoolBinary := optionalLiveExecutable(t, "WARMPOOL_BINARY")
	traeAuthFile := optionalLiveProtectedFile(t, "TRAE_AUTH_FILE")
	traeAuth, err := traexauth.ReadSource(traeAuthFile)
	if err != nil {
		t.Fatalf("read live TraeX authentication source: %v", err)
	}
	accessToken := liveTraeXAccessToken(t, traeAuth)

	userHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	// Keep HOME short enough for the portable Unix socket path limit.
	root, err := os.MkdirTemp(userHome, ".dmtl-")
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

	controllerID := newTraeXLiveIdentity(t)
	deviceID := newTraeXLiveIdentity(t)
	treeID := newTraeXLiveIdentity(t)
	parentAgentID := newTraeXLiveIdentity(t)
	agentID := newTraeXLiveIdentity(t)
	delegationHome := filepath.Join(root, "delegation")
	configPath := filepath.Join(delegationHome, "peer.json")
	managedTraeHome := filepath.Join(root, "managed-trae")
	managedTraeCLIHome := filepath.Join(managedTraeHome, "cli")
	workspaceRoot := filepath.Join(root, "workspaces")
	statePath := filepath.Join(delegationHome, "state", "peer.sqlite3")
	managedAuthPath, err := traexauth.Sync(traeAuth, managedTraeHome)
	if err != nil {
		t.Fatalf("synchronize live TraeX authentication: %v", err)
	}
	runTraeXLive(t, os.Environ(), delegationBinary,
		"setup", "peer", "--config", configPath,
		"--host-kind", "traex",
		"--controller-id", controllerID, "--device-id", deviceID,
		"--device-name", "managed-worker-traex-live", "--broker-url", "ws://127.0.0.1:1",
		"--auth-mode", "none",
		"--cli-command", traeXBinary,
		"--cli-launcher", warmpoolBinary,
		"--cli-launcher-prefix-argument=run",
		"--cli-launcher-prefix-argument=--",
		"--trae-auth-file", traeAuthFile,
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
				"run", "--", traeXBinary,
			},
		},
		CLIRuntimeExecutable: traeXBinary,
		GitBinary:            resolveLiveExecutable(t, "git"),
		CodexHome:            managedTraeHome,
		TraeAuthSourceFile:   traeAuthFile,
		ManagedTraeAuthFile:  managedAuthPath,
		WorkspaceRoot:        workspaceRoot,
		MaxWorkerSlots:       1,
		Store:                state,
		ResultPackages:       resultPackages,
		ReportError:          reportError,
	})
	if err != nil {
		t.Fatal(err)
	}
	managedTraeCLIHome, err = filepath.EvalSymlinks(managedTraeCLIHome)
	if err != nil {
		t.Fatalf("resolve managed TraeX CLI home: %v", err)
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
		Prompt: fmt.Sprintf(
			"Use the shell tool exactly once to run: if cat %s >/dev/null 2>&1; then printf SOURCE_AUTH_RESULT=readable; else printf SOURCE_AUTH_RESULT=blocked; fi; printf '\\n'; if cat %s >/dev/null 2>&1; then printf MANAGED_AUTH_RESULT=readable; else printf MANAGED_AUTH_RESULT=blocked; fi; printf '\\n'; rm -f .delegation-source-auth-probe .delegation-managed-auth-probe; if ln -s %s .delegation-source-auth-probe; then printf SOURCE_ALIAS_SETUP=ok; else printf SOURCE_ALIAS_SETUP=failed; fi; printf '\\n'; if ln -s %s .delegation-managed-auth-probe; then printf MANAGED_ALIAS_SETUP=ok; else printf MANAGED_ALIAS_SETUP=failed; fi; printf '\\n'; if cat .delegation-source-auth-probe >/dev/null 2>&1; then printf SOURCE_ALIAS_RESULT=readable; else printf SOURCE_ALIAS_RESULT=blocked; fi; printf '\\n'; if cat .delegation-managed-auth-probe >/dev/null 2>&1; then printf MANAGED_ALIAS_RESULT=readable; else printf MANAGED_ALIAS_RESULT=blocked; fi; rm -f .delegation-source-auth-probe .delegation-managed-auth-probe. Then reply with exactly the command output. Never read or print either file's contents.",
			shellSingleQuote(traeAuthFile),
			shellSingleQuote(managedAuthPath),
			shellSingleQuote(traeAuthFile),
			shellSingleQuote(managedAuthPath),
		),
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
	rolloutPath := assertTraeXLiveRollout(t, managedTraeCLIHome, worker.CodexThreadID)
	assertTraeXAccountBoundary(t, rolloutPath, traeAuthFile, managedAuthPath, accessToken)
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
		OperationID: newTraeXLiveIdentity(t),
		Key:         started.Worker.WorkerKey,
		Message:     "Reply with exactly DELEGATION_TRAEX_ACCOUNT_REUSE_OK and do not call tools.",
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
	assertTraeXAccountBoundary(t, rolloutPath, traeAuthFile, managedAuthPath, accessToken)
	rollout, err := os.ReadFile(rolloutPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rollout, []byte("DELEGATION_TRAEX_ACCOUNT_REUSE_OK")) {
		t.Fatal("cold-resumed real TraeX account turn omitted the expected result")
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

func containsTraeXLiveProcessValue(environment []byte, name, value string) bool {
	want := name + "=" + value
	for _, entry := range strings.Split(string(environment), "\x00") {
		if entry == want {
			return true
		}
	}
	return false
}

func containsTraeXLiveProcessArguments(cmdline []byte, want ...string) bool {
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
		running, err := traeXLiveProcessRunning(pid)
		if err == nil && !running {
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
