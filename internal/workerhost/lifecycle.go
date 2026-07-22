package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
)

func (h *Host) startNewThread(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	prompt string,
) (StartedTurn, <-chan struct{}, error) {
	var result threadResult
	err := client.ThreadStart(ctx, threadStartParams{
		CWD: worker.WorkspacePath, ApprovalPolicy: "never",
		Config: h.managedConfig(worker), ServiceName: "delegation",
		ThreadSource: workerSource, DeveloperMessage: workerInstructions,
	}, &result)
	if err != nil {
		if errors.Is(err, appserver.ErrRequestNotWritten) {
			restored, restoreErr := h.restoreWorkerAfterUnsent(worker, store.WorkerPending, err)
			return StartedTurn{Worker: restored}, nil, restoreErr
		}
		if h.shouldRetire(client, err) {
			failureErr := h.failWorker(worker.WorkerKey, "thread_start_ambiguous", err)
			return StartedTurn{Worker: worker}, h.retireClient(client, failureErr), failureErr
		}
		return StartedTurn{Worker: worker}, nil,
			h.failWorker(worker.WorkerKey, "thread_start_failed", err)
	}
	if err := identity.ValidateID(result.Thread.ID); err != nil {
		protocolErr := fmt.Errorf("app-server returned invalid threadId: %w", err)
		return StartedTurn{Worker: worker}, h.retireClient(client, protocolErr), protocolErr
	}
	worker, err = h.recordWorkerChange(
		h.state.AttachWorkerThread(ctx, worker.WorkerKey, result.Thread.ID, time.Now()),
	)
	if err != nil {
		failureErr := h.failWorker(worker.WorkerKey, "thread_start_ambiguous", err)
		return StartedTurn{Worker: worker}, h.retireClient(client, failureErr), failureErr
	}
	if err := h.verifyWorkerMCP(ctx, client, worker.CodexThreadID); err != nil {
		if errors.Is(err, appserver.ErrRequestNotWritten) {
			restored, restoreErr := h.restoreWorkerAfterUnsent(worker, store.WorkerPending, err)
			return StartedTurn{Worker: restored}, nil, restoreErr
		}
		if h.shouldRetire(client, err) {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		recovery, failureErr := h.failWorkerMCP(client, worker.WorkerKey, err)
		return StartedTurn{Worker: worker}, recovery, failureErr
	}
	worker, err = h.recordWorkerChange(h.state.MarkWorkerReady(ctx, worker.WorkerKey, time.Now()))
	if err != nil {
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID)
	return h.startTurn(ctx, client, worker, prompt, store.WorkerPending)
}

func (h *Host) resumeThread(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	retryStatus store.WorkerStatus,
) (store.WorkerReservation, <-chan struct{}, error) {
	var result threadResult
	err := client.ThreadResume(ctx, threadResumeParams{
		ThreadID: worker.CodexThreadID, CWD: worker.WorkspacePath,
		ApprovalPolicy: "never",
		Config:         h.managedConfig(worker), DeveloperMessage: workerInstructions,
		ExcludeTurns: true,
	}, &result)
	if err != nil {
		if errors.Is(err, appserver.ErrRequestNotWritten) {
			restored, restoreErr := h.restoreWorkerAfterUnsent(worker, retryStatus, err)
			return restored, nil, restoreErr
		}
		if h.shouldRetire(client, err) {
			return worker, h.retireClient(client, err), err
		}
		return worker, nil, h.failWorker(worker.WorkerKey, "thread_resume_failed", err)
	}
	if result.Thread.ID != worker.CodexThreadID {
		protocolErr := fmt.Errorf(
			"app-server resumed thread %q instead of %q",
			result.Thread.ID,
			worker.CodexThreadID,
		)
		return worker, h.retireClient(client, protocolErr), protocolErr
	}
	worker, err = h.recordWorkerChange(
		h.state.AttachWorkerThread(ctx, worker.WorkerKey, worker.CodexThreadID, time.Now()),
	)
	if err != nil {
		return worker, h.retireClient(client, err), err
	}
	if err := h.verifyWorkerMCP(ctx, client, worker.CodexThreadID); err != nil {
		if errors.Is(err, appserver.ErrRequestNotWritten) {
			restored, restoreErr := h.restoreWorkerAfterUnsent(worker, retryStatus, err)
			return restored, nil, restoreErr
		}
		if h.shouldRetire(client, err) {
			return worker, h.retireClient(client, err), err
		}
		recovery, failureErr := h.failWorkerMCP(client, worker.WorkerKey, err)
		return worker, recovery, failureErr
	}
	worker, err = h.recordWorkerChange(h.state.MarkWorkerReady(ctx, worker.WorkerKey, time.Now()))
	if err != nil {
		return worker, h.retireClient(client, err), err
	}
	h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID)
	return worker, nil, nil
}

func (h *Host) startTurn(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	prompt string,
	retryStatus store.WorkerStatus,
) (StartedTurn, <-chan struct{}, error) {
	var result turnStartResult
	err := client.TurnStart(ctx, turnStartParams{
		ThreadID: worker.CodexThreadID,
		Input:    []textInput{{Type: "text", Text: prompt, TextElements: []any{}}},
	}, &result)
	if err != nil {
		if errors.Is(err, appserver.ErrRequestNotWritten) {
			restored, restoreErr := h.restoreWorkerAfterUnsent(worker, retryStatus, err)
			return StartedTurn{Worker: restored}, nil, restoreErr
		}
		if h.shouldRetire(client, err) {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		return StartedTurn{Worker: worker}, nil,
			h.failWorker(worker.WorkerKey, "turn_start_failed", err)
	}
	if err := identity.ValidateID(result.Turn.ID); err != nil {
		protocolErr := fmt.Errorf("app-server returned invalid turnId: %w", err)
		return StartedTurn{Worker: worker}, h.retireClient(client, protocolErr), protocolErr
	}
	worker, err = h.recordWorkerChange(
		h.state.MarkWorkerRunning(ctx, worker.WorkerKey, result.Turn.ID, time.Now()),
	)
	if err != nil {
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	return StartedTurn{Worker: worker}, nil, nil
}

func (h *Host) retryPendingThread(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	prompt string,
) (StartedTurn, <-chan struct{}, error) {
	var err error
	if h.isLoaded(client, worker.WorkerKey, worker.CodexThreadID) {
		worker, err = h.recordWorkerChange(
			h.state.AttachWorkerThread(ctx, worker.WorkerKey, worker.CodexThreadID, time.Now()),
		)
		if err != nil {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		if err := h.verifyWorkerMCP(ctx, client, worker.CodexThreadID); err != nil {
			if errors.Is(err, appserver.ErrRequestNotWritten) {
				restored, restoreErr := h.restoreWorkerAfterUnsent(worker, store.WorkerPending, err)
				return StartedTurn{Worker: restored}, nil, restoreErr
			}
			if h.shouldRetire(client, err) {
				return StartedTurn{Worker: worker}, h.retireClient(client, err), err
			}
			recovery, failureErr := h.failWorkerMCP(client, worker.WorkerKey, err)
			return StartedTurn{Worker: worker}, recovery, failureErr
		}
		worker, err = h.recordWorkerChange(h.state.MarkWorkerReady(ctx, worker.WorkerKey, time.Now()))
		if err != nil {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
	} else {
		var recovery <-chan struct{}
		worker, recovery, err = h.resumeThread(ctx, client, worker, store.WorkerPending)
		if err != nil {
			return StartedTurn{Worker: worker}, recovery, err
		}
	}
	return h.startTurn(ctx, client, worker, prompt, store.WorkerPending)
}

func (h *Host) restoreWorkerAfterUnsent(
	worker store.WorkerReservation,
	target store.WorkerStatus,
	cause error,
) (store.WorkerReservation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	var (
		restored store.WorkerReservation
		err      error
	)
	switch target {
	case store.WorkerPending:
		restored, err = h.recordWorkerChange(
			h.state.RestoreWorkerPendingAfterUnsent(ctx, worker.WorkerKey, time.Now()),
		)
	case store.WorkerIdle:
		restored, err = h.recordWorkerChange(
			h.state.RestoreWorkerIdleAfterUnsent(ctx, worker.WorkerKey, time.Now()),
		)
	default:
		err = fmt.Errorf("unsupported unsent recovery status %q", target)
	}
	if err != nil {
		fatalErr := errors.Join(cause, fmt.Errorf("restore worker after unsent request: %w", err))
		h.fail(fatalErr)
		return worker, fatalErr
	}
	return restored, cause
}

func (h *Host) acquireOperation(ctx context.Context) (func(), error) {
	for {
		h.clientMu.Lock()
		if h.fatalErr != nil {
			err := h.fatalErr
			h.clientMu.Unlock()
			return nil, err
		}
		if h.closed {
			h.clientMu.Unlock()
			return nil, ErrClosed
		}
		recovering := h.recovering
		h.clientMu.Unlock()
		if recovering != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-recovering:
			}
			continue
		}

		h.operations.RLock()
		h.clientMu.Lock()
		stable := !h.closed && h.fatalErr == nil && h.recovering == nil
		closed := h.closed
		fatalErr := h.fatalErr
		h.clientMu.Unlock()
		if stable {
			return h.operations.RUnlock, nil
		}
		h.operations.RUnlock()
		if closed {
			return nil, ErrClosed
		}
		if fatalErr != nil {
			return nil, fatalErr
		}
	}
}

func (h *Host) waitForCurrentRecovery(ctx context.Context) error {
	h.clientMu.Lock()
	recovering := h.recovering
	h.clientMu.Unlock()
	if recovering == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-recovering:
		return nil
	}
}

func (h *Host) ensureClient(ctx context.Context) (application, error) {
	for {
		h.clientMu.Lock()
		if h.fatalErr != nil {
			err := h.fatalErr
			h.clientMu.Unlock()
			return nil, err
		}
		if h.closed {
			h.clientMu.Unlock()
			return nil, ErrClosed
		}
		if h.recovering != nil {
			h.clientMu.Unlock()
			return nil, errClientRecovering
		}
		if h.client != nil {
			client := h.client
			h.clientMu.Unlock()
			return client, nil
		}
		if h.starting != nil {
			starting := h.starting
			h.clientMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-starting:
			}
			continue
		}
		starting := make(chan struct{})
		h.starting = starting
		h.clientMu.Unlock()

		if err := h.validateRuntimeDirectories(); err != nil {
			h.clientMu.Lock()
			h.starting = nil
			close(starting)
			h.clientMu.Unlock()
			return nil, err
		}
		client, err := h.startApplication(ctx, appserver.Options{
			Binary: h.codexBinary, SupervisorBinary: h.delegationBinary,
			CodexHome:   h.codexHome,
			Environment: h.codexEnvironment, UnsetEnvironment: h.codexUnsetEnvironment,
		})
		h.clientMu.Lock()
		if err != nil {
			h.starting = nil
			close(starting)
			h.clientMu.Unlock()
			return nil, err
		}
		if h.closed || h.recovering != nil {
			closed := h.closed
			h.clientMu.Unlock()
			closeContext, cancel := context.WithTimeout(context.Background(), stateTimeout)
			closeErr := client.Close(closeContext)
			cancel()
			if closeErr != nil {
				h.reportError(closeErr)
				if closed || errors.Is(closeErr, appserver.ErrProcessExitUnconfirmed) {
					h.fail(fmt.Errorf("close unclaimed managed app-server: %w", closeErr))
				}
			}
			h.clientMu.Lock()
			h.starting = nil
			close(starting)
			h.clientMu.Unlock()
			if closed {
				return nil, ErrClosed
			}
			return nil, errClientRecovering
		}
		h.client = client
		h.loaded = make(map[store.WorkerKey]string)
		h.completionDrains[client] = make(chan struct{})
		h.monitors.Add(1)
		go h.monitorClient(client)
		h.starting = nil
		close(starting)
		h.clientMu.Unlock()
		return client, nil
	}
}

func (h *Host) shouldRetire(client application, err error) bool {
	var rpcError *appserver.RPCError
	if errors.As(err, &rpcError) || errors.Is(err, appserver.ErrBusy) ||
		errors.Is(err, appserver.ErrMessageTooLarge) || errors.Is(err, ErrMCPInjectionBlocked) {
		return false
	}
	if errors.Is(err, appserver.ErrRequestNotWritten) {
		return false
	}
	select {
	case <-client.Done():
		return true
	default:
	}
	// Once a request has been written, a transport or caller-context error
	// leaves its side effects ambiguous. Retire the shared process so persisted
	// recovery state remains authoritative.
	return err != nil
}

func (h *Host) validateRuntimeDirectories() error {
	if err := config.ValidatePrivateDirectory(h.codexHome); err != nil {
		return fmt.Errorf("validate managed CODEX_HOME: %w", err)
	}
	if err := codexconfig.ValidateManagedHome(h.codexHome); err != nil {
		return err
	}
	workspacePath := h.workspaceRoot.Name()
	if err := config.ValidatePrivateDirectory(workspacePath); err != nil {
		return fmt.Errorf("validate managed workspace root: %w", err)
	}
	anchored, err := h.workspaceRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect held managed workspace root: %w", err)
	}
	visible, err := os.Stat(workspacePath)
	if err != nil {
		return fmt.Errorf("inspect visible managed workspace root: %w", err)
	}
	if !os.SameFile(anchored, visible) {
		return errors.New("managed workspace root changed after it was opened")
	}
	return nil
}

func (h *Host) failWorker(key store.WorkerKey, code string, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	_, err := h.recordWorkerChange(h.state.FailWorker(ctx, key, code, time.Now()))
	if err != nil {
		fatalErr := errors.Join(cause, fmt.Errorf("record worker failure: %w", err))
		h.fail(fatalErr)
		return fatalErr
	}
	return cause
}

func (h *Host) failWorkerMCP(
	client application,
	key store.WorkerKey,
	cause error,
) (<-chan struct{}, error) {
	failureErr := h.failWorker(
		key,
		"mcp_injection_blocked",
		errors.Join(ErrMCPInjectionBlocked, cause),
	)
	if !errors.Is(cause, ErrMCPInjectionBlocked) {
		return nil, failureErr
	}
	// An inventory mismatch means an unexpected MCP child may already be
	// running inside this shared app-server. Persist the worker failure before
	// retiring the process, then fence new work on confirmed recovery.
	return h.retireClient(client, failureErr), failureErr
}

func (h *Host) validateStoredAuthority(ctx context.Context) error {
	workers, err := h.state.ListWorkers(ctx)
	if err != nil {
		return err
	}
	for _, worker := range workers {
		if worker.ControllerID != h.controllerID || worker.DeviceID != h.deviceID {
			return errors.New("peer state contains a worker from another controller or device")
		}
		if worker.WorkspacePath != h.workspacePath(worker.WorkerKey) {
			return errors.New("peer state contains a worker outside the configured workspace root")
		}
		if worker.ProfileVersion != workerProfileVersion {
			return errors.New("peer state contains a worker with an unsupported managed profile")
		}
	}
	return nil
}

func (h *Host) prepareWorkspace(key store.WorkerKey) error {
	name := workspaceName(key)
	err := h.workspaceRoot.Mkdir(name, 0o700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create managed workspace: %w", err)
	}
	info, err := h.workspaceRoot.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect managed workspace: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("managed workspace must be a directory, not a symbolic link")
	}
	if err := h.workspaceRoot.Chmod(name, 0o700); err != nil {
		return fmt.Errorf("protect managed workspace: %w", err)
	}
	anchored, err := h.workspaceRoot.Stat(name)
	if err != nil {
		return fmt.Errorf("inspect anchored managed workspace: %w", err)
	}
	visible, err := os.Stat(h.workspacePath(key))
	if err != nil {
		return fmt.Errorf("inspect visible managed workspace: %w", err)
	}
	if !os.SameFile(anchored, visible) {
		return errors.New("managed workspace root changed after it was opened")
	}
	return nil
}

func (h *Host) workspacePath(key store.WorkerKey) string {
	return filepath.Join(h.workspaceRoot.Name(), workspaceName(key))
}

func workspaceName(key store.WorkerKey) string {
	return key.TreeID + "-" + key.AgentID
}

func (h *Host) lockFor(key store.WorkerKey) *sync.Mutex {
	var hash uint32 = 2166136261
	for _, value := range []string{key.ControllerID, key.TreeID, key.AgentID} {
		for index := range len(value) {
			hash ^= uint32(value[index])
			hash *= 16777619
		}
	}
	return &h.workerLock[hash%workerLockCount]
}

func (h *Host) markLoaded(client application, key store.WorkerKey, threadID string) {
	h.clientMu.Lock()
	if h.client == client {
		h.loaded[key] = threadID
	}
	h.clientMu.Unlock()
}

func (h *Host) isLoaded(client application, key store.WorkerKey, threadID string) bool {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	return h.client == client && h.loaded[key] == threadID
}

func (h *Host) unmarkThread(client application, threadID string) {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	if h.client != client {
		return
	}
	for key, loadedThreadID := range h.loaded {
		if loadedThreadID == threadID {
			delete(h.loaded, key)
		}
	}
}

func decodeNotification(params json.RawMessage, target any) error {
	if len(params) == 0 {
		return errors.New("notification is missing params")
	}
	if err := json.Unmarshal(params, target); err != nil {
		return err
	}
	return nil
}
