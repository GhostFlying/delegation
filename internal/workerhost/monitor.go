package workerhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

type queuedCompletion struct {
	client    application
	completed turnCompletedNotification
	drained   bool
}

func (h *Host) monitorClient(client application) {
	defer h.monitors.Done()
	retired := false
	for notification := range client.Notifications() {
		if retired && notification.Method != "turn/completed" {
			continue
		}
		if err := h.handleNotification(client, notification); err != nil {
			h.reportError(err)
			if errors.Is(err, errInvalidLifecycleNotification) && !retired {
				h.retireClient(client, err)
				retired = true
			}
		}
	}
	if !retired {
		err := client.Err()
		if err == nil {
			err = errors.New("managed app-server stopped unexpectedly")
		}
		h.retireClient(client, err)
	}
	h.completionEvents <- queuedCompletion{client: client, drained: true}
}

var errInvalidLifecycleNotification = errors.New("invalid app-server lifecycle notification")

func (h *Host) handleNotification(client application, notification appserver.Notification) error {
	switch notification.Method {
	case "turn/completed":
		var completed turnCompletedNotification
		if err := decodeNotification(notification.Params, &completed); err != nil {
			return errors.Join(errInvalidLifecycleNotification, err)
		}
		if err := identity.ValidateID(completed.ThreadID); err != nil {
			return errors.Join(errInvalidLifecycleNotification, fmt.Errorf("threadId %w", err))
		}
		if err := identity.ValidateID(completed.Turn.ID); err != nil {
			return errors.Join(errInvalidLifecycleNotification, fmt.Errorf("turnId %w", err))
		}
		return h.enqueueCompletion(client, completed)
	case "thread/status/changed":
		if h.hostKind != hostkind.TraeX {
			return nil
		}
		var changed threadStatusChangedNotification
		if err := decodeNotification(notification.Params, &changed); err != nil {
			return errors.Join(errInvalidLifecycleNotification, err)
		}
		if err := identity.ValidateID(changed.ThreadID); err != nil {
			return errors.Join(errInvalidLifecycleNotification, fmt.Errorf("threadId %w", err))
		}
		if changed.Status.Type != "idle" {
			return nil
		}
		completed, err := h.readTraeXCompletion(client, changed.ThreadID)
		if err != nil {
			return errors.Join(errInvalidLifecycleNotification, err)
		}
		if completed == nil {
			return nil
		}
		return h.enqueueCompletion(client, *completed)
	case "thread/closed":
		var closed struct {
			ThreadID string `json:"threadId"`
		}
		if err := decodeNotification(notification.Params, &closed); err != nil {
			return errors.Join(errInvalidLifecycleNotification, err)
		}
		if err := identity.ValidateID(closed.ThreadID); err != nil {
			return errors.Join(errInvalidLifecycleNotification, fmt.Errorf("threadId %w", err))
		}
		h.unmarkThread(client, closed.ThreadID)
		return errors.Join(
			errInvalidLifecycleNotification,
			fmt.Errorf("managed thread %s closed unexpectedly", closed.ThreadID),
		)
	case "error":
		h.reportError(errors.New("managed app-server reported a thread error"))
	case "mcpServer/startupStatus/updated", "thread/started", "turn/started":
		// These bounded lifecycle notifications are useful diagnostics, but the
		// persisted worker state is driven by RPC responses and turn completion.
	}
	return nil
}

func (h *Host) enqueueCompletion(
	client application,
	completed turnCompletedNotification,
) error {
	select {
	case h.completionEvents <- queuedCompletion{client: client, completed: completed}:
		return nil
	default:
		return errors.Join(
			errInvalidLifecycleNotification,
			errors.New("managed completion queue is full"),
		)
	}
}

func (h *Host) readTraeXCompletion(
	client application,
	threadID string,
) (*turnCompletedNotification, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	worker, err := h.state.WorkerForThread(ctx, h.controllerID, threadID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find idle TraeX worker: %w", err)
	}
	var expectedTurnID string
	var previousTurnID string
	preparedTurn := false
	preparedInitial := false
	switch worker.Status {
	case store.WorkerRunning, store.WorkerInterrupted:
		expectedTurnID = worker.ActiveTurnID
	case store.WorkerReady:
		preparedTurn = true
		intent, err := h.state.GetPreparedWorkerTurnStartIntent(ctx, worker.WorkerKey)
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("load idle TraeX turn intent: %w", err)
		}
		previousTurnID = intent.PreviousTurnID
		preparedInitial = intent.PreviousTurnID == "" &&
			intent.Rollout.Status == store.WorkerRolloutUnavailable
	case store.WorkerIdle, store.WorkerFinalizing, store.WorkerFailed:
		return nil, nil
	default:
		return nil, nil
	}
	var read threadResult
	if err := client.ThreadRead(ctx, threadReadParams{
		ThreadID: threadID, IncludeTurns: true,
	}, &read); err != nil {
		return nil, fmt.Errorf("read idle TraeX thread: %w", err)
	}
	if read.Thread.ID != threadID {
		return nil, errors.New("app-server read an unexpected idle TraeX thread")
	}
	h.markLoaded(client, worker.WorkerKey, threadID, read.Thread.Path)
	var completed turn
	if expectedTurnID != "" {
		for _, candidate := range read.Thread.Turns {
			if candidate.ID == expectedTurnID {
				completed = candidate
				break
			}
		}
		if completed.ID == "" {
			return nil, errors.New("idle TraeX thread is missing its active turn")
		}
	} else {
		candidates, err := turnsAfter(read.Thread.Turns, previousTurnID)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 && preparedTurn {
			return nil, nil
		}
		if len(candidates) != 1 {
			return nil, errors.New("idle TraeX thread does not contain one prepared turn")
		}
		completed = candidates[0]
	}
	if err := identity.ValidateID(completed.ID); err != nil {
		return nil, fmt.Errorf("idle TraeX turnId %w", err)
	}
	switch completed.Status {
	case "completed", "failed", "interrupted":
	case "inProgress":
		if preparedTurn {
			return nil, nil
		}
		fallthrough
	default:
		return nil, fmt.Errorf(
			"idle TraeX thread returned non-terminal turn status %q",
			completed.Status,
		)
	}
	notification := &turnCompletedNotification{ThreadID: threadID, Turn: completed}
	if preparedInitial && read.Thread.Path != nil {
		notification.Rollout = h.locateInitialTraeXRollout(ctx, threadID, *read.Thread.Path)
	}
	return notification, nil
}

func (h *Host) locateInitialTraeXRollout(
	ctx context.Context,
	threadID, path string,
) *store.WorkerRolloutLocator {
	retryDelay := rolloutFlushRetryMin
	for attempt := range rolloutFlushAttempts {
		locator, err := rolloutcapture.Locate(h.rolloutHome, threadID, path)
		if err == nil {
			return &store.WorkerRolloutLocator{
				Status: store.WorkerRolloutAvailable, CodexHome: h.rolloutHome,
				Path: locator.Path, Offset: 0,
			}
		}
		if !errors.Is(err, os.ErrNotExist) || attempt == rolloutFlushAttempts-1 {
			return nil
		}
		if err := h.waitForRolloutFlush(ctx, retryDelay); err != nil {
			return nil
		}
		retryDelay = min(retryDelay*2, rolloutFlushRetryMax)
	}
	return nil
}

func (h *Host) processCompletions() {
	defer h.background.Done()
	defer close(h.completionDone)
	for queued := range h.completionEvents {
		if queued.drained {
			h.closeCompletionDrain(queued.client)
			continue
		}
		if err := h.applyCompletion(queued.completed); err != nil {
			h.reportError(err)
			h.deferCompletion(queued.client, queued.completed)
			h.retireClient(queued.client, err)
		}
	}
}

func (h *Host) deferCompletion(client application, completed turnCompletedNotification) {
	h.clientMu.Lock()
	h.deferredCompletions[client] = append(h.deferredCompletions[client], completed)
	h.clientMu.Unlock()
}

func (h *Host) closeCompletionDrain(client application) {
	h.clientMu.Lock()
	drain := h.completionDrains[client]
	h.clientMu.Unlock()
	if drain != nil {
		close(drain)
	}
}

func (h *Host) waitCompletionDrain(client application) error {
	h.clientMu.Lock()
	drain := h.completionDrains[client]
	h.clientMu.Unlock()
	if drain == nil {
		return errors.New("managed app-server completion drain is unavailable")
	}
	timer := time.NewTimer(stateTimeout)
	defer timer.Stop()
	select {
	case <-drain:
		return nil
	case <-timer.C:
		return errors.New("timed out draining managed app-server completions")
	}
}

func (h *Host) takeDeferredCompletions(client application) []turnCompletedNotification {
	h.clientMu.Lock()
	deferred := h.deferredCompletions[client]
	delete(h.deferredCompletions, client)
	delete(h.completionDrains, client)
	h.clientMu.Unlock()
	return deferred
}

func (h *Host) completeTurn(completed turnCompletedNotification) error {
	h.operations.RLock()
	defer h.operations.RUnlock()
	lookupContext, lookupCancel := context.WithTimeout(context.Background(), stateTimeout)
	worker, err := h.state.WorkerForThread(lookupContext, h.controllerID, completed.ThreadID)
	lookupCancel()
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("find completed worker: %w", err)
	}
	lock := h.lockFor(worker.WorkerKey)
	lock.Lock()
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	worker, err = h.state.GetWorker(ctx, worker.WorkerKey)
	if err != nil {
		return fmt.Errorf("reload completed worker: %w", err)
	}
	if worker.Status == store.WorkerReady {
		intent, intentErr := h.state.GetPreparedWorkerTurnStartIntent(ctx, worker.WorkerKey)
		if intentErr != nil {
			return fmt.Errorf("load completed worker turn intent: %w", intentErr)
		}
		var (
			resolution store.WorkerTurnStartResolution
			bindErr    error
		)
		if completed.Rollout != nil {
			resolution, bindErr = h.state.BindInitialWorkerTurnStartIntent(
				ctx,
				worker.WorkerKey,
				intent.IntentID,
				completed.Turn.ID,
				*completed.Rollout,
				time.Now(),
			)
		} else {
			resolution, bindErr = h.state.BindWorkerTurnStartIntent(
				ctx, worker.WorkerKey, intent.IntentID, completed.Turn.ID, time.Now(),
			)
		}
		worker, bindErr = h.recordWorkerChange(resolution.Worker, bindErr)
		if bindErr != nil {
			return fmt.Errorf("bind completed worker turn intent: %w", bindErr)
		}
	}
	if (worker.Status != store.WorkerRunning && worker.Status != store.WorkerInterrupted) ||
		worker.ActiveTurnID != completed.Turn.ID {
		return nil
	}
	return h.beginTerminalResultFinalization(ctx, worker, completed.Turn)
}

func (h *Host) beginTerminalResultFinalization(
	ctx context.Context,
	worker store.WorkerReservation,
	completed turn,
) error {
	target := store.WorkerFailed
	failureCode := "unsupported_turn_status"
	switch completed.Status {
	case "completed":
		target = store.WorkerIdle
		failureCode = ""
	case "interrupted":
		target = store.WorkerInterrupted
		failureCode = "turn_interrupted"
	case "failed":
		failureCode = "turn_failed"
	}
	finalization, err := h.state.BeginWorkerResultFinalization(
		ctx, worker.WorkerKey, completed.ID, target, failureCode, time.Now(),
	)
	if _, err := h.recordWorkerChange(finalization.Worker, err); err != nil {
		return fmt.Errorf("begin completed worker result finalization: %w", err)
	}
	h.signalArtifactWork()
	return nil
}

func (h *Host) retireClient(client application, cause error) <-chan struct{} {
	h.clientMu.Lock()
	if h.client != client {
		recovering := h.recovering
		h.clientMu.Unlock()
		return recovering
	}
	h.client = nil
	h.loaded = make(map[store.WorkerKey]loadedThread)
	if h.recovering == nil {
		h.recovering = make(chan struct{})
	}
	recovering := h.recovering
	h.clientMu.Unlock()

	go h.closeAndRecover(client, cause, recovering)
	return recovering
}

func (h *Host) closeAndRecover(client application, cause error, recovering chan struct{}) {
	closeContext, cancel := context.WithTimeout(context.Background(), stateTimeout)
	closeErr := client.Close(closeContext)
	cancel()
	if cause != nil {
		h.reportError(cause)
	}
	if closeErr != nil {
		h.reportError(closeErr)
	}
	if errors.Is(closeErr, appserver.ErrProcessExitUnconfirmed) {
		h.finishRecovery(recovering, fmt.Errorf("close managed app-server: %w", closeErr))
		return
	}
	if drainErr := h.waitCompletionDrain(client); drainErr != nil {
		h.finishRecovery(recovering, drainErr)
		return
	}

	for _, completed := range h.takeDeferredCompletions(client) {
		if err := h.applyCompletion(completed); err != nil {
			fatalErr := fmt.Errorf("retry managed completion before recovery: %w", err)
			h.reportError(fatalErr)
			h.finishRecovery(recovering, fatalErr)
			return
		}
	}
	h.operations.Lock()
	recoveryContext, cancel := context.WithTimeout(context.Background(), stateTimeout)
	recovered, recoveryErr := h.recordWorkerRecovery(h.state.RecoverWorkers(
		recoveryContext, h.controllerID, h.deviceID, time.Now(),
	))
	var preparedIntents bool
	if recoveryErr == nil {
		preparedIntents, recoveryErr = h.finalizeBoundTurnsAfterClientExit(recoveryContext)
	}
	if recoveryErr == nil && preparedIntents {
		recoveryErr = h.reconcilePreparedTurnIntentsAfterClientExit(
			recoveryContext,
			recovering,
		)
		if errors.Is(recoveryErr, ErrClosed) {
			recoveryErr = nil
		}
	}
	cancel()
	h.operations.Unlock()
	if recoveryErr != nil {
		fatalErr := fmt.Errorf("recover workers after app-server exit: %w", recoveryErr)
		h.reportError(fatalErr)
		h.finishRecovery(recovering, fatalErr)
		return
	}
	for _, worker := range recovered {
		if worker.Status == store.WorkerFinalizing {
			h.signalArtifactWork()
		}
	}
	if cause == nil && closeErr != nil {
		h.finishRecovery(recovering, fmt.Errorf("close managed app-server: %w", closeErr))
		return
	}
	h.finishRecovery(recovering, nil)
}

func (h *Host) finalizeBoundTurnsAfterClientExit(ctx context.Context) (bool, error) {
	intents, err := h.state.ListUnresolvedWorkerTurnStartIntents(
		ctx, h.controllerID, h.deviceID, store.MaximumWorkerTurnStartIntentReceipts,
	)
	if err != nil {
		return false, err
	}
	prepared := false
	boundIntents := make([]store.WorkerTurnStartIntent, 0, len(intents))
	boundWorkers := make([]store.WorkerReservation, 0, len(intents))
	for _, intent := range intents {
		if intent.State == store.WorkerTurnStartPrepared {
			prepared = true
			continue
		}
		if intent.State != store.WorkerTurnStartBound {
			continue
		}
		worker, err := h.state.GetWorker(ctx, intent.WorkerKey)
		if err != nil {
			return false, err
		}
		if worker.Status == store.WorkerFinalizing {
			h.signalArtifactWork()
			continue
		}
		if worker.Status != store.WorkerRunning || worker.ActiveTurnID != intent.TurnID {
			return false, store.ErrWorkerTurnStartIntentConflict
		}
		boundIntents = append(boundIntents, intent)
		boundWorkers = append(boundWorkers, worker)
	}
	targets, err := h.resolveResultTargetsAfterClientExit(ctx, boundIntents)
	if err != nil {
		return false, err
	}
	for index, intent := range boundIntents {
		worker := boundWorkers[index]
		target := targets[index]
		finalization, err := h.state.BeginWorkerResultFinalization(
			ctx,
			worker.WorkerKey,
			intent.TurnID,
			target.status,
			target.failureCode,
			time.Now(),
		)
		if _, err := h.recordWorkerChange(finalization.Worker, err); err != nil {
			return false, err
		}
		h.signalArtifactWork()
	}
	return prepared, nil
}

func (h *Host) finishRecovery(recovering chan struct{}, fatalErr error) {
	if fatalErr != nil {
		h.fail(fatalErr)
	}
	h.clientMu.Lock()
	if h.recovering == recovering {
		h.recovering = nil
		close(recovering)
	}
	h.clientMu.Unlock()
}

func (h *Host) awaitRecovery(ctx context.Context, recovering <-chan struct{}) error {
	if recovering == nil {
		return nil
	}
	timer := time.NewTimer(stateTimeout)
	defer timer.Stop()
	select {
	case <-recovering:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		err := errors.New("timed out recovering workers after app-server exit")
		h.reportError(err)
		return err
	}
}

func (h *Host) Close(ctx context.Context) error {
	h.shutdownOnce.Do(h.beginShutdown)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.shutdownDone:
		return h.shutdownErr
	}
}

func (h *Host) beginShutdown() {
	h.clientMu.Lock()
	h.closed = true
	h.doneOnce.Do(func() { close(h.done) })
	client := h.client
	starting := h.starting
	recovering := h.recovering
	h.clientMu.Unlock()
	go h.shutdown(client, starting, recovering)
}

func (h *Host) shutdown(
	client application,
	starting, recovering <-chan struct{},
) {
	defer close(h.shutdownDone)
	if starting != nil {
		<-starting
	}
	if client != nil {
		recovering = h.retireClient(client, nil)
	}
	if recovering != nil {
		<-recovering
	}
	h.monitors.Wait()
	close(h.completionEvents)
	<-h.completionDone
	h.artifactCancel()
	h.background.Wait()
	h.operations.Lock()
	h.workspaceOperations.Lock()
	cleanupErr := h.cleanupWorkspaceTransfers(context.Background())
	artifactErr := h.artifactRoot.Close()
	workspaceErr := h.workspaceRoot.Close()
	h.workspaceOperations.Unlock()
	h.operations.Unlock()
	h.shutdownErr = errors.Join(cleanupErr, artifactErr, workspaceErr, h.Err())
}
