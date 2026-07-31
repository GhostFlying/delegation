package workerhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rolloutcapture"
	"github.com/GhostFlying/delegation/internal/store"
)

const rolloutLocatorFailureCode = "rollout_locator_unavailable"

func (h *Host) prepareTurnStartIntent(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	operationID string,
) (store.WorkerTurnStartIntent, error) {
	intentID, err := identity.NewID()
	if err != nil {
		return store.WorkerTurnStartIntent{}, fmt.Errorf("create worker turn intent ID: %w", err)
	}
	packageID, err := identity.NewID()
	if err != nil {
		return store.WorkerTurnStartIntent{}, fmt.Errorf("create result package ID: %w", err)
	}
	reservation := int64(protocol.MaximumResultManifestBytes + protocol.MaximumResultRolloutBytes)
	if worker.WorkspaceID != "" {
		reservation = protocol.MaximumResultPackageBytes
	}
	intent, _, err := h.state.PrepareWorkerTurnStartIntent(
		ctx,
		store.PrepareWorkerTurnStartIntentRequest{
			WorkerKey: worker.WorkerKey, IntentID: intentID, DeviceID: h.deviceID,
			ManagedThreadID: worker.CodexThreadID, PreviousTurnID: worker.LastBoundTurnID,
			PackageID: packageID, OperationID: operationID,
			Rollout: h.rolloutLocator(client, worker), ReservationLimitBytes: reservation,
		},
		time.Now(),
	)
	return intent, err
}

func (h *Host) reconcilePreparedTurnIntent(
	ctx context.Context,
	client application,
	intent store.WorkerTurnStartIntent,
) (StartedTurn, <-chan struct{}, error) {
	worker, err := h.state.GetWorker(ctx, intent.WorkerKey)
	if err != nil {
		return StartedTurn{}, nil, err
	}
	if worker.Status != store.WorkerReady || worker.Revision != intent.PreparedRevision {
		err := h.failAmbiguousTurnStart(store.ErrWorkerTurnStartIntentConflict)
		return StartedTurn{Worker: worker}, nil, err
	}
	if !h.isLoaded(client, worker.WorkerKey, worker.CodexThreadID) {
		params, err := h.threadResumeParams(ctx, worker, &intent)
		if err != nil {
			return StartedTurn{Worker: worker}, nil, err
		}
		var result threadResult
		err = client.ThreadResume(ctx, params, &result)
		if err != nil {
			if h.shouldRetire(client, err) {
				return StartedTurn{Worker: worker}, h.retireClient(client, err), err
			}
			return StartedTurn{Worker: worker}, nil, err
		}
		if result.Thread.ID != worker.CodexThreadID {
			err := h.failAmbiguousTurnStart(
				errors.New("app-server resumed an unexpected worker thread during turn reconciliation"),
			)
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		if err := h.validateThreadWorkspace(result, worker); err != nil {
			err = h.failAmbiguousTurnStart(err)
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		if err := h.verifyWorkerMCP(ctx, client, worker.CodexThreadID); err != nil {
			if errors.Is(err, ErrMCPInjectionBlocked) {
				err = h.failAmbiguousTurnStart(err)
				return StartedTurn{Worker: worker}, h.retireClient(client, err), err
			}
			if h.shouldRetire(client, err) {
				return StartedTurn{Worker: worker}, h.retireClient(client, err), err
			}
			return StartedTurn{Worker: worker}, nil, err
		}
	}
	var read threadResult
	if err := client.ThreadRead(ctx, threadReadParams{
		ThreadID: worker.CodexThreadID, IncludeTurns: true,
	}, &read); err != nil {
		if h.shouldRetire(client, err) {
			return StartedTurn{Worker: worker}, h.retireClient(client, err), err
		}
		return StartedTurn{Worker: worker}, nil, err
	}
	if read.Thread.ID != worker.CodexThreadID {
		err := h.failAmbiguousTurnStart(
			errors.New("app-server read an unexpected worker thread during turn reconciliation"),
		)
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, read.Thread.Path)
	turns, err := turnsAfter(read.Thread.Turns, intent.PreviousTurnID)
	if err != nil {
		err = h.failAmbiguousTurnStart(err)
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	if len(turns) == 0 {
		ambiguousErr := h.failAmbiguousTurnStart(errors.New("managed thread contains no new turn"))
		return StartedTurn{Worker: worker}, nil, ambiguousErr
	}
	if len(turns) != 1 {
		err := h.failAmbiguousTurnStart(
			errors.New("managed thread contains multiple turns after the prepared turn boundary"),
		)
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	observed := turns[0]
	if err := identity.ValidateID(observed.ID); err != nil {
		err = h.failAmbiguousTurnStart(fmt.Errorf("reconciled turn ID: %w", err))
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	terminal := false
	switch observed.Status {
	case "inProgress":
	case "completed", "failed", "interrupted":
		terminal = true
	default:
		err := h.failAmbiguousTurnStart(
			fmt.Errorf("unsupported reconciled turn status %q", observed.Status),
		)
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	resolution, err := h.bindWorkerTurnStartIntent(
		ctx, client, worker, intent, observed.ID,
	)
	worker, err = h.recordWorkerChange(resolution.Worker, err)
	if err != nil {
		if errors.Is(err, store.ErrWorkerTurnStartIntentConflict) {
			err = h.failAmbiguousTurnStart(err)
		}
		return StartedTurn{Worker: worker}, h.retireClient(client, err), err
	}
	started := StartedTurn{Worker: worker, Operation: resolution.Operation}
	if !terminal {
		return started, nil, nil
	}
	if err := h.beginTerminalResultFinalization(ctx, worker, observed); err != nil {
		return started, h.retireClient(client, err), err
	}
	worker, err = h.state.GetWorker(ctx, worker.WorkerKey)
	started.Worker = worker
	return started, nil, err
}

func (h *Host) failAmbiguousTurnStart(cause error) error {
	err := fmt.Errorf("%w; turn-start reservation is retained: %v", errTurnStartAmbiguous, cause)
	h.fail(err)
	return err
}

func (h *Host) recoverUnresolvedTurnIntents(ctx context.Context) error {
	intents, err := h.state.ListUnresolvedWorkerTurnStartIntents(
		ctx, h.controllerID, h.deviceID, store.MaximumWorkerTurnStartIntentReceipts,
	)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		worker, err := h.state.GetWorker(ctx, intent.WorkerKey)
		if err != nil {
			return err
		}
		if worker.Status == store.WorkerFinalizing {
			h.signalArtifactWork()
			continue
		}
		lock := h.lockFor(intent.WorkerKey)
		lock.Lock()
		client, clientErr := h.ensureClient(ctx)
		if clientErr == nil {
			switch intent.State {
			case store.WorkerTurnStartPrepared:
				_, _, clientErr = h.reconcilePreparedTurnIntent(ctx, client, intent)
			case store.WorkerTurnStartBound:
				clientErr = h.recoverBoundTurnIntent(ctx, client, intent, worker)
			case store.WorkerTurnStartRejected:
				clientErr = store.ErrWorkerTurnStartIntentConflict
			}
		}
		lock.Unlock()
		if errors.Is(clientErr, errTurnStartAmbiguous) {
			h.reportError(clientErr)
			continue
		}
		if clientErr != nil {
			return clientErr
		}
	}
	return nil
}

func (h *Host) reconcilePreparedTurnIntentsAfterClientExit(
	ctx context.Context,
	recovering chan struct{},
) error {
	intents, err := h.state.ListUnresolvedWorkerTurnStartIntents(
		ctx, h.controllerID, h.deviceID, store.MaximumWorkerTurnStartIntentReceipts,
	)
	if err != nil {
		return err
	}
	var client application
	for _, candidate := range intents {
		if candidate.State != store.WorkerTurnStartPrepared {
			continue
		}
		lock := h.lockFor(candidate.WorkerKey)
		lock.Lock()
		intent, loadErr := h.state.GetPreparedWorkerTurnStartIntent(ctx, candidate.WorkerKey)
		if errors.Is(loadErr, store.ErrNotFound) {
			lock.Unlock()
			continue
		}
		if loadErr != nil {
			lock.Unlock()
			return loadErr
		}
		if intent.IntentID != candidate.IntentID {
			lock.Unlock()
			return store.ErrWorkerTurnStartIntentConflict
		}
		if client == nil {
			client, loadErr = h.ensureClientForRecovery(ctx, recovering)
			if loadErr != nil {
				lock.Unlock()
				return loadErr
			}
		}
		_, nestedRecovery, reconcileErr := h.reconcilePreparedTurnIntent(ctx, client, intent)
		lock.Unlock()
		if nestedRecovery != nil {
			return errors.Join(
				reconcileErr,
				errors.New("replacement app-server retired during prepared turn reconciliation"),
			)
		}
		if reconcileErr != nil {
			return reconcileErr
		}
	}
	return nil
}

func (h *Host) recoverBoundTurnIntent(
	ctx context.Context,
	client application,
	intent store.WorkerTurnStartIntent,
	worker store.WorkerReservation,
) error {
	if worker.Status != store.WorkerRunning || worker.ActiveTurnID != intent.TurnID {
		return store.ErrWorkerTurnStartIntentConflict
	}
	if !h.isLoaded(client, worker.WorkerKey, worker.CodexThreadID) {
		params, err := h.threadResumeParams(ctx, worker, &intent)
		if err != nil {
			return err
		}
		var result threadResult
		if err := client.ThreadResume(ctx, params, &result); err != nil {
			return err
		}
		if result.Thread.ID != worker.CodexThreadID {
			return errors.New("app-server resumed an unexpected bound worker thread")
		}
		if err := h.validateThreadWorkspace(result, worker); err != nil {
			return err
		}
		if err := h.verifyWorkerMCP(ctx, client, worker.CodexThreadID); err != nil {
			return err
		}
	}
	var read threadResult
	if err := client.ThreadRead(ctx, threadReadParams{
		ThreadID: worker.CodexThreadID, IncludeTurns: true,
	}, &read); err != nil {
		return err
	}
	if read.Thread.ID != worker.CodexThreadID {
		return errors.New("app-server read an unexpected bound worker thread")
	}
	h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, read.Thread.Path)
	turns, err := turnsAfter(read.Thread.Turns, intent.PreviousTurnID)
	if err != nil || len(turns) != 1 || turns[0].ID != intent.TurnID {
		if err == nil {
			err = errors.New("bound managed thread does not contain its exact turn")
		}
		return err
	}
	switch turns[0].Status {
	case "inProgress":
		return nil
	case "completed", "failed", "interrupted":
		return h.beginTerminalResultFinalization(ctx, worker, turns[0])
	default:
		return fmt.Errorf("unsupported recovered turn status %q", turns[0].Status)
	}
}

func (h *Host) refreshLoadedThreadPath(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
) error {
	var read threadResult
	err := client.ThreadRead(ctx, threadReadParams{
		ThreadID: worker.CodexThreadID, IncludeTurns: false,
	}, &read)
	if err != nil {
		if h.shouldRetire(client, err) {
			return err
		}
		h.reportError(fmt.Errorf("read managed rollout path before turn start: %w", err))
		h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, nil)
		return nil
	}
	if read.Thread.ID != worker.CodexThreadID {
		return errors.New("app-server read an unexpected worker thread before turn start")
	}
	h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, read.Thread.Path)
	return nil
}

func (h *Host) refreshInitialRolloutPathAfterTurnStart(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
) error {
	if h.initialRolloutWait <= 0 {
		return nil
	}
	deadline := time.Now().Add(h.initialRolloutWait)
	wait := 25 * time.Millisecond
	var lastReadErr error
	reportLastReadError := func() {
		if lastReadErr != nil {
			h.reportError(fmt.Errorf("read initial managed rollout path after turn start: %w", lastReadErr))
			lastReadErr = nil
		}
	}
	for {
		if !time.Now().Before(deadline) {
			reportLastReadError()
			return nil
		}
		var read threadResult
		readContext, cancel := context.WithDeadline(ctx, deadline)
		err := client.ThreadRead(readContext, threadReadParams{
			ThreadID: worker.CodexThreadID, IncludeTurns: false,
		}, &read)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				select {
				case <-client.Done():
					return fmt.Errorf("read initial managed rollout path after turn start: %w", err)
				default:
				}
				reportLastReadError()
				return nil
			}
			if h.shouldRetire(client, err) {
				return fmt.Errorf("read initial managed rollout path after turn start: %w", err)
			}
			lastReadErr = err
		} else {
			lastReadErr = nil
			if read.Thread.ID != worker.CodexThreadID {
				return errors.New("app-server read an unexpected initial worker thread after turn start")
			}
			h.markLoaded(client, worker.WorkerKey, worker.CodexThreadID, read.Thread.Path)
			if read.Thread.Path != nil {
				if _, err := rolloutcapture.Locate(
					h.rolloutHome, worker.CodexThreadID, *read.Thread.Path,
				); err == nil {
					return nil
				}
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			reportLastReadError()
			return nil
		}
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if wait < 250*time.Millisecond {
			wait = min(2*wait, 250*time.Millisecond)
		}
	}
}

func (h *Host) bindWorkerTurnStartIntent(
	ctx context.Context,
	client application,
	worker store.WorkerReservation,
	intent store.WorkerTurnStartIntent,
	turnID string,
) (store.WorkerTurnStartResolution, error) {
	if intent.PreviousTurnID == "" && intent.Rollout.Status == store.WorkerRolloutUnavailable {
		rollout := h.rolloutLocator(client, worker)
		if rollout.Status == store.WorkerRolloutAvailable {
			rollout.Offset = 0
			return h.state.BindInitialWorkerTurnStartIntent(
				ctx, worker.WorkerKey, intent.IntentID, turnID, rollout, time.Now(),
			)
		}
	}
	return h.state.BindWorkerTurnStartIntent(
		ctx, worker.WorkerKey, intent.IntentID, turnID, time.Now(),
	)
}

func turnsAfter(turns []turn, previousTurnID string) ([]turn, error) {
	start := 0
	if previousTurnID != "" {
		start = -1
		for index, candidate := range turns {
			if candidate.ID == previousTurnID {
				start = index + 1
				break
			}
		}
		if start < 0 {
			return nil, errors.New("managed thread is missing the previous turn boundary")
		}
	}
	return turns[start:], nil
}

func (h *Host) rolloutLocator(
	client application,
	worker store.WorkerReservation,
) store.WorkerRolloutLocator {
	h.clientMu.Lock()
	loaded, ok := h.loaded[worker.WorkerKey]
	current := h.client == client
	h.clientMu.Unlock()
	if !current || !ok || loaded.ID != worker.CodexThreadID || loaded.Path == "" {
		return store.WorkerRolloutLocator{
			Status: store.WorkerRolloutUnavailable, FailureCode: rolloutLocatorFailureCode,
		}
	}
	locator, err := rolloutcapture.Locate(h.rolloutHome, worker.CodexThreadID, loaded.Path)
	if err != nil {
		home, homeErr := os.Stat(h.rolloutHome)
		if worker.LastBoundTurnID != "" || !errors.Is(err, os.ErrNotExist) ||
			homeErr != nil || !home.IsDir() {
			h.reportError(fmt.Errorf("locate managed rollout before turn start: %w", err))
		}
		return store.WorkerRolloutLocator{
			Status: store.WorkerRolloutUnavailable, FailureCode: rolloutLocatorFailureCode,
		}
	}
	return store.WorkerRolloutLocator{
		Status: store.WorkerRolloutAvailable, CodexHome: h.rolloutHome,
		Path: locator.Path, Offset: locator.Offset,
	}
}

func (h *Host) rejectTurnStartIntent(
	key store.WorkerKey,
	intentID, failureCode string,
) (store.WorkerTurnStartResolution, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	resolution, err := h.state.RejectWorkerTurnStartIntent(
		ctx, key, intentID, failureCode, time.Now(),
	)
	if err != nil {
		fatalErr := fmt.Errorf("reject worker turn-start intent: %w", err)
		h.fail(fatalErr)
		return store.WorkerTurnStartResolution{}, fatalErr
	}
	worker, err := h.recordWorkerChange(resolution.Worker, nil)
	resolution.Worker = worker
	return resolution, err
}
