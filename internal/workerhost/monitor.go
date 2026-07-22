package workerhost

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/identity"
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
		select {
		case h.completionEvents <- queuedCompletion{client: client, completed: completed}:
			return nil
		default:
			return errors.Join(
				errInvalidLifecycleNotification,
				errors.New("managed completion queue is full"),
			)
		}
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
	case "mcpServer/startupStatus/updated", "thread/started", "thread/status/changed", "turn/started":
		// These bounded lifecycle notifications are useful diagnostics, but the
		// persisted worker state is driven by RPC responses and turn completion.
	}
	return nil
}

func (h *Host) processCompletions() {
	defer h.background.Done()
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
	if (worker.Status != store.WorkerRunning && worker.Status != store.WorkerInterrupted) ||
		worker.ActiveTurnID != completed.Turn.ID {
		return nil
	}
	switch completed.Turn.Status {
	case "completed", "interrupted":
		if _, err := h.state.MarkWorkerIdle(ctx, worker.WorkerKey, time.Now()); err != nil {
			return fmt.Errorf("record completed worker: %w", err)
		}
	case "failed":
		if _, err := h.state.FailWorker(ctx, worker.WorkerKey, "turn_failed", time.Now()); err != nil {
			return fmt.Errorf("record failed worker: %w", err)
		}
	default:
		if _, err := h.state.FailWorker(ctx, worker.WorkerKey, "unsupported_turn_status", time.Now()); err != nil {
			return fmt.Errorf("record unsupported turn status %q: %w", completed.Turn.Status, err)
		}
	}
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
	h.loaded = make(map[store.WorkerKey]string)
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

	h.operations.Lock()
	recoveryContext, cancel := context.WithTimeout(context.Background(), stateTimeout)
	_, recoveryErr := h.state.RecoverWorkers(
		recoveryContext, h.controllerID, h.deviceID, time.Now(),
	)
	cancel()
	h.operations.Unlock()
	if recoveryErr != nil {
		fatalErr := fmt.Errorf("recover workers after app-server exit: %w", recoveryErr)
		h.reportError(fatalErr)
		h.finishRecovery(recovering, fatalErr)
		return
	}
	for _, completed := range h.takeDeferredCompletions(client) {
		if err := h.applyCompletion(completed); err != nil {
			fatalErr := fmt.Errorf("retry managed completion after recovery: %w", err)
			h.reportError(fatalErr)
			h.finishRecovery(recovering, fatalErr)
			return
		}
	}
	if cause == nil && closeErr != nil {
		h.finishRecovery(recovering, fmt.Errorf("close managed app-server: %w", closeErr))
		return
	}
	h.finishRecovery(recovering, nil)
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
	h.background.Wait()
	h.operations.Lock()
	workspaceErr := h.workspaceRoot.Close()
	h.operations.Unlock()
	h.shutdownErr = errors.Join(workspaceErr, h.Err())
}
