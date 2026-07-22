package workerhost

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/appserver"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	operationFailureAppServerRejected    = "app_server_rejected"
	operationFailureAppServerUnavailable = "app_server_unavailable"
	operationFailureRequestNotWritten    = "request_not_written"
	operationFailureWorkerFailed         = "worker_failed"
	operationFailureWorkerNotIdle        = "worker_not_idle"
	operationFailureWorkerNotRunning     = "worker_not_running"
)

type turnSteerParams struct {
	ThreadID            string      `json:"threadId"`
	ClientUserMessageID string      `json:"clientUserMessageId"`
	Input               []textInput `json:"input"`
	ExpectedTurnID      string      `json:"expectedTurnId"`
}

type turnSteerResult struct {
	TurnID string `json:"turnId"`
}

type turnInterruptParams struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
}

func (h *Host) Send(ctx context.Context, request SendRequest) (OperationResult, error) {
	if err := validateOperationInput(request.Key, request.MessageID, request.Message); err != nil {
		return OperationResult{}, err
	}
	release, err := h.acquireOperation(ctx)
	if err != nil {
		return OperationResult{}, err
	}
	lock := h.lockFor(request.Key)
	lock.Lock()
	result, recovery, operationErr := h.sendLocked(ctx, request)
	lock.Unlock()
	release()
	return h.finishOperationCall(ctx, result, recovery, operationErr)
}

func (h *Host) sendLocked(
	ctx context.Context,
	request SendRequest,
) (OperationResult, <-chan struct{}, error) {
	operationContext, cancel, err := detachedOperationContext(ctx)
	if err != nil {
		return OperationResult{}, nil, err
	}
	defer cancel()
	result, replay, err := h.beginWorkerOperation(
		operationContext,
		request.Key,
		request.MessageID,
		store.WorkerOperationSend,
		[]byte(request.Message),
	)
	if err != nil || replay {
		return result, nil, err
	}
	switch result.Worker.Status {
	case store.WorkerIdle, store.WorkerInterrupted:
		result, err = h.completeWorkerOperation(operationContext, result, store.WorkerOutcomeQueued, "")
		return result, nil, err
	case store.WorkerFailed:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerFailed,
			fmt.Errorf("%w: %s", ErrWorkerFailed, result.Worker.FailureCode),
		)
	case store.WorkerRunning:
	case store.WorkerReserved, store.WorkerPending, store.WorkerStarting, store.WorkerPreflight, store.WorkerReady:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerNotRunning,
			fmt.Errorf("%w: status is %s", ErrWorkerNotRunning, result.Worker.Status),
		)
	default:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerNotRunning,
			fmt.Errorf("unknown worker status %q", result.Worker.Status),
		)
	}
	client, err := h.runningClient(result.Worker)
	if err != nil {
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureAppServerUnavailable,
			err,
		)
	}
	var response turnSteerResult
	err = client.TurnSteer(operationContext, turnSteerParams{
		ThreadID: result.Worker.CodexThreadID, ClientUserMessageID: request.MessageID,
		Input:          []textInput{{Type: "text", Text: request.Message, TextElements: []any{}}},
		ExpectedTurnID: result.Worker.ActiveTurnID,
	}, &response)
	if err != nil {
		if isFinishedTurnRPC(err) {
			result, completionErr := h.completeWorkerOperation(
				operationContext,
				result,
				store.WorkerOutcomeQueued,
				"",
			)
			return result, nil, completionErr
		}
		if h.shouldRetire(client, err) {
			return result, h.retireClient(client, err), err
		}
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureCode(err),
			err,
		)
	}
	if response.TurnID != result.Worker.ActiveTurnID {
		protocolErr := fmt.Errorf(
			"app-server steered turn %q instead of %q",
			response.TurnID,
			result.Worker.ActiveTurnID,
		)
		return result, h.retireClient(client, protocolErr), protocolErr
	}
	result, err = h.completeWorkerOperation(operationContext, result, store.WorkerOutcomeSteered, "")
	return result, nil, err
}

func (h *Host) Followup(ctx context.Context, request FollowupRequest) (OperationResult, error) {
	if err := validateOperationInput(request.Key, request.OperationID, request.Message); err != nil {
		return OperationResult{}, err
	}
	release, err := h.acquireOperation(ctx)
	if err != nil {
		return OperationResult{}, err
	}
	lock := h.lockFor(request.Key)
	lock.Lock()
	result, recovery, operationErr := h.followupOperationLocked(ctx, request)
	lock.Unlock()
	release()
	return h.finishOperationCall(ctx, result, recovery, operationErr)
}

func (h *Host) followupOperationLocked(
	ctx context.Context,
	request FollowupRequest,
) (OperationResult, <-chan struct{}, error) {
	operationContext, cancel, err := detachedOperationContext(ctx)
	if err != nil {
		return OperationResult{}, nil, err
	}
	defer cancel()
	result, replay, err := h.beginWorkerOperation(
		operationContext,
		request.Key,
		request.OperationID,
		store.WorkerOperationFollowup,
		[]byte(request.Message),
	)
	if err != nil || replay {
		return result, nil, err
	}
	started, recovery, err := h.followupLocked(operationContext, request)
	if started.Worker.WorkerKey == request.Key {
		result.Worker = started.Worker
	}
	if err == nil {
		result, err = h.completeWorkerOperation(
			operationContext,
			result,
			store.WorkerOutcomeStarted,
			"",
		)
		return result, nil, err
	}
	if recovery != nil {
		return result, recovery, err
	}
	return h.failWorkerOperation(
		operationContext,
		result,
		operationFailureCode(err),
		err,
	)
}

func (h *Host) Interrupt(ctx context.Context, request InterruptRequest) (OperationResult, error) {
	if request.Key.ControllerID != h.controllerID {
		return OperationResult{}, errors.New("worker belongs to another controller")
	}
	if err := request.Key.Validate(); err != nil {
		return OperationResult{}, err
	}
	if err := identity.ValidateID(request.OperationID); err != nil {
		return OperationResult{}, fmt.Errorf("operationId %w", err)
	}
	release, err := h.acquireOperation(ctx)
	if err != nil {
		return OperationResult{}, err
	}
	lock := h.lockFor(request.Key)
	lock.Lock()
	result, recovery, operationErr := h.interruptLocked(ctx, request)
	lock.Unlock()
	release()
	return h.finishOperationCall(ctx, result, recovery, operationErr)
}

func (h *Host) interruptLocked(
	ctx context.Context,
	request InterruptRequest,
) (OperationResult, <-chan struct{}, error) {
	operationContext, cancel, err := detachedOperationContext(ctx)
	if err != nil {
		return OperationResult{}, nil, err
	}
	defer cancel()
	result, replay, err := h.beginWorkerOperation(
		operationContext,
		request.Key,
		request.OperationID,
		store.WorkerOperationInterrupt,
		nil,
	)
	if err != nil || replay {
		return result, nil, err
	}
	switch result.Worker.Status {
	case store.WorkerIdle, store.WorkerInterrupted:
		result, err = h.completeWorkerOperation(
			operationContext,
			result,
			store.WorkerOutcomeInterrupted,
			"",
		)
		return result, nil, err
	case store.WorkerFailed:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerFailed,
			fmt.Errorf("%w: %s", ErrWorkerFailed, result.Worker.FailureCode),
		)
	case store.WorkerRunning:
	case store.WorkerReserved, store.WorkerPending, store.WorkerStarting, store.WorkerPreflight, store.WorkerReady:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerNotRunning,
			fmt.Errorf("%w: status is %s", ErrWorkerNotRunning, result.Worker.Status),
		)
	default:
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureWorkerNotRunning,
			fmt.Errorf("unknown worker status %q", result.Worker.Status),
		)
	}
	client, err := h.runningClient(result.Worker)
	if err != nil {
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureAppServerUnavailable,
			err,
		)
	}
	err = client.TurnInterrupt(operationContext, turnInterruptParams{
		ThreadID: result.Worker.CodexThreadID,
		TurnID:   result.Worker.ActiveTurnID,
	}, &struct{}{})
	if err != nil {
		if isFinishedTurnRPC(err) {
			result, completionErr := h.completeWorkerOperation(
				operationContext,
				result,
				store.WorkerOutcomeInterrupted,
				"",
			)
			return result, nil, completionErr
		}
		if h.shouldRetire(client, err) {
			return result, h.retireClient(client, err), err
		}
		return h.failWorkerOperation(
			operationContext,
			result,
			operationFailureCode(err),
			err,
		)
	}
	result, err = h.completeWorkerOperation(
		operationContext,
		result,
		store.WorkerOutcomeInterrupted,
		"",
	)
	return result, nil, err
}

func validateOperationInput(key store.WorkerKey, operationID, message string) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(operationID); err != nil {
		return fmt.Errorf("operationId %w", err)
	}
	return validatePrompt(message)
}

func (h *Host) beginWorkerOperation(
	ctx context.Context,
	key store.WorkerKey,
	operationID string,
	action store.WorkerOperationAction,
	payload []byte,
) (OperationResult, bool, error) {
	if key.ControllerID != h.controllerID {
		return OperationResult{}, false, errors.New("worker belongs to another controller")
	}
	worker, err := h.state.GetWorker(ctx, key)
	if err != nil {
		return OperationResult{}, false, err
	}
	if worker.DeviceID != h.deviceID {
		return OperationResult{}, false, errors.New("worker belongs to another device")
	}
	receipt, replay, err := h.state.BeginWorkerOperation(
		ctx,
		operationID,
		action,
		key,
		payload,
		time.Now(),
	)
	return OperationResult{Receipt: receipt, Worker: worker}, replay, err
}

func (h *Host) completeWorkerOperation(
	ctx context.Context,
	result OperationResult,
	outcome store.WorkerOperationOutcome,
	failureCode string,
) (OperationResult, error) {
	receipt, err := h.state.CompleteWorkerOperation(
		ctx,
		result.Worker.WorkerKey,
		result.Receipt.OperationID,
		outcome,
		failureCode,
		time.Now(),
	)
	if err != nil {
		fatalErr := fmt.Errorf("record worker operation outcome: %w", err)
		h.fail(fatalErr)
		return result, fatalErr
	}
	result.Receipt = receipt
	worker, err := h.state.GetWorker(ctx, result.Worker.WorkerKey)
	if err != nil {
		fatalErr := fmt.Errorf("reload worker operation result: %w", err)
		h.fail(fatalErr)
		return result, fatalErr
	}
	result.Worker = worker
	return result, nil
}

func (h *Host) failWorkerOperation(
	ctx context.Context,
	result OperationResult,
	failureCode string,
	cause error,
) (OperationResult, <-chan struct{}, error) {
	completed, err := h.completeWorkerOperation(
		ctx,
		result,
		store.WorkerOutcomeFailed,
		failureCode,
	)
	return completed, nil, errors.Join(cause, err)
}

func (h *Host) finishOperationCall(
	ctx context.Context,
	result OperationResult,
	recovery <-chan struct{},
	err error,
) (OperationResult, error) {
	if waitErr := h.awaitRecovery(ctx, recovery); waitErr != nil {
		err = errors.Join(err, waitErr)
	}
	if recovery == nil || result.Worker.WorkerKey == (store.WorkerKey{}) {
		return result, err
	}
	reloadContext, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	worker, reloadErr := h.state.GetWorker(reloadContext, result.Worker.WorkerKey)
	if reloadErr != nil {
		return result, errors.Join(err, fmt.Errorf("reload recovered worker: %w", reloadErr))
	}
	result.Worker = worker
	return result, err
}

func (h *Host) runningClient(worker store.WorkerReservation) (application, error) {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	if h.recovering != nil {
		return nil, errClientRecovering
	}
	if h.client == nil || h.loaded[worker.WorkerKey] != worker.CodexThreadID {
		return nil, errors.New("running worker thread is not loaded in the managed app-server")
	}
	return h.client, nil
}

func operationFailureCode(err error) string {
	if errors.Is(err, appserver.ErrRequestNotWritten) {
		return operationFailureRequestNotWritten
	}
	if errors.Is(err, ErrWorkerNotIdle) {
		return operationFailureWorkerNotIdle
	}
	if errors.Is(err, ErrWorkerNotRunning) {
		return operationFailureWorkerNotRunning
	}
	if errors.Is(err, ErrWorkerFailed) {
		return operationFailureWorkerFailed
	}
	var rpcError *appserver.RPCError
	if errors.As(err, &rpcError) {
		return operationFailureAppServerRejected
	}
	return operationFailureAppServerUnavailable
}

func isFinishedTurnRPC(err error) bool {
	var rpcError *appserver.RPCError
	if !errors.As(err, &rpcError) {
		return false
	}
	message := strings.ToLower(rpcError.Message)
	return strings.Contains(message, "no active turn") ||
		strings.Contains(message, "expected active turn id")
}
