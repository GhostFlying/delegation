package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type agentOperationRequest struct {
	action       protocol.AgentOperationAction
	operationID  string
	agentID      string
	message      string
	workerMethod string
	workerParams any
}

func (s *session) startAgentOperation(
	responseContext context.Context,
	sessionContext context.Context,
	request protocol.Envelope,
) error {
	if request.TreeID == "" || request.Source == nil {
		return s.writeError(
			responseContext, request, protocol.ErrorInvalidRequest,
			"agent operation requires a principal",
		)
	}
	if request.Source.DeviceID != s.deviceID {
		return s.writeError(
			responseContext, request, protocol.ErrorForbidden, "agent operation access denied",
		)
	}
	operation, err := decodeAgentOperation(request)
	if err != nil {
		return s.writeError(
			responseContext, request, protocol.ErrorInvalidParams,
			"invalid agent operation payload",
		)
	}
	receipt, err := s.server.registry.BeginAgentOperation(
		responseContext,
		store.AgentOperationIntent{
			Source:        *request.Source,
			OperationID:   operation.operationID,
			AgentID:       operation.agentID,
			Action:        operation.action,
			PayloadDigest: sha256.Sum256([]byte(operation.message)),
		},
		s.server.now(),
	)
	if err != nil {
		return s.handleAgentStoreError(responseContext, request, "begin agent operation", err)
	}
	if receipt.Outcome != protocol.AgentOperationOutcomePending {
		if receipt.Outcome == protocol.AgentOperationOutcomeQueued {
			s.notifyAgentMailbox(receipt)
		}
		return s.writeResult(responseContext, request, agentOperationResult(receipt))
	}
	select {
	case s.asyncSem <- struct{}{}:
	default:
		return s.writeError(
			responseContext, request, protocol.ErrorUnavailable,
			"too many pending agent operations",
		)
	}
	s.async.Add(1)
	s.server.agentOperations.enqueue(
		agentOperationQueueKey{treeID: request.TreeID, agentID: operation.agentID},
		func() {
			defer s.async.Done()
			defer func() { <-s.asyncSem }()
			s.runAgentOperation(sessionContext, request, operation, receipt)
		},
	)
	return nil
}

func (s *session) runAgentOperation(
	responseContext context.Context,
	request protocol.Envelope,
	operation agentOperationRequest,
	receipt store.AgentOperationReceipt,
) {
	operationContext, cancelOperation := context.WithTimeout(
		s.server.context, agentOperationRequestTimeout,
	)
	updated, err := s.performAgentOperation(
		operationContext, request, operation, receipt,
	)
	cancelOperation()
	if err != nil && !isContextError(err) {
		s.server.reportError(&internalError{operation: "control managed agent", err: err})
	}
	writeContext, cancelWrite := context.WithTimeout(responseContext, writeTimeout)
	defer cancelWrite()
	if err := s.writeResult(writeContext, request, agentOperationResult(updated)); err != nil &&
		!isContextError(err) {
		_ = s.connection.CloseNow()
	}
}

func (s *session) performAgentOperation(
	ctx context.Context,
	request protocol.Envelope,
	operation agentOperationRequest,
	receipt store.AgentOperationReceipt,
) (store.AgentOperationReceipt, error) {
	target := s.server.connection(receipt.TargetDeviceID)
	if target == nil {
		if operation.action == protocol.AgentOperationSend {
			return s.queueAgentMessage(*request.Source, receipt, operation.message)
		}
		return receipt, nil
	}
	payload, callErr := target.callPeer(
		ctx, operation.workerMethod, request.TreeID, *request.Source, operation.workerParams,
	)
	if callErr != nil {
		return receipt, nil
	}
	result, decodeErr := protocol.DecodePayload[protocol.WorkerOperationResult](payload)
	if decodeErr != nil || validateTargetOperationResult(result, receipt) != nil {
		_ = target.connection.CloseNow()
		return receipt, errors.New("target returned an invalid worker operation result")
	}
	if result.Outcome == protocol.AgentOperationOutcomePending {
		return receipt, nil
	}
	if result.Action == protocol.AgentOperationSend &&
		result.Outcome == protocol.AgentOperationOutcomeQueued {
		return s.queueAgentMessage(*request.Source, receipt, operation.message)
	}
	return s.finishAgentOperation(
		*request.Source, receipt, result.Outcome, result.FailureCode,
	)
}

func (s *session) queueAgentMessage(
	source control.PrincipalIdentity,
	receipt store.AgentOperationReceipt,
	message string,
) (store.AgentOperationReceipt, error) {
	persistContext, cancelPersist := context.WithTimeout(s.server.context, cleanupTimeout)
	updated, _, err := s.server.registry.QueueAgentMessageAndFinishOperation(
		persistContext, receipt.Key, message, s.server.now(),
	)
	cancelPersist()
	if err == nil {
		s.notifyAgentMailbox(updated)
		return updated, nil
	}
	if !errors.Is(err, store.ErrConflict) {
		return receipt, fmt.Errorf("queue managed agent message: %w", err)
	}
	current, getErr := s.getAgentOperation(source, receipt)
	if getErr == nil && current.Outcome != protocol.AgentOperationOutcomePending {
		if current.Outcome == protocol.AgentOperationOutcomeQueued {
			s.notifyAgentMailbox(current)
		}
		return current, nil
	}
	failed, finishErr := s.finishAgentOperation(
		source, receipt, protocol.AgentOperationOutcomeFailed, "message_id_conflict",
	)
	if finishErr != nil {
		return receipt, errors.Join(
			fmt.Errorf("queue managed agent message: %w", err), finishErr,
		)
	}
	return failed, nil
}

func (s *session) finishAgentOperation(
	source control.PrincipalIdentity,
	receipt store.AgentOperationReceipt,
	outcome protocol.AgentOperationOutcome,
	failureCode string,
) (store.AgentOperationReceipt, error) {
	persistContext, cancelPersist := context.WithTimeout(s.server.context, cleanupTimeout)
	updated, err := s.server.registry.FinishAgentOperation(
		persistContext, receipt.Key, outcome, failureCode, s.server.now(),
	)
	cancelPersist()
	if err == nil {
		return updated, nil
	}
	current, getErr := s.getAgentOperation(source, receipt)
	if getErr == nil && current.Outcome != protocol.AgentOperationOutcomePending {
		return current, nil
	}
	return receipt, fmt.Errorf("record agent operation result: %w", err)
}

func (s *session) getAgentOperation(
	source control.PrincipalIdentity,
	receipt store.AgentOperationReceipt,
) (store.AgentOperationReceipt, error) {
	lookupContext, cancelLookup := context.WithTimeout(s.server.context, cleanupTimeout)
	defer cancelLookup()
	return s.server.registry.GetAgentOperation(
		lookupContext,
		source,
		receipt.Key.OperationID,
	)
}

func decodeAgentOperation(request protocol.Envelope) (agentOperationRequest, error) {
	switch request.Method {
	case protocol.MethodSendAgent:
		params, err := protocol.DecodePayload[protocol.SendAgentParams](request.Payload)
		if err != nil || params.Validate() != nil {
			return agentOperationRequest{}, errors.New("invalid send operation")
		}
		return agentOperationRequest{
			action: protocol.AgentOperationSend, operationID: params.MessageID,
			agentID: params.AgentID, message: params.Message,
			workerMethod: protocol.MethodSendWorker,
			workerParams: protocol.SendWorkerParams(params),
		}, nil
	case protocol.MethodFollowupAgent:
		params, err := protocol.DecodePayload[protocol.FollowupAgentParams](request.Payload)
		if err != nil || params.Validate() != nil {
			return agentOperationRequest{}, errors.New("invalid follow-up operation")
		}
		return agentOperationRequest{
			action: protocol.AgentOperationFollowup, operationID: params.OperationID,
			agentID: params.AgentID, message: params.Message,
			workerMethod: protocol.MethodFollowupWorker,
			workerParams: protocol.FollowupWorkerParams(params),
		}, nil
	case protocol.MethodInterruptAgent:
		params, err := protocol.DecodePayload[protocol.InterruptAgentParams](request.Payload)
		if err != nil || params.Validate() != nil {
			return agentOperationRequest{}, errors.New("invalid interrupt operation")
		}
		return agentOperationRequest{
			action: protocol.AgentOperationInterrupt, operationID: params.OperationID,
			agentID: params.AgentID, workerMethod: protocol.MethodInterruptWorker,
			workerParams: protocol.InterruptWorkerParams(params),
		}, nil
	default:
		return agentOperationRequest{}, errors.New("unsupported agent operation")
	}
}

func validateTargetOperationResult(
	result protocol.WorkerOperationResult,
	receipt store.AgentOperationReceipt,
) error {
	if err := result.Validate(); err != nil {
		return err
	}
	if result.OperationID != receipt.Key.OperationID || result.AgentID != receipt.AgentID ||
		result.Action != receipt.Action {
		return errors.New("target worker operation result does not match the durable operation")
	}
	return nil
}

func (s *session) notifyAgentMailbox(receipt store.AgentOperationReceipt) {
	s.server.mailboxNotifier.notify(mailboxKey{
		controllerID: receipt.Key.ControllerID,
		treeID:       receipt.Key.TreeID,
		agentID:      receipt.AgentID,
	})
}

func agentOperationResult(receipt store.AgentOperationReceipt) protocol.AgentOperationResult {
	return protocol.AgentOperationResult{
		OperationID: receipt.Key.OperationID,
		AgentID:     receipt.AgentID,
		Action:      receipt.Action,
		Outcome:     receipt.Outcome,
		FailureCode: receipt.FailureCode,
	}
}
