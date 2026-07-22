package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func (s managedWorkerSpawner) SendWorker(
	ctx context.Context,
	request connector.WorkerSendRequest,
) (protocol.WorkerOperationResult, error) {
	key, err := s.authorizeWorkerOperation(ctx, request.TreeID, request.Source, request.Params.AgentID)
	if err != nil {
		return protocol.WorkerOperationResult{}, err
	}
	hostResult, hostErr := s.host.Send(ctx, workerhost.SendRequest{
		Key: key, MessageID: request.Params.MessageID, Message: request.Params.Message,
	})
	return mapWorkerOperationResult(
		hostResult,
		key,
		request.Source.AgentID,
		s.deviceID,
		request.Params.MessageID,
		store.WorkerOperationSend,
		[]byte(request.Params.Message),
		hostErr,
	)
}

func (s managedWorkerSpawner) FollowupWorker(
	ctx context.Context,
	request connector.WorkerFollowupRequest,
) (protocol.WorkerOperationResult, error) {
	key, err := s.authorizeWorkerOperation(ctx, request.TreeID, request.Source, request.Params.AgentID)
	if err != nil {
		return protocol.WorkerOperationResult{}, err
	}
	hostResult, hostErr := s.host.Followup(ctx, workerhost.FollowupRequest{
		Key: key, OperationID: request.Params.OperationID, Message: request.Params.Message,
	})
	return mapWorkerOperationResult(
		hostResult,
		key,
		request.Source.AgentID,
		s.deviceID,
		request.Params.OperationID,
		store.WorkerOperationFollowup,
		[]byte(request.Params.Message),
		hostErr,
	)
}

func (s managedWorkerSpawner) InterruptWorker(
	ctx context.Context,
	request connector.WorkerInterruptRequest,
) (protocol.WorkerOperationResult, error) {
	key, err := s.authorizeWorkerOperation(ctx, request.TreeID, request.Source, request.Params.AgentID)
	if err != nil {
		return protocol.WorkerOperationResult{}, err
	}
	hostResult, hostErr := s.host.Interrupt(ctx, workerhost.InterruptRequest{
		Key: key, OperationID: request.Params.OperationID,
	})
	return mapWorkerOperationResult(
		hostResult,
		key,
		request.Source.AgentID,
		s.deviceID,
		request.Params.OperationID,
		store.WorkerOperationInterrupt,
		nil,
		hostErr,
	)
}

func (s managedWorkerSpawner) authorizeWorkerOperation(
	ctx context.Context,
	treeID string,
	source control.PrincipalIdentity,
	agentID string,
) (store.WorkerKey, error) {
	if s.host == nil || s.state == nil {
		return store.WorkerKey{}, errors.New("managed worker runtime is unavailable")
	}
	if err := source.Validate(); err != nil {
		return store.WorkerKey{}, fmt.Errorf("worker operation source: %w", err)
	}
	if source.ControllerID != s.controllerID || source.TreeID != treeID || source.ParentAgentID != "" {
		return store.WorkerKey{}, errors.New("worker operation source is not the tree root")
	}
	key := store.WorkerKey{
		ControllerID: s.controllerID,
		TreeID:       treeID,
		AgentID:      agentID,
	}
	worker, err := s.state.GetWorker(ctx, key)
	if err != nil {
		return store.WorkerKey{}, fmt.Errorf("load managed worker: %w", err)
	}
	if worker.ParentAgentID != source.AgentID || worker.DeviceID != s.deviceID {
		return store.WorkerKey{}, errors.New("worker operation source or target does not match its reservation")
	}
	return key, nil
}

func mapWorkerOperationResult(
	hostResult workerhost.OperationResult,
	key store.WorkerKey,
	parentAgentID, deviceID, operationID string,
	action store.WorkerOperationAction,
	payload []byte,
	hostErr error,
) (protocol.WorkerOperationResult, error) {
	if err := validateHostOperationResult(
		hostResult,
		key,
		parentAgentID,
		deviceID,
		operationID,
		action,
		payload,
	); err != nil {
		return protocol.WorkerOperationResult{}, errors.Join(hostErr, err)
	}
	protocolAction, err := mapWorkerOperationAction(hostResult.Receipt.Action)
	if err != nil {
		return protocol.WorkerOperationResult{}, errors.Join(hostErr, err)
	}
	protocolOutcome, err := mapWorkerOperationOutcome(hostResult.Receipt.Outcome)
	if err != nil {
		return protocol.WorkerOperationResult{}, errors.Join(hostErr, err)
	}
	result := protocol.WorkerOperationResult{
		OperationID: hostResult.Receipt.OperationID,
		AgentID:     hostResult.Receipt.AgentID,
		Action:      protocolAction,
		Outcome:     protocolOutcome,
		FailureCode: hostResult.Receipt.FailureCode,
	}
	if err := result.Validate(); err != nil {
		return protocol.WorkerOperationResult{}, errors.Join(
			hostErr,
			fmt.Errorf("map managed worker operation result: %w", err),
		)
	}
	return result, hostErr
}

func validateHostOperationResult(
	result workerhost.OperationResult,
	key store.WorkerKey,
	parentAgentID, deviceID, operationID string,
	action store.WorkerOperationAction,
	payload []byte,
) error {
	if err := result.Receipt.Validate(); err != nil {
		return fmt.Errorf("managed worker operation receipt: %w", err)
	}
	if err := result.Worker.Validate(); err != nil {
		return fmt.Errorf("managed worker reservation: %w", err)
	}
	if result.Receipt.WorkerKey != key || result.Worker.WorkerKey != key ||
		result.Receipt.OperationID != operationID || result.Receipt.Action != action ||
		result.Worker.ParentAgentID != parentAgentID || result.Worker.DeviceID != deviceID {
		return errors.New("managed worker operation result does not match its request")
	}
	digest := sha256.Sum256(payload)
	if result.Receipt.PayloadDigest != hex.EncodeToString(digest[:]) {
		return errors.New("managed worker operation payload digest does not match its request")
	}
	return nil
}

func mapWorkerOperationAction(action store.WorkerOperationAction) (protocol.AgentOperationAction, error) {
	switch action {
	case store.WorkerOperationSend:
		return protocol.AgentOperationSend, nil
	case store.WorkerOperationFollowup:
		return protocol.AgentOperationFollowup, nil
	case store.WorkerOperationInterrupt:
		return protocol.AgentOperationInterrupt, nil
	default:
		return "", fmt.Errorf("unsupported managed worker operation action %q", action)
	}
}

func mapWorkerOperationOutcome(outcome store.WorkerOperationOutcome) (protocol.AgentOperationOutcome, error) {
	switch outcome {
	case store.WorkerOutcomePending:
		return protocol.AgentOperationOutcomePending, nil
	case store.WorkerOutcomeQueued:
		return protocol.AgentOperationOutcomeQueued, nil
	case store.WorkerOutcomeSteered:
		return protocol.AgentOperationOutcomeSteered, nil
	case store.WorkerOutcomeStarted:
		return protocol.AgentOperationOutcomeStarted, nil
	case store.WorkerOutcomeInterrupted:
		return protocol.AgentOperationOutcomeInterrupted, nil
	case store.WorkerOutcomeFailed:
		return protocol.AgentOperationOutcomeFailed, nil
	default:
		return "", fmt.Errorf("unsupported managed worker operation outcome %q", outcome)
	}
}
