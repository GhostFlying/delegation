package broker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) startAgentSpawn(
	responseContext context.Context,
	sessionContext context.Context,
	request protocol.Envelope,
) error {
	select {
	case s.asyncSem <- struct{}{}:
	default:
		return s.writeError(
			responseContext, request, protocol.ErrorUnavailable, "too many pending agent spawns",
		)
	}
	operationContext, cancel := context.WithTimeout(sessionContext, agentSpawnRequestTimeout)
	s.async.Add(1)
	go func() {
		defer s.async.Done()
		defer func() { <-s.asyncSem }()
		defer cancel()
		if err := s.handleSpawnAgent(operationContext, request); err != nil && !isContextError(err) {
			s.server.reportError(&internalError{operation: "spawn managed agent", err: err})
		}
	}()
	return nil
}

func (s *session) handleSpawnAgent(ctx context.Context, request protocol.Envelope) error {
	if request.TreeID == "" || request.Source == nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidRequest, "agent spawn requires a principal")
	}
	if request.Source.DeviceID != s.deviceID {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "agent spawn access denied")
	}
	params, err := protocol.DecodePayload[protocol.SpawnAgentParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid agent spawn payload")
	}
	agentID, err := s.server.newID()
	if err != nil {
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return fmt.Errorf("create agent ID: %w", err)
	}
	receipt, err := s.server.registry.BeginAgentSpawn(ctx, store.AgentSpawnIntent{
		Source: *request.Source, SpawnID: params.SpawnID, AgentID: agentID,
		TargetDeviceID: params.TargetDeviceID, TaskName: params.TaskName,
		PromptDigest: sha256.Sum256([]byte(params.Message)),
	}, s.server.now())
	if err != nil {
		return s.handleAgentStoreError(ctx, request, "begin agent spawn", err)
	}
	if receipt.Agent.Status != protocol.AgentSpawnPending {
		return s.writeResult(ctx, request, terminalSpawnAgentResult(receipt.Agent))
	}
	indeterminate := protocol.SpawnAgentResult{
		Agent: receipt.Agent, Outcome: protocol.AgentSpawnOutcomeIndeterminate,
	}
	target := s.server.connection(params.TargetDeviceID)
	if target == nil {
		return s.writeResult(ctx, request, indeterminate)
	}
	payload, callErr := target.callPeer(
		ctx,
		protocol.MethodSpawnWorker,
		request.TreeID,
		*request.Source,
		protocol.SpawnWorkerParams{
			SpawnID: params.SpawnID, AgentID: receipt.Agent.Principal.AgentID,
			TaskName: params.TaskName, Message: params.Message,
		},
	)
	if callErr != nil {
		return s.writeResult(ctx, request, indeterminate)
	}
	result, err := protocol.DecodePayload[protocol.SpawnWorkerResult](payload)
	if err != nil || validateTargetWorkerResult(result, receipt.Agent) != nil {
		_ = target.connection.CloseNow()
		return s.writeResult(ctx, request, indeterminate)
	}
	key := store.AgentSpawnKey{
		ControllerID:  request.ControllerID,
		TreeID:        request.TreeID,
		SourceAgentID: request.Source.AgentID,
		SpawnID:       params.SpawnID,
	}
	var updated store.AgentSpawnReceipt
	switch result.Outcome {
	case protocol.AgentSpawnOutcomeIndeterminate:
		return s.writeResult(ctx, request, indeterminate)
	case protocol.AgentSpawnOutcomeBusy:
		return s.writeResult(ctx, request, protocol.SpawnAgentResult{
			Agent: receipt.Agent, Outcome: protocol.AgentSpawnOutcomeBusy,
		})
	case protocol.AgentSpawnOutcomeStarted:
		updated, err = s.server.registry.MarkAgentSpawnStarted(ctx, key, s.server.now())
	case protocol.AgentSpawnOutcomeFailed:
		updated, err = s.server.registry.MarkAgentSpawnFailed(
			ctx, key, result.FailureCode, s.server.now(),
		)
	default:
		panic("validated worker result has an unknown outcome")
	}
	if err != nil {
		_ = s.writeResult(ctx, request, indeterminate)
		return fmt.Errorf("record agent spawn result: %w", err)
	}
	return s.writeResult(ctx, request, protocol.SpawnAgentResult{
		Agent: updated.Agent, Outcome: result.Outcome,
	})
}

func (s *session) handleListAgents(ctx context.Context, request protocol.Envelope) error {
	if request.TreeID == "" || request.Source == nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidRequest, "agent list requires a principal")
	}
	if request.Source.DeviceID != s.deviceID {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "agent list access denied")
	}
	params, err := protocol.DecodePayload[protocol.ListAgentsParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid agent list payload")
	}
	page, err := s.server.registry.ListAgents(ctx, *request.Source, store.AgentPageRequest{
		AfterSequence: params.AfterSequence, Limit: params.Limit,
	})
	if err != nil {
		return s.handleAgentStoreError(ctx, request, "list agents", err)
	}
	return s.writeResult(ctx, request, protocol.ListAgentsResult{
		Agents: page.Agents, NextSequence: page.NextSequence,
	})
}

func (s *session) handleAgentStoreError(
	ctx context.Context,
	request protocol.Envelope,
	operation string,
	err error,
) error {
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return s.writeError(ctx, request, protocol.ErrorForbidden, "agent access denied")
	}
	if errors.Is(err, store.ErrNotFound) {
		return s.writeError(ctx, request, protocol.ErrorNotFound, "agent resource not found")
	}
	if errors.Is(err, store.ErrConflict) {
		return s.writeError(ctx, request, protocol.ErrorConflict, "agent request conflicts with existing state")
	}
	if errors.Is(err, store.ErrAgentLimit) {
		return s.writeError(ctx, request, protocol.ErrorUnavailable, "tree agent limit reached")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return &internalError{operation: operation, err: err}
}

func (s *Server) connection(deviceID string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.connections[deviceID]
	if current == nil || current.revision.Load() < s.latestRevisions[deviceID] ||
		!current.workerReady.Load() {
		return nil
	}
	return current
}

func validateTargetWorkerResult(result protocol.SpawnWorkerResult, agent protocol.AgentSummary) error {
	if err := result.Validate(); err != nil {
		return err
	}
	if result.SpawnID != agent.SpawnID || result.Principal != agent.Principal {
		return errors.New("target worker result does not match the durable agent identity")
	}
	return nil
}

func terminalSpawnAgentResult(agent protocol.AgentSummary) protocol.SpawnAgentResult {
	outcome := protocol.AgentSpawnOutcomeStarted
	if agent.Status == protocol.AgentSpawnFailed {
		outcome = protocol.AgentSpawnOutcomeFailed
	}
	return protocol.SpawnAgentResult{Agent: agent, Outcome: outcome}
}
