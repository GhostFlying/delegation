package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func (s *session) handleBrokerRequest(request protocol.Envelope) error {
	switch request.Method {
	case protocol.MethodSpawnWorker:
		return s.handleSpawnWorkerRequest(request)
	case protocol.MethodSendWorker:
		return s.handleSendWorkerRequest(request)
	case protocol.MethodFollowupWorker:
		return s.handleFollowupWorkerRequest(request)
	case protocol.MethodInterruptWorker:
		return s.handleInterruptWorkerRequest(request)
	case protocol.MethodInspectWorkspace:
		return s.handleInspectWorkspaceRequest(request)
	case protocol.MethodPrepareWorkspace:
		return s.handlePrepareWorkspaceRequest(request)
	case protocol.MethodCreateWorkspaceTransfer:
		return s.handleCreateWorkspaceTransferRequest(request)
	case protocol.MethodReadWorkspaceArtifact:
		return s.handleReadWorkspaceArtifactRequest(request)
	case protocol.MethodBeginWorkspaceTransfer:
		return s.handleBeginWorkspaceTransferRequest(request)
	case protocol.MethodWriteWorkspaceArtifact:
		return s.handleWriteWorkspaceArtifactRequest(request)
	case protocol.MethodFinishWorkspaceTransfer:
		return s.handleFinishWorkspaceTransferRequest(request)
	case protocol.MethodCancelWorkspaceTransfer:
		return s.handleCancelWorkspaceTransferRequest(request)
	default:
		return s.writeError(request, protocol.ErrorMethodNotFound, "method not found")
	}
}

func (s *session) handleInspectWorkspaceRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil || request.Source.DeviceID != s.client.hello.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace inspection request")
	}
	params, err := protocol.DecodePayload[protocol.InspectWorkspaceParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace inspection payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceManager.InspectWorkspace(ctx, WorkspaceInspectRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		if operationErr != nil {
			s.client.reportError(fmt.Errorf("inspect source workspace: %w", operationErr))
			if writeErr := s.writeError(request, protocol.ErrorUnavailable, "source workspace inspection failed"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		if result.Validate() != nil || result.SyncID != params.SyncID {
			if writeErr := s.writeError(request, protocol.ErrorInternal, "source returned invalid workspace metadata"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handlePrepareWorkspaceRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace prepare request")
	}
	params, err := protocol.DecodePayload[protocol.PrepareWorkspaceParams](request.Payload)
	if err != nil || params.Validate() != nil || params.SourceAgentID != request.Source.AgentID ||
		params.SourceDeviceID != request.Source.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace prepare payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceManager.PrepareWorkspace(ctx, WorkspacePrepareRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		if operationErr != nil {
			s.client.reportError(fmt.Errorf("prepare target workspace: %w", operationErr))
			if writeErr := s.writeError(request, protocol.ErrorUnavailable, "target workspace preparation failed"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		if result.Validate() != nil || result.WorkspaceID != params.WorkspaceID {
			if writeErr := s.writeError(request, protocol.ErrorInternal, "target returned invalid workspace metadata"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleSpawnWorkerRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid worker spawn request")
	}
	params, err := protocol.DecodePayload[protocol.SpawnWorkerParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid worker spawn payload")
	}
	source := *request.Source
	return s.startInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workerSpawner.SpawnWorker(ctx, WorkerSpawnRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		if operationErr != nil {
			s.client.reportError(fmt.Errorf("handle broker worker spawn: %w", operationErr))
			if writeErr := s.writeError(request, protocol.ErrorUnavailable, "peer worker dispatch failed"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		if resultErr := validateWorkerSpawnResult(result, request, params, s.client.hello.DeviceID); resultErr != nil {
			s.client.reportError(resultErr)
			if writeErr := s.writeError(request, protocol.ErrorInternal, "peer returned an invalid worker result"); writeErr != nil {
				s.close(writeErr)
				return
			}
			s.close(resultErr)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleSendWorkerRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid worker send request")
	}
	params, err := protocol.DecodePayload[protocol.SendWorkerParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid worker send payload")
	}
	source := *request.Source
	if s.client.workerController == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "peer worker control is unavailable")
	}
	return s.startInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workerController.SendWorker(ctx, WorkerSendRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		s.finishWorkerOperationRequest(
			request,
			params.MessageID,
			params.AgentID,
			protocol.AgentOperationSend,
			result,
			operationErr,
		)
	})
}

func (s *session) handleFollowupWorkerRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid worker follow-up request")
	}
	params, err := protocol.DecodePayload[protocol.FollowupWorkerParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid worker follow-up payload")
	}
	source := *request.Source
	if s.client.workerController == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "peer worker control is unavailable")
	}
	return s.startInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workerController.FollowupWorker(ctx, WorkerFollowupRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		s.finishWorkerOperationRequest(
			request,
			params.OperationID,
			params.AgentID,
			protocol.AgentOperationFollowup,
			result,
			operationErr,
		)
	})
}

func (s *session) handleInterruptWorkerRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid worker interrupt request")
	}
	params, err := protocol.DecodePayload[protocol.InterruptWorkerParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid worker interrupt payload")
	}
	source := *request.Source
	if s.client.workerController == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "peer worker control is unavailable")
	}
	return s.startInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workerController.InterruptWorker(ctx, WorkerInterruptRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		})
		s.finishWorkerOperationRequest(
			request,
			params.OperationID,
			params.AgentID,
			protocol.AgentOperationInterrupt,
			result,
			operationErr,
		)
	})
}

func validateBrokerWorkerRequest(request protocol.Envelope) error {
	if request.TreeID == "" || request.Source == nil || request.Source.ParentAgentID != "" {
		return errors.New("worker request source is not a tree root")
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if request.Source.ControllerID != request.ControllerID || request.Source.TreeID != request.TreeID {
		return errors.New("worker request source does not match the envelope")
	}
	return nil
}

func (s *session) startInbound(request protocol.Envelope, run func(context.Context)) error {
	return s.startInboundOperation(request, false, run)
}

func (s *session) startWorkspaceInbound(request protocol.Envelope, run func(context.Context)) error {
	return s.startInboundOperation(request, true, run)
}

func (s *session) startInboundOperation(
	request protocol.Envelope,
	workspace bool,
	run func(context.Context),
) error {
	select {
	case s.inboundSem <- struct{}{}:
	default:
		return s.writeError(request, protocol.ErrorUnavailable, "peer worker dispatch is busy")
	}
	s.inboundMu.Lock()
	if s.context.Err() != nil || workspace && s.workspaceStopping {
		s.inboundMu.Unlock()
		<-s.inboundSem
		return ErrUnavailable
	}
	if _, duplicate := s.inbound[request.RequestID]; duplicate {
		s.inboundMu.Unlock()
		<-s.inboundSem
		return errors.New("broker reused an active requestId")
	}
	operationContext, cancel := context.WithCancel(s.context)
	s.inbound[request.RequestID] = cancel
	if workspace {
		s.workspaceInbound.Add(1)
	}
	s.inboundMu.Unlock()
	go func() {
		defer func() {
			cancel()
			s.inboundMu.Lock()
			delete(s.inbound, request.RequestID)
			s.inboundMu.Unlock()
			<-s.inboundSem
			if workspace {
				s.workspaceInbound.Done()
			}
		}()
		run(operationContext)
	}()
	return nil
}

func (s *session) stopWorkspaceInbound() {
	s.inboundMu.Lock()
	s.workspaceStopping = true
	s.inboundMu.Unlock()
	s.workspaceInbound.Wait()
}

func (s *session) finishWorkerOperationRequest(
	request protocol.Envelope,
	operationID, agentID string,
	action protocol.AgentOperationAction,
	result protocol.WorkerOperationResult,
	operationErr error,
) {
	resultErr := validateWorkerOperationResult(result, operationID, agentID, action)
	if operationErr != nil {
		s.client.reportError(fmt.Errorf("handle broker worker %s: %w", action, operationErr))
		if resultErr != nil || result.Outcome == protocol.AgentOperationOutcomePending {
			if writeErr := s.writeError(request, protocol.ErrorUnavailable, "peer worker operation failed"); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
	}
	if resultErr != nil {
		s.client.reportError(resultErr)
		if writeErr := s.writeError(request, protocol.ErrorInternal, "peer returned an invalid worker result"); writeErr != nil {
			s.close(writeErr)
			return
		}
		s.close(resultErr)
		return
	}
	if writeErr := s.writeResult(request, result); writeErr != nil {
		s.close(writeErr)
	}
}

func validateWorkerOperationResult(
	result protocol.WorkerOperationResult,
	operationID, agentID string,
	action protocol.AgentOperationAction,
) error {
	if err := result.Validate(); err != nil {
		return fmt.Errorf("worker manager returned an invalid operation result: %w", err)
	}
	if result.OperationID != operationID || result.AgentID != agentID || result.Action != action {
		return errors.New("worker manager returned a mismatched operation result")
	}
	return nil
}

func validateWorkerSpawnResult(
	result protocol.SpawnWorkerResult,
	request protocol.Envelope,
	params protocol.SpawnWorkerParams,
	deviceID string,
) error {
	if err := result.Validate(); err != nil {
		return fmt.Errorf("worker spawner returned an invalid result: %w", err)
	}
	if request.Source == nil || result.SpawnID != params.SpawnID ||
		result.Principal.ControllerID != request.ControllerID ||
		result.Principal.TreeID != request.TreeID ||
		result.Principal.AgentID != params.AgentID ||
		result.Principal.ParentAgentID != request.Source.AgentID ||
		result.Principal.DeviceID != deviceID {
		return errors.New("worker spawner returned a mismatched result")
	}
	return nil
}
