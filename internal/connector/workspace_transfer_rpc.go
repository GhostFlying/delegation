package connector

import (
	"context"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func (s *session) handleCreateWorkspaceTransferRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil || request.Source.DeviceID != s.client.hello.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace transfer creation request")
	}
	params, err := protocol.DecodePayload[protocol.CreateWorkspaceTransferParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace transfer creation payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceTransfer.CreateWorkspaceTransfer(
			ctx, WorkspaceCreateTransferRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishWorkspaceTransferError(request, "create source workspace transfer", operationErr)
			return
		}
		if result.Validate() != nil || result.Transfer.TransferID != params.TransferID ||
			result.Transfer.WorkspaceID != params.WorkspaceID {
			s.finishInvalidWorkspaceTransferResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleReadWorkspaceArtifactRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil || request.Source.DeviceID != s.client.hello.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace artifact read request")
	}
	params, err := protocol.DecodePayload[protocol.ReadWorkspaceArtifactParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace artifact read payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceTransfer.ReadWorkspaceArtifact(
			ctx, WorkspaceReadArtifactRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishWorkspaceTransferError(request, "read source workspace artifact", operationErr)
			return
		}
		if result.Validate() != nil || result.TransferID != params.TransferID ||
			result.Kind != params.Kind || result.Offset != params.Offset || len(result.Data) > params.Limit {
			s.finishInvalidWorkspaceTransferResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleBeginWorkspaceTransferRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace transfer begin request")
	}
	params, err := protocol.DecodePayload[protocol.BeginWorkspaceTransferParams](request.Payload)
	if err != nil || params.Validate() != nil || params.SourceAgentID != request.Source.AgentID ||
		params.SourceDeviceID != request.Source.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace transfer begin payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceTransfer.BeginWorkspaceTransfer(
			ctx, WorkspaceBeginTransferRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishWorkspaceTransferError(request, "begin target workspace transfer", operationErr)
			return
		}
		if result.Validate() != nil || result.TransferID != params.Transfer.TransferID {
			s.finishInvalidWorkspaceTransferResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleWriteWorkspaceArtifactRequest(request protocol.Envelope) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace artifact write request")
	}
	params, err := protocol.DecodePayload[protocol.WriteWorkspaceArtifactParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace artifact write payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.workspaceTransfer.WriteWorkspaceArtifact(
			ctx, WorkspaceWriteArtifactRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishWorkspaceTransferError(request, "write target workspace artifact", operationErr)
			return
		}
		if result.Validate() != nil || result.TransferID != params.TransferID ||
			result.NextOffset != params.Offset+int64(len(params.Data)) {
			s.finishInvalidWorkspaceTransferResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleFinishWorkspaceTransferRequest(request protocol.Envelope) error {
	return s.handleWorkspaceTransferControlRequest(request, false)
}

func (s *session) handleCancelWorkspaceTransferRequest(request protocol.Envelope) error {
	return s.handleWorkspaceTransferControlRequest(request, true)
}

func (s *session) handleWorkspaceTransferControlRequest(request protocol.Envelope, cancel bool) error {
	if err := validateBrokerWorkerRequest(request); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid workspace transfer control request")
	}
	params, err := protocol.DecodePayload[protocol.WorkspaceTransferControlParams](request.Payload)
	if err != nil || params.Validate() != nil || params.SourceAgentID != request.Source.AgentID ||
		params.SourceDeviceID != request.Source.DeviceID {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid workspace transfer control payload")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		controlRequest := WorkspaceTransferControlRequest{
			TreeID: request.TreeID, Source: source, Params: params,
		}
		if cancel {
			result, operationErr := s.client.workspaceTransfer.CancelWorkspaceTransfer(ctx, controlRequest)
			if operationErr != nil {
				s.finishWorkspaceTransferError(request, "cancel workspace transfer", operationErr)
				return
			}
			if result.Validate() != nil || result.TransferID != params.TransferID {
				s.finishInvalidWorkspaceTransferResult(request)
				return
			}
			if writeErr := s.writeResult(request, result); writeErr != nil {
				s.close(writeErr)
			}
			return
		}
		result, operationErr := s.client.workspaceTransfer.FinishWorkspaceTransfer(ctx, controlRequest)
		if operationErr != nil {
			s.finishWorkspaceTransferError(request, "finish target workspace transfer", operationErr)
			return
		}
		if result.Validate() != nil || result.Workspace.WorkspaceID != params.WorkspaceID {
			s.finishInvalidWorkspaceTransferResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) finishWorkspaceTransferError(request protocol.Envelope, operation string, err error) {
	s.client.reportError(fmt.Errorf("%s: %w", operation, err))
	if writeErr := s.writeError(request, protocol.ErrorUnavailable, "workspace transfer operation failed"); writeErr != nil {
		s.close(writeErr)
	}
}

func (s *session) finishInvalidWorkspaceTransferResult(request protocol.Envelope) {
	if writeErr := s.writeError(request, protocol.ErrorInternal, "peer returned invalid workspace transfer metadata"); writeErr != nil {
		s.close(writeErr)
	}
}
