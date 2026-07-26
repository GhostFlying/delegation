package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
)

type resultPackageAuthority int

const (
	resultPackageSourceWorker resultPackageAuthority = iota
	resultPackageSinkRoot
)

func (s *session) handleReadResultPackagePartRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSourceWorker,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package read request")
	}
	params, err := protocol.DecodePayload[protocol.ReadResultPackagePartParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package read payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.ReadResultPackagePart(
			ctx, ResultPackageReadRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "read result package part", operationErr)
			return
		}
		if result.Validate() != nil || result.PackageID != params.PackageID ||
			result.Kind != params.Kind || result.Offset != params.Offset || len(result.Data) > params.Limit {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleBeginResultPackageRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSinkRoot,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package begin request")
	}
	params, err := protocol.DecodePayload[protocol.BeginResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package begin payload")
	}
	manifest, err := params.Metadata.DecodeManifest()
	if err != nil || manifest.ControllerID != request.ControllerID || manifest.TreeID != request.TreeID {
		return s.writeError(request, protocol.ErrorInvalidParams, "result package manifest authority is invalid")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.BeginResultPackage(
			ctx, ResultPackageBeginRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "begin result package", operationErr)
			return
		}
		if result.Validate() != nil || result.PackageID != params.PackageID ||
			result.AttemptID != params.AttemptID {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleWriteResultPackagePartRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSinkRoot,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package write request")
	}
	params, err := protocol.DecodePayload[protocol.WriteResultPackagePartParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package write payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.WriteResultPackagePart(
			ctx, ResultPackageWriteRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "write result package part", operationErr)
			return
		}
		if !validResultPackageWriteResponse(params, result) {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func validResultPackageWriteResponse(
	params protocol.WriteResultPackagePartParams,
	result protocol.WriteResultPackagePartResult,
) bool {
	return result.Validate() == nil && result.AttemptID == params.AttemptID &&
		result.PackageID == params.PackageID && result.Kind == params.Kind &&
		result.NextOffset >= params.Offset+int64(len(params.Data))
}

func (s *session) handleFinishResultPackageRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSinkRoot,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package finish request")
	}
	params, err := protocol.DecodePayload[protocol.FinishResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package finish payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.FinishResultPackage(
			ctx, ResultPackageFinishRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "finish result package", operationErr)
			return
		}
		if result.Validate() != nil || result.AttemptID != params.AttemptID ||
			result.PackageID != params.PackageID {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleCancelResultPackageRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSinkRoot,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package cancel request")
	}
	params, err := protocol.DecodePayload[protocol.CancelResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package cancel payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.CancelResultPackage(
			ctx, ResultPackageCancelRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "cancel result package", operationErr)
			return
		}
		if result.Validate() != nil || result.AttemptID != params.AttemptID ||
			result.PackageID != params.PackageID {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleAcknowledgeResultPackageRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSourceWorker,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package acknowledgement request")
	}
	params, err := protocol.DecodePayload[protocol.AcknowledgeResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package acknowledgement payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.AcknowledgeResultPackage(
			ctx, ResultPackageAcknowledgeRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "acknowledge result package", operationErr)
			return
		}
		if result.Validate() != nil || result.PackageID != params.PackageID ||
			result.Sequence != params.Sequence {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func (s *session) handleReleaseResultPackageRequest(request protocol.Envelope) error {
	if err := validateBrokerResultPackageRequest(
		request, s.client.hello.DeviceID, resultPackageSourceWorker,
	); err != nil {
		return s.writeError(request, protocol.ErrorInvalidRequest, "invalid result package release request")
	}
	params, err := protocol.DecodePayload[protocol.ReleaseResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(request, protocol.ErrorInvalidParams, "invalid result package release payload")
	}
	if s.client.resultPackages == nil {
		return s.writeError(request, protocol.ErrorUnavailable, "result package manager is unavailable")
	}
	source := *request.Source
	return s.startWorkspaceInbound(request, func(ctx context.Context) {
		result, operationErr := s.client.resultPackages.ReleaseResultPackage(
			ctx, ResultPackageReleaseRequest{TreeID: request.TreeID, Source: source, Params: params},
		)
		if operationErr != nil {
			s.finishResultPackageError(request, "release result package", operationErr)
			return
		}
		if result.Validate() != nil || result != protocol.ReleaseResultPackageResult(params) {
			s.finishInvalidResultPackageResult(request)
			return
		}
		if writeErr := s.writeResult(request, result); writeErr != nil {
			s.close(writeErr)
		}
	})
}

func validateBrokerResultPackageRequest(
	request protocol.Envelope,
	deviceID string,
	authority resultPackageAuthority,
) error {
	if request.TreeID == "" || request.Source == nil {
		return errors.New("result package request source is missing")
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if request.Source.ControllerID != request.ControllerID ||
		request.Source.TreeID != request.TreeID || request.Source.DeviceID != deviceID {
		return errors.New("result package request source does not match the envelope or peer")
	}
	switch authority {
	case resultPackageSourceWorker:
		if request.Source.ParentAgentID == "" {
			return errors.New("result package source authority must be a worker")
		}
	case resultPackageSinkRoot:
		if request.Source.ParentAgentID != "" {
			return errors.New("result package sink authority must be a tree root")
		}
	default:
		return errors.New("unsupported result package request authority")
	}
	return nil
}

func (s *session) finishResultPackageError(request protocol.Envelope, operation string, err error) {
	s.client.reportError(fmt.Errorf("%s: %w", operation, err))
	if writeErr := s.writeError(request, protocol.ErrorUnavailable, "result package operation failed"); writeErr != nil {
		s.close(writeErr)
	}
}

func (s *session) finishInvalidResultPackageResult(request protocol.Envelope) {
	if writeErr := s.writeError(
		request, protocol.ErrorInternal, "peer returned invalid result package metadata",
	); writeErr != nil {
		s.close(writeErr)
	}
}
