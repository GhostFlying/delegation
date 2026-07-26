package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleAuthorizeResultApply(
	ctx context.Context,
	request protocol.Envelope,
) error {
	if err := validateAuthorizeResultApplyRequest(request, s.deviceID); err != nil {
		if errors.Is(err, errResultApplyAuthorityMismatch) {
			return s.writeError(ctx, request, protocol.ErrorForbidden, "result apply authorization denied")
		}
		return s.writeError(
			ctx, request, protocol.ErrorInvalidRequest,
			"result apply authorization requires a root principal",
		)
	}
	params, err := protocol.DecodePayload[protocol.AuthorizeResultApplyParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid result apply authorization")
	}
	result, err := s.server.registry.AuthorizeResultApply(
		ctx, s.deviceID, *request.Source, params, s.server.now(),
	)
	if err != nil {
		return s.handleAuthorizeResultApplyStoreError(ctx, request, err)
	}
	if err := result.Validate(); err != nil || result.ApplyID != params.ApplyID ||
		result.PackageID != params.PackageID {
		if err == nil {
			err = errors.New("result apply authorization differs from its request")
		}
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "validate result apply authorization", err: err}
	}
	return s.writeResult(ctx, request, result)
}

func (s *session) handleAuthorizeResultApplyStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	code, message, internal := mapAuthorizeResultApplyStoreError(err)
	if isContextError(err) {
		return err
	}
	if writeErr := s.writeError(ctx, request, code, message); writeErr != nil {
		return writeErr
	}
	if internal {
		return &internalError{operation: "authorize result apply", err: err}
	}
	return nil
}

var errResultApplyAuthorityMismatch = errors.New("result apply principal differs from its connection or envelope")

func validateAuthorizeResultApplyRequest(request protocol.Envelope, deviceID string) error {
	if request.TreeID == "" || request.Source == nil {
		return errors.New("result apply principal is missing")
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if request.Source.ParentAgentID != "" {
		return errors.New("result apply principal is not a root")
	}
	if request.Source.ControllerID != request.ControllerID || request.Source.TreeID != request.TreeID ||
		request.Source.DeviceID != deviceID {
		return errResultApplyAuthorityMismatch
	}
	return nil
}

func mapAuthorizeResultApplyStoreError(err error) (int, string, bool) {
	switch {
	case errors.Is(err, store.ErrAuthorizationDenied), errors.Is(err, store.ErrNotFound):
		return protocol.ErrorForbidden, "result apply authorization denied", false
	case errors.Is(err, store.ErrConflict):
		return protocol.ErrorConflict, "result apply authorization conflicts with broker state", false
	case isContextError(err):
		return protocol.ErrorUnavailable, "broker unavailable", false
	default:
		return protocol.ErrorUnavailable, "broker unavailable", true
	}
}
