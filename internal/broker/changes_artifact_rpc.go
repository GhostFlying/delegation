package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handlePublishChangesArtifact(
	ctx context.Context,
	request protocol.Envelope,
) error {
	if request.TreeID == "" || request.Source == nil {
		return s.writeError(
			ctx, request, protocol.ErrorInvalidRequest,
			"changes artifact publication requires a worker principal",
		)
	}
	if request.Source.ParentAgentID == "" || request.Source.DeviceID != s.deviceID {
		return s.writeError(
			ctx, request, protocol.ErrorForbidden, "changes artifact publication denied",
		)
	}
	params, err := protocol.DecodePayload[protocol.PublishChangesArtifactParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(
			ctx, request, protocol.ErrorInvalidParams, "invalid changes artifact metadata",
		)
	}
	result, err := s.server.registry.PublishChangesArtifact(
		ctx, s.deviceID, *request.Source, params, s.server.now(),
	)
	if err != nil {
		return s.handlePublishChangesArtifactStoreError(ctx, request, err)
	}
	if err := result.Validate(); err != nil {
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "validate published changes artifact", err: err}
	}
	s.server.artifactNotifier.notify(treeKey{
		controllerID: request.Source.ControllerID,
		treeID:       request.Source.TreeID,
	})
	return s.writeResult(ctx, request, result)
}

func (s *session) handlePublishChangesArtifactStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	switch {
	case errors.Is(err, store.ErrAuthorizationDenied), errors.Is(err, store.ErrNotFound):
		return s.writeError(
			ctx, request, protocol.ErrorForbidden, "changes artifact publication denied",
		)
	case errors.Is(err, store.ErrConflict),
		errors.Is(err, store.ErrChangesArtifactSequenceExhausted):
		return s.writeError(
			ctx, request, protocol.ErrorConflict, "changes artifact conflicts with broker state",
		)
	case isContextError(err):
		return err
	default:
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "publish changes artifact", err: err}
	}
}
