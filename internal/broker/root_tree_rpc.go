package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleEnsureRootTree(ctx context.Context, request protocol.Envelope) error {
	if request.TreeID != "" || request.Source != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidRequest, "invalid root tree request")
	}
	params, err := protocol.DecodePayload[protocol.EnsureRootTreeParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid root tree payload")
	}
	tree, principal, err := s.server.registry.EnsureRootTree(
		ctx,
		s.server.controllerID,
		params.ExternalThreadID,
		s.deviceID,
		s.server.now(),
	)
	if err == nil {
		return s.writeResult(ctx, request, protocol.EnsureRootTreeResult{
			Tree: tree, Principal: principal,
		})
	}
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrConflict) {
		return s.writeError(ctx, request, protocol.ErrorConflict, "root tree binding conflicts with existing state")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return &internalError{operation: "ensure root tree", err: err}
}
