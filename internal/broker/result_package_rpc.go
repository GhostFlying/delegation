package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handlePublishResultPackage(
	ctx context.Context,
	request protocol.Envelope,
) error {
	if err := validatePublishResultPackageRequest(request, s.deviceID); err != nil {
		if errors.Is(err, errResultPackageConnectionMismatch) {
			return s.writeError(
				ctx,
				request,
				protocol.ErrorForbidden,
				"result package publication denied",
			)
		}
		return s.writeError(
			ctx,
			request,
			protocol.ErrorInvalidRequest,
			"result package publication requires a worker principal",
		)
	}
	params, err := protocol.DecodePayload[protocol.PublishResultPackageParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(
			ctx,
			request,
			protocol.ErrorInvalidParams,
			"invalid result package metadata",
		)
	}
	result, err := s.server.registry.PublishResultPackage(
		ctx,
		s.deviceID,
		*request.Source,
		params,
		s.server.now(),
	)
	if err != nil {
		return s.handlePublishResultPackageStoreError(ctx, request, err)
	}
	manifest, err := params.Metadata.DecodeManifest()
	if err != nil {
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "validate published result package", err: err}
	}
	if err := result.Validate(); err != nil || result.PackageID != manifest.PackageID {
		if err == nil {
			err = errors.New("published packageId differs from its manifest")
		}
		_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
		return &internalError{operation: "validate published result package", err: err}
	}

	// The metadata acknowledgement must be on the wire before relay can call
	// back into this source session. In particular, self-target delivery relies
	// on the session read loop returning from this handler first.
	if err := s.writeResult(ctx, request, result); err != nil {
		return err
	}
	s.server.resultRelays.schedule(resultPackageRelayRequest{
		source:    *request.Source,
		packageID: result.PackageID,
	})
	return nil
}

var errResultPackageConnectionMismatch = errors.New("result package source device differs from its connection")

func validatePublishResultPackageRequest(request protocol.Envelope, deviceID string) error {
	if request.TreeID == "" || request.Source == nil {
		return errors.New("result package publication source is missing")
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if request.Source.ControllerID != request.ControllerID || request.Source.TreeID != request.TreeID {
		return errors.New("result package principal differs from the request envelope")
	}
	if request.Source.ParentAgentID == "" {
		return errors.New("result package publication source is not a worker")
	}
	if request.Source.DeviceID != deviceID {
		return errResultPackageConnectionMismatch
	}
	return nil
}

func (s *session) handlePublishResultPackageStoreError(
	ctx context.Context,
	request protocol.Envelope,
	err error,
) error {
	code, message, internal := mapPublishResultPackageStoreError(err)
	if isContextError(err) {
		return err
	}
	if writeErr := s.writeError(ctx, request, code, message); writeErr != nil {
		return writeErr
	}
	if internal {
		return &internalError{operation: "publish result package", err: err}
	}
	return nil
}

func mapPublishResultPackageStoreError(err error) (int, string, bool) {
	switch {
	case errors.Is(err, store.ErrResultPackageLifecycleNotReady):
		return protocol.ErrorUnavailable, "lifecycle_not_ready", false
	case errors.Is(err, store.ErrAuthorizationDenied), errors.Is(err, store.ErrNotFound):
		return protocol.ErrorForbidden, "result package publication denied", false
	case errors.Is(err, store.ErrConflict):
		return protocol.ErrorConflict, "result package conflicts with broker state", false
	case isContextError(err):
		return protocol.ErrorUnavailable, "broker unavailable", false
	default:
		return protocol.ErrorUnavailable, "broker unavailable", true
	}
}

func validateResultPackageRelayRequest(request resultPackageRelayRequest) error {
	if err := request.source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if request.source.ParentAgentID == "" {
		return errors.New("result package relay source must be a worker")
	}
	if err := (&protocol.PublishResultPackageResult{PackageID: request.packageID}).Validate(); err != nil {
		return err
	}
	return nil
}
