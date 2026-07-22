package broker

import (
	"context"
	"errors"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

func (s *session) handleListDevices(ctx context.Context, request protocol.Envelope) error {
	if authorized, err := s.authorizeDeviceRead(ctx, request); !authorized {
		return err
	}
	params, err := protocol.DecodePayload[protocol.ListDevicesParams](request.Payload)
	if err != nil || params.Validate(store.MaximumDevicePage) != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid device list payload")
	}
	page, err := s.server.registry.ListDevices(ctx, s.server.controllerID, store.DevicePageRequest{
		AfterDeviceID:    params.AfterDeviceID,
		Limit:            params.Limit,
		ExpectedRevision: params.ExpectedRevision,
	})
	if err == nil {
		return s.writeResult(ctx, request, protocol.ListDevicesResult{
			Revision: page.Revision, Devices: page.Devices, NextCursor: page.NextCursor,
		})
	}
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrRevisionChanged) {
		return s.writeError(ctx, request, protocol.ErrorConflict, "device registry changed; restart pagination")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return &internalError{operation: "list devices", err: err}
}

func (s *session) handleDescribeDevice(ctx context.Context, request protocol.Envelope) error {
	if authorized, err := s.authorizeDeviceRead(ctx, request); !authorized {
		return err
	}
	params, err := protocol.DecodePayload[protocol.DescribeDeviceParams](request.Payload)
	if err != nil || params.Validate() != nil {
		return s.writeError(ctx, request, protocol.ErrorInvalidParams, "invalid device describe payload")
	}
	record, err := s.server.registry.DescribeDevice(ctx, s.server.controllerID, params.DeviceID)
	if err == nil {
		return s.writeResult(ctx, request, protocol.DescribeDeviceResult{
			Revision: record.RegistryRevision, Device: record.Device,
		})
	}
	if isContextError(err) {
		return err
	}
	if errors.Is(err, store.ErrNotFound) {
		return s.writeError(ctx, request, protocol.ErrorNotFound, "device not found")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return &internalError{operation: "describe device", err: err}
}

func (s *session) authorizeDeviceRead(ctx context.Context, request protocol.Envelope) (bool, error) {
	if request.TreeID == "" || request.Source == nil {
		return false, s.writeError(ctx, request, protocol.ErrorInvalidRequest, "device registry request requires a principal")
	}
	if request.Source.DeviceID != s.deviceID {
		return false, s.writeError(ctx, request, protocol.ErrorForbidden, "device registry access denied")
	}
	_, err := s.server.registry.AuthorizePrincipal(ctx, *request.Source, control.CapabilityDeviceRead)
	if err == nil {
		return true, nil
	}
	if isContextError(err) {
		return false, err
	}
	if errors.Is(err, store.ErrAuthorizationDenied) {
		return false, s.writeError(ctx, request, protocol.ErrorForbidden, "device registry access denied")
	}
	_ = s.writeError(ctx, request, protocol.ErrorUnavailable, "broker unavailable")
	return false, &internalError{operation: "authorize device registry read", err: err}
}
