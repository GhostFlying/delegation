package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	resultApplyRPCTreeID   = "123e4567-e89b-42d3-a456-426614174820"
	resultApplyRPCRootID   = "123e4567-e89b-42d3-a456-426614174821"
	resultApplyRPCWorkerID = "123e4567-e89b-42d3-a456-426614174822"
	resultApplyRPCOtherID  = "123e4567-e89b-42d3-a456-426614174823"
)

func TestAuthorizeResultApplyRequestRequiresEnvelopeBoundRootAuthority(t *testing.T) {
	root := control.NewRootPrincipal(
		brokerTestControllerID, resultApplyRPCTreeID, resultApplyRPCRootID, brokerTestDeviceID,
	)
	valid := protocol.Envelope{
		ControllerID: brokerTestControllerID,
		TreeID:       resultApplyRPCTreeID,
		Source:       identityPointer(root.Identity()),
	}
	if err := validateAuthorizeResultApplyRequest(valid, brokerTestDeviceID); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		mutate       func(*protocol.Envelope)
		wantMismatch bool
	}{
		{name: "missing tree", mutate: func(request *protocol.Envelope) { request.TreeID = "" }},
		{name: "missing principal", mutate: func(request *protocol.Envelope) { request.Source = nil }},
		{name: "worker principal", mutate: func(request *protocol.Envelope) {
			worker := control.NewWorkerPrincipal(
				brokerTestControllerID, resultApplyRPCTreeID, resultApplyRPCWorkerID,
				resultApplyRPCRootID, brokerTestDeviceID,
			)
			request.Source = identityPointer(worker.Identity())
		}},
		{name: "controller mismatch", wantMismatch: true, mutate: func(request *protocol.Envelope) {
			request.Source.ControllerID = resultApplyRPCOtherID
		}},
		{name: "tree mismatch", wantMismatch: true, mutate: func(request *protocol.Envelope) {
			request.Source.TreeID = resultApplyRPCOtherID
		}},
		{name: "connection device mismatch", wantMismatch: true, mutate: func(request *protocol.Envelope) {
			request.Source.DeviceID = resultApplyRPCOtherID
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			source := *valid.Source
			request.Source = &source
			test.mutate(&request)
			err := validateAuthorizeResultApplyRequest(request, brokerTestDeviceID)
			if err == nil {
				t.Fatal("result apply request was accepted")
			}
			if errors.Is(err, errResultApplyAuthorityMismatch) != test.wantMismatch {
				t.Fatalf("result apply request error = %v, mismatch = %t", err, test.wantMismatch)
			}
		})
	}
}

func TestAuthorizeResultApplyStoreErrorMapping(t *testing.T) {
	tests := []struct {
		err         error
		wantCode    int
		wantMessage string
		wantReport  bool
	}{
		{store.ErrAuthorizationDenied, protocol.ErrorForbidden, "result apply authorization denied", false},
		{store.ErrNotFound, protocol.ErrorForbidden, "result apply authorization denied", false},
		{store.ErrConflict, protocol.ErrorConflict, "result apply authorization conflicts with broker state", false},
		{context.Canceled, protocol.ErrorUnavailable, "broker unavailable", false},
		{errors.New("database unavailable"), protocol.ErrorUnavailable, "broker unavailable", true},
	}
	for _, test := range tests {
		code, message, report := mapAuthorizeResultApplyStoreError(test.err)
		if code != test.wantCode || message != test.wantMessage || report != test.wantReport {
			t.Fatalf(
				"map %v = (%d, %q, %t), want (%d, %q, %t)",
				test.err, code, message, report,
				test.wantCode, test.wantMessage, test.wantReport,
			)
		}
	}
}

func identityPointer(identity control.PrincipalIdentity) *control.PrincipalIdentity {
	return &identity
}
