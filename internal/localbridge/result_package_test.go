package localbridge

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	bridgeResultPackageID   = "123e4567-e89b-42d3-a456-426614174310"
	bridgeResultWorkerID    = "123e4567-e89b-42d3-a456-426614174311"
	bridgeResultThreadID    = "123e4567-e89b-42d3-a456-426614174312"
	bridgeResultTurnID      = "123e4567-e89b-42d3-a456-426614174313"
	bridgeResultOtherTreeID = "123e4567-e89b-42d3-a456-426614174314"
)

type fakeResultPackageAvailabilityProvider struct {
	availability protocol.ResultPackageAvailability
	err          error
	lookups      []ResultPackageAvailabilityLookup
}

func (p *fakeResultPackageAvailabilityProvider) LookupResultPackageAvailability(
	_ context.Context,
	lookup ResultPackageAvailabilityLookup,
) (protocol.ResultPackageAvailability, error) {
	p.lookups = append(p.lookups, lookup)
	return p.availability, p.err
}

func TestDecorateAgentWaitVerifiesResultPackageAgainstLocalInbox(t *testing.T) {
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	for _, availability := range []protocol.ResultPackageAvailability{
		protocol.ResultPackageAvailable,
		protocol.ResultPackageEvicted,
	} {
		t.Run(string(availability), func(t *testing.T) {
			manifest := bridgeResultManifest()
			payload := encodeBridgeWaitResult(t, protocol.WaitAgentResult{
				Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
				Artifacts: []protocol.ChangesArtifactMetadata{},
				Results: []protocol.ResultPackageHandle{{
					Manifest: manifest, Availability: protocol.ResultPackageUnverified,
					Sequence: 1, DeliveredAt: 2,
				}},
				NextResultCursor: 1,
			})
			provider := &fakeResultPackageAvailabilityProvider{availability: availability}
			server := &Server{results: provider}
			decorated, rpcErr := server.decorateAgentWait(
				context.Background(), bridgeResultWaitRequest(root), payload,
			)
			if rpcErr != nil {
				t.Fatalf("decorate result package: %#v", rpcErr)
			}
			result, err := protocol.DecodePayload[protocol.WaitAgentResult](decorated)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Results) != 1 || result.Results[0].Availability != availability {
				t.Fatalf("decorated result = %#v", result)
			}
			wantLookups := []ResultPackageAvailabilityLookup{{Root: root, Manifest: manifest}}
			if !reflect.DeepEqual(provider.lookups, wantLookups) {
				t.Fatalf("availability lookups = %#v, want %#v", provider.lookups, wantLookups)
			}
		})
	}
}

func TestDecorateAgentWaitLeavesEmptyResultPageIndependentOfInbox(t *testing.T) {
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	payload := encodeBridgeWaitResult(t, protocol.WaitAgentResult{
		Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
		Artifacts: []protocol.ChangesArtifactMetadata{}, Results: []protocol.ResultPackageHandle{},
	})
	decorated, rpcErr := (&Server{}).decorateAgentWait(
		context.Background(), bridgeResultWaitRequest(root), payload,
	)
	if rpcErr != nil || !reflect.DeepEqual(decorated, payload) {
		t.Fatalf("empty result decoration = %s, %#v", decorated, rpcErr)
	}
}

func TestDecorateAgentWaitFailsClosedWithoutVerifiedLocalAvailability(t *testing.T) {
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	base := protocol.ResultPackageHandle{
		Manifest: bridgeResultManifest(), Availability: protocol.ResultPackageUnverified,
		Sequence: 1, DeliveredAt: 2,
	}
	tests := []struct {
		name     string
		request  request
		handle   protocol.ResultPackageHandle
		provider ResultPackageAvailabilityProvider
		wantCode int
	}{
		{name: "missing provider", request: bridgeResultWaitRequest(root), handle: base, wantCode: protocol.ErrorUnavailable},
		{
			name: "provider error", request: bridgeResultWaitRequest(root), handle: base,
			provider: &fakeResultPackageAvailabilityProvider{err: errors.New("inbox unavailable")},
			wantCode: protocol.ErrorUnavailable,
		},
		{
			name: "provider unverified", request: bridgeResultWaitRequest(root), handle: base,
			provider: &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageUnverified},
			wantCode: protocol.ErrorInternal,
		},
		{
			name: "broker claimed available", request: bridgeResultWaitRequest(root),
			handle: func() protocol.ResultPackageHandle {
				value := base
				value.Availability = protocol.ResultPackageAvailable
				return value
			}(),
			provider: &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageAvailable},
			wantCode: protocol.ErrorInternal,
		},
		{
			name: "wrong tree", request: bridgeResultWaitRequest(root),
			handle: func() protocol.ResultPackageHandle {
				value := base
				value.Manifest.TreeID = bridgeResultOtherTreeID
				return value
			}(),
			provider: &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageAvailable},
			wantCode: protocol.ErrorInternal,
		},
		{
			name: "invalid manifest", request: bridgeResultWaitRequest(root),
			handle: func() protocol.ResultPackageHandle {
				value := base
				value.Manifest.PackageID = "invalid"
				return value
			}(),
			provider: &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageAvailable},
			wantCode: protocol.ErrorInternal,
		},
		{
			name: "request tree mismatch",
			request: func() request {
				value := bridgeResultWaitRequest(root)
				value.TreeID = bridgeResultOtherTreeID
				return value
			}(),
			handle:   base,
			provider: &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageAvailable},
			wantCode: protocol.ErrorUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := encodeBridgeWaitResult(t, protocol.WaitAgentResult{
				Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
				Artifacts: []protocol.ChangesArtifactMetadata{},
				Results:   []protocol.ResultPackageHandle{test.handle}, NextResultCursor: 1,
			})
			_, rpcErr := (&Server{results: test.provider}).decorateAgentWait(
				context.Background(), test.request, payload,
			)
			if rpcErr == nil || rpcErr.Code != test.wantCode {
				t.Fatalf("decorate error = %#v, want code %d", rpcErr, test.wantCode)
			}
		})
	}
}

func TestDecorateAgentWaitRejectsOversizedResultPageBeforeInboxLookup(t *testing.T) {
	root := control.NewRootPrincipal(
		bridgeTestControllerID, bridgeTestTreeID, bridgeTestAgentID, bridgeTestDeviceID,
	).Identity()
	handle := protocol.ResultPackageHandle{
		Manifest: bridgeResultManifest(), Availability: protocol.ResultPackageUnverified,
		Sequence: 1, DeliveredAt: 2,
	}
	provider := &fakeResultPackageAvailabilityProvider{availability: protocol.ResultPackageAvailable}
	payload := encodeBridgeWaitResult(t, protocol.WaitAgentResult{
		Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
		Artifacts: []protocol.ChangesArtifactMetadata{},
		Results:   []protocol.ResultPackageHandle{handle, handle}, NextResultCursor: 1,
	})
	_, rpcErr := (&Server{results: provider}).decorateAgentWait(
		context.Background(), bridgeResultWaitRequest(root), payload,
	)
	if rpcErr == nil || rpcErr.Code != protocol.ErrorInternal || len(provider.lookups) != 0 {
		t.Fatalf("oversized result page = %#v, lookups %#v", rpcErr, provider.lookups)
	}
}

func bridgeResultManifest() protocol.ResultManifest {
	return protocol.ResultManifest{
		Version: protocol.ResultManifestVersion, PackageID: bridgeResultPackageID,
		ControllerID: bridgeTestControllerID, TreeID: bridgeTestTreeID,
		SourceAgentID: bridgeResultWorkerID, SourceDeviceID: bridgeTestDeviceID,
		ManagedThreadID: bridgeResultThreadID, TurnID: bridgeResultTurnID,
		LifecycleRevision: 1,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        1,
		Rollout: protocol.ResultRolloutComponent{
			Status: protocol.ResultRolloutAvailable, RawSize: 1,
			RawSHA256: strings.Repeat("a", 64),
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status:       protocol.ResultWorkspaceNotManaged,
			BaseWarnings: []string{}, ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{{
			Kind: protocol.ResultPackagePartRollout, Size: 1, SHA256: strings.Repeat("b", 64),
		}},
	}
}

func bridgeResultWaitRequest(root control.PrincipalIdentity) request {
	return request{
		Version: Version, RequestID: "l_123e4567-e89b-42d3-a456-426614174315",
		Method: protocol.MethodWaitAgent, TreeID: root.TreeID, Source: &root,
		Payload: json.RawMessage(`{}`),
	}
}

func encodeBridgeWaitResult(t *testing.T, result protocol.WaitAgentResult) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
