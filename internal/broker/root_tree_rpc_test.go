package broker

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	brokerTestThreadID       = "123e4567-e89b-42d3-a456-426614174102"
	brokerTestSecondDeviceID = "123e4567-e89b-42d3-a456-426614174103"
)

type rootTreeFaultRegistry struct {
	Registry
	err error
}

func (r *rootTreeFaultRegistry) EnsureRootTree(
	context.Context,
	string,
	string,
	string,
	time.Time,
) (control.Tree, control.Principal, error) {
	return control.Tree{}, control.Principal{}, r.err
}

func TestControllerEnsuresIdempotentRootTree(t *testing.T) {
	harness := newBrokerHarnessForRole(t, config.AuthModeToken, time.Second, control.DeviceRoleController)
	connection, _, err := dialBroker(harness, &harness.deviceToken)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "done")
	helloResult := sendHello(t, connection, control.DeviceRoleController)
	if !slices.Contains(helloResult.Features, protocol.FeatureRootTree) {
		t.Fatalf("broker features = %v", helloResult.Features)
	}

	rootRequest := request(t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{
		ExternalThreadID: brokerTestThreadID,
	})
	response := writeAndRead(t, connection, rootRequest)
	if response.Error != nil {
		t.Fatalf("ensure root tree error = %#v", response.Error)
	}
	result := decodeResult[protocol.EnsureRootTreeResult](t, response)
	if result.Tree.ExternalThreadID != brokerTestThreadID ||
		result.Tree.RootDeviceID != brokerTestDeviceID ||
		!result.Principal.Matches(treeRootIdentity(result.Tree)) {
		t.Fatalf("root tree result = %#v", result)
	}
	if !result.Principal.Has(control.CapabilityDeviceRead) {
		t.Fatalf("root principal capabilities = %v", result.Principal.Capabilities)
	}

	repeated := writeAndRead(t, connection, request(t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{
		ExternalThreadID: brokerTestThreadID,
	}))
	if repeated.Error != nil {
		t.Fatalf("repeated root tree error = %#v", repeated.Error)
	}
	if got := decodeResult[protocol.EnsureRootTreeResult](t, repeated); !reflect.DeepEqual(got, result) {
		t.Fatalf("repeated root tree = %#v, want %#v", got, result)
	}
	authorized, err := harness.registry.AuthorizePrincipal(
		context.Background(), result.Principal.Identity(), control.CapabilityDeviceRead,
	)
	if err != nil || !reflect.DeepEqual(authorized, result.Principal) {
		t.Fatalf("stored root principal = %#v, error %v", authorized, err)
	}
}

func TestRootTreeRPCRejectsWorkerInvalidShapeAndRebind(t *testing.T) {
	t.Run("worker", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		sendHello(t, connection, control.DeviceRoleWorker)
		response := writeAndRead(t, connection, request(
			t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: brokerTestThreadID},
		))
		if response.Error == nil || response.Error.Code != protocol.ErrorForbidden {
			t.Fatalf("worker root tree response = %#v", response)
		}
	})

	t.Run("shape and payload", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		sendHello(t, connection, control.DeviceRoleController)
		invalidShape := request(
			t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: brokerTestThreadID},
		)
		invalidShape.TreeID = brokerTestThreadID
		response := writeAndRead(t, connection, invalidShape)
		if response.Error == nil || response.Error.Code != protocol.ErrorInvalidRequest {
			t.Fatalf("invalid root tree shape response = %#v", response)
		}
		response = writeAndRead(t, connection, request(
			t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: "not-a-uuid"},
		))
		if response.Error == nil || response.Error.Code != protocol.ErrorInvalidParams {
			t.Fatalf("invalid root tree payload response = %#v", response)
		}
	})

	t.Run("rebind", func(t *testing.T) {
		harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
		first, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close(websocket.StatusNormalClosure, "done")
		sendHello(t, first, control.DeviceRoleController)
		rootRequest := request(
			t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: brokerTestThreadID},
		)
		if response := writeAndRead(t, first, rootRequest); response.Error != nil {
			t.Fatalf("initial root tree response = %#v", response)
		}

		second, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Close(websocket.StatusNormalClosure, "done")
		secondHello := helloRequest(t, control.DeviceRoleController)
		secondPayload := hello(control.DeviceRoleController)
		secondPayload.DeviceID = brokerTestSecondDeviceID
		secondHello.Payload = marshalPayload(t, secondPayload)
		if response := writeAndRead(t, second, secondHello); response.Error != nil {
			t.Fatalf("second hello response = %#v", response)
		}
		response := writeAndRead(t, second, rootRequest)
		if response.Error == nil || response.Error.Code != protocol.ErrorConflict {
			t.Fatalf("root rebind response = %#v", response)
		}
	})
}

func TestRootTreeRPCReportsUnexpectedStoreFailure(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	sendHello(t, connection, control.DeviceRoleController)
	failure := errors.New("root tree database failed")
	harness.server.registry = &rootTreeFaultRegistry{Registry: harness.registry, err: failure}
	response := writeAndRead(t, connection, request(
		t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: brokerTestThreadID},
	))
	if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable {
		t.Fatalf("root tree store failure response = %#v", response)
	}
	expectReported(t, harness.reported, failure)
}

func treeRootIdentity(tree control.Tree) control.PrincipalIdentity {
	return control.PrincipalIdentity{
		ControllerID: tree.ControllerID,
		TreeID:       tree.TreeID,
		AgentID:      tree.RootAgentID,
		DeviceID:     tree.RootDeviceID,
	}
}
