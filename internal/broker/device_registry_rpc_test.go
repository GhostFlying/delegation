package broker

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/coder/websocket"
)

const (
	brokerTestThirdDeviceID  = "123e4567-e89b-42d3-a456-426614174104"
	brokerTestFourthDeviceID = "123e4567-e89b-42d3-a456-426614174105"
	brokerTestMissingID      = "123e4567-e89b-42d3-a456-426614174109"
	brokerTestForgedAgentID  = "123e4567-e89b-42d3-a456-426614174110"
	brokerTestForgedTreeID   = "123e4567-e89b-42d3-a456-426614174111"
)

type deviceRPCFaultRegistry struct {
	Registry
	authorizeErr  error
	listErr       error
	describeErr   error
	authorizeCall atomic.Int64
	listCall      atomic.Int64
	describeCall  atomic.Int64
}

func (r *deviceRPCFaultRegistry) AuthorizePrincipal(
	ctx context.Context,
	claimed control.PrincipalIdentity,
	required control.Capability,
) (control.Principal, error) {
	r.authorizeCall.Add(1)
	if r.authorizeErr != nil {
		return control.Principal{}, r.authorizeErr
	}
	return r.Registry.AuthorizePrincipal(ctx, claimed, required)
}

func (r *deviceRPCFaultRegistry) ListDevices(
	ctx context.Context,
	controllerID string,
	request store.DevicePageRequest,
) (store.DevicePage, error) {
	r.listCall.Add(1)
	if r.listErr != nil {
		return store.DevicePage{}, r.listErr
	}
	return r.Registry.ListDevices(ctx, controllerID, request)
}

func (r *deviceRPCFaultRegistry) DescribeDevice(
	ctx context.Context,
	controllerID, deviceID string,
) (store.DeviceRecord, error) {
	r.describeCall.Add(1)
	if r.describeErr != nil {
		return store.DeviceRecord{}, r.describeErr
	}
	return r.Registry.DescribeDevice(ctx, controllerID, deviceID)
}

func TestAuthorizedDeviceRegistryRPCs(t *testing.T) {
	harness := newBrokerHarnessForRole(t, config.AuthModeToken, time.Second, control.DeviceRoleController)
	connection, _, err := dialBroker(harness, &harness.deviceToken)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, connection, control.DeviceRoleController)
	principal := ensureRootPrincipal(t, connection)
	registerBrokerTestDevice(t, harness.registry, brokerTestSecondDeviceID)
	registerBrokerTestDevice(t, harness.registry, brokerTestThirdDeviceID)

	firstResponse := writeAndRead(t, connection, principalRequest(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 2}, principal,
	))
	if firstResponse.Error != nil {
		t.Fatalf("first device page error = %#v", firstResponse.Error)
	}
	first := decodeResult[protocol.ListDevicesResult](t, firstResponse)
	if first.Revision != 3 || len(first.Devices) != 2 ||
		first.Devices[0].DeviceID != brokerTestDeviceID ||
		first.Devices[1].DeviceID != brokerTestSecondDeviceID ||
		first.NextCursor != brokerTestSecondDeviceID {
		t.Fatalf("first device page = %#v", first)
	}

	secondResponse := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodListDevices,
		protocol.ListDevicesParams{
			AfterDeviceID: first.NextCursor, Limit: 2, ExpectedRevision: &first.Revision,
		},
		principal,
	))
	if secondResponse.Error != nil {
		t.Fatalf("second device page error = %#v", secondResponse.Error)
	}
	second := decodeResult[protocol.ListDevicesResult](t, secondResponse)
	if second.Revision != first.Revision || len(second.Devices) != 1 ||
		second.Devices[0].DeviceID != brokerTestThirdDeviceID || second.NextCursor != "" {
		t.Fatalf("second device page = %#v", second)
	}

	describeResponse := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodDescribeDevice,
		protocol.DescribeDeviceParams{DeviceID: brokerTestThirdDeviceID},
		principal,
	))
	if describeResponse.Error != nil {
		t.Fatalf("describe device error = %#v", describeResponse.Error)
	}
	described := decodeResult[protocol.DescribeDeviceResult](t, describeResponse)
	if described.Revision != first.Revision || !reflect.DeepEqual(described.Device, second.Devices[0]) {
		t.Fatalf("described device = %#v, want %#v", described, second.Devices[0])
	}

	registerBrokerTestDevice(t, harness.registry, brokerTestFourthDeviceID)
	stale := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodListDevices,
		protocol.ListDevicesParams{
			AfterDeviceID: first.NextCursor, Limit: 2, ExpectedRevision: &first.Revision,
		},
		principal,
	))
	if stale.Error == nil || stale.Error.Code != protocol.ErrorConflict {
		t.Fatalf("stale device page response = %#v", stale)
	}
	missing := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodDescribeDevice,
		protocol.DescribeDeviceParams{DeviceID: brokerTestMissingID},
		principal,
	))
	if missing.Error == nil || missing.Error.Code != protocol.ErrorNotFound {
		t.Fatalf("missing device response = %#v", missing)
	}
	invalid := writeAndRead(t, connection, principalRequest(
		t,
		protocol.MethodListDevices,
		protocol.ListDevicesParams{AfterDeviceID: first.NextCursor, Limit: 2},
		principal,
	))
	if invalid.Error == nil || invalid.Error.Code != protocol.ErrorInvalidParams {
		t.Fatalf("unbound device cursor response = %#v", invalid)
	}
}

func TestDeviceRegistryRPCRejectsAuthorizationBypass(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	connection, _, err := dialBroker(harness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, connection, control.DeviceRoleController)
	principal := ensureRootPrincipal(t, connection)
	wrapped := &deviceRPCFaultRegistry{Registry: harness.registry}
	harness.server.registry = wrapped

	missingSource := writeAndRead(t, connection, request(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 10},
	))
	if missingSource.Error == nil || missingSource.Error.Code != protocol.ErrorInvalidRequest {
		t.Fatalf("missing source response = %#v", missingSource)
	}

	mismatchedDevice := principal.Identity()
	mismatchedDevice.DeviceID = brokerTestSecondDeviceID
	mismatch := writeAndRead(t, connection, identityRequest(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 10}, mismatchedDevice,
	))
	if mismatch.Error == nil || mismatch.Error.Code != protocol.ErrorForbidden ||
		wrapped.authorizeCall.Load() != 0 || wrapped.listCall.Load() != 0 {
		t.Fatalf("connection device mismatch = %#v, authorize %d, list %d", mismatch, wrapped.authorizeCall.Load(), wrapped.listCall.Load())
	}

	forgedAgent := principal.Identity()
	forgedAgent.AgentID = brokerTestForgedAgentID
	forged := writeAndRead(t, connection, identityRequest(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 10}, forgedAgent,
	))
	if forged.Error == nil || forged.Error.Code != protocol.ErrorForbidden || wrapped.listCall.Load() != 0 {
		t.Fatalf("forged agent response = %#v, list %d", forged, wrapped.listCall.Load())
	}

	forgedTree := principal.Identity()
	forgedTree.TreeID = brokerTestForgedTreeID
	crossTree := writeAndRead(t, connection, identityRequest(
		t, protocol.MethodDescribeDevice, protocol.DescribeDeviceParams{DeviceID: brokerTestDeviceID}, forgedTree,
	))
	if crossTree.Error == nil || crossTree.Error.Code != protocol.ErrorForbidden || wrapped.describeCall.Load() != 0 {
		t.Fatalf("cross-tree response = %#v, describe %d", crossTree, wrapped.describeCall.Load())
	}

	wrapped.authorizeErr = store.ErrAuthorizationDenied
	denied := writeAndRead(t, connection, principalRequest(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 10}, principal,
	))
	if denied.Error == nil || denied.Error.Code != protocol.ErrorForbidden || wrapped.listCall.Load() != 0 {
		t.Fatalf("capability denial response = %#v, list %d", denied, wrapped.listCall.Load())
	}

	workerHarness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	worker, _, err := dialBroker(workerHarness, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close(websocket.StatusNormalClosure, "done")
	sendHello(t, worker, control.DeviceRoleWorker)
	workerPrincipal := control.NewRootPrincipal(
		brokerTestControllerID, brokerTestForgedTreeID, brokerTestForgedAgentID, brokerTestDeviceID,
	)
	workerResponse := writeAndRead(t, worker, principalRequest(
		t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: 10}, workerPrincipal,
	))
	if workerResponse.Error == nil || workerResponse.Error.Code != protocol.ErrorForbidden {
		t.Fatalf("worker device list response = %#v", workerResponse)
	}
}

func TestDeviceRegistryRPCReportsUnexpectedStoreFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		fault  func(*deviceRPCFaultRegistry, error)
	}{
		{name: "authorization", method: protocol.MethodListDevices, fault: func(registry *deviceRPCFaultRegistry, err error) {
			registry.authorizeErr = err
		}},
		{name: "list", method: protocol.MethodListDevices, fault: func(registry *deviceRPCFaultRegistry, err error) {
			registry.listErr = err
		}},
		{name: "describe", method: protocol.MethodDescribeDevice, fault: func(registry *deviceRPCFaultRegistry, err error) {
			registry.describeErr = err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
			connection, _, err := dialBroker(harness, nil)
			if err != nil {
				t.Fatal(err)
			}
			sendHello(t, connection, control.DeviceRoleController)
			principal := ensureRootPrincipal(t, connection)
			failure := errors.New(test.name + " database failed")
			wrapped := &deviceRPCFaultRegistry{Registry: harness.registry}
			test.fault(wrapped, failure)
			harness.server.registry = wrapped
			payload := any(protocol.ListDevicesParams{Limit: 10})
			if test.method == protocol.MethodDescribeDevice {
				payload = protocol.DescribeDeviceParams{DeviceID: brokerTestDeviceID}
			}
			response := writeAndRead(t, connection, principalRequest(t, test.method, payload, principal))
			if response.Error == nil || response.Error.Code != protocol.ErrorUnavailable {
				t.Fatalf("unexpected store failure response = %#v", response)
			}
			expectReported(t, harness.reported, failure)
		})
	}
}

func ensureRootPrincipal(t *testing.T, connection *websocket.Conn) control.Principal {
	t.Helper()
	response := writeAndRead(t, connection, request(
		t, protocol.MethodEnsureRootTree, protocol.EnsureRootTreeParams{ExternalThreadID: brokerTestThreadID},
	))
	if response.Error != nil {
		t.Fatalf("ensure root tree error = %#v", response.Error)
	}
	return decodeResult[protocol.EnsureRootTreeResult](t, response).Principal
}

func principalRequest(
	t *testing.T,
	method string,
	payload any,
	principal control.Principal,
) protocol.Envelope {
	t.Helper()
	return identityRequest(t, method, payload, principal.Identity())
}

func identityRequest(
	t *testing.T,
	method string,
	payload any,
	identity control.PrincipalIdentity,
) protocol.Envelope {
	t.Helper()
	request := request(t, method, payload)
	request.TreeID = identity.TreeID
	request.Source = &identity
	return request
}

func registerBrokerTestDevice(t *testing.T, registry *store.Store, deviceID string) {
	t.Helper()
	descriptor := hello(control.DeviceRoleWorker).Descriptor()
	descriptor.DeviceID = deviceID
	descriptor.Name = deviceID
	if _, err := registry.RegisterTrustedDevice(context.Background(), descriptor, time.Unix(20, 0)); err != nil {
		t.Fatal(err)
	}
}
