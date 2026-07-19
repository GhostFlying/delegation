package store

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
)

const (
	deviceSecondControllerID = "123e4567-e89b-42d3-a456-426614174030"
	deviceSecondID           = "123e4567-e89b-42d3-a456-426614174031"
)

func TestAuthenticatedDeviceLeaseAndRevocation(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	mac := CredentialMAC{1}
	credential := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, time.Unix(1, 0))
	if err := registry.CreateCredential(ctx, credential); err != nil {
		t.Fatal(err)
	}
	descriptor := deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker)
	device, err := registry.RegisterAuthenticatedDevice(ctx, mac, descriptor, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !device.Online || device.Revision != 1 || device.LastSeenAt != 100 {
		t.Fatalf("first device lease = %#v", device)
	}
	device, err = registry.RegisterAuthenticatedDevice(ctx, mac, descriptor, time.Unix(90, 0))
	if err != nil {
		t.Fatal(err)
	}
	if device.Revision != 2 || device.LastSeenAt != 90 {
		t.Fatalf("reconnected device lease = %#v", device)
	}
	device, err = registry.HeartbeatDevice(ctx, testControllerID, testDeviceID, 2, time.Unix(90, 0))
	if err != nil || device.Revision != 2 {
		t.Fatalf("idempotent heartbeat = %#v, error %v", device, err)
	}
	device, err = registry.HeartbeatDevice(ctx, testControllerID, testDeviceID, 2, time.Unix(110, 0))
	if err != nil || device.Revision != 2 || device.LastSeenAt != 110 {
		t.Fatalf("new heartbeat = %#v, error %v", device, err)
	}
	if _, err := registry.MarkDeviceOffline(
		ctx, testControllerID, testDeviceID, 1, time.Unix(120, 0),
	); !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale disconnect error = %v, want ErrStaleRevision", err)
	}
	device, err = registry.MarkDeviceOffline(ctx, testControllerID, testDeviceID, 2, time.Unix(120, 0))
	if err != nil || device.Online || device.Revision != 3 || device.LastSeenAt != 120 {
		t.Fatalf("offline device = %#v, error %v", device, err)
	}
	device, err = registry.MarkDeviceOffline(ctx, testControllerID, testDeviceID, 3, time.Unix(130, 0))
	if err != nil || device.Revision != 3 || device.LastSeenAt != 120 {
		t.Fatalf("idempotent disconnect = %#v, error %v", device, err)
	}
	if _, err := registry.HeartbeatDevice(
		ctx, testControllerID, testDeviceID, 3, time.Unix(130, 0),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("offline heartbeat error = %v, want ErrConflict", err)
	}
	device, err = registry.RegisterAuthenticatedDevice(ctx, mac, descriptor, time.Unix(115, 0))
	if err != nil || !device.Online || device.Revision != 4 || device.LastSeenAt != 115 {
		t.Fatalf("renewed device lease = %#v, error %v", device, err)
	}
	if err := registry.DisableCredential(ctx, testControllerID, testDeviceID); err != nil {
		t.Fatal(err)
	}
	device, err = queryDevice(ctx, registry.db, testControllerID, testDeviceID)
	if err != nil || device.Online || device.Revision != 5 {
		t.Fatalf("revoked device = %#v, error %v", device, err)
	}
	if _, err := registry.AuthenticateCredential(ctx, mac); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("revoked authentication error = %v, want ErrCredentialDisabled", err)
	}
}

func TestDeviceRegistrationEnforcesCredentialBinding(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	mac := CredentialMAC{2}
	credential := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, time.Unix(1, 0))
	if err := registry.CreateCredential(ctx, credential); err != nil {
		t.Fatal(err)
	}
	wrongRole := deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController)
	if _, err := registry.RegisterAuthenticatedDevice(
		ctx, mac, wrongRole, time.Unix(1, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("role escalation error = %v, want authorization denial", err)
	}
	wrongDevice := deviceDescriptor(testControllerID, deviceSecondID, control.DeviceRoleWorker)
	if _, err := registry.RegisterAuthenticatedDevice(
		ctx, mac, wrongDevice, time.Unix(1, 0),
	); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("device impersonation error = %v, want authorization denial", err)
	}
	trusted := deviceDescriptor(deviceSecondControllerID, deviceSecondID, control.DeviceRoleController)
	if device, err := registry.RegisterTrustedDevice(ctx, trusted, time.Unix(1, 0)); err != nil || device.Revision != 1 {
		t.Fatalf("trusted registration = %#v, error %v", device, err)
	}
	if err := registry.DisableCredential(ctx, testControllerID, testDeviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterAuthenticatedDevice(
		ctx, mac, deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker), time.Unix(2, 0),
	); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("revoked registration error = %v, want ErrCredentialDisabled", err)
	}
}

func TestBrokerEpochExpiryAndControllerIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleController)
	second := deviceDescriptor(testControllerID, deviceSecondID, control.DeviceRoleWorker)
	other := deviceDescriptor(deviceSecondControllerID, testDeviceID, control.DeviceRoleController)
	for index, descriptor := range []control.DeviceDescriptor{first, second, other} {
		if _, err := registry.RegisterTrustedDevice(ctx, descriptor, time.Unix(int64(index+1)*10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if persisted, err := queryDevice(ctx, registry.db, testControllerID, testDeviceID); err != nil || !persisted.Online {
		t.Fatalf("device after Store.Open = %#v, error %v", persisted, err)
	}
	transition, err := registry.BeginBrokerEpoch(ctx, testControllerID)
	if err != nil || transition != (PresenceTransition{Revision: 3, Count: 2}) {
		t.Fatalf("broker epoch = %#v, error %v", transition, err)
	}
	for _, deviceID := range []string{testDeviceID, deviceSecondID} {
		device, err := queryDevice(ctx, registry.db, testControllerID, deviceID)
		if err != nil || device.Online || device.Revision != 3 {
			t.Fatalf("epoch device %s = %#v, error %v", deviceID, device, err)
		}
	}
	if device, err := queryDevice(ctx, registry.db, deviceSecondControllerID, testDeviceID); err != nil ||
		!device.Online || device.Revision != 1 {
		t.Fatalf("other controller device = %#v, error %v", device, err)
	}
	transition, err = registry.BeginBrokerEpoch(ctx, testControllerID)
	if err != nil || transition != (PresenceTransition{Revision: 3}) {
		t.Fatalf("empty broker epoch = %#v, error %v", transition, err)
	}
	if _, err := registry.RegisterTrustedDevice(ctx, first, time.Unix(40, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterTrustedDevice(ctx, second, time.Unix(50, 0)); err != nil {
		t.Fatal(err)
	}
	transition, err = registry.ExpireDevices(ctx, testControllerID, time.Unix(45, 0))
	if err != nil || transition != (PresenceTransition{Revision: 6, Count: 1}) {
		t.Fatalf("device expiry = %#v, error %v", transition, err)
	}
	firstDevice, _ := queryDevice(ctx, registry.db, testControllerID, testDeviceID)
	secondDevice, _ := queryDevice(ctx, registry.db, testControllerID, deviceSecondID)
	if firstDevice.Online || firstDevice.Revision != 6 || !secondDevice.Online || secondDevice.Revision != 5 {
		t.Fatalf("expired devices = %#v, %#v", firstDevice, secondDevice)
	}
}

func TestRegistrationAndRevocationAreSerialized(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	mac := CredentialMAC{3}
	if err := registry.CreateCredential(
		ctx, NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, time.Unix(1, 0)),
	); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var registerErr error
	var revokeErr error
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, registerErr = registry.RegisterAuthenticatedDevice(
			ctx, mac, deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker), time.Unix(1, 0),
		)
	}()
	go func() {
		defer wait.Done()
		<-start
		revokeErr = registry.DisableCredential(ctx, testControllerID, testDeviceID)
	}()
	close(start)
	wait.Wait()
	if revokeErr != nil || registerErr != nil && !errors.Is(registerErr, ErrCredentialDisabled) {
		t.Fatalf("register/revoke errors = %v, %v", registerErr, revokeErr)
	}
	if _, err := registry.AuthenticateCredential(ctx, mac); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("final credential error = %v, want disabled", err)
	}
	if device, err := queryDevice(ctx, registry.db, testControllerID, testDeviceID); err == nil && device.Online {
		t.Fatalf("revoked device remained online: %#v", device)
	} else if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestDeviceRegistryRejectsOversizedStateAndRevisionExhaustion(t *testing.T) {
	registry := openTestStore(t)
	ctx := context.Background()
	if _, err := registry.RegisterTrustedDevice(
		ctx, deviceDescriptor(testControllerID, testDeviceID, control.DeviceRoleWorker), time.Unix(1, 0),
	); err != nil {
		t.Fatal(err)
	}
	for name, corrupted := range map[string]string{
		"ascii":      strings.Repeat("x", maximumFeaturesJSON+1),
		"multibyte":  strings.Repeat("界", maximumFeaturesJSON/3+1),
		"nul prefix": "\x00" + strings.Repeat("x", maximumFeaturesJSON+1),
	} {
		if _, err := registry.db.Exec(`
UPDATE devices SET features_json = ? WHERE controller_id = ? AND device_id = ?
`, corrupted, testControllerID, testDeviceID); err != nil {
			t.Fatal(err)
		}
		if _, err := queryDevice(ctx, registry.db, testControllerID, testDeviceID); err == nil ||
			!strings.Contains(err.Error(), "exceed size limit") {
			t.Fatalf("%s oversized stored features error = %v", name, err)
		}
	}
	if _, err := registry.db.Exec(`
UPDATE controller_registries SET revision = ? WHERE controller_id = ?
`, int64(math.MaxInt64), testControllerID); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterTrustedDevice(
		ctx, deviceDescriptor(testControllerID, deviceSecondID, control.DeviceRoleWorker), time.Unix(2, 0),
	); !errors.Is(err, ErrRevisionExhausted) {
		t.Fatalf("exhausted registration error = %v, want ErrRevisionExhausted", err)
	}
	if _, err := queryDevice(ctx, registry.db, testControllerID, deviceSecondID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("exhausted registration created a device: %v", err)
	}
}

func deviceDescriptor(controllerID, deviceID string, role control.DeviceRole) control.DeviceDescriptor {
	return control.DeviceDescriptor{
		ControllerID:    controllerID,
		DeviceID:        deviceID,
		Name:            "builder",
		Role:            role,
		OS:              "linux",
		Arch:            "amd64",
		RuntimeVersion:  "0.1.0-alpha.0.m1",
		ProtocolVersion: 1,
		Features:        []string{"deviceRegistryV1"},
	}
}
