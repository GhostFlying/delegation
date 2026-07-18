package store

import (
	"context"
	"errors"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
)

const (
	testControllerID = "123e4567-e89b-42d3-a456-426614174000"
	testDeviceID     = "123e4567-e89b-42d3-a456-426614174001"
)

func TestCredentialBindingAndDisable(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	firstMAC := CredentialMAC{1}
	first := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, firstMAC, testTime())
	if err := store.CreateCredential(ctx, first); err != nil {
		t.Fatal(err)
	}
	got, err := store.AuthenticateCredential(ctx, firstMAC)
	if err != nil {
		t.Fatal(err)
	}
	if got != first {
		t.Fatalf("AuthenticateCredential() = %#v, want %#v", got, first)
	}

	controller := NewCredential(testControllerID, testDeviceID, control.DeviceRoleController, CredentialMAC{2}, testTime())
	if err := store.CreateCredential(ctx, controller); !errors.Is(err, ErrConflict) {
		t.Fatalf("role-changing duplicate error = %v, want ErrConflict", err)
	}
	if got, err := store.AuthenticateCredential(ctx, firstMAC); err != nil || got != first {
		t.Fatalf("credential after rejected role change = %#v, %v; want %#v", got, err, first)
	}
	if err := store.DisableCredential(ctx, testControllerID, testDeviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateCredential(ctx, firstMAC); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("disabled credential error = %v, want ErrCredentialDisabled", err)
	}
}

func TestCredentialMACIsUniqueAcrossDevices(t *testing.T) {
	store := openTestStore(t)
	mac := CredentialMAC{7}
	first := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, testTime())
	if err := store.CreateCredential(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := NewCredential(
		testControllerID,
		"123e4567-e89b-42d3-a456-426614174002",
		control.DeviceRoleWorker,
		mac,
		testTime(),
	)
	if err := store.CreateCredential(context.Background(), second); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate token MAC error = %v, want ErrConflict", err)
	}
}

func TestPendingCredentialCanBeActivatedOrRemovedExactly(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	mac := CredentialMAC{3}
	pending := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, testTime())
	pending.Disabled = true
	pending.Pending = true
	if err := store.CreateCredential(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateCredential(ctx, mac); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("pending authentication error = %v", err)
	}
	if got, err := store.Credential(ctx, testControllerID, testDeviceID); err != nil || got != pending {
		t.Fatalf("Credential() = %#v, %v; want %#v", got, err, pending)
	}
	if err := store.ActivateCredential(ctx, testControllerID, testDeviceID, CredentialMAC{9}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-MAC activation error = %v, want ErrNotFound", err)
	}
	if err := store.ActivateCredential(ctx, testControllerID, testDeviceID, mac); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateCredential(ctx, testControllerID, testDeviceID, mac); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
	active := pending
	active.Disabled = false
	active.Pending = false
	if got, err := store.AuthenticateCredential(ctx, mac); err != nil || got != active {
		t.Fatalf("active credential = %#v, %v; want %#v", got, err, active)
	}
	if err := store.DeletePendingCredential(ctx, testControllerID, testDeviceID, mac); !errors.Is(err, ErrNotFound) {
		t.Fatalf("active credential deletion error = %v, want ErrNotFound", err)
	}

	otherDevice := "123e4567-e89b-42d3-a456-426614174003"
	otherMAC := CredentialMAC{4}
	other := NewCredential(testControllerID, otherDevice, control.DeviceRoleWorker, otherMAC, testTime())
	other.Disabled = true
	other.Pending = true
	if err := store.CreateCredential(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := store.DeletePendingCredential(ctx, testControllerID, otherDevice, otherMAC); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Credential(ctx, testControllerID, otherDevice); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted pending credential error = %v, want ErrNotFound", err)
	}
}

func TestRevokedCredentialCannotBeActivatedOrDeletedAsPending(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	mac := CredentialMAC{8}
	credential := NewCredential(testControllerID, testDeviceID, control.DeviceRoleWorker, mac, testTime())
	if err := store.CreateCredential(ctx, credential); err != nil {
		t.Fatal(err)
	}
	if err := store.DisableCredential(ctx, testControllerID, testDeviceID); err != nil {
		t.Fatal(err)
	}
	if err := store.ActivateCredential(ctx, testControllerID, testDeviceID, mac); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked activation error = %v, want ErrNotFound", err)
	}
	if err := store.DeletePendingCredential(ctx, testControllerID, testDeviceID, mac); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revoked pending deletion error = %v, want ErrNotFound", err)
	}
	stored, err := store.Credential(ctx, testControllerID, testDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	want := credential
	want.Disabled = true
	if stored != want {
		t.Fatalf("revoked credential = %#v, want %#v", stored, want)
	}
}
