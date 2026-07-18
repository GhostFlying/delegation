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
