package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
	delegationcredential "github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/store"
)

func TestCredentialUsageListsIssueAndRevoke(t *testing.T) {
	_, stderr, code := runCredentialTestCommand([]string{"credential"})
	if code != exitUsage || !strings.Contains(stderr, "<issue|revoke>") {
		t.Fatalf("credential usage code = %d, stderr = %q", code, stderr)
	}
}

func TestCredentialRevokeIsDurableAndIdempotent(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	tokenPath := filepath.Join(t.TempDir(), "device.token")
	if _, stderr, code := runCredentialTestCommand(
		credentialIssueArgs(environment, credentialTestDeviceID, tokenPath),
	); code != 0 {
		t.Fatalf("issue code = %d, stderr = %q", code, stderr)
	}
	before, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	master := mustReadToken(t, environment.masterPath)
	deviceToken := mustReadToken(t, tokenPath)
	mac := delegationcredential.MAC(master, deviceToken)

	stdout, stderr, code := runCredentialTestCommand(credentialRevokeArgs(environment))
	if code != 0 {
		t.Fatalf("revoke code = %d, stderr = %q", code, stderr)
	}
	result := decodeCredentialResult(t, stdout)
	if result.Action != "revoked" || result.DeviceID != credentialTestDeviceID || result.TokenFile != "" {
		t.Fatalf("revoke result = %#v", result)
	}
	assertTokenNotExposed(t, stdout+stderr, tokenPath)
	after, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("revoke changed the device token file")
	}
	registry := openCredentialTestStore(t, environment.statePath)
	if _, err := registry.AuthenticateCredential(context.Background(), mac); !errors.Is(err, store.ErrCredentialDisabled) {
		t.Fatalf("revoked authentication error = %v, want ErrCredentialDisabled", err)
	}
	stored, err := registry.Credential(context.Background(), credentialTestControllerID, credentialTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Disabled || stored.Pending {
		t.Fatalf("revoked credential state = %#v", stored)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	if _, stderr, code := runCredentialTestCommand(credentialRevokeArgs(environment)); code != 0 {
		t.Fatalf("idempotent revoke code = %d, stderr = %q", code, stderr)
	}
}

func TestCredentialRevokeRejectsPendingEnrollment(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	registry := openCredentialTestStore(t, environment.statePath)
	pending := store.NewCredential(
		credentialTestControllerID,
		credentialTestDeviceID,
		control.DeviceRoleWorker,
		store.CredentialMAC{1},
		credentialNow(),
	)
	pending.Disabled = true
	pending.Pending = true
	if err := registry.CreateCredential(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	_, stderr, code := runCredentialTestCommand(credentialRevokeArgs(environment))
	if code == 0 || !strings.Contains(stderr, "pending credential enrollment cannot be revoked") {
		t.Fatalf("pending revoke code = %d, stderr = %q", code, stderr)
	}
	registry = openCredentialTestStore(t, environment.statePath)
	defer registry.Close()
	stored, err := registry.Credential(context.Background(), credentialTestControllerID, credentialTestDeviceID)
	if err != nil || stored != pending {
		t.Fatalf("pending credential after revoke = %#v, error %v", stored, err)
	}
}

func TestCredentialRevokeReportsCommittedOutputFailure(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	tokenPath := filepath.Join(t.TempDir(), "device.token")
	if _, stderr, code := runCredentialTestCommand(
		credentialIssueArgs(environment, credentialTestDeviceID, tokenPath),
	); code != 0 {
		t.Fatalf("issue code = %d, stderr = %q", code, stderr)
	}
	var stderr bytes.Buffer
	code := Run(credentialRevokeArgs(environment), failingCredentialWriter{}, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "credential state was committed") {
		t.Fatalf("output failure code = %d, stderr = %q", code, stderr.String())
	}
	master := mustReadToken(t, environment.masterPath)
	deviceToken := mustReadToken(t, tokenPath)
	registry := openCredentialTestStore(t, environment.statePath)
	defer registry.Close()
	if _, err := registry.AuthenticateCredential(
		context.Background(), delegationcredential.MAC(master, deviceToken),
	); !errors.Is(err, store.ErrCredentialDisabled) {
		t.Fatalf("credential after output failure = %v, want ErrCredentialDisabled", err)
	}
}

type failingCredentialWriter struct{}

func (failingCredentialWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected output failure")
}

func credentialRevokeArgs(environment credentialTestEnvironment) []string {
	return []string{
		"credential", "revoke",
		"--config", environment.configPath,
		"--device-id", credentialTestDeviceID,
		"--json",
	}
}
