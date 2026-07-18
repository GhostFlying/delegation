package cli

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	delegationcredential "github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

const (
	credentialTestControllerID = "123e4567-e89b-42d3-a456-426614174100"
	credentialTestDeviceID     = "123e4567-e89b-42d3-a456-426614174101"
)

type credentialTestEnvironment struct {
	configPath string
	statePath  string
	masterPath string
}

func TestCredentialIssueAndRecover(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	tokenPath := filepath.Join(t.TempDir(), "device.token")
	issueArgs := credentialIssueArgs(environment, credentialTestDeviceID, tokenPath)

	stdout, stderr, code := runCredentialTestCommand(issueArgs)
	if code != 0 {
		t.Fatalf("issue code = %d, want 0; stderr = %q", code, stderr)
	}
	result := decodeCredentialResult(t, stdout)
	if result.Action != "issued" || result.Recovered || result.DeviceID != credentialTestDeviceID {
		t.Fatalf("issue result = %#v", result)
	}
	if stderr != "" {
		t.Fatalf("issue stderr = %q, want empty", stderr)
	}

	master := mustReadToken(t, environment.masterPath)
	deviceToken := mustReadToken(t, tokenPath)
	if subtle.ConstantTimeCompare(master[:], deviceToken[:]) == 1 {
		t.Fatal("device token equals broker master token")
	}
	assertTokenNotExposed(t, stdout+stderr, tokenPath)
	mac := delegationcredential.MAC(master, deviceToken)
	registry := openCredentialTestStore(t, environment.statePath)
	authenticated, err := registry.AuthenticateCredential(context.Background(), mac)
	if err != nil {
		t.Fatal(err)
	}
	if authenticated.DeviceID != credentialTestDeviceID || authenticated.Role != control.DeviceRoleWorker {
		t.Fatalf("authenticated credential = %#v", authenticated)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code = runCredentialTestCommand(issueArgs)
	if code != 0 {
		t.Fatalf("recovery code = %d, want 0; stderr = %q", code, stderr)
	}
	result = decodeCredentialResult(t, stdout)
	if !result.Recovered {
		t.Fatalf("recovery result = %#v, want recovered", result)
	}
	after, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("recovery replaced the existing device token")
	}
}

func TestCredentialIssueRecoversCommittedPendingToken(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	tokenPath := filepath.Join(t.TempDir(), "pending.token")
	master := mustReadToken(t, environment.masterPath)
	deviceToken, err := tokenfile.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tokenfile.WriteNew(tokenPath, deviceToken); err != nil {
		t.Fatal(err)
	}
	registry := openCredentialTestStore(t, environment.statePath)
	pending := store.NewCredential(
		credentialTestControllerID,
		credentialTestDeviceID,
		control.DeviceRoleWorker,
		delegationcredential.MAC(master, deviceToken),
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

	stdout, stderr, code := runCredentialTestCommand(
		credentialIssueArgs(environment, credentialTestDeviceID, tokenPath),
	)
	if code != 0 {
		t.Fatalf("recovery code = %d, want 0; stderr = %q", code, stderr)
	}
	if result := decodeCredentialResult(t, stdout); !result.Recovered {
		t.Fatalf("recovery result = %#v, want recovered", result)
	}
	registry = openCredentialTestStore(t, environment.statePath)
	defer registry.Close()
	if _, err := registry.AuthenticateCredential(
		context.Background(), delegationcredential.MAC(master, deviceToken),
	); err != nil {
		t.Fatalf("authenticate recovered credential: %v", err)
	}
}

func TestConcurrentCredentialIssueForSameDeviceHasOneWinner(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	paths := []string{
		filepath.Join(t.TempDir(), "first.token"),
		filepath.Join(t.TempDir(), "second.token"),
	}
	type outcome struct {
		stdout string
		stderr string
		code   int
	}
	outcomes := make([]outcome, len(paths))
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index, path := range paths {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			outcomes[index].stdout, outcomes[index].stderr, outcomes[index].code = runCredentialTestCommand(
				credentialIssueArgs(environment, credentialTestDeviceID, path),
			)
		}()
	}
	close(start)
	wait.Wait()

	successes := 0
	files := 0
	for index, outcome := range outcomes {
		if outcome.code == 0 {
			successes++
		}
		if _, err := os.Lstat(paths[index]); err == nil {
			files++
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if successes != 1 || files != 1 {
		t.Fatalf("concurrent issue successes = %d, token files = %d; outcomes = %#v", successes, files, outcomes)
	}
}

func TestPendingCredentialRecoveryLease(t *testing.T) {
	fixedNow := time.Unix(2_000_000_000, 0).UTC()
	originalNow := credentialNow
	credentialNow = func() time.Time { return fixedNow }
	defer func() { credentialNow = originalNow }()

	cases := []struct {
		name        string
		age         time.Duration
		wantSuccess bool
	}{
		{name: "fresh", age: pendingCredentialRecoveryLease - time.Second},
		{name: "boundary", age: pendingCredentialRecoveryLease, wantSuccess: true},
		{name: "expired", age: pendingCredentialRecoveryLease + time.Second, wantSuccess: true},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			registry := openCredentialTestStore(t, filepath.Join(t.TempDir(), "broker.sqlite3"))
			defer registry.Close()
			master := tokenfile.Token{1}
			pending := store.NewCredential(
				credentialTestControllerID,
				credentialTestDeviceID,
				control.DeviceRoleWorker,
				store.CredentialMAC{byte(index + 1)},
				fixedNow.Add(-testCase.age),
			)
			pending.Disabled = true
			pending.Pending = true
			if err := registry.CreateCredential(context.Background(), pending); err != nil {
				t.Fatal(err)
			}
			tokenPath := filepath.Join(t.TempDir(), "device.token")
			_, committed, err := issueCredential(
				context.Background(), registry, master, credentialTestControllerID,
				credentialTestDeviceID, control.DeviceRoleWorker, tokenPath,
			)
			if !testCase.wantSuccess {
				if !errors.Is(err, store.ErrConflict) || committed {
					t.Fatalf("fresh pending result = committed %v, error %v", committed, err)
				}
				if stored, loadErr := registry.Credential(
					context.Background(), credentialTestControllerID, credentialTestDeviceID,
				); loadErr != nil || stored != pending {
					t.Fatalf("fresh pending changed to %#v, error %v", stored, loadErr)
				}
				if _, statErr := os.Lstat(tokenPath); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("fresh pending token exists: %v", statErr)
				}
				return
			}
			if err != nil || !committed {
				t.Fatalf("recover expired pending = committed %v, error %v", committed, err)
			}
			stored, loadErr := registry.Credential(
				context.Background(), credentialTestControllerID, credentialTestDeviceID,
			)
			if loadErr != nil || stored.Disabled || stored.Pending || stored.MAC == pending.MAC {
				t.Fatalf("reissued credential = %#v, error %v", stored, loadErr)
			}
			mustReadToken(t, tokenPath)
		})
	}
}

func TestCredentialIssueRejectsBrokerTokenAsOutput(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	before, err := os.ReadFile(environment.masterPath)
	if err != nil {
		t.Fatal(err)
	}
	_, stderr, code := runCredentialTestCommand(
		credentialIssueArgs(environment, credentialTestDeviceID, environment.masterPath),
	)
	if code == 0 || !strings.Contains(stderr, "conflicts with broker master token") {
		t.Fatalf("collision code = %d, stderr = %q", code, stderr)
	}
	after, err := os.ReadFile(environment.masterPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("collision check changed the broker master token")
	}
}

func TestCredentialIssueRequiresExplicitDeviceID(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	_, stderr, code := runCredentialTestCommand([]string{
		"credential", "issue",
		"--config", environment.configPath,
		"--role", "device",
		"--out", filepath.Join(t.TempDir(), "device.token"),
	})
	if code == 0 || !strings.Contains(stderr, "deviceId must be a UUID") {
		t.Fatalf("missing deviceId code = %d, stderr = %q", code, stderr)
	}
}

func TestCredentialIssueRequiresBrokerTokenAuthentication(t *testing.T) {
	environment := setupCredentialTestBroker(t, "none")
	_, stderr, code := runCredentialTestCommand(
		credentialIssueArgs(environment, credentialTestDeviceID, filepath.Join(t.TempDir(), "device.token")),
	)
	if code == 0 || !strings.Contains(stderr, "requires broker token authentication") {
		t.Fatalf("none-auth issue code = %d, stderr = %q", code, stderr)
	}
}

func TestCredentialStateSidecarsCannotAliasAuthorityFiles(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	statePath := filepath.Join(root, "broker.sqlite3")
	masterPath := filepath.Join(root, "master.token")
	if err := os.WriteFile(masterPath, []byte("authority"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rejectCredentialPathCollisions(statePath+"-journal", configPath, statePath, masterPath); err == nil {
		t.Fatal("rollback journal output path was not rejected")
	}
	if err := os.Link(masterPath, statePath+"-wal"); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	if err := rejectCredentialAuthorityPathCollisions(configPath, statePath, masterPath); err == nil {
		t.Fatal("broker state WAL hard link was not rejected")
	}
}

func setupCredentialTestBroker(t *testing.T, authMode string) credentialTestEnvironment {
	t.Helper()
	root := t.TempDir()
	environment := credentialTestEnvironment{
		configPath: filepath.Join(root, "config.json"),
		statePath:  filepath.Join(root, "state", "broker.sqlite3"),
		masterPath: filepath.Join(root, "secrets", "broker.token"),
	}
	args := []string{
		"setup", "broker",
		"--config", environment.configPath,
		"--controller-id", credentialTestControllerID,
		"--listen", "127.0.0.1:8787",
		"--state", environment.statePath,
		"--auth-mode", authMode,
		"--json",
	}
	if authMode == "token" {
		args = append(args, "--token-file", environment.masterPath)
	}
	_, stderr, code := runCredentialTestCommand(args)
	if code != 0 {
		t.Fatalf("setup broker code = %d, want 0; stderr = %q", code, stderr)
	}
	return environment
}

func credentialIssueArgs(environment credentialTestEnvironment, deviceID, tokenPath string) []string {
	return []string{
		"credential", "issue",
		"--config", environment.configPath,
		"--role", "device",
		"--device-id", deviceID,
		"--out", tokenPath,
		"--json",
	}
}

func TestCredentialUsesBrokerConfiguredState(t *testing.T) {
	environment := setupCredentialTestBroker(t, "token")
	otherHome := t.TempDir()
	t.Setenv("DELEGATION_HOME", otherHome)
	stdout, stderr, code := runCredentialTestCommand(credentialIssueArgs(
		environment,
		credentialTestDeviceID,
		filepath.Join(t.TempDir(), "device.token"),
	))
	if code != 0 {
		t.Fatalf("issue code = %d, stderr = %q", code, stderr)
	}
	if result := decodeCredentialResult(t, stdout); result.StatePath != environment.statePath {
		t.Fatalf("credential state path = %q, want %q", result.StatePath, environment.statePath)
	}
	if _, err := os.Stat(filepath.Join(otherHome, "state", "broker.sqlite3")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential opened environment-derived state: %v", err)
	}
}

func TestCredentialRejectsSchemaOneWithoutMutatingOldAuthority(t *testing.T) {
	oldHome := t.TempDir()
	t.Setenv("DELEGATION_HOME", oldHome)
	statePath := filepath.Join(oldHome, "state", "broker.sqlite3")
	registry := openCredentialTestStore(t, statePath)
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "custom", "config.json")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	masterPath := filepath.Join(oldHome, "secrets", "broker.token")
	if _, err := tokenfile.Ensure(masterPath); err != nil {
		t.Fatal(err)
	}
	oldConfig := delegationconfig.Config{
		SchemaVersion: 1,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  credentialTestControllerID,
		Broker: delegationconfig.BrokerConfig{
			Listen: "0.0.0.0:9876",
			Auth: delegationconfig.AuthConfig{
				Mode:      delegationconfig.AuthModeToken,
				TokenFile: masterPath,
			},
		},
	}
	configData, err := json.Marshal(oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	stateBefore, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	masterBefore, err := os.ReadFile(masterPath)
	if err != nil {
		t.Fatal(err)
	}
	outputToken := filepath.Join(t.TempDir(), "device.token")
	_, stderr, code := runCredentialTestCommand([]string{
		"credential", "issue",
		"--config", configPath,
		"--role", "device",
		"--device-id", credentialTestDeviceID,
		"--out", outputToken,
	})
	if code == 0 {
		t.Fatal("credential issue accepted a schema 1 broker config")
	}
	for _, text := range []string{"move the config aside", "--controller-id", "--listen", "--auth-mode", "--token-file", "--state"} {
		if !strings.Contains(stderr, text) {
			t.Fatalf("schema 1 credential error = %q, want %q", stderr, text)
		}
	}
	stateAfter, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	masterAfter, err := os.ReadFile(masterPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stateAfter, stateBefore) || !bytes.Equal(masterAfter, masterBefore) {
		t.Fatal("schema 1 rejection mutated the existing broker authority")
	}
	if _, err := os.Lstat(outputToken); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("schema 1 rejection created an output token: %v", err)
	}
	newDefaultState := filepath.Join(filepath.Dir(configPath), "state", "broker.sqlite3")
	if _, err := os.Lstat(newDefaultState); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("schema 1 rejection created a state database beside the custom config: %v", err)
	}
}

func runCredentialTestCommand(args []string) (string, string, int) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}

func decodeCredentialResult(t *testing.T, raw string) credentialResult {
	t.Helper()
	var result credentialResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatalf("decode credential result %q: %v", raw, err)
	}
	return result
}

func openCredentialTestStore(t *testing.T, path string) *store.Store {
	t.Helper()
	registry, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func mustReadToken(t *testing.T, path string) tokenfile.Token {
	t.Helper()
	token, err := tokenfile.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func assertTokenNotExposed(t *testing.T, output, tokenPath string) {
	t.Helper()
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output, strings.TrimSpace(string(raw))) {
		t.Fatal("command output exposed device token material")
	}
}
