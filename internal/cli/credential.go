package cli

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	delegationcredential "github.com/GhostFlying/delegation/internal/credential"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type credentialResult struct {
	Action       string             `json:"action"`
	Role         control.DeviceRole `json:"role"`
	ConfigPath   string             `json:"configPath"`
	StatePath    string             `json:"statePath"`
	ControllerID string             `json:"controllerId"`
	DeviceID     string             `json:"deviceId"`
	TokenFile    string             `json:"tokenFile,omitempty"`
	Recovered    bool               `json:"recovered,omitempty"`
}

const pendingCredentialRecoveryLease = 5 * time.Minute

var credentialNow = func() time.Time {
	return time.Now().UTC()
}

func runCredential(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation credential <issue|revoke> [options]")
		return exitUsage
	}
	switch args[0] {
	case "issue":
		return runCredentialIssue(args[1:], stdout, stderr)
	case "revoke":
		return runCredentialRevoke(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported credential action %q\n", args[0])
		return exitUsage
	}
}

func runCredentialIssue(args []string, stdout, stderr io.Writer) int {
	defaultConfig, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation credential issue", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfig, "broker configuration file path")
	roleName := flags.String("role", "", "credential role: controller or device")
	deviceID := flags.String("device-id", "", "stable device UUID")
	tokenPath := flags.String("out", "", "new device token file path")
	jsonOutput := flags.Bool("json", false, "print credential result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	role := control.DeviceRole(*roleName)
	if err := role.Validate(); err != nil {
		return writeError(stderr, err)
	}
	if err := identity.ValidateID(*deviceID); err != nil {
		return writeError(stderr, fmt.Errorf("deviceId %w", err))
	}
	if *tokenPath == "" {
		return writeError(stderr, errors.New("--out is required"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedToken, err := absolutePath(*tokenPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, master, err := loadBrokerCredentialAuthority(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedState := cfg.Broker.StateFile
	if err := rejectCredentialAuthorityPathCollisions(resolvedConfig, resolvedState, cfg.Broker.Auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	if err := rejectCredentialPathCollisions(resolvedToken, resolvedConfig, resolvedState, cfg.Broker.Auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	registry, err := store.Open(context.Background(), resolvedState)
	if err != nil {
		return writeError(stderr, err)
	}
	result, committed, operationErr := issueCredential(
		context.Background(), registry, master, cfg.ControllerID, *deviceID, role, resolvedToken,
	)
	closeErr := registry.Close()
	if operationErr != nil {
		return writeError(stderr, errors.Join(operationErr, closeErr))
	}
	if closeErr != nil {
		return writeCommittedError(stderr, committed, fmt.Errorf("close broker state: %w", closeErr))
	}
	result.ConfigPath = resolvedConfig
	result.StatePath = resolvedState
	return writeCredentialResult(stdout, stderr, result, *jsonOutput, committed)
}

func issueCredential(
	ctx context.Context,
	registry *store.Store,
	master tokenfile.Token,
	controllerID string,
	deviceID string,
	role control.DeviceRole,
	tokenPath string,
) (credentialResult, bool, error) {
	if err := identity.ValidateID(deviceID); err != nil {
		return credentialResult{}, false, fmt.Errorf("deviceId %w", err)
	}
	now := credentialNow()
	existingToken, tokenExists, err := readOptionalToken(tokenPath)
	if err != nil {
		return credentialResult{}, false, err
	}
	stored, err := registry.Credential(ctx, controllerID, deviceID)
	switch {
	case err == nil:
		if stored.Role != role {
			return credentialResult{}, false, errors.New("device credential already exists with a different role")
		}
		if stored.Pending {
			if tokenExists {
				if !macMatches(master, existingToken, stored.MAC) {
					return credentialResult{}, false, errors.New("pending credential does not match the existing output token")
				}
				if err := registry.ActivateCredential(ctx, controllerID, deviceID, stored.MAC); err != nil {
					return credentialResult{}, true, fmt.Errorf("activate recovered credential: %w", err)
				}
				return issuedCredentialResult(controllerID, deviceID, role, tokenPath, true), true, nil
			}
			if now.Before(time.Unix(stored.IssuedAt, 0).Add(pendingCredentialRecoveryLease)) {
				return credentialResult{}, false, fmt.Errorf(
					"%w: pending credential enrollment may still be active; retry after the recovery lease expires",
					store.ErrConflict,
				)
			}
			if err := registry.DeletePendingCredential(ctx, controllerID, deviceID, stored.MAC); err != nil {
				return credentialResult{}, false, fmt.Errorf("discard incomplete credential enrollment: %w", err)
			}
			break
		}
		if stored.Disabled {
			return credentialResult{}, false, errors.New("device credential has been revoked and cannot be reissued with the same deviceId")
		}
		if !tokenExists {
			return credentialResult{}, true, errors.New("active credential exists but its output token is unavailable")
		}
		if !macMatches(master, existingToken, stored.MAC) {
			return credentialResult{}, true, errors.New("active credential does not match the existing output token")
		}
		return issuedCredentialResult(controllerID, deviceID, role, tokenPath, true), true, nil
	case !errors.Is(err, store.ErrNotFound):
		return credentialResult{}, false, err
	case tokenExists:
		return credentialResult{}, false, errors.New("output token exists but no matching credential enrollment was found")
	}

	deviceToken, err := distinctToken(master)
	if err != nil {
		return credentialResult{}, false, err
	}
	mac := delegationcredential.MAC(master, deviceToken)
	pending := store.NewCredential(controllerID, deviceID, role, mac, now)
	pending.Disabled = true
	pending.Pending = true
	if err := registry.CreateCredential(ctx, pending); err != nil {
		return credentialResult{}, false, err
	}
	fileCommitted, err := tokenfile.WriteNew(tokenPath, deviceToken)
	if err != nil {
		if fileCommitted {
			return credentialResult{}, true, fmt.Errorf(
				"token file is committed and credential remains pending; rerun the same command: %w", err,
			)
		}
		cleanupErr := registry.DeletePendingCredential(ctx, controllerID, deviceID, mac)
		return credentialResult{}, false, errors.Join(err, cleanupErr)
	}
	if err := registry.ActivateCredential(ctx, controllerID, deviceID, mac); err != nil {
		return credentialResult{}, true, fmt.Errorf(
			"token file is committed and credential remains pending; rerun the same command: %w", err,
		)
	}
	return issuedCredentialResult(controllerID, deviceID, role, tokenPath, false), true, nil
}

func loadBrokerCredentialAuthority(path string) (delegationconfig.Config, tokenfile.Token, error) {
	cfg, err := delegationconfig.Read(path)
	if err != nil {
		return delegationconfig.Config{}, tokenfile.Token{}, err
	}
	if cfg.Role != delegationconfig.RoleBroker {
		return delegationconfig.Config{}, tokenfile.Token{}, errors.New("credential management requires a broker configuration")
	}
	if cfg.Broker.Auth.Mode != delegationconfig.AuthModeToken {
		return delegationconfig.Config{}, tokenfile.Token{}, errors.New("credential management requires broker token authentication")
	}
	master, err := tokenfile.Read(cfg.Broker.Auth.TokenFile)
	if err != nil {
		return delegationconfig.Config{}, tokenfile.Token{}, fmt.Errorf("read broker master token: %w", err)
	}
	return cfg, master, nil
}

func rejectCredentialPathCollisions(tokenPath, configPath, statePath, masterPath string) error {
	reserved := []struct {
		name string
		path string
	}{
		{name: "broker configuration", path: configPath},
		{name: "broker master token", path: masterPath},
	}
	for _, path := range sqliteStateFiles(statePath) {
		reserved = append(reserved, struct {
			name string
			path string
		}{name: "broker state or sidecar", path: path})
	}
	for _, candidate := range reserved {
		equivalent, err := pathsEquivalent(tokenPath, candidate.path)
		if err != nil {
			return err
		}
		if equivalent {
			return fmt.Errorf("output token path conflicts with %s", candidate.name)
		}
	}
	return nil
}

func rejectCredentialAuthorityPathCollisions(configPath, statePath, masterPath string) error {
	protectedFiles := []struct {
		name string
		path string
	}{
		{name: "broker configuration", path: configPath},
		{name: "broker master token", path: masterPath},
	}
	for _, stateFile := range sqliteStateFiles(statePath) {
		for _, protected := range protectedFiles {
			if protected.path == "" {
				continue
			}
			equivalent, err := pathsEquivalent(stateFile, protected.path)
			if err != nil {
				return err
			}
			if equivalent {
				return fmt.Errorf("broker state path conflicts with %s", protected.name)
			}
		}
	}
	return nil
}

func sqliteStateFiles(statePath string) []string {
	return []string{statePath, statePath + "-journal", statePath + "-wal", statePath + "-shm"}
}

func readOptionalToken(path string) (tokenfile.Token, bool, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return tokenfile.Token{}, false, nil
	} else if err != nil {
		return tokenfile.Token{}, false, fmt.Errorf("inspect output token: %w", err)
	}
	token, err := tokenfile.Read(path)
	if err != nil {
		return tokenfile.Token{}, false, fmt.Errorf("read output token: %w", err)
	}
	return token, true, nil
}

func distinctToken(master tokenfile.Token) (tokenfile.Token, error) {
	for range 4 {
		candidate, err := tokenfile.Generate()
		if err != nil {
			return tokenfile.Token{}, err
		}
		if subtle.ConstantTimeCompare(candidate[:], master[:]) != 1 {
			return candidate, nil
		}
	}
	return tokenfile.Token{}, errors.New("generate device token distinct from broker master token")
}

func macMatches(master, deviceToken tokenfile.Token, expected store.CredentialMAC) bool {
	actual := delegationcredential.MAC(master, deviceToken)
	return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
}

func issuedCredentialResult(
	controllerID, deviceID string,
	role control.DeviceRole,
	tokenPath string,
	recovered bool,
) credentialResult {
	return credentialResult{
		Action:       "issued",
		Role:         role,
		ControllerID: controllerID,
		DeviceID:     deviceID,
		TokenFile:    tokenPath,
		Recovered:    recovered,
	}
}

func writeCredentialResult(
	stdout, stderr io.Writer,
	result credentialResult,
	jsonOutput bool,
	committed bool,
) int {
	data, err := renderCredentialResult(result, jsonOutput)
	if err != nil {
		return writeCommittedError(stderr, committed, err)
	}
	if _, err := stdout.Write(data); err != nil {
		return writeCommittedError(stderr, committed, fmt.Errorf("write credential result: %w", err))
	}
	return 0
}

func renderCredentialResult(result credentialResult, jsonOutput bool) ([]byte, error) {
	var output bytes.Buffer
	if jsonOutput {
		if err := json.NewEncoder(&output).Encode(result); err != nil {
			return nil, fmt.Errorf("encode credential result: %w", err)
		}
		return output.Bytes(), nil
	}
	fmt.Fprintf(&output, "credential %s\n", result.Action)
	fmt.Fprintf(&output, "role: %s\n", result.Role)
	fmt.Fprintf(&output, "config: %s\n", result.ConfigPath)
	fmt.Fprintf(&output, "state: %s\n", result.StatePath)
	fmt.Fprintf(&output, "controllerId: %s\n", result.ControllerID)
	fmt.Fprintf(&output, "deviceId: %s\n", result.DeviceID)
	if result.TokenFile != "" {
		fmt.Fprintf(&output, "tokenFile: %s\n", result.TokenFile)
	}
	if result.Recovered {
		fmt.Fprintln(&output, "recovered: true")
	}
	return output.Bytes(), nil
}

func writeCommittedError(stderr io.Writer, committed bool, err error) int {
	if committed {
		return writeError(stderr, fmt.Errorf("credential state was committed: %w", err))
	}
	return writeError(stderr, err)
}
