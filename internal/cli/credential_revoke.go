package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/store"
)

func runCredentialRevoke(args []string, stdout, stderr io.Writer) int {
	defaultConfig, err := delegationconfig.DefaultBrokerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation credential revoke", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfig, "broker configuration file path")
	deviceID := flags.String("device-id", "", "device UUID")
	jsonOutput := flags.Bool("json", false, "print credential result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if err := identity.ValidateID(*deviceID); err != nil {
		return writeError(stderr, fmt.Errorf("deviceId %w", err))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, _, err := loadBrokerCredentialAuthority(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedState := cfg.Broker.StateFile
	if err := pathguard.ValidateBrokerAuthority(resolvedConfig, resolvedState, cfg.Broker.Auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	registry, err := store.OpenCurrent(context.Background(), resolvedState)
	if err != nil {
		return writeError(stderr, err)
	}
	stored, operationErr := registry.Credential(context.Background(), cfg.ControllerID, *deviceID)
	if operationErr == nil && stored.Pending {
		operationErr = fmt.Errorf("%w: pending credential enrollment cannot be revoked", store.ErrConflict)
	}
	if operationErr == nil {
		operationErr = registry.DisableCredential(context.Background(), cfg.ControllerID, *deviceID)
	}
	closeErr := registry.Close()
	if operationErr != nil {
		return writeError(stderr, errors.Join(operationErr, closeErr))
	}
	if closeErr != nil {
		return writeCommittedError(stderr, true, fmt.Errorf("close broker state: %w", closeErr))
	}
	return writeCredentialResult(stdout, stderr, credentialResult{
		Action:       "revoked",
		ConfigPath:   resolvedConfig,
		StatePath:    resolvedState,
		ControllerID: cfg.ControllerID,
		DeviceID:     *deviceID,
	}, *jsonOutput, true)
}
