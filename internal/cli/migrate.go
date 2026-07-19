package cli

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type migrateConfigResult struct {
	Action       string                `json:"action"`
	SourcePath   string                `json:"sourcePath"`
	ConfigPath   string                `json:"configPath"`
	Role         delegationconfig.Role `json:"role"`
	ControllerID string                `json:"controllerId"`
	DeviceID     string                `json:"deviceId,omitempty"`
	TokenFile    string                `json:"tokenFile,omitempty"`
}

func runMigrate(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "config" {
		fmt.Fprintln(stderr, "usage: delegation migrate config --from <legacy-config> --to <new-config> [--token-file <fresh-peer-token>]")
		return exitUsage
	}
	flags := flag.NewFlagSet("delegation migrate config", flag.ContinueOnError)
	flags.SetOutput(stderr)
	from := flags.String("from", "", "protected schema-v3 configuration to read")
	to := flags.String("to", "", "new broker.json or peer.json path")
	tokenPath := flags.String("token-file", "", "fresh peer token for a legacy device configuration")
	jsonOutput := flags.Bool("json", false, "print migration result as JSON")
	if code := parseFlags(flags, args[1:]); code >= 0 {
		return code
	}
	if *from == "" || *to == "" {
		return writeError(stderr, errors.New("--from and --to are required"))
	}
	source, err := absolutePath(*from)
	if err != nil {
		return writeError(stderr, err)
	}
	destination, err := absolutePath(*to)
	if err != nil {
		return writeError(stderr, err)
	}
	if filepath.Clean(source) == filepath.Clean(destination) {
		return writeError(stderr, errors.New("legacy source and v4 destination must differ"))
	}
	legacy, err := delegationconfig.ReadLegacy(source)
	if err != nil {
		return writeError(stderr, err)
	}
	migrated, err := migrateConfig(legacy, destination, *tokenPath)
	if err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(destination); err != nil {
		return writeError(stderr, err)
	}
	if err := writeInsecureTransportWarning(stderr, migrated); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.PrepareWrite(destination); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.WriteNew(destination, migrated); err != nil {
		return writeError(stderr, err)
	}
	return writeMigrateConfigResult(stdout, stderr, migrateConfigResult{
		Action:       "migrated",
		SourcePath:   source,
		ConfigPath:   destination,
		Role:         migrated.Role,
		ControllerID: migrated.ControllerID,
		DeviceID:     migrated.DeviceID,
		TokenFile:    migrated.Broker.Auth.TokenFile,
	}, *jsonOutput)
}

func migrateConfig(legacy delegationconfig.Config, destination, replacementToken string) (delegationconfig.Config, error) {
	migrated := legacy
	migrated.SchemaVersion = delegationconfig.CurrentSchemaVersion
	switch legacy.Role {
	case delegationconfig.RoleBroker:
		if replacementToken != "" {
			return delegationconfig.Config{}, errors.New("--token-file is only valid when migrating a legacy device")
		}
		if err := pathguard.ValidateBrokerAuthority(
			destination, migrated.Broker.StateFile, migrated.Broker.Auth.TokenFile,
		); err != nil {
			return delegationconfig.Config{}, err
		}
		if err := store.ValidatePath(migrated.Broker.StateFile); err != nil {
			return delegationconfig.Config{}, err
		}
		if migrated.Broker.Auth.Mode == delegationconfig.AuthModeToken {
			if err := tokenfile.Validate(migrated.Broker.Auth.TokenFile); err != nil {
				return delegationconfig.Config{}, fmt.Errorf("validate retained broker master token: %w", err)
			}
		}
	case delegationconfig.LegacyRoleController:
		if replacementToken != "" {
			return delegationconfig.Config{}, errors.New("legacy controller migration retains its existing credential; omit --token-file")
		}
		migrated.Role = delegationconfig.RolePeer
		url, err := delegationconfig.UpgradeLegacyBrokerURL(
			legacy.Broker.URL, legacy.Broker.AllowInsecureNonLoopback,
		)
		if err != nil {
			return delegationconfig.Config{}, err
		}
		migrated.Broker.URL = url
		if err := validateMigratedPeer(destination, migrated); err != nil {
			return delegationconfig.Config{}, err
		}
	case delegationconfig.LegacyRoleDevice:
		migrated.Role = delegationconfig.RolePeer
		url, err := delegationconfig.UpgradeLegacyBrokerURL(
			legacy.Broker.URL, legacy.Broker.AllowInsecureNonLoopback,
		)
		if err != nil {
			return delegationconfig.Config{}, err
		}
		migrated.Broker.URL = url
		if legacy.Broker.Auth.Mode == delegationconfig.AuthModeToken {
			if replacementToken == "" {
				return delegationconfig.Config{}, errors.New("legacy device migration requires --token-file with a freshly issued peer credential")
			}
			resolvedToken, err := absolutePath(replacementToken)
			if err != nil {
				return delegationconfig.Config{}, err
			}
			if err := requireFreshToken(legacy.Broker.Auth.TokenFile, resolvedToken); err != nil {
				return delegationconfig.Config{}, err
			}
			migrated.Broker.Auth.TokenFile = resolvedToken
		} else if replacementToken != "" {
			return delegationconfig.Config{}, errors.New("--token-file cannot be used with auth mode none")
		}
		if err := validateMigratedPeer(destination, migrated); err != nil {
			return delegationconfig.Config{}, err
		}
	default:
		return delegationconfig.Config{}, fmt.Errorf("unsupported legacy role %q", legacy.Role)
	}
	if err := migrated.Validate(); err != nil {
		return delegationconfig.Config{}, err
	}
	return migrated, nil
}

func validateMigratedPeer(destination string, cfg delegationconfig.Config) error {
	if err := pathguard.ValidateConnectorAuthority(destination, cfg.Broker.Auth.TokenFile); err != nil {
		return err
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeToken {
		return tokenfile.Validate(cfg.Broker.Auth.TokenFile)
	}
	return nil
}

func requireFreshToken(legacyPath, replacementPath string) error {
	if filepath.Clean(legacyPath) == filepath.Clean(replacementPath) {
		return errors.New("fresh peer token must use a different file from the legacy device token")
	}
	legacyInfo, legacyErr := os.Stat(legacyPath)
	replacementInfo, replacementErr := os.Stat(replacementPath)
	if legacyErr == nil && replacementErr == nil && os.SameFile(legacyInfo, replacementInfo) {
		return errors.New("fresh peer token must not alias the legacy device token")
	}
	if replacementErr != nil {
		return fmt.Errorf("inspect fresh peer token: %w", replacementErr)
	}
	if legacyErr != nil && !errors.Is(legacyErr, os.ErrNotExist) {
		return fmt.Errorf("inspect legacy device token: %w", legacyErr)
	}
	replacement, err := tokenfile.Read(replacementPath)
	if err != nil {
		return fmt.Errorf("read fresh peer token: %w", err)
	}
	if legacyErr == nil {
		legacy, err := tokenfile.Read(legacyPath)
		if err != nil {
			return fmt.Errorf("read legacy device token: %w", err)
		}
		if subtle.ConstantTimeCompare(legacy[:], replacement[:]) == 1 {
			return errors.New("fresh peer token must differ from the legacy device token")
		}
	}
	return nil
}

func writeMigrateConfigResult(stdout, stderr io.Writer, result migrateConfigResult, jsonOutput bool) int {
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			return writeError(stderr, fmt.Errorf("encode migration result: %w", err))
		}
		return 0
	}
	fmt.Fprintln(stdout, "config migrated")
	fmt.Fprintf(stdout, "source: %s\n", result.SourcePath)
	fmt.Fprintf(stdout, "config: %s\n", result.ConfigPath)
	fmt.Fprintf(stdout, "role: %s\n", result.Role)
	fmt.Fprintf(stdout, "controllerId: %s\n", result.ControllerID)
	if result.DeviceID != "" {
		fmt.Fprintf(stdout, "deviceId: %s\n", result.DeviceID)
	}
	if result.TokenFile != "" {
		fmt.Fprintf(stdout, "tokenFile: %s\n", result.TokenFile)
	}
	return 0
}
