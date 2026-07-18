package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type setupResult struct {
	Role         delegationconfig.Role `json:"role"`
	ConfigPath   string                `json:"configPath"`
	ControllerID string                `json:"controllerId"`
	DeviceID     string                `json:"deviceId,omitempty"`
	StatePath    string                `json:"statePath,omitempty"`
	TokenFile    string                `json:"tokenFile,omitempty"`
}

func runSetup(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation setup <broker|controller|device> [options]")
		return exitUsage
	}

	switch delegationconfig.Role(args[0]) {
	case delegationconfig.RoleBroker:
		return runSetupBroker(args[1:], stdout, stderr)
	case delegationconfig.RoleController, delegationconfig.RoleDevice:
		return runSetupDevice(delegationconfig.Role(args[0]), args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported setup role %q\n", args[0])
		return exitUsage
	}
}

func runSetupBroker(args []string, stdout, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation setup broker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	controllerID := flags.String("controller-id", "", "stable controller UUID; generated when omitted")
	listen := flags.String("listen", "127.0.0.1:8787", "broker listen address")
	statePath := flags.String("state", "", "broker state database path; defaults beside the config")
	authMode := flags.String("auth-mode", string(delegationconfig.AuthModeToken), "authentication mode: token or none")
	tokenFile := flags.String("token-file", "", "token file path; generated when omitted in token mode")
	allowInsecure := flags.Bool("allow-insecure-nonloopback", false, "acknowledge plaintext non-loopback transport")
	jsonOutput := flags.Bool("json", false, "print setup result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	if *statePath == "" {
		*statePath = filepath.Join(filepath.Dir(resolvedConfig), "state", "broker.sqlite3")
	}
	resolvedState, err := absolutePath(*statePath)
	if err != nil {
		return writeError(stderr, err)
	}
	if *controllerID == "" {
		*controllerID, err = identity.NewID()
		if err != nil {
			return writeError(stderr, err)
		}
	}
	auth, err := resolveAuth(*authMode, *tokenFile, filepath.Join(filepath.Dir(resolvedConfig), "secrets", "broker.token"))
	if err != nil {
		return writeError(stderr, err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  *controllerID,
		Broker: delegationconfig.BrokerConfig{
			Listen:                   *listen,
			StateFile:                resolvedState,
			Auth:                     auth,
			AllowInsecureNonLoopback: *allowInsecure,
		},
	}
	if err := cfg.Validate(); err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if err := store.ValidatePath(resolvedState); err != nil {
		return writeError(stderr, err)
	}
	if err := rejectCredentialAuthorityPathCollisions(resolvedConfig, resolvedState, auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	if err := writeInsecureTransportWarning(stderr, cfg); err != nil {
		return writeError(stderr, err)
	}
	if auth.Mode == delegationconfig.AuthModeToken {
		equivalent, err := pathsEquivalent(resolvedConfig, auth.TokenFile)
		if err != nil {
			return writeError(stderr, err)
		}
		if equivalent {
			return writeError(stderr, errors.New("config and token file must use different paths"))
		}
		if _, err := tokenfile.Ensure(auth.TokenFile); err != nil {
			return writeError(stderr, err)
		}
	}
	if err := delegationconfig.WriteNew(resolvedConfig, cfg); err != nil {
		return writeError(stderr, err)
	}
	return writeSetupResult(stdout, stderr, setupResult{
		Role:         cfg.Role,
		ConfigPath:   resolvedConfig,
		ControllerID: cfg.ControllerID,
		StatePath:    cfg.Broker.StateFile,
		TokenFile:    auth.TokenFile,
	}, *jsonOutput)
}

func runSetupDevice(role delegationconfig.Role, args []string, stdout, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation setup "+string(role), flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	controllerID := flags.String("controller-id", "", "controller UUID")
	deviceID := flags.String("device-id", "", "stable device UUID; required in token mode, generated in none mode")
	deviceName := flags.String("device-name", "", "device display name; hostname when omitted")
	brokerURL := flags.String("broker-url", "", "broker ws:// or wss:// URL")
	authMode := flags.String("auth-mode", string(delegationconfig.AuthModeToken), "authentication mode: token or none")
	tokenFile := flags.String("token-file", "", "existing device token file path")
	allowInsecure := flags.Bool("allow-insecure-nonloopback", false, "acknowledge plaintext non-loopback transport")
	jsonOutput := flags.Bool("json", false, "print setup result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *controllerID == "" {
		return writeError(stderr, errors.New("--controller-id is required"))
	}
	if *brokerURL == "" {
		return writeError(stderr, errors.New("--broker-url is required"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	auth, err := resolveAuth(*authMode, *tokenFile, "")
	if err != nil {
		return writeError(stderr, err)
	}
	if *deviceID == "" {
		if auth.Mode == delegationconfig.AuthModeToken {
			return writeError(stderr, errors.New("--device-id is required in token mode because the credential is bound to a device"))
		}
		*deviceID, err = identity.NewID()
		if err != nil {
			return writeError(stderr, err)
		}
	}
	if *deviceName == "" {
		*deviceName, err = os.Hostname()
		if err != nil {
			return writeError(stderr, fmt.Errorf("resolve hostname: %w", err))
		}
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          role,
		ControllerID:  *controllerID,
		DeviceID:      *deviceID,
		DeviceName:    *deviceName,
		Broker: delegationconfig.BrokerConfig{
			URL:                      *brokerURL,
			Auth:                     auth,
			AllowInsecureNonLoopback: *allowInsecure,
		},
	}
	if err := cfg.Validate(); err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if auth.Mode == delegationconfig.AuthModeToken {
		if err := tokenfile.Validate(auth.TokenFile); err != nil {
			return writeError(stderr, err)
		}
	}
	if err := writeInsecureTransportWarning(stderr, cfg); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.WriteNew(resolvedConfig, cfg); err != nil {
		return writeError(stderr, err)
	}
	return writeSetupResult(stdout, stderr, setupResult{
		Role:         cfg.Role,
		ConfigPath:   resolvedConfig,
		ControllerID: cfg.ControllerID,
		DeviceID:     cfg.DeviceID,
		TokenFile:    auth.TokenFile,
	}, *jsonOutput)
}

func writeInsecureTransportWarning(stderr io.Writer, cfg delegationconfig.Config) error {
	if !cfg.UsesInsecureNonLoopbackTransport() {
		return nil
	}
	var endpoint string
	if cfg.Role == delegationconfig.RoleBroker {
		endpoint = "listener " + cfg.Broker.Listen
	} else {
		endpoint = "broker URL " + cfg.Broker.URL
	}
	if _, err := fmt.Fprintf(
		stderr,
		"delegation: warning: %s uses plaintext non-loopback transport; restrict this endpoint to a trusted encrypted private network such as Tailscale or an encrypted tunnel\n",
		endpoint,
	); err != nil {
		return fmt.Errorf("write security warning: %w", err)
	}
	return nil
}

func resolveAuth(rawMode, rawTokenFile, defaultTokenFile string) (delegationconfig.AuthConfig, error) {
	mode := delegationconfig.AuthMode(rawMode)
	if mode == delegationconfig.AuthModeNone {
		if rawTokenFile != "" {
			return delegationconfig.AuthConfig{}, errors.New("--token-file cannot be used with auth mode none")
		}
		return delegationconfig.AuthConfig{Mode: mode}, nil
	}
	if mode != delegationconfig.AuthModeToken {
		return delegationconfig.AuthConfig{}, fmt.Errorf("unsupported auth mode %q", rawMode)
	}
	if rawTokenFile == "" {
		rawTokenFile = defaultTokenFile
	}
	if rawTokenFile == "" {
		return delegationconfig.AuthConfig{}, errors.New("--token-file is required in token mode")
	}
	path, err := absolutePath(rawTokenFile)
	if err != nil {
		return delegationconfig.AuthConfig{}, err
	}
	return delegationconfig.AuthConfig{Mode: mode, TokenFile: path}, nil
}

func ensureConfigAvailable(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("config already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect config: %w", err)
	}
	return nil
}

func pathsEquivalent(first, second string) (bool, error) {
	firstCanonical, err := canonicalFuturePath(first)
	if err != nil {
		return false, err
	}
	secondCanonical, err := canonicalFuturePath(second)
	if err != nil {
		return false, err
	}
	// A conservative folded comparison also covers case-insensitive macOS and
	// removable filesystems. Distinct config and token names should not rely on
	// case alone even when the current filesystem permits it.
	if strings.EqualFold(firstCanonical, secondCanonical) {
		return true, nil
	}
	firstInfo, firstErr := os.Stat(firstCanonical)
	if firstErr != nil && !errors.Is(firstErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", firstCanonical, firstErr)
	}
	secondInfo, secondErr := os.Stat(secondCanonical)
	if secondErr != nil && !errors.Is(secondErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect path identity for %s: %w", secondCanonical, secondErr)
	}
	return firstErr == nil && secondErr == nil && os.SameFile(firstInfo, secondInfo), nil
}

func canonicalFuturePath(path string) (string, error) {
	return resolveFuturePath(filepath.Clean(path), 0)
}

func resolveFuturePath(path string, followedLinks int) (string, error) {
	root, components := splitAbsolutePath(path)
	resolved := root
	for index, component := range components {
		candidate := filepath.Join(resolved, component)
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) {
			return filepath.Join(append([]string{resolved}, components[index:]...)...), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve path aliases for %s: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = candidate
			continue
		}
		if followedLinks >= 255 {
			return "", fmt.Errorf("resolve path aliases for %s: too many symbolic links", path)
		}
		target, err := os.Readlink(candidate)
		if err != nil {
			return "", fmt.Errorf("read path alias %s: %w", candidate, err)
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(resolved, target)
		}
		remaining := append([]string{target}, components[index+1:]...)
		return resolveFuturePath(filepath.Join(remaining...), followedLinks+1)
	}
	return filepath.Clean(resolved), nil
}

func splitAbsolutePath(path string) (string, []string) {
	current := filepath.Clean(path)
	var reversed []string
	for {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		reversed = append(reversed, filepath.Base(current))
		current = parent
	}
	components := make([]string, len(reversed))
	for i := range reversed {
		components[len(reversed)-1-i] = reversed[i]
	}
	return current, components
}

func absolutePath(path string) (string, error) {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path: %w", err)
	}
	return resolved, nil
}

func parseFlags(flags *flag.FlagSet, args []string) int {
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return exitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(flags.Output(), "delegation: unexpected argument %q\n", flags.Arg(0))
		return exitUsage
	}
	return -1
}

func writeSetupResult(stdout, stderr io.Writer, result setupResult, jsonOutput bool) int {
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			return writeError(stderr, fmt.Errorf("encode setup result: %w", err))
		}
		return 0
	}
	fmt.Fprintf(stdout, "configured %s\n", result.Role)
	fmt.Fprintf(stdout, "config: %s\n", result.ConfigPath)
	fmt.Fprintf(stdout, "controllerId: %s\n", result.ControllerID)
	if result.DeviceID != "" {
		fmt.Fprintf(stdout, "deviceId: %s\n", result.DeviceID)
	}
	if result.StatePath != "" {
		fmt.Fprintf(stdout, "state: %s\n", result.StatePath)
	}
	if result.TokenFile != "" {
		fmt.Fprintf(stdout, "tokenFile: %s\n", result.TokenFile)
	}
	return 0
}

func writeError(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "delegation: %v\n", err)
	return 1
}
