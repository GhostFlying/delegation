package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type setupResult struct {
	Role          delegationconfig.Role `json:"role"`
	ConfigPath    string                `json:"configPath"`
	ControllerID  string                `json:"controllerId"`
	DeviceID      string                `json:"deviceId,omitempty"`
	StatePath     string                `json:"statePath,omitempty"`
	CodexHome     string                `json:"codexHome,omitempty"`
	WorkspaceRoot string                `json:"workspaceRoot,omitempty"`
	TokenFile     string                `json:"tokenFile,omitempty"`
}

func runSetup(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation setup <broker|peer> [options]")
		return exitUsage
	}

	switch delegationconfig.Role(args[0]) {
	case delegationconfig.RoleBroker:
		return runSetupBroker(args[1:], stdout, stderr)
	case delegationconfig.RolePeer:
		return runSetupPeer(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported setup role %q\n", args[0])
		return exitUsage
	}
}

func runSetupBroker(args []string, stdout, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultBrokerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation setup broker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	controllerID := flags.String("controller-id", "", "stable Delegation network UUID; generated when omitted")
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
	if err := pathguard.ValidateBrokerAuthority(resolvedConfig, resolvedState, auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	if err := store.ValidatePath(resolvedState); err != nil {
		return writeError(stderr, err)
	}
	if err := writeInsecureTransportWarning(stderr, cfg); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.PrepareWrite(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if auth.Mode == delegationconfig.AuthModeToken {
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

func runSetupPeer(args []string, stdout, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPeerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation setup peer", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	controllerID := flags.String("controller-id", "", "Delegation network UUID")
	deviceID := flags.String("device-id", "", "stable device UUID; required in token mode, generated in none mode")
	deviceName := flags.String("device-name", "", "device display name; hostname when omitted")
	brokerURL := flags.String("broker-url", "", "broker ws:// or wss:// URL")
	authMode := flags.String("auth-mode", string(delegationconfig.AuthModeToken), "authentication mode: token or none")
	tokenFile := flags.String("token-file", "", "existing peer token file path")
	codexBinary := flags.String("codex-binary", "codex", "Codex executable path or name")
	codexHome := flags.String("codex-home", "", "managed worker CODEX_HOME; defaults beside the peer config")
	workspaceRoot := flags.String("workspace-root", "", "managed worker workspace root; defaults beside the peer config")
	statePath := flags.String("state", "", "peer reservation database path; defaults beside the peer config")
	maxWorkerSlots := flags.Int("max-worker-slots", 4, "maximum concurrent managed workers")
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
	resolvedCodexBinary, err := resolveExecutable(*codexBinary)
	if err != nil {
		return writeError(stderr, fmt.Errorf("resolve Codex executable: %w", err))
	}
	if *codexHome == "" {
		*codexHome = filepath.Join(filepath.Dir(resolvedConfig), "codex")
	}
	resolvedCodexHome, err := absolutePath(*codexHome)
	if err != nil {
		return writeError(stderr, err)
	}
	if target, evalErr := filepath.EvalSymlinks(resolvedCodexHome); evalErr == nil {
		resolvedCodexHome = target
	} else if !errors.Is(evalErr, os.ErrNotExist) {
		return writeError(stderr, fmt.Errorf("resolve worker CODEX_HOME: %w", evalErr))
	}
	if *workspaceRoot == "" {
		*workspaceRoot = filepath.Join(filepath.Dir(resolvedConfig), "workspaces")
	}
	resolvedWorkspaceRoot, err := absolutePath(*workspaceRoot)
	if err != nil {
		return writeError(stderr, err)
	}
	if target, evalErr := filepath.EvalSymlinks(resolvedWorkspaceRoot); evalErr == nil {
		resolvedWorkspaceRoot = target
	} else if !errors.Is(evalErr, os.ErrNotExist) {
		return writeError(stderr, fmt.Errorf("resolve worker workspace root: %w", evalErr))
	}
	if *statePath == "" {
		*statePath = filepath.Join(filepath.Dir(resolvedConfig), "state", "peer.sqlite3")
	}
	resolvedState, err := absolutePath(*statePath)
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
		Role:          delegationconfig.RolePeer,
		ControllerID:  *controllerID,
		DeviceID:      *deviceID,
		DeviceName:    *deviceName,
		Broker: delegationconfig.BrokerConfig{
			URL:                      *brokerURL,
			Auth:                     auth,
			AllowInsecureNonLoopback: *allowInsecure,
		},
		Peer: delegationconfig.PeerConfig{
			CodexBinary:    resolvedCodexBinary,
			CodexHome:      resolvedCodexHome,
			WorkspaceRoot:  resolvedWorkspaceRoot,
			StateFile:      resolvedState,
			MaxWorkerSlots: *maxWorkerSlots,
		},
	}
	if err := cfg.Validate(); err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if err := pathguard.ValidatePeerAuthority(resolvedConfig, resolvedState, auth.TokenFile); err != nil {
		return writeError(stderr, err)
	}
	if err := store.ValidatePath(resolvedState); err != nil {
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
	if err := prepareManagedDirectory(resolvedCodexHome, "worker CODEX_HOME"); err != nil {
		return writeError(stderr, err)
	}
	if err := prepareManagedDirectory(resolvedWorkspaceRoot, "worker workspace root"); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.WriteNew(resolvedConfig, cfg); err != nil {
		return writeError(stderr, err)
	}
	return writeSetupResult(stdout, stderr, setupResult{
		Role:          cfg.Role,
		ConfigPath:    resolvedConfig,
		ControllerID:  cfg.ControllerID,
		DeviceID:      cfg.DeviceID,
		StatePath:     cfg.Peer.StateFile,
		CodexHome:     cfg.Peer.CodexHome,
		WorkspaceRoot: cfg.Peer.WorkspaceRoot,
		TokenFile:     auth.TokenFile,
	}, *jsonOutput)
}

func writeInsecureTransportWarning(stderr io.Writer, cfg delegationconfig.Config) error {
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeNone {
		if _, err := fmt.Fprintln(
			stderr,
			"delegation: warning: authentication is disabled; any client that can reach this network may join, enumerate peers, dispatch work, impersonate a peer, or fence a peer using the same deviceId; on Tailscale this trusts the entire tailnet",
		); err != nil {
			return fmt.Errorf("write authentication warning: %w", err)
		}
	}
	if cfg.UsesInsecureNonLoopbackTransport() {
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
			return fmt.Errorf("write transport warning: %w", err)
		}
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
	if result.CodexHome != "" {
		fmt.Fprintf(stdout, "managed CODEX_HOME: %s\n", result.CodexHome)
	}
	if result.WorkspaceRoot != "" {
		fmt.Fprintf(stdout, "workspace root: %s\n", result.WorkspaceRoot)
	}
	if result.TokenFile != "" {
		fmt.Fprintf(stdout, "tokenFile: %s\n", result.TokenFile)
	}
	return 0
}

func resolveExecutable(name string) (string, error) {
	if name == "" {
		return "", errors.New("Codex executable is required")
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	resolved, err = filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("Codex executable must be a regular file")
	}
	return resolved, nil
}

func prepareManagedDirectory(path, description string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", description, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("protect %s: %w", description, err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", description, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must resolve to a directory", description)
	}
	return nil
}

func writeError(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "delegation: %v\n", err)
	return 1
}
