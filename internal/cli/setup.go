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
	"strings"

	"github.com/GhostFlying/delegation/internal/clicommand"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tailscaleauth"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

type setupResult struct {
	Role                 delegationconfig.Role          `json:"role"`
	InstanceID           string                         `json:"instanceId"`
	HostKind             hostkind.Kind                  `json:"hostKind"`
	ConfigPath           string                         `json:"configPath"`
	ControllerID         string                         `json:"controllerId"`
	DeviceID             string                         `json:"deviceId,omitempty"`
	StatePath            string                         `json:"statePath,omitempty"`
	CodexHome            string                         `json:"codexHome,omitempty"`
	GitBinary            string                         `json:"gitBinary,omitempty"`
	WorkspaceRoot        string                         `json:"workspaceRoot,omitempty"`
	TokenFile            string                         `json:"tokenFile,omitempty"`
	StatusListen         string                         `json:"statusListen,omitempty"`
	Transport            delegationconfig.TransportMode `json:"transport"`
	TailscaleHostname    string                         `json:"tailscaleHostname,omitempty"`
	TailscaleStateDir    string                         `json:"tailscaleStateDir,omitempty"`
	TailscaleAuthKeyFile string                         `json:"tailscaleAuthKeyFile,omitempty"`
}

type repeatedStringFlag []string

func (values *repeatedStringFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func (values *repeatedStringFlag) String() string {
	return strings.Join(*values, ",")
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
	flags := flag.NewFlagSet("delegation setup broker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "configuration file path; defaults under the selected instance")
	instanceID := flags.String("instance", delegationconfig.DefaultInstanceID, "local Delegation instance name")
	hostKind := flags.String("host-kind", string(hostkind.Codex), "network host kind: codex or traex")
	controllerID := flags.String("controller-id", "", "stable Delegation network UUID; generated when omitted")
	listen := flags.String("listen", "127.0.0.1:8787", "broker listen address")
	statusListen := flags.String("status-listen", "127.0.0.1:8788", "loopback broker status listen address")
	statePath := flags.String("state", "", "broker state database path; defaults beside the config")
	authMode := flags.String("auth-mode", string(delegationconfig.AuthModeToken), "authentication mode: token or none")
	tokenFile := flags.String("token-file", "", "token file path; generated when omitted in token mode")
	transport := flags.String("transport", "tcp", "network transport: tcp or tailscale")
	tailscaleHostname := flags.String("tailscale-hostname", "", "embedded Tailscale node hostname")
	tailscaleAuthKeyFile := flags.String(
		"tailscale-auth-key-file",
		"",
		"protected Tailscale enrollment key file",
	)
	tailscaleStateDir := flags.String(
		"tailscale-state-dir",
		"",
		"persistent embedded Tailscale state directory; defaults under the selected instance",
	)
	allowInsecure := flags.Bool("allow-insecure-nonloopback", false, "acknowledge plaintext non-loopback transport")
	jsonOutput := flags.Bool("json", false, "print setup result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if err := delegationconfig.ValidateInstanceID(*instanceID); err != nil {
		return writeError(stderr, err)
	}
	if *instanceID != delegationconfig.DefaultInstanceID &&
		*configPath == "" && os.Getenv("DELEGATION_CONFIG") != "" {
		return writeError(
			stderr,
			errors.New("named setup with DELEGATION_CONFIG requires an explicit --config path"),
		)
	}
	if *configPath == "" {
		var err error
		*configPath, err = delegationconfig.DefaultBrokerPathForInstance(*instanceID)
		if err != nil {
			return writeError(stderr, err)
		}
	}
	if *instanceID != delegationconfig.DefaultInstanceID {
		if !flagWasSet(flags, "listen") || !flagWasSet(flags, "status-listen") {
			return writeError(
				stderr,
				errors.New("named broker instances require explicit --listen and --status-listen addresses"),
			)
		}
	}
	networkHostKind := hostkind.Kind(*hostKind)
	if err := networkHostKind.Validate(); err != nil {
		return writeError(stderr, err)
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	resourceRoot := setupResourceRoot(resolvedConfig, *instanceID)
	transportConfig, err := resolveSetupTransport(
		flags,
		*transport,
		*tailscaleHostname,
		*tailscaleAuthKeyFile,
		*tailscaleStateDir,
		filepath.Join(resourceRoot, "state", "tailscale", "broker"),
	)
	if err != nil {
		return writeError(stderr, err)
	}
	if transportConfig.Mode == delegationconfig.TransportModeTailscale &&
		!flagWasSet(flags, "listen") {
		*listen = ":8787"
	}
	if *statePath == "" {
		*statePath = filepath.Join(resourceRoot, "state", "broker.sqlite3")
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
	auth, err := resolveAuth(*authMode, *tokenFile, filepath.Join(resourceRoot, "secrets", "broker.token"))
	if err != nil {
		return writeError(stderr, err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		InstanceID:    *instanceID,
		HostKind:      networkHostKind,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  *controllerID,
		Transport:     transportConfig,
		Broker: delegationconfig.BrokerConfig{
			Listen:                   *listen,
			StatusListen:             *statusListen,
			StateFile:                resolvedState,
			Auth:                     auth,
			AllowInsecureNonLoopback: *allowInsecure,
		},
	}
	if err := runtimeconfig.Validate(cfg); err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if err := validateBrokerSetupAuthority(resolvedConfig, resolvedState, auth.TokenFile, transportConfig); err != nil {
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
	if err := runtimeconfig.WriteNew(resolvedConfig, cfg); err != nil {
		return writeError(stderr, err)
	}
	return writeSetupResult(stdout, stderr, setupResult{
		Role:                 cfg.Role,
		InstanceID:           cfg.InstanceID,
		HostKind:             cfg.HostKind,
		ConfigPath:           resolvedConfig,
		ControllerID:         cfg.ControllerID,
		StatePath:            cfg.Broker.StateFile,
		TokenFile:            auth.TokenFile,
		StatusListen:         cfg.Broker.StatusListen,
		Transport:            cfg.Transport.Mode,
		TailscaleHostname:    transportHostname(cfg.Transport),
		TailscaleStateDir:    transportStateDir(cfg.Transport),
		TailscaleAuthKeyFile: transportAuthKeyFile(cfg.Transport),
	}, *jsonOutput)
}

func runSetupPeer(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation setup peer", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "configuration file path; defaults under the selected instance")
	instanceID := flags.String("instance", delegationconfig.DefaultInstanceID, "local Delegation instance name")
	hostKind := flags.String("host-kind", string(hostkind.Codex), "network host kind: codex or traex")
	controllerID := flags.String("controller-id", "", "Delegation network UUID")
	deviceID := flags.String("device-id", "", "stable device UUID; required in token mode, generated in none mode")
	deviceName := flags.String("device-name", "", "device display name; hostname when omitted")
	brokerURL := flags.String("broker-url", "", "broker ws:// or wss:// URL")
	authMode := flags.String("auth-mode", string(delegationconfig.AuthModeToken), "authentication mode: token or none")
	tokenFile := flags.String("token-file", "", "existing peer token file path")
	codexBinary := flags.String("codex-binary", "codex", "Codex executable path or name")
	cliCommand := flags.String("cli-command", "", "CLI executable path or name")
	var cliArguments repeatedStringFlag
	flags.Var(&cliArguments, "cli-argument", "exact CLI argument; repeat for each argv element")
	cliLauncher := flags.String("cli-launcher", "", "shell-free CLI launcher executable path or name")
	var cliLauncherPrefixArguments repeatedStringFlag
	flags.Var(
		&cliLauncherPrefixArguments,
		"cli-launcher-prefix-argument",
		"exact launcher prefix argument; repeat for each argv element",
	)
	gitBinary := flags.String("git-binary", "git", "Git executable path or name")
	codexHome := flags.String(
		"codex-home",
		"",
		"managed CLI home; compatibility flag persisted as peer.codexHome",
	)
	workspaceRoot := flags.String("workspace-root", "", "managed worker workspace root; defaults beside the peer config")
	statePath := flags.String("state", "", "peer reservation database path; defaults beside the peer config")
	transport := flags.String("transport", "tcp", "network transport: tcp or tailscale")
	tailscaleHostname := flags.String("tailscale-hostname", "", "embedded Tailscale node hostname")
	tailscaleAuthKeyFile := flags.String(
		"tailscale-auth-key-file",
		"",
		"protected Tailscale enrollment key file",
	)
	tailscaleStateDir := flags.String(
		"tailscale-state-dir",
		"",
		"persistent embedded Tailscale state directory; defaults under the selected instance",
	)
	maxWorkerSlots := flags.Int(
		"max-worker-slots",
		4,
		"maximum concurrent managed workers; result storage admission is separate",
	)
	allowInsecure := flags.Bool("allow-insecure-nonloopback", false, "acknowledge plaintext non-loopback transport")
	jsonOutput := flags.Bool("json", false, "print setup result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if err := delegationconfig.ValidateInstanceID(*instanceID); err != nil {
		return writeError(stderr, err)
	}
	if *instanceID != delegationconfig.DefaultInstanceID &&
		*configPath == "" && os.Getenv("DELEGATION_CONFIG") != "" {
		return writeError(
			stderr,
			errors.New("named setup with DELEGATION_CONFIG requires an explicit --config path"),
		)
	}
	if *configPath == "" {
		var err error
		*configPath, err = delegationconfig.DefaultPeerPathForInstance(*instanceID)
		if err != nil {
			return writeError(stderr, err)
		}
	}
	networkHostKind := hostkind.Kind(*hostKind)
	if err := networkHostKind.Validate(); err != nil {
		return writeError(stderr, err)
	}
	if *controllerID == "" {
		return writeError(stderr, errors.New("--controller-id is required"))
	}
	if *brokerURL == "" {
		return writeError(stderr, errors.New("--broker-url is required"))
	}
	structuredCLI := flagWasSet(flags, "cli-command") ||
		flagWasSet(flags, "cli-argument") ||
		flagWasSet(flags, "cli-launcher") ||
		flagWasSet(flags, "cli-launcher-prefix-argument")
	if networkHostKind == hostkind.TraeX && flagWasSet(flags, "codex-binary") {
		return writeError(stderr, errors.New("--codex-binary is supported only for Codex peers"))
	}
	if structuredCLI && flagWasSet(flags, "codex-binary") {
		return writeError(stderr, errors.New("--codex-binary cannot be combined with structured CLI flags"))
	}
	if structuredCLI && (!flagWasSet(flags, "cli-command") || strings.TrimSpace(*cliCommand) == "") {
		return writeError(stderr, errors.New("--cli-command is required with structured CLI flags"))
	}
	if flagWasSet(flags, "cli-launcher-prefix-argument") &&
		(!flagWasSet(flags, "cli-launcher") || strings.TrimSpace(*cliLauncher) == "") {
		return writeError(
			stderr,
			errors.New("--cli-launcher-prefix-argument requires --cli-launcher"),
		)
	}
	if networkHostKind == hostkind.TraeX {
		if !structuredCLI {
			return writeError(stderr, errors.New("TraeX peer setup requires --cli-command and --cli-launcher"))
		}
		if !flagWasSet(flags, "cli-launcher") || strings.TrimSpace(*cliLauncher) == "" {
			return writeError(stderr, errors.New("TraeX peer setup requires --cli-launcher"))
		}
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	resourceRoot := setupResourceRoot(resolvedConfig, *instanceID)
	transportConfig, err := resolveSetupTransport(
		flags,
		*transport,
		*tailscaleHostname,
		*tailscaleAuthKeyFile,
		*tailscaleStateDir,
		filepath.Join(resourceRoot, "state", "tailscale", "peer"),
	)
	if err != nil {
		return writeError(stderr, err)
	}
	auth, err := resolveAuth(*authMode, *tokenFile, "")
	if err != nil {
		return writeError(stderr, err)
	}
	configuredCLI := delegationconfig.CLIConfig{
		Arguments: append([]string(nil), cliArguments...),
	}
	if structuredCLI {
		configuredCLI.Command, err = resolveCLIExecutable(networkHostKind, *cliCommand)
		if err != nil {
			return writeError(stderr, fmt.Errorf("resolve CLI command: %w", err))
		}
		if flagWasSet(flags, "cli-launcher") {
			launcher, resolveErr := resolveCLILauncher(
				*cliLauncher,
				cliLauncherPrefixArguments,
			)
			if resolveErr != nil {
				return writeError(stderr, fmt.Errorf("resolve CLI launcher: %w", resolveErr))
			}
			configuredCLI.Launcher = &launcher
		}
	} else {
		configuredCLI.Command, err = resolveCodexExecutable(*codexBinary)
		if err != nil {
			return writeError(stderr, fmt.Errorf("resolve Codex executable: %w", err))
		}
	}
	resolvedGitBinary, err := resolveGitExecutable(*gitBinary)
	if err != nil {
		return writeError(stderr, fmt.Errorf("resolve Git executable: %w", err))
	}
	if *codexHome == "" {
		homeDirectory := "codex"
		if networkHostKind == hostkind.TraeX {
			homeDirectory = "trae"
		}
		*codexHome = filepath.Join(resourceRoot, homeDirectory)
	}
	resolvedCodexHome, err := absolutePath(*codexHome)
	if err != nil {
		return writeError(stderr, err)
	}
	if networkHostKind == hostkind.TraeX {
		info, statErr := os.Lstat(resolvedCodexHome)
		if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return writeError(stderr, errors.New("managed TRAE_HOME must not be a symbolic link"))
		}
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return writeError(stderr, fmt.Errorf("inspect managed TRAE_HOME: %w", statErr))
		}
	}
	if target, evalErr := filepath.EvalSymlinks(resolvedCodexHome); evalErr == nil {
		resolvedCodexHome = target
	} else if !errors.Is(evalErr, os.ErrNotExist) {
		return writeError(stderr, fmt.Errorf("resolve managed CLI home: %w", evalErr))
	}
	if *workspaceRoot == "" {
		*workspaceRoot = filepath.Join(resourceRoot, "workspaces")
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
		*statePath = filepath.Join(resourceRoot, "state", "peer.sqlite3")
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
		InstanceID:    *instanceID,
		HostKind:      networkHostKind,
		Role:          delegationconfig.RolePeer,
		ControllerID:  *controllerID,
		DeviceID:      *deviceID,
		DeviceName:    *deviceName,
		Transport:     transportConfig,
		Broker: delegationconfig.BrokerConfig{
			URL:                      *brokerURL,
			Auth:                     auth,
			AllowInsecureNonLoopback: *allowInsecure,
		},
		Peer: delegationconfig.PeerConfig{
			CLI:            &configuredCLI,
			GitBinary:      resolvedGitBinary,
			CodexHome:      resolvedCodexHome,
			WorkspaceRoot:  resolvedWorkspaceRoot,
			StateFile:      resolvedState,
			MaxWorkerSlots: *maxWorkerSlots,
		},
	}
	if err := runtimeconfig.Validate(cfg); err != nil {
		return writeError(stderr, err)
	}
	if err := ensureConfigAvailable(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	if err := validatePeerSetupAuthority(
		resolvedConfig,
		resolvedState,
		auth.TokenFile,
		resolvedCodexHome,
		resolvedWorkspaceRoot,
		transportConfig,
	); err != nil {
		return writeError(stderr, err)
	}
	for name, executable := range map[string]string{
		"CLI command": configuredCLI.Command,
		"Git binary":  resolvedGitBinary,
	} {
		if err := pathguard.ValidateManagedExecutable(
			name, executable, resolvedCodexHome, resolvedWorkspaceRoot,
		); err != nil {
			return writeError(stderr, err)
		}
	}
	if configuredCLI.Launcher != nil {
		if err := pathguard.ValidateManagedExecutable(
			"CLI launcher",
			configuredCLI.Launcher.Executable,
			resolvedCodexHome,
			resolvedWorkspaceRoot,
		); err != nil {
			return writeError(stderr, err)
		}
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
	if err := codexconfig.ValidateManagedRuntimeHome(networkHostKind, resolvedCodexHome); err != nil {
		return writeError(stderr, err)
	}
	if err := delegationconfig.PrepareWrite(resolvedConfig); err != nil {
		return writeError(stderr, err)
	}
	var preparedDirectories []string
	committed := false
	defer func() {
		if committed {
			return
		}
		for index := len(preparedDirectories) - 1; index >= 0; index-- {
			_ = os.Remove(preparedDirectories[index])
		}
	}()
	codexHomeCreated, err := prepareManagedDirectory(resolvedCodexHome, "managed CLI home")
	if err != nil {
		return writeError(stderr, err)
	}
	if codexHomeCreated {
		preparedDirectories = append(preparedDirectories, resolvedCodexHome)
	}
	workspaceCreated, err := prepareManagedDirectory(resolvedWorkspaceRoot, "worker workspace root")
	if err != nil {
		return writeError(stderr, err)
	}
	if workspaceCreated {
		preparedDirectories = append(preparedDirectories, resolvedWorkspaceRoot)
	}
	if err := validatePeerSetupAuthority(
		resolvedConfig,
		resolvedState,
		auth.TokenFile,
		resolvedCodexHome,
		resolvedWorkspaceRoot,
		transportConfig,
	); err != nil {
		return writeError(stderr, err)
	}
	if err := runtimeconfig.WriteNew(resolvedConfig, cfg); err != nil {
		return writeError(stderr, err)
	}
	committed = true
	return writeSetupResult(stdout, stderr, setupResult{
		Role:                 cfg.Role,
		InstanceID:           cfg.InstanceID,
		HostKind:             cfg.HostKind,
		ConfigPath:           resolvedConfig,
		ControllerID:         cfg.ControllerID,
		DeviceID:             cfg.DeviceID,
		StatePath:            cfg.Peer.StateFile,
		CodexHome:            cfg.Peer.CodexHome,
		GitBinary:            cfg.Peer.GitBinary,
		WorkspaceRoot:        cfg.Peer.WorkspaceRoot,
		TokenFile:            auth.TokenFile,
		Transport:            cfg.Transport.Mode,
		TailscaleHostname:    transportHostname(cfg.Transport),
		TailscaleStateDir:    transportStateDir(cfg.Transport),
		TailscaleAuthKeyFile: transportAuthKeyFile(cfg.Transport),
	}, *jsonOutput)
}

func resolveSetupTransport(
	flags *flag.FlagSet,
	rawMode, hostname, authKeyFile, stateDir, defaultStateDir string,
) (delegationconfig.TransportConfig, error) {
	switch rawMode {
	case "tcp":
		for _, name := range []string{
			"tailscale-hostname",
			"tailscale-auth-key-file",
			"tailscale-state-dir",
		} {
			if flagWasSet(flags, name) {
				return delegationconfig.TransportConfig{}, fmt.Errorf(
					"--%s requires --transport tailscale",
					name,
				)
			}
		}
		return delegationconfig.TransportConfig{Mode: delegationconfig.TransportModeTCP}, nil
	case "tailscale":
		if hostname == "" {
			return delegationconfig.TransportConfig{}, errors.New(
				"--tailscale-hostname is required with --transport tailscale",
			)
		}
		if authKeyFile == "" {
			return delegationconfig.TransportConfig{}, errors.New(
				"--tailscale-auth-key-file is required with --transport tailscale",
			)
		}
		resolvedAuthKeyFile, err := absolutePath(authKeyFile)
		if err != nil {
			return delegationconfig.TransportConfig{}, err
		}
		if stateDir == "" {
			stateDir = defaultStateDir
		}
		resolvedStateDir, err := absolutePath(stateDir)
		if err != nil {
			return delegationconfig.TransportConfig{}, err
		}
		if err := store.ValidateTailscaleStateDir(resolvedStateDir); err != nil {
			return delegationconfig.TransportConfig{}, err
		}
		if _, err := tailscaleauth.Read(resolvedAuthKeyFile); err != nil {
			return delegationconfig.TransportConfig{}, err
		}
		return delegationconfig.TransportConfig{
			Mode: delegationconfig.TransportModeTailscale,
			Tailscale: &delegationconfig.TailscaleConfig{
				StateDir:    resolvedStateDir,
				Hostname:    hostname,
				AuthKeyFile: resolvedAuthKeyFile,
			},
		}, nil
	default:
		return delegationconfig.TransportConfig{}, fmt.Errorf(
			"unsupported transport %q; must be tcp or tailscale",
			rawMode,
		)
	}
}

func validateBrokerSetupAuthority(
	configPath, statePath, tokenPath string,
	transport delegationconfig.TransportConfig,
) error {
	if transport.Mode != delegationconfig.TransportModeTailscale {
		return pathguard.ValidateBrokerAuthority(configPath, statePath, tokenPath)
	}
	return pathguard.ValidateBrokerTailscaleAuthority(
		configPath,
		statePath,
		tokenPath,
		transport.Tailscale.StateDir,
		transport.Tailscale.AuthKeyFile,
	)
}

func validatePeerSetupAuthority(
	configPath, statePath, tokenPath, codexHome, workspaceRoot string,
	transport delegationconfig.TransportConfig,
) error {
	if transport.Mode != delegationconfig.TransportModeTailscale {
		return pathguard.ValidatePeerRuntimeAuthority(
			configPath,
			statePath,
			tokenPath,
			codexHome,
			workspaceRoot,
		)
	}
	return pathguard.ValidatePeerTailscaleAuthority(
		configPath,
		statePath,
		tokenPath,
		codexHome,
		workspaceRoot,
		transport.Tailscale.StateDir,
		transport.Tailscale.AuthKeyFile,
	)
}

func transportHostname(transport delegationconfig.TransportConfig) string {
	if transport.Tailscale == nil {
		return ""
	}
	return transport.Tailscale.Hostname
}

func transportStateDir(transport delegationconfig.TransportConfig) string {
	if transport.Tailscale == nil {
		return ""
	}
	return transport.Tailscale.StateDir
}

func transportAuthKeyFile(transport delegationconfig.TransportConfig) string {
	if transport.Tailscale == nil {
		return ""
	}
	return transport.Tailscale.AuthKeyFile
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

func flagWasSet(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(flag *flag.Flag) {
		if flag.Name == name {
			found = true
		}
	})
	return found
}

func setupResourceRoot(configPath, instanceID string) string {
	configDirectory := filepath.Dir(configPath)
	if instanceID == delegationconfig.DefaultInstanceID {
		return configDirectory
	}
	if filepath.Base(configDirectory) == instanceID &&
		filepath.Base(filepath.Dir(configDirectory)) == "instances" {
		return configDirectory
	}
	return filepath.Join(configDirectory, "instances", instanceID)
}

func writeSetupResult(stdout, stderr io.Writer, result setupResult, jsonOutput bool) int {
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			return writeError(stderr, fmt.Errorf("encode setup result: %w", err))
		}
		return 0
	}
	fmt.Fprintf(stdout, "configured %s\n", result.Role)
	fmt.Fprintf(stdout, "instance: %s\n", result.InstanceID)
	fmt.Fprintf(stdout, "host kind: %s\n", result.HostKind)
	fmt.Fprintf(stdout, "config: %s\n", result.ConfigPath)
	fmt.Fprintf(stdout, "controllerId: %s\n", result.ControllerID)
	fmt.Fprintf(stdout, "transport: %s\n", transportModeName(result.Transport))
	if result.DeviceID != "" {
		fmt.Fprintf(stdout, "deviceId: %s\n", result.DeviceID)
	}
	if result.StatePath != "" {
		fmt.Fprintf(stdout, "state: %s\n", result.StatePath)
	}
	if result.CodexHome != "" {
		homeName := "CODEX_HOME"
		if result.HostKind == hostkind.TraeX {
			homeName = "TRAE_HOME"
		}
		fmt.Fprintf(stdout, "managed %s: %s\n", homeName, result.CodexHome)
	}
	if result.GitBinary != "" {
		fmt.Fprintf(stdout, "Git binary: %s\n", result.GitBinary)
	}
	if result.WorkspaceRoot != "" {
		fmt.Fprintf(stdout, "workspace root: %s\n", result.WorkspaceRoot)
	}
	if result.TokenFile != "" {
		fmt.Fprintf(stdout, "tokenFile: %s\n", result.TokenFile)
	}
	if result.StatusListen != "" {
		fmt.Fprintf(stdout, "status: http://%s/status\n", result.StatusListen)
	}
	if result.TailscaleHostname != "" {
		fmt.Fprintf(stdout, "Tailscale hostname: %s\n", result.TailscaleHostname)
	}
	if result.TailscaleStateDir != "" {
		fmt.Fprintf(stdout, "Tailscale state: %s\n", result.TailscaleStateDir)
	}
	if result.TailscaleAuthKeyFile != "" {
		fmt.Fprintf(stdout, "Tailscale enrollment key file: %s\n", result.TailscaleAuthKeyFile)
	}
	return 0
}

func transportModeName(mode delegationconfig.TransportMode) string {
	switch mode {
	case delegationconfig.TransportModeTCP:
		return "tcp"
	case delegationconfig.TransportModeTailscale:
		return "tailscale"
	default:
		return fmt.Sprintf("unknown(%d)", mode)
	}
}

func resolveCodexExecutable(name string) (string, error) {
	resolved, err := clicommand.Resolve(hostkind.Codex, name)
	if err != nil {
		return "", err
	}
	return resolved.CommandPath, nil
}

func resolveCLIExecutable(kind hostkind.Kind, name string) (string, error) {
	resolved, err := clicommand.Resolve(kind, name)
	if err != nil {
		return "", err
	}
	return resolved.CommandPath, nil
}

func resolveCLILauncher(name string, prefixArguments []string) (clilaunch.Spec, error) {
	if strings.TrimSpace(name) == "" {
		return clilaunch.Spec{}, errors.New("CLI launcher executable is required")
	}
	executable, err := exec.LookPath(name)
	if err != nil {
		return clilaunch.Spec{}, err
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return clilaunch.Spec{}, fmt.Errorf("resolve CLI launcher path: %w", err)
	}
	spec := clilaunch.Spec{
		Executable:      executable,
		PrefixArguments: append([]string(nil), prefixArguments...),
	}
	if _, err := clilaunch.Resolve(spec); err != nil {
		return clilaunch.Spec{}, err
	}
	return spec, nil
}

func resolveGitExecutable(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("Git executable is required")
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve Git executable path: %w", err)
	}
	runner, err := gitworkspace.NewRunner(resolved)
	if err != nil {
		return "", err
	}
	return runner.Binary, nil
}

func prepareManagedDirectory(path, description string) (bool, error) {
	_, statErr := os.Lstat(path)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return false, fmt.Errorf("inspect %s: %w", description, statErr)
	}
	created := errors.Is(statErr, os.ErrNotExist)
	if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
		return false, fmt.Errorf("prepare %s: %w", description, err)
	}
	return created, nil
}

func writeError(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "delegation: %v\n", err)
	return 1
}
