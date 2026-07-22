package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/userservice"
)

type serviceInstallResult struct {
	State           userservice.State `json:"state"`
	Kind            userservice.Kind  `json:"kind"`
	Artifact        string            `json:"artifact"`
	ConfigPath      string            `json:"configPath"`
	EnvironmentFile string            `json:"environmentFile,omitempty"`
}

func runService(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation service <install|run> [options]")
		return exitUsage
	}
	switch args[0] {
	case "install":
		return runServiceInstall(args[1:], stdout, stderr)
	case "run":
		return runServiceRuntime(args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported service action %q\n", args[0])
		return exitUsage
	}
}

func runServiceInstall(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation service install", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "broker or peer configuration file path (required)")
	environmentFile := flags.String(
		"environment-file",
		"",
		"protected peer provider environment file path (required for peer services)",
	)
	jsonOutput := flags.Bool("json", false, "print installation result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" {
		return writeError(stderr, errors.New("--config is required because broker and peer services may coexist"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedEnvironment := ""
	if *environmentFile != "" {
		resolvedEnvironment, err = absolutePath(*environmentFile)
		if err != nil {
			return writeError(stderr, err)
		}
	}
	if cfg.Role == delegationconfig.RoleBroker {
		if resolvedEnvironment != "" {
			return writeError(stderr, errors.New("broker service must not use --environment-file"))
		}
		if _, err := loadBrokerAuthority(resolvedConfig, cfg); err != nil {
			return writeError(stderr, err)
		}
	} else {
		if resolvedEnvironment == "" {
			return writeError(stderr, errors.New("peer service install requires --environment-file"))
		}
		if _, err := loadConnectorAuthority(resolvedConfig, cfg); err != nil {
			return writeError(stderr, err)
		}
		if err := validatePeerServiceEnvironmentPath(resolvedConfig, resolvedEnvironment, cfg); err != nil {
			return writeError(stderr, err)
		}
		if _, err := serviceenv.LoadProtectedFile(resolvedEnvironment); err != nil {
			return writeError(stderr, err)
		}
	}
	serviceRole, err := configuredServiceRole(cfg.Role)
	if err != nil {
		return writeError(stderr, err)
	}
	binaryPath, err := os.Executable()
	if err != nil {
		return writeError(stderr, fmt.Errorf("resolve runtime executable: %w", err))
	}
	binaryPath, err = absolutePath(binaryPath)
	if err != nil {
		return writeError(stderr, err)
	}
	invocation := userservice.Invocation{
		BinaryPath:      binaryPath,
		ConfigPath:      resolvedConfig,
		EnvironmentFile: resolvedEnvironment,
	}
	installed, err := userservice.Install(serviceRole, invocation)
	if err != nil && installed.State == "" {
		return writeError(stderr, err)
	}
	result := serviceInstallResult{
		State:           installed.State,
		Kind:            installed.Kind,
		Artifact:        installed.Artifact,
		ConfigPath:      resolvedConfig,
		EnvironmentFile: resolvedEnvironment,
	}
	outputErr := writeServiceInstallResult(stdout, result, *jsonOutput)
	if err != nil || outputErr != nil {
		return writeServiceInstallFailure(stderr, result, errors.Join(err, outputErr))
	}
	return 0
}

func configuredServiceRole(role delegationconfig.Role) (userservice.ServiceRole, error) {
	switch role {
	case delegationconfig.RoleBroker:
		return userservice.ServiceRoleBroker, nil
	case delegationconfig.RolePeer:
		return userservice.ServiceRolePeer, nil
	default:
		return "", fmt.Errorf("unsupported service role %q", role)
	}
}

func writeServiceInstallResult(output io.Writer, result serviceInstallResult, jsonOutput bool) error {
	var rendered bytes.Buffer
	if jsonOutput {
		if err := json.NewEncoder(&rendered).Encode(result); err != nil {
			return fmt.Errorf("encode service installation: %w", err)
		}
	} else {
		fmt.Fprintf(&rendered, "service state: %s\n", result.State)
		fmt.Fprintf(&rendered, "kind: %s\n", result.Kind)
		fmt.Fprintf(&rendered, "artifact: %s\n", result.Artifact)
		fmt.Fprintf(&rendered, "config: %s\n", result.ConfigPath)
		if result.EnvironmentFile != "" {
			fmt.Fprintf(&rendered, "environment file: %s\n", result.EnvironmentFile)
		}
		if result.State == userservice.StateActive {
			fmt.Fprintln(&rendered, "activation: enabled and started")
		} else {
			fmt.Fprintln(&rendered, "activation: not completed")
		}
	}
	if _, err := io.Copy(output, &rendered); err != nil {
		return fmt.Errorf("write service installation: %w", err)
	}
	return nil
}

func writeServiceInstallFailure(stderr io.Writer, result serviceInstallResult, err error) int {
	fmt.Fprintf(
		stderr,
		"delegation: service install state %s; kind %s; artifact %s; config %s: %v\n",
		result.State,
		result.Kind,
		result.Artifact,
		result.ConfigPath,
		err,
	)
	return 1
}

func runServiceRuntime(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation service run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "broker or peer configuration file path (required)")
	environmentFile := flags.String("environment-file", "", "protected peer provider environment file path")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" {
		return writeError(stderr, errors.New("--config is required because broker and peer services may coexist"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedEnvironment := ""
	if *environmentFile != "" {
		resolvedEnvironment, err = absolutePath(*environmentFile)
		if err != nil {
			return writeError(stderr, err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var runErr error
	switch cfg.Role {
	case delegationconfig.RoleBroker:
		if resolvedEnvironment != "" {
			return writeError(stderr, errors.New("broker service must not use --environment-file"))
		}
		runErr = runBrokerService(ctx, resolvedConfig, cfg, stderr, brokerRuntimeOptions{})
	case delegationconfig.RolePeer:
		if resolvedEnvironment != "" {
			if err := validatePeerServiceEnvironmentPath(resolvedConfig, resolvedEnvironment, cfg); err != nil {
				return writeError(stderr, err)
			}
			runErr = runConnectorServiceWithEnvironmentFile(
				ctx,
				resolvedConfig,
				cfg,
				resolvedEnvironment,
				stderr,
			)
		} else {
			runErr = runConnectorService(ctx, resolvedConfig, cfg, stderr)
		}
	default:
		runErr = fmt.Errorf("unsupported service role %q", cfg.Role)
	}
	if runErr != nil {
		return writeError(stderr, runErr)
	}
	return 0
}

func validatePeerServiceEnvironmentPath(
	configPath string,
	environmentPath string,
	cfg delegationconfig.Config,
) error {
	if cfg.Role != delegationconfig.RolePeer {
		return errors.New("peer service environment requires a peer configuration")
	}
	return pathguard.ValidatePeerServiceEnvironment(
		environmentPath,
		configPath,
		cfg.Peer.StateFile,
		cfg.Broker.Auth.TokenFile,
		cfg.Peer.CodexHome,
		cfg.Peer.WorkspaceRoot,
	)
}
