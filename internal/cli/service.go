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
	"github.com/GhostFlying/delegation/internal/userservice"
)

type serviceInstallResult struct {
	State      userservice.State `json:"state"`
	Kind       userservice.Kind  `json:"kind"`
	Artifact   string            `json:"artifact"`
	ConfigPath string            `json:"configPath"`
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
	defaultPath, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation service install", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	jsonOutput := flags.Bool("json", false, "print installation result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role == delegationconfig.RoleBroker {
		if _, err := loadBrokerAuthority(resolvedConfig, cfg); err != nil {
			return writeError(stderr, err)
		}
	} else if _, err := loadConnectorAuthority(resolvedConfig, cfg); err != nil {
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
	installed, err := userservice.Install(binaryPath, resolvedConfig)
	if err != nil && installed.State == "" {
		return writeError(stderr, err)
	}
	result := serviceInstallResult{
		State:      installed.State,
		Kind:       installed.Kind,
		Artifact:   installed.Artifact,
		ConfigPath: resolvedConfig,
	}
	outputErr := writeServiceInstallResult(stdout, result, *jsonOutput)
	if err != nil || outputErr != nil {
		return writeServiceInstallFailure(stderr, result, errors.Join(err, outputErr))
	}
	return 0
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
		fmt.Fprintln(&rendered, "activation: not attempted")
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
	defaultPath, err := delegationconfig.DefaultPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation service run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "configuration file path")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var runErr error
	switch cfg.Role {
	case delegationconfig.RoleBroker:
		runErr = runBrokerService(ctx, resolvedConfig, cfg, stderr, brokerRuntimeOptions{})
	case delegationconfig.RoleController, delegationconfig.RoleDevice:
		runErr = runConnectorService(ctx, resolvedConfig, cfg, stderr)
	default:
		runErr = fmt.Errorf("unsupported service role %q", cfg.Role)
	}
	if runErr != nil {
		return writeError(stderr, runErr)
	}
	return 0
}
