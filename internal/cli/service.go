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
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/traexrepair"
	"github.com/GhostFlying/delegation/internal/userservice"
	"github.com/GhostFlying/delegation/internal/workerreadiness"
)

type serviceInstallResult struct {
	State           userservice.State `json:"state"`
	Kind            userservice.Kind  `json:"kind"`
	Artifact        string            `json:"artifact"`
	ConfigPath      string            `json:"configPath"`
	EnvironmentFile string            `json:"environmentFile,omitempty"`
}

type serviceRepairResult struct {
	State           string              `json:"state"`
	ConfigPath      string              `json:"configPath"`
	QuarantinePath  string              `json:"quarantinePath"`
	ManifestPath    string              `json:"manifestPath"`
	ProfilesRemoved int                 `json:"profilesRemoved"`
	Quarantined     []traexrepair.Entry `json:"quarantined"`
}

type serviceRepairDependencies struct {
	run     func(context.Context, traexrepair.Options) (traexrepair.Result, error)
	qualify func(context.Context, string, string, delegationconfig.Config, string) error
}

func runService(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation service <install|repair|run> [options]")
		return exitUsage
	}
	switch args[0] {
	case "install":
		return runServiceInstall(args[1:], stdout, stderr)
	case "run":
		return runServiceRuntime(args[1:], stderr)
	case "repair":
		return runServiceRepair(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported service action %q\n", args[0])
		return exitUsage
	}
}

func runServiceRepair(args []string, stdout, stderr io.Writer) int {
	return runServiceRepairWithDependencies(
		args, stdout, stderr, serviceRepairDependencies{},
	)
}

func runServiceRepairWithDependencies(
	args []string, stdout, stderr io.Writer, dependencies serviceRepairDependencies,
) int {
	if dependencies.run == nil {
		dependencies.run = traexrepair.Run
	}
	if dependencies.qualify == nil {
		dependencies.qualify = qualifyTraeXRepair
	}
	flags := flag.NewFlagSet("delegation service repair", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "TraeX peer configuration file path (required)")
	environmentFile := flags.String("environment-file", "", "protected peer provider environment file path (required)")
	jsonOutput := flags.Bool("json", false, "print repair result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" || *environmentFile == "" {
		return writeError(stderr, errors.New("--config and --environment-file are required"))
	}
	if runtime.GOOS == "windows" {
		return writeError(stderr, errors.New("TraeX service repair is unsupported on Windows"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	resolvedEnvironment, err := absolutePath(*environmentFile)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, originalConfig, profilesRemoved, err := runtimeconfig.ReadForRepair(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role != delegationconfig.RolePeer || cfg.EffectiveHostKind() != hostkind.TraeX {
		return writeError(stderr, errors.New("service repair supports only TraeX peers"))
	}
	if err := validatePeerServiceEnvironmentPath(resolvedConfig, resolvedEnvironment, cfg); err != nil {
		return writeError(stderr, err)
	}
	if _, err := serviceenv.LoadProtectedFile(resolvedEnvironment); err != nil {
		return writeError(stderr, err)
	}
	replacementConfig, err := runtimeconfig.Encode(cfg)
	if err != nil {
		return writeError(stderr, err)
	}
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		return writeError(stderr, err)
	}
	defer lease.Close()
	result, err := dependencies.run(context.Background(), traexrepair.Options{
		ConfigPath: resolvedConfig, ManagedHome: cfg.Peer.CodexHome,
		OriginalConfig: originalConfig, ReplacementConfig: replacementConfig,
		Doctor: func(context.Context) error {
			repaired, err := runtimeconfig.Read(resolvedConfig)
			if err != nil {
				return err
			}
			if err := validatePeerServiceEnvironmentPath(resolvedConfig, resolvedEnvironment, repaired); err != nil {
				return err
			}
			if _, err := serviceenv.LoadProtectedFile(resolvedEnvironment); err != nil {
				return err
			}
			_, err = loadConnectorAuthority(resolvedConfig, repaired)
			return err
		},
		Smoke: func(ctx context.Context, transaction traexrepair.Result) error {
			return dependencies.qualify(
				ctx, resolvedConfig, resolvedEnvironment, cfg, transaction.QuarantinePath,
			)
		},
	})
	if err != nil {
		if errors.Is(err, traexrepair.ErrRollbackFailed) {
			if stateErr := persistRepairRollbackFailure(
				cfg.Peer.StateFile, resolvedConfig, resolvedEnvironment,
			); stateErr != nil {
				err = errors.Join(err, fmt.Errorf("persist rollback_failed readiness: %w", stateErr))
			}
		}
		return writeError(stderr, err)
	}
	if err := createRepairReadinessEpoch(
		cfg.Peer.StateFile, resolvedConfig, resolvedEnvironment,
	); err != nil {
		return writeError(stderr, fmt.Errorf(
			"repair committed but create readiness epoch failed; rerun worker recheck: %w", err,
		))
	}
	output := serviceRepairResult{
		State: "repaired", ConfigPath: resolvedConfig,
		QuarantinePath: result.QuarantinePath, ManifestPath: result.ManifestPath,
		ProfilesRemoved: profilesRemoved, Quarantined: result.Entries,
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(output); err != nil {
			return writeError(stderr, err)
		}
		return 0
	}
	fmt.Fprintln(stdout, "TraeX service repair: repaired")
	fmt.Fprintf(stdout, "config: %s\n", output.ConfigPath)
	fmt.Fprintf(stdout, "profiles removed: %d\n", output.ProfilesRemoved)
	fmt.Fprintf(stdout, "quarantine: %s\n", output.QuarantinePath)
	fmt.Fprintf(stdout, "manifest: %s\n", output.ManifestPath)
	return 0
}

func createRepairReadinessEpoch(statePath, configPath, environmentPath string) error {
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	runtimeDigest, configDigest, err := repairReadinessDigests(configPath, environmentPath)
	if err != nil {
		return err
	}
	_, err = state.RecheckWorkerReadiness(
		context.Background(), runtimeDigest, configDigest, time.Now().UnixMilli(),
	)
	return err
}

func persistRepairRollbackFailure(statePath, configPath, environmentPath string) error {
	state, err := store.OpenPeer(context.Background(), statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	if _, err := state.WorkerReadiness(context.Background()); errors.Is(err, store.ErrNotFound) {
		runtimeDigest, configDigest, digestErr := repairReadinessDigests(configPath, environmentPath)
		if digestErr != nil {
			return digestErr
		}
		if _, ensureErr := state.EnsureWorkerReadinessEpoch(
			context.Background(), runtimeDigest, configDigest, time.Now().UnixMilli(),
		); ensureErr != nil {
			return ensureErr
		}
	} else if err != nil {
		return err
	}
	_, err = state.FailWorkerReadiness(
		context.Background(), protocol.WorkerRepairRollbackFailed, time.Now().UnixMilli(),
	)
	return err
}

func repairReadinessDigests(configPath, environmentPath string) (string, string, error) {
	runtimePath, err := os.Executable()
	if err != nil {
		return "", "", err
	}
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		return "", "", err
	}
	runtimeDigest, err := workerreadiness.RuntimeDigest(runtimePath)
	if err != nil {
		return "", "", err
	}
	configDigest, err := workerreadiness.ConfigDigest(configPath, environmentPath)
	if err != nil {
		return "", "", err
	}
	return runtimeDigest, configDigest, nil
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
	cfg, err := runtimeconfig.Read(resolvedConfig)
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
		if err := validateExistingBrokerStateHost(cfg); err != nil {
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
		InstanceID:      cfg.EffectiveInstanceID(),
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
	resolvedEnvironment := ""
	if *environmentFile != "" {
		resolvedEnvironment, err = absolutePath(*environmentFile)
		if err != nil {
			return writeError(stderr, err)
		}
	}
	cfg, err := readServiceRuntimeConfig(
		resolvedConfig, resolvedEnvironment, runtime.GOOS,
	)
	if err != nil {
		return writeError(stderr, err)
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

func readServiceRuntimeConfig(
	configPath, environmentPath, operatingSystem string,
) (delegationconfig.Config, error) {
	cfg, _, validationErr := runtimeconfig.ReadForStartupClassification(configPath)
	if validationErr != nil {
		if !errors.Is(
			validationErr, delegationconfig.ErrPeerCLIProfileArgumentsUnsupported,
		) {
			return delegationconfig.Config{}, validationErr
		}
		repairable, _, removed, repairErr := runtimeconfig.ReadForRepair(configPath)
		if repairErr != nil || removed == 0 {
			return delegationconfig.Config{}, validationErr
		}
		persistErr := persistStoppedPeerReadinessFailure(
			repairable.Peer.StateFile, configPath, environmentPath,
			protocol.WorkerProfileUnsupported,
		)
		return delegationconfig.Config{}, errors.Join(validationErr, persistErr)
	}
	if cfg.Role == delegationconfig.RolePeer && cfg.EffectiveHostKind() == hostkind.TraeX &&
		operatingSystem == "windows" {
		unsupportedErr := errors.New("TraeX worker host is unsupported on Windows")
		persistErr := persistStoppedPeerReadinessFailure(
			cfg.Peer.StateFile, configPath, environmentPath, protocol.WorkerHostUnsupported,
		)
		return delegationconfig.Config{}, errors.Join(unsupportedErr, persistErr)
	}
	return cfg, nil
}

func persistStoppedPeerReadinessFailure(
	statePath, configPath, environmentPath, failureCode string,
) error {
	lease, err := store.AcquirePeerLease(statePath)
	if err != nil {
		return fmt.Errorf("acquire peer lease to persist %s readiness: %w", failureCode, err)
	}
	defer lease.Close()
	readiness, err := persistWorkerReadinessFailure(
		context.Background(), statePath, configPath, environmentPath, failureCode,
	)
	if err != nil {
		return err
	}
	return readinessInterventionError(readiness)
}

func persistWorkerReadinessFailure(
	ctx context.Context, statePath, configPath, environmentPath, failureCode string,
) (protocol.WorkerReadiness, error) {
	state, err := store.OpenPeer(ctx, statePath)
	if err != nil {
		return protocol.WorkerReadiness{}, err
	}
	defer state.Close()
	runtimeDigest, configDigest, err := repairReadinessDigests(configPath, environmentPath)
	if err != nil {
		return protocol.WorkerReadiness{}, err
	}
	if _, err := state.EnsureWorkerReadinessEpoch(
		ctx, runtimeDigest, configDigest, time.Now().UnixMilli(),
	); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	return state.FailWorkerReadiness(ctx, failureCode, time.Now().UnixMilli())
}

func validatePeerServiceEnvironmentPath(
	configPath string,
	environmentPath string,
	cfg delegationconfig.Config,
) error {
	if cfg.Role != delegationconfig.RolePeer {
		return errors.New("peer service environment requires a peer configuration")
	}
	if cfg.Transport.Mode == delegationconfig.TransportModeTailscale {
		tailscaleConfig := cfg.Transport.Tailscale
		if tailscaleConfig == nil {
			return errors.New("peer tailscale configuration is required")
		}
		return pathguard.ValidatePeerTailscaleServiceEnvironment(
			environmentPath,
			configPath,
			cfg.Peer.StateFile,
			cfg.Broker.Auth.TokenFile,
			cfg.Peer.CodexHome,
			cfg.Peer.WorkspaceRoot,
			tailscaleConfig.StateDir,
			tailscaleConfig.AuthKeyFile,
		)
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
