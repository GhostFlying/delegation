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

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/localupgrade"
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
		fmt.Fprintln(stderr, "usage: delegation service <install|repair|upgrade|run> [options]")
		return exitUsage
	}
	switch args[0] {
	case "install":
		return runServiceInstall(args[1:], stdout, stderr)
	case "run":
		return runServiceRuntime(args[1:], stderr)
	case "repair":
		return runServiceRepair(args[1:], stdout, stderr)
	case "upgrade":
		return runServiceUpgrade(args[1:], stdout, stderr)
	case "upgrade-compatibility":
		return runServiceUpgradeCompatibility(args[1:], stdout, stderr)
	case "upgrade-activate":
		return runServiceUpgradeActivator(args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "delegation: unsupported service action %q\n", args[0])
		return exitUsage
	}
}

func runServiceUpgrade(args []string, stdout, stderr io.Writer) int {
	return runServiceUpgradeWithDependencies(args, stdout, stderr, serviceUpgradeCommandDependencies{})
}

type serviceUpgradeCommandDependencies struct {
	bootstrap func(context.Context, delegationconfig.Config, string, string, string) (localbridge.UpgradeSnapshot, error)
	cancel    func(context.Context, delegationconfig.Config, string, string) (localbridge.UpgradeSnapshot, error)
}

func runServiceUpgradeWithDependencies(
	args []string, stdout, stderr io.Writer, dependencies serviceUpgradeCommandDependencies,
) int {
	if dependencies.bootstrap == nil {
		dependencies.bootstrap = bootstrapServiceUpgrade
	}
	if dependencies.cancel == nil {
		dependencies.cancel = cancelServiceUpgrade
	}
	flags := flag.NewFlagSet("delegation service upgrade", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "broker or peer configuration file path (required)")
	targetVersion := flags.String("target-version", "", "canonical target release version")
	environmentFile := flags.String("environment-file", "", "protected peer provider environment file")
	bootstrap := flags.Bool("bootstrap", false, "authorize this local service upgrade")
	cancelUpgrade := flags.Bool("cancel", false, "cancel a pre-COMMIT transaction")
	transactionID := flags.String("transaction-id", "", "upgrade transaction ID")
	timeout := flags.Duration("timeout", 30*time.Minute, "bounded upgrade request timeout")
	jsonOutput := flags.Bool("json", false, "print upgrade result as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" || *timeout <= 0 {
		return writeError(stderr, errors.New("--config and a positive --timeout are required"))
	}
	if *cancelUpgrade {
		if *transactionID == "" || *targetVersion != "" || *environmentFile != "" || *bootstrap {
			return writeError(stderr, errors.New("--cancel requires --transaction-id and excludes --target-version, --environment-file, and --bootstrap"))
		}
	} else if *targetVersion == "" || *transactionID != "" {
		return writeError(stderr, errors.New("upgrade requires --target-version and excludes --transaction-id"))
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
	cfg, _, _, _, err := runtimeconfig.ReadForUpgrade(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role == delegationconfig.RolePeer && !*cancelUpgrade {
		if resolvedEnvironment == "" {
			return writeError(stderr, errors.New("peer service upgrade requires --environment-file"))
		}
		if !*cancelUpgrade && !*bootstrap {
			return writeError(stderr, errors.New("peer service upgrade requires --bootstrap or broker coordination"))
		}
	} else if cfg.Role == delegationconfig.RoleBroker && resolvedEnvironment != "" {
		return writeError(stderr, errors.New("broker service upgrade must not use --environment-file"))
	}
	if !*cancelUpgrade && !*bootstrap {
		return writeError(stderr, errors.New("coordinated broker upgrade is not available without --bootstrap"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var result localbridge.UpgradeSnapshot
	if *cancelUpgrade {
		result, err = dependencies.cancel(ctx, cfg, resolvedConfig, *transactionID)
	} else {
		result, err = dependencies.bootstrap(
			ctx, cfg, resolvedConfig, resolvedEnvironment, *targetVersion,
		)
	}
	if err != nil {
		return writeError(stderr, err)
	}
	return writeServiceUpgradeResult(stdout, stderr, result, *jsonOutput)
}

func activateAndWaitForLocalUpgrade(
	ctx context.Context, endpoint string, current localbridge.UpgradeSnapshot,
) (localbridge.UpgradeSnapshot, error) {
	return activateAndWaitForUpgrade(
		ctx, current,
		func(ctx context.Context, transactionID string) (localbridge.UpgradeSnapshot, error) {
			return localbridge.ActivateUpgrade(ctx, endpoint, transactionID)
		},
		func(ctx context.Context) (*localbridge.UpgradeSnapshot, error) {
			return localbridge.ReadUpgrade(ctx, endpoint)
		},
	)
}

func activateAndWaitForUpgrade(
	ctx context.Context, current localbridge.UpgradeSnapshot,
	activate func(context.Context, string) (localbridge.UpgradeSnapshot, error),
	read func(context.Context) (*localbridge.UpgradeSnapshot, error),
) (localbridge.UpgradeSnapshot, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	last := current
	var lastActivationErr error
	for {
		switch last.State {
		case string(localupgrade.StateCommitted):
			return last, nil
		case string(localupgrade.StateRolledBack), string(localupgrade.StateRollbackFailed):
			return last, fmt.Errorf("local upgrade ended in %s", last.State)
		case string(localupgrade.StateArmed), string(localupgrade.StateActivating),
			string(localupgrade.StateForwardRecoveryRequired):
			next, err := activate(ctx, last.TransactionID)
			if err == nil {
				last = next
				lastActivationErr = nil
			} else {
				lastActivationErr = err
			}
		}
		select {
		case <-ctx.Done():
			return last, errors.Join(
				fmt.Errorf("wait for local upgrade transaction %s: %w", last.TransactionID, ctx.Err()),
				lastActivationErr,
			)
		case <-ticker.C:
		}
		next, err := read(ctx)
		if err != nil {
			continue
		}
		if next == nil || next.TransactionID != current.TransactionID {
			return last, errors.New("local upgrade status changed transaction identity")
		}
		last = *next
	}
}

func writeServiceUpgradeResult(
	stdout, stderr io.Writer, result localbridge.UpgradeSnapshot, jsonOutput bool,
) int {
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			return writeError(stderr, err)
		}
		return 0
	}
	fmt.Fprintf(stdout, "upgrade transaction: %s\n", result.TransactionID)
	fmt.Fprintf(stdout, "state: %s\n", result.State)
	fmt.Fprintf(stdout, "version: %s -> %s\n", result.SourceVersion, result.TargetVersion)
	fmt.Fprintf(stdout, "commit authorized: %t\n", result.CommitAuthorized)
	if result.FailureCode != "" {
		fmt.Fprintf(stdout, "failure: %s\n", result.FailureCode)
	}
	return 0
}

func runServiceUpgradeCompatibility(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation service upgrade-compatibility", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "configuration path")
	environmentFile := flags.String("environment-file", "", "peer environment path")
	jsonOutput := flags.Bool("json", false, "print JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" || !*jsonOutput {
		return writeError(stderr, errors.New("--config and --json are required"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, err := runtimeconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role == delegationconfig.RolePeer && *environmentFile == "" {
		return writeError(stderr, errors.New("peer compatibility probe requires --environment-file"))
	}
	if cfg.Role == delegationconfig.RoleBroker && *environmentFile != "" {
		return writeError(stderr, errors.New("broker compatibility probe must not use --environment-file"))
	}
	compatibility, err := localupgrade.CurrentCompatibility(cfg.Role, buildinfo.Version)
	if err != nil {
		return writeError(stderr, err)
	}
	if err := json.NewEncoder(stdout).Encode(compatibility); err != nil {
		return writeError(stderr, err)
	}
	return 0
}

func runServiceUpgradeActivator(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("delegation service upgrade-activate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	upgradeRoot := flags.String("upgrade-root", "", "protected upgrade root")
	transactionID := flags.String("transaction-id", "", "upgrade transaction ID")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *upgradeRoot == "" || *transactionID == "" {
		return writeError(stderr, errors.New("--upgrade-root and --transaction-id are required"))
	}
	resolvedRoot, err := absolutePath(*upgradeRoot)
	if err != nil {
		return writeError(stderr, err)
	}
	transactionStore, err := localupgrade.OpenExistingStore(resolvedRoot)
	if err != nil {
		return writeError(stderr, err)
	}
	journal, err := transactionStore.Load()
	if err != nil || journal.TransactionID != *transactionID {
		return writeError(stderr, errors.Join(err, errors.New("activator transaction identity mismatch")))
	}
	if err := validateUpgradeActivatorRuntime(journal); err != nil {
		return writeError(stderr, err)
	}
	cfg := delegationconfig.Config{
		Role: journal.Role, InstanceID: journal.InstanceID, ControllerID: journal.ControllerID,
		DeviceID: journal.DeviceID,
	}
	rootParent := filepath.Dir(filepath.Dir(filepath.Dir(resolvedRoot)))
	wantRoot, err := localupgrade.RootForConfig(rootParent, cfg)
	if err != nil || wantRoot != resolvedRoot || cfg.Role != journal.Role ||
		cfg.ControllerID != journal.ControllerID || cfg.DeviceID != journal.DeviceID ||
		cfg.EffectiveInstanceID() != journal.InstanceID {
		return writeError(stderr, errors.Join(err, errors.New("activator root or service identity mismatch")))
	}
	activator, err := localupgrade.NewActivator(
		transactionStore, localupgrade.DefaultActivationOperations(qualifyLocalUpgrade),
	)
	if err != nil {
		return writeError(stderr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	_, err = activator.Run(ctx, *transactionID)
	if err != nil {
		return writeError(stderr, err)
	}
	return 0
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
