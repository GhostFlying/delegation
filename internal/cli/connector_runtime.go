package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/clicommand"
	"github.com/GhostFlying/delegation/internal/clilaunch"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/gitworkspace"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/resultpackagefiles"
	"github.com/GhostFlying/delegation/internal/rootapply"
	"github.com/GhostFlying/delegation/internal/serviceenv"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/GhostFlying/delegation/internal/workerhost"
)

func runConnectorService(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
	stderr io.Writer,
) (resultErr error) {
	return runConnectorServiceWithProviderEnvironment(
		ctx,
		configPath,
		cfg,
		"",
		serviceenv.LoadInherited,
		stderr,
	)
}

func runConnectorServiceWithEnvironmentFile(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
	environmentFile string,
	stderr io.Writer,
) error {
	return runConnectorServiceWithProviderEnvironment(
		ctx,
		configPath,
		cfg,
		environmentFile,
		func() (serviceenv.Resolved, error) {
			return serviceenv.LoadProtectedFile(environmentFile)
		},
		stderr,
	)
}

func runConnectorServiceWithProviderEnvironment(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
	environmentFile string,
	loadProviderEnvironment func() (serviceenv.Resolved, error),
	stderr io.Writer,
) (resultErr error) {
	authority, err := loadConnectorAuthority(configPath, cfg)
	if err != nil {
		return err
	}
	if err := writeInsecureTransportWarning(stderr, cfg); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	if loadProviderEnvironment == nil {
		return errors.New("managed provider environment loader is required")
	}
	if environmentFile != "" {
		if err := validatePeerServiceEnvironmentPath(configPath, environmentFile, cfg); err != nil {
			return err
		}
	}
	providerEnvironment, err := loadProviderEnvironment()
	if err != nil {
		return err
	}
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		return err
	}
	peerState, err := store.OpenPeer(ctx, cfg.Peer.StateFile)
	if err != nil {
		return errors.Join(err, lease.Close())
	}
	closeResources := func() error {
		return errors.Join(peerState.Close(), lease.Close())
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeResources())
	}()
	cliLaunch := authority.cliLaunch
	appServerLaunch := authority.appServerLaunch
	codexEnvironment := make(map[string]string, len(cliLaunch.Environment)+len(providerEnvironment.Environment))
	for name, value := range cliLaunch.Environment {
		codexEnvironment[name] = value
	}
	for name, value := range providerEnvironment.Environment {
		codexEnvironment[name] = value
	}
	runtimeBinary, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve delegation executable: %w", err)
	}
	runtimeBinary, err = filepath.Abs(runtimeBinary)
	if err != nil {
		return fmt.Errorf("resolve delegation executable: %w", err)
	}
	if target, evalErr := filepath.EvalSymlinks(runtimeBinary); evalErr == nil {
		runtimeBinary = target
	} else {
		return fmt.Errorf("resolve delegation executable: %w", evalErr)
	}
	var stderrMu sync.Mutex
	writeStderr := func(format string, args ...any) error {
		stderrMu.Lock()
		defer stderrMu.Unlock()
		_, err := fmt.Fprintf(stderr, format, args...)
		return err
	}
	resultPackages, err := resultpackagefiles.New(ctx, resultpackagefiles.Options{
		ControllerID:  cfg.ControllerID,
		DeviceID:      cfg.DeviceID,
		WorkspaceRoot: cfg.Peer.WorkspaceRoot,
		Store:         peerState,
	})
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, resultPackages.Close())
	}()
	workers, err := workerhost.New(ctx, workerhost.Options{
		ControllerID: cfg.ControllerID, DeviceID: cfg.DeviceID,
		HostKind:       cfg.EffectiveHostKind(),
		PeerConfigPath: configPath, DelegationBinary: runtimeBinary,
		CLILaunch:               appServerLaunch,
		CLIRuntimeExecutable:    cliLaunch.RuntimePath,
		GitBinary:               cfg.Peer.GitBinary,
		CodexHome:               cfg.Peer.CodexHome,
		CodexEnvironment:        codexEnvironment,
		CodexUnsetEnvironment:   cliLaunch.UnsetEnvironment,
		ProviderEnvironmentFile: environmentFile,
		WorkspaceRoot:           cfg.Peer.WorkspaceRoot, MaxWorkerSlots: cfg.Peer.MaxWorkerSlots,
		CodexConfig: providerEnvironment.Config, Store: peerState,
		ResultPackages: resultPackages,
		ReportError: func(err error) {
			_ = writeStderr("delegation: managed worker host: %v\n", err)
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeWorkerHost(workers, 30*time.Second))
	}()
	changesHost, ok := any(workers).(managedChangesArtifactHost)
	if !ok {
		return errors.New("managed worker host does not provide changes artifact publication")
	}
	changesSource := managedChangesArtifactSource{
		host: changesHost, workers: workers,
		controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
	}
	workerManager := managedWorkerSpawner{
		host: workers, state: peerState,
		controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
	}
	excludedGitEnvironment := []string{"CODEX_ACCESS_TOKEN", "CODEX_API_KEY", "OPENAI_API_KEY"}
	excludedGitEnvironment = append(
		excludedGitEnvironment,
		codexconfig.CredentialEnvironmentVariables(providerEnvironment.Config)...,
	)
	applyRunner, err := gitworkspace.NewRunner(cfg.Peer.GitBinary, excludedGitEnvironment...)
	if err != nil {
		return fmt.Errorf("create root apply Git runner: %w", err)
	}
	resultApplies, err := rootapply.New(rootapply.Options{
		WorkspaceRoot: cfg.Peer.WorkspaceRoot,
		Runner:        applyRunner,
		Packages:      resultPackages,
	})
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, resultApplies.Close())
	}()
	workerManager.resultPackages = resultPackages
	resultSource := managedResultPackageSource{
		packages: resultPackages, state: peerState,
		controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
	}
	client, err := connector.New(connector.Options{
		BrokerURL:                cfg.Broker.URL,
		AllowInsecureNonLoopback: cfg.Broker.AllowInsecureNonLoopback,
		ControllerID:             cfg.ControllerID,
		DeviceID:                 cfg.DeviceID,
		DeviceName:               cfg.DeviceName,
		HostKind:                 cfg.EffectiveHostKind(),
		AuthMode:                 cfg.Broker.Auth.Mode,
		Token:                    authority.token,
		WorkerSpawner:            workerManager,
		WorkerController:         workerManager,
		WorkerLifecycleSource: managedWorkerLifecycleSource{
			host: workers, controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
		},
		ChangesArtifactSource: changesSource,
		ResultPackageSource:   resultSource,
		WorkspaceManager:      workerManager,
		ResultPackageManager:  workerManager,
		ReportError: func(err error) {
			_ = reportConnectorError(writeStderr, err)
		},
	})
	if err != nil {
		return err
	}
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil {
		return err
	}
	bridgeIdentity := localbridge.ServiceIdentity{
		ControllerID: cfg.ControllerID,
		DeviceID:     cfg.DeviceID,
	}
	if cfg.EffectiveInstanceID() != delegationconfig.DefaultInstanceID {
		bridgeIdentity.InstanceID = cfg.EffectiveInstanceID()
	}
	bridge, err := localbridge.ListenWithResultApply(
		endpoint,
		bridgeIdentity,
		client,
		peerAuthorizer{
			state: peerState, controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
		},
		peerLocalStatusProvider{
			client: client, state: peerState,
			controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
			deviceName: cfg.DeviceName, maxWorkerSlots: cfg.Peer.MaxWorkerSlots,
		},
		localResultPackageAvailabilityProvider{manager: resultPackages},
		resultApplies,
	)
	if err != nil {
		return err
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	connectorDone := make(chan error, 1)
	bridgeDone := make(chan error, 1)
	go func() {
		connectorDone <- client.Run(runContext)
	}()
	go func() {
		bridgeDone <- bridge.Serve(runContext)
	}()
	if err := writeStderr("delegation: %s connector service started\n", cfg.Role); err != nil {
		cancel()
		_ = bridge.Close()
		<-connectorDone
		<-bridgeDone
		return fmt.Errorf("write connector readiness: %w", err)
	}

	var firstName string
	var firstErr error
	select {
	case <-ctx.Done():
	case firstErr = <-connectorDone:
		firstName = "connector"
	case firstErr = <-bridgeDone:
		firstName = "local bridge"
	case <-workers.Done():
		firstName = "managed worker host"
		firstErr = workers.Err()
	}
	cancel()
	closeErr := bridge.Close()
	var connectorErr, bridgeErr error
	if firstName == "connector" {
		connectorErr = firstErr
		bridgeErr = <-bridgeDone
	} else if firstName == "local bridge" {
		bridgeErr = firstErr
		connectorErr = <-connectorDone
	} else {
		connectorErr = <-connectorDone
		bridgeErr = <-bridgeDone
	}
	if ctx.Err() != nil {
		return errors.Join(closeErr, connectorErr, bridgeErr)
	}
	if firstErr == nil {
		firstErr = errors.New("stopped unexpectedly")
	}
	return errors.Join(fmt.Errorf("%s stopped: %w", firstName, firstErr), closeErr, connectorErr, bridgeErr)
}

func reportConnectorError(writeStderr func(string, ...any) error, err error) error {
	if errors.Is(err, connector.ErrStateRecoveryRequired) {
		return writeStderr("delegation: connector halted; state recovery required: %v\n", err)
	}
	return writeStderr("delegation: connector reconnecting: %v\n", err)
}

func resolveConfiguredCLILaunch(
	configured delegationconfig.CLIConfig,
	runtimeExecutable string,
) (clilaunch.Spec, error) {
	launch := clilaunch.Spec{
		Executable:      runtimeExecutable,
		PrefixArguments: slices.Clone(configured.Arguments),
	}
	if configured.Launcher != nil {
		launch.Executable = configured.Launcher.Executable
		launch.PrefixArguments = append(
			slices.Clone(configured.Launcher.PrefixArguments),
			runtimeExecutable,
		)
		launch.PrefixArguments = append(launch.PrefixArguments, configured.Arguments...)
	}
	resolved, err := clilaunch.Resolve(launch)
	if err != nil {
		return clilaunch.Spec{}, fmt.Errorf("resolve configured CLI launch: %w", err)
	}
	return resolved, nil
}

type workerHostCloser interface {
	Close(context.Context) error
}

func closeWorkerHost(host workerHostCloser, timeout time.Duration) error {
	closeContext, cancel := context.WithTimeout(context.Background(), timeout)
	boundedErr := host.Close(closeContext)
	cancel()
	if !errors.Is(boundedErr, context.DeadlineExceeded) &&
		!errors.Is(boundedErr, context.Canceled) {
		return boundedErr
	}

	// Host shutdown owns terminal worker-state writes. Waiting synchronously
	// keeps the peer store and process lease alive until those writes finish;
	// returning here would let cmd/delegation's os.Exit kill deferred cleanup.
	terminalErr := host.Close(context.Background())
	return errors.Join(boundedErr, terminalErr)
}

type peerAuthorizer struct {
	state        *store.PeerStore
	controllerID string
	deviceID     string
}

func (a peerAuthorizer) ManagedWorkerThread(
	ctx context.Context,
	controllerID, externalThreadID string,
) (bool, error) {
	if a.state == nil || controllerID != a.controllerID {
		return false, errors.New("root thread does not belong to this peer network")
	}
	_, err := a.state.WorkerForThread(ctx, controllerID, externalThreadID)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (a peerAuthorizer) AuthorizeWorker(
	ctx context.Context,
	principal control.PrincipalIdentity,
) error {
	if a.state == nil || principal.ControllerID != a.controllerID ||
		principal.ParentAgentID == "" || principal.DeviceID != a.deviceID {
		return errors.New("worker principal does not belong to this peer")
	}
	worker, err := a.state.GetWorker(ctx, store.WorkerKey{
		ControllerID: principal.ControllerID,
		TreeID:       principal.TreeID,
		AgentID:      principal.AgentID,
	})
	if err != nil {
		return err
	}
	if worker.ParentAgentID != principal.ParentAgentID || worker.DeviceID != principal.DeviceID {
		return errors.New("worker principal does not match its reservation")
	}
	switch worker.Status {
	case store.WorkerPreflight, store.WorkerReady, store.WorkerRunning, store.WorkerIdle:
		return nil
	case store.WorkerReserved, store.WorkerPending, store.WorkerStarting,
		store.WorkerFinalizing, store.WorkerInterrupted, store.WorkerFailed:
		return errors.New("worker reservation is not active")
	default:
		return errors.New("worker reservation has an unsupported status")
	}
}

type connectorAuthority struct {
	token           *tokenfile.Token
	cliLaunch       clicommand.Launch
	appServerLaunch clilaunch.Spec
}

func loadConnectorAuthority(
	configPath string,
	cfg delegationconfig.Config,
) (connectorAuthority, error) {
	if cfg.Role != delegationconfig.RolePeer {
		return connectorAuthority{}, errors.New("connector runtime requires a peer configuration")
	}
	if err := pathguard.ValidatePeerRuntimeAuthority(
		configPath,
		cfg.Peer.StateFile,
		cfg.Broker.Auth.TokenFile,
		cfg.Peer.CodexHome,
		cfg.Peer.WorkspaceRoot,
	); err != nil {
		return connectorAuthority{}, err
	}
	configuredCLI := cfg.Peer.EffectiveCLI()
	for name, executable := range map[string]string{
		"CLI command": configuredCLI.Command,
		"Git binary":  cfg.Peer.GitBinary,
	} {
		if err := pathguard.ValidateManagedExecutable(
			name, executable, cfg.Peer.CodexHome, cfg.Peer.WorkspaceRoot,
		); err != nil {
			return connectorAuthority{}, err
		}
	}
	if configuredCLI.Launcher != nil {
		if err := pathguard.ValidateManagedExecutable(
			"CLI launcher",
			configuredCLI.Launcher.Executable,
			cfg.Peer.CodexHome,
			cfg.Peer.WorkspaceRoot,
		); err != nil {
			return connectorAuthority{}, err
		}
	}
	cliLaunch, err := clicommand.Resolve(cfg.EffectiveHostKind(), configuredCLI.Command)
	if err != nil {
		return connectorAuthority{}, fmt.Errorf("resolve configured CLI command: %w", err)
	}
	appServerLaunch, err := resolveConfiguredCLILaunch(configuredCLI, cliLaunch.RuntimePath)
	if err != nil {
		return connectorAuthority{}, err
	}
	for name, executable := range map[string]string{
		"resolved CLI command":  cliLaunch.CommandPath,
		"CLI runtime":           cliLaunch.RuntimePath,
		"resolved CLI launcher": appServerLaunch.Executable,
	} {
		if err := pathguard.ValidateManagedExecutable(
			name, executable, cfg.Peer.CodexHome, cfg.Peer.WorkspaceRoot,
		); err != nil {
			return connectorAuthority{}, err
		}
	}
	if err := delegationconfig.ValidatePrivateDirectory(cfg.Peer.CodexHome); err != nil {
		return connectorAuthority{}, fmt.Errorf("validate managed CLI home: %w", err)
	}
	if err := codexconfig.ValidateManagedRuntimeHome(
		cfg.EffectiveHostKind(),
		cfg.Peer.CodexHome,
	); err != nil {
		return connectorAuthority{}, err
	}
	if err := delegationconfig.ValidatePrivateDirectory(cfg.Peer.WorkspaceRoot); err != nil {
		return connectorAuthority{}, fmt.Errorf("validate managed workspace root: %w", err)
	}
	authority := connectorAuthority{
		cliLaunch:       cliLaunch,
		appServerLaunch: appServerLaunch,
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeNone {
		return authority, nil
	}
	token, err := tokenfile.Read(cfg.Broker.Auth.TokenFile)
	if err != nil {
		return connectorAuthority{}, fmt.Errorf("read peer token: %w", err)
	}
	authority.token = &token
	return authority, nil
}
