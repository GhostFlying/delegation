package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/codexcommand"
	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/pathguard"
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
	token, err := loadConnectorAuthority(configPath, cfg)
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
	codexLaunch, err := codexcommand.Resolve(cfg.Peer.CodexBinary)
	if err != nil {
		return fmt.Errorf("resolve configured Codex command: %w", err)
	}
	codexEnvironment := make(map[string]string, len(codexLaunch.Environment)+len(providerEnvironment.Environment))
	for name, value := range codexLaunch.Environment {
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
	workers, err := workerhost.New(ctx, workerhost.Options{
		ControllerID: cfg.ControllerID, DeviceID: cfg.DeviceID,
		PeerConfigPath: configPath, DelegationBinary: runtimeBinary,
		CodexBinary: codexLaunch.NativePath, CodexHome: cfg.Peer.CodexHome,
		CodexEnvironment:        codexEnvironment,
		CodexUnsetEnvironment:   codexLaunch.UnsetEnvironment,
		ProviderEnvironmentFile: environmentFile,
		WorkspaceRoot:           cfg.Peer.WorkspaceRoot, MaxWorkerSlots: cfg.Peer.MaxWorkerSlots,
		CodexConfig: providerEnvironment.Config, Store: peerState,
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
	workerManager := managedWorkerSpawner{
		host: workers, state: peerState,
		controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
	}
	client, err := connector.New(connector.Options{
		BrokerURL:                cfg.Broker.URL,
		AllowInsecureNonLoopback: cfg.Broker.AllowInsecureNonLoopback,
		ControllerID:             cfg.ControllerID,
		DeviceID:                 cfg.DeviceID,
		DeviceName:               cfg.DeviceName,
		AuthMode:                 cfg.Broker.Auth.Mode,
		Token:                    token,
		WorkerSpawner:            workerManager,
		WorkerController:         workerManager,
		WorkerLifecycleSource: managedWorkerLifecycleSource{
			host: workers, controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
		},
		ReportError: func(err error) {
			_ = writeStderr("delegation: connector reconnecting: %v\n", err)
		},
	})
	if err != nil {
		return err
	}
	endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
	if err != nil {
		return err
	}
	bridge, err := localbridge.ListenWithAuthorization(
		endpoint,
		localbridge.ServiceIdentity{
			ControllerID: cfg.ControllerID,
			DeviceID:     cfg.DeviceID,
		},
		client,
		peerAuthorizer{
			state: peerState, controllerID: cfg.ControllerID, deviceID: cfg.DeviceID,
		},
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
		store.WorkerInterrupted, store.WorkerFailed:
		return errors.New("worker reservation is not active")
	default:
		return errors.New("worker reservation has an unsupported status")
	}
}

func loadConnectorAuthority(
	configPath string,
	cfg delegationconfig.Config,
) (*tokenfile.Token, error) {
	if cfg.Role != delegationconfig.RolePeer {
		return nil, errors.New("connector runtime requires a peer configuration")
	}
	if err := pathguard.ValidatePeerRuntimeAuthority(
		configPath,
		cfg.Peer.StateFile,
		cfg.Broker.Auth.TokenFile,
		cfg.Peer.CodexHome,
		cfg.Peer.WorkspaceRoot,
	); err != nil {
		return nil, err
	}
	if err := delegationconfig.ValidatePrivateDirectory(cfg.Peer.CodexHome); err != nil {
		return nil, fmt.Errorf("validate managed CODEX_HOME: %w", err)
	}
	if err := codexconfig.ValidateManagedHome(cfg.Peer.CodexHome); err != nil {
		return nil, err
	}
	if err := delegationconfig.ValidatePrivateDirectory(cfg.Peer.WorkspaceRoot); err != nil {
		return nil, fmt.Errorf("validate managed workspace root: %w", err)
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeNone {
		return nil, nil
	}
	token, err := tokenfile.Read(cfg.Broker.Auth.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read peer token: %w", err)
	}
	return &token, nil
}
