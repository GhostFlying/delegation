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
	"path/filepath"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/coordinatedupgrade"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/localupgrade"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
	"github.com/GhostFlying/delegation/internal/statuspage"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	peerStatusReadTimeout        = 5 * time.Second
	maximumStatusOutput          = 16 * 1024
	peerStatusUnavailableError   = "delegation: peer status unavailable; ensure the peer service is running\n"
	brokerStatusUnavailableError = "delegation: broker status unavailable; ensure the broker service and status listener are running\n"
	statusOutputError            = "delegation: write status output failed\n"
)

type connectorStatusSource interface {
	Status() connector.Status
}

type peerStatusStore interface {
	ReadPeerStatusSnapshot(context.Context, string, string) (store.PeerStatusSnapshot, error)
	WorkerReadiness(context.Context) (protocol.WorkerReadiness, error)
}

type peerLocalStatusProvider struct {
	client         connectorStatusSource
	state          peerStatusStore
	transport      delegationconfig.TransportStatus
	controllerID   string
	deviceID       string
	deviceName     string
	maxWorkerSlots int
	upgrade        localbridge.UpgradeManager
}

func (p peerLocalStatusProvider) LocalStatus(ctx context.Context) (localbridge.StatusSnapshot, error) {
	if p.client == nil || p.state == nil {
		return localbridge.StatusSnapshot{}, errors.New("peer status sources are unavailable")
	}
	durable, err := p.state.ReadPeerStatusSnapshot(ctx, p.controllerID, p.deviceID)
	if err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("read durable peer status: %w", err)
	}
	durableReadiness, err := p.state.WorkerReadiness(ctx)
	if err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("read durable worker readiness: %w", err)
	}
	connected := p.client.Status()
	brokerWorkerRevision := connected.WorkerRevision
	connectionState := localbridge.ConnectionConnecting
	connectionErrorCode := connected.ConnectionErrorCode
	workerSyncReady := connected.Connected && connected.WorkerRevision == durable.WorkerRevision
	if connected.StateRecoveryRequired {
		brokerWorkerRevision = connected.RecoveryBrokerWorkerRevision
		connectionState = localbridge.ConnectionStateRecoveryRequired
	} else if workerSyncReady {
		connectionState = localbridge.ConnectionReady
		connectionErrorCode = ""
	} else if connected.Connected {
		connectionState = localbridge.ConnectionSynchronizing
		connectionErrorCode = ""
	}
	status := localbridge.StatusSnapshot{
		TransportStatus:            p.transport,
		Version:                    buildinfo.Version,
		ControllerID:               p.controllerID,
		DeviceID:                   p.deviceID,
		DeviceName:                 p.deviceName,
		ServiceRunning:             true,
		ConnectionState:            connectionState,
		ConnectionErrorCode:        connectionErrorCode,
		Connected:                  connected.Connected,
		RegistryRevision:           connected.RegistryRevision,
		WorkerRevision:             durable.WorkerRevision,
		BrokerWorkerRevision:       brokerWorkerRevision,
		RecoveryPeerWorkerRevision: connected.RecoveryPeerWorkerRevision,
		WorkerSyncReady:            workerSyncReady,
		WorkerReady:                durableReadiness.IsReady(),
		Dispatchable:               workerSyncReady && durableReadiness.IsReady(),
		MaxWorkerSlots:             p.maxWorkerSlots,
	}
	setDurablePeerStatus(&status, durable, durableReadiness)
	if p.upgrade != nil {
		status.Upgrade, err = p.upgrade.LocalUpgrade(ctx)
		if err != nil {
			return localbridge.StatusSnapshot{}, fmt.Errorf("read local upgrade status: %w", err)
		}
	}
	if err := status.Validate(); err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("build peer status: %w", err)
	}
	return status, nil
}

func setDurablePeerStatus(
	status *localbridge.StatusSnapshot,
	durable store.PeerStatusSnapshot,
	readiness protocol.WorkerReadiness,
) {
	status.WorkerRevision = durable.WorkerRevision
	status.WorkerReadiness = readiness
	status.Workers = localbridge.WorkerCounts{
		Total:       int64(durable.Workers.Total),
		Reserved:    int64(durable.Workers.Reserved),
		Pending:     int64(durable.Workers.Pending),
		Starting:    int64(durable.Workers.Starting),
		Preflight:   int64(durable.Workers.Preflight),
		Ready:       int64(durable.Workers.Ready),
		Running:     int64(durable.Workers.Running),
		Finalizing:  int64(durable.Workers.Finalizing),
		Idle:        int64(durable.Workers.Idle),
		Interrupted: int64(durable.Workers.Interrupted),
		Failed:      int64(durable.Workers.Failed),
		Occupied:    int64(durable.Workers.Occupied),
	}
	status.Artifacts = localbridge.ArtifactCounts{
		CapturePending: int64(durable.Artifacts.CaptureBacklog),
		PublishPending: int64(durable.Artifacts.PublishBacklog),
		Retained:       int64(durable.Artifacts.Retained),
		RetainedBytes:  durable.Artifacts.RetainedBytes,
	}
	status.Results = localbridge.ResultCounts{
		OutboxCapturePending:   int64(durable.Results.OutboxCapturePending),
		OutboxPublishPending:   int64(durable.Results.OutboxPublishPending),
		OutboxDeliveryPending:  int64(durable.Results.OutboxDeliveryPending),
		OutboxDelivered:        int64(durable.Results.OutboxDelivered),
		OutboxReleasePending:   int64(durable.Results.OutboxReleasePending),
		OutboxRetainedBytes:    durable.Results.OutboxRetainedBytes,
		InboxReceiving:         int64(durable.Results.InboxReceiving),
		InboxAvailable:         int64(durable.Results.InboxAvailable),
		InboxEvictionPending:   int64(durable.Results.InboxEvictionPending),
		InboxEvicted:           int64(durable.Results.InboxEvicted),
		InboxRetainedBytes:     durable.Results.InboxRetainedBytes,
		RolloutCaptureFailed:   int64(durable.Results.RolloutCaptureFailed),
		WorkspaceCaptureFailed: int64(durable.Results.WorkspaceCaptureFailed),
	}
}

func readStoppedPeerStatus(
	ctx context.Context, cfg delegationconfig.Config,
) (status localbridge.StatusSnapshot, err error) {
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	defer func() { err = errors.Join(err, lease.Close()) }()
	if err := store.ValidatePath(cfg.Peer.StateFile); err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	info, err := os.Lstat(cfg.Peer.StateFile)
	if err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return localbridge.StatusSnapshot{}, errors.New("peer state must be a regular file")
	}

	state, err := store.OpenPeer(ctx, cfg.Peer.StateFile)
	if err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	defer func() { err = errors.Join(err, state.Close()) }()
	durable, err := state.ReadPeerStatusSnapshot(ctx, cfg.ControllerID, cfg.DeviceID)
	if err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	readiness, err := state.WorkerReadiness(ctx)
	if err != nil {
		return localbridge.StatusSnapshot{}, err
	}
	status = localbridge.StatusSnapshot{
		TransportStatus: cfg.Transport.Status(),
		Version:         buildinfo.Version,
		ControllerID:    cfg.ControllerID,
		DeviceID:        cfg.DeviceID,
		DeviceName:      cfg.DeviceName,
		ServiceRunning:  false,
		ConnectionState: localbridge.ConnectionConnecting,
		WorkerReady:     readiness.IsReady(),
		MaxWorkerSlots:  cfg.Peer.MaxWorkerSlots,
	}
	setDurablePeerStatus(&status, durable, readiness)
	if err := status.Validate(); err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("build stopped peer status: %w", err)
	}
	return status, nil
}

type statusReader func(
	context.Context, string, localbridge.ServiceIdentity,
) (localbridge.StatusSnapshot, error)
type brokerStatusReader func(context.Context, string) (statuspage.Snapshot, error)
type upgradeStatusReader func(context.Context, string) (*localbridge.UpgradeSnapshot, error)
type controllerUpgradeStatusReader func(context.Context, string) (*localbridge.ControllerUpgradeSnapshot, error)

func runStatus(args []string, stdout, stderr io.Writer) int {
	return runStatusWithAllReaders(
		args, stdout, stderr, localbridge.ReadStatusForIdentity, readBrokerStatus, localbridge.ReadUpgrade,
		localbridge.ReadControllerUpgrade,
	)
}

func runStatusWithReader(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	read statusReader,
) int {
	return runStatusWithAllReaders(args, stdout, stderr, read, nil, nil, nil)
}

func runStatusWithReaders(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	readPeer statusReader,
	readBroker brokerStatusReader,
) int {
	return runStatusWithAllReaders(args, stdout, stderr, readPeer, readBroker, nil, nil)
}

func runStatusWithAllReaders(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	readPeer statusReader,
	readBroker brokerStatusReader,
	readUpgrade upgradeStatusReader,
	readControllerUpgrade controllerUpgradeStatusReader,
) int {
	flags := flag.NewFlagSet("delegation status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "broker or peer configuration file path (required)")
	jsonOutput := flags.Bool("json", false, "print status as JSON")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	if *configPath == "" {
		return writeError(stderr, errors.New("--config is required because broker and peer may coexist"))
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	cfg, validationErr := runtimeconfig.Read(resolvedConfig)
	if validationErr != nil {
		if !errors.Is(
			validationErr, delegationconfig.ErrPeerCLIProfileArgumentsUnsupported,
		) {
			return writeError(stderr, validationErr)
		}
		classified, _, classificationErr := runtimeconfig.ReadForStartupClassification(resolvedConfig)
		if !errors.Is(
			classificationErr, delegationconfig.ErrPeerCLIProfileArgumentsUnsupported,
		) {
			return writeError(stderr, validationErr)
		}
		repaired, _, removed, repairErr := runtimeconfig.ReadForRepair(resolvedConfig)
		if repairErr != nil || removed == 0 {
			return writeError(stderr, validationErr)
		}
		if repaired.ControllerID != classified.ControllerID ||
			repaired.DeviceID != classified.DeviceID ||
			repaired.EffectiveInstanceID() != classified.EffectiveInstanceID() {
			return writeError(stderr, validationErr)
		}
		cfg = repaired
	}
	if cfg.Role == delegationconfig.RoleBroker {
		if readBroker == nil {
			return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
		}
		if cfg.Broker.StatusListen == "" {
			offline, offlineErr := readStoppedBrokerStatus(cfg, resolvedConfig)
			if offlineErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			return writeBrokerStatus(stdout, stderr, offline, *jsonOutput)
		}
		ctx, cancel := context.WithTimeout(context.Background(), peerStatusReadTimeout)
		defer cancel()
		status, err := readBroker(ctx, cfg.Broker.StatusListen)
		if err != nil {
			offline, offlineErr := readStoppedBrokerStatus(cfg, resolvedConfig)
			if offlineErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			return writeBrokerStatus(stdout, stderr, offline, *jsonOutput)
		}
		statusInstanceID := status.InstanceID
		if statusInstanceID == "" {
			statusInstanceID = delegationconfig.DefaultInstanceID
		}
		if status.Validate() != nil || status.ControllerID != cfg.ControllerID ||
			statusInstanceID != cfg.EffectiveInstanceID() ||
			status.TransportStatus != cfg.Transport.Status() {
			return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
		}
		status.ServiceRunning = true
		if readUpgrade != nil {
			endpoint, _, endpointErr := localUpgradeEndpoint(cfg)
			if endpointErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			upgrade, upgradeErr := readUpgrade(ctx, endpoint)
			if upgradeErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			status.Upgrade = toStatusPageUpgrade(upgrade)
		}
		if readControllerUpgrade != nil {
			endpoint, _, endpointErr := localUpgradeEndpoint(cfg)
			if endpointErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			upgrade, upgradeErr := readControllerUpgrade(ctx, endpoint)
			if upgradeErr != nil {
				return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
			}
			if upgrade != nil {
				status.ControllerUpgrade = toStatusPageControllerUpgrade(*upgrade)
			}
		}
		return writeBrokerStatus(stdout, stderr, status, *jsonOutput)
	}
	if cfg.Role != delegationconfig.RolePeer {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil || readPeer == nil {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerStatusReadTimeout)
	expectedIdentity := localbridge.ServiceIdentity{
		InstanceID: cfg.EffectiveInstanceID(), ControllerID: cfg.ControllerID, DeviceID: cfg.DeviceID,
	}
	status, err := readPeer(ctx, endpoint, expectedIdentity)
	cancel()
	if err == nil {
		if !status.ServiceRunning || status.Validate() != nil ||
			status.ControllerID != cfg.ControllerID || status.DeviceID != cfg.DeviceID ||
			status.TransportStatus != cfg.Transport.Status() {
			return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
		}
		return writePeerStatus(stdout, stderr, status, *jsonOutput)
	}
	if errors.Is(err, localbridge.ErrStatusSnapshotInvalid) ||
		errors.Is(err, localbridge.ErrServiceIdentityMismatch) {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	offlineCtx, offlineCancel := context.WithTimeout(
		context.Background(), peerStatusReadTimeout,
	)
	offline, offlineErr := readStoppedPeerStatus(offlineCtx, cfg)
	offlineCancel()
	if offlineErr != nil {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	if upgrade, upgradeErr := readStoppedUpgrade(cfg, resolvedConfig); upgradeErr == nil {
		offline.Upgrade = upgrade
	} else if !errors.Is(upgradeErr, os.ErrNotExist) {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	return writePeerStatus(stdout, stderr, offline, *jsonOutput)
}

func readStoppedBrokerStatus(
	cfg delegationconfig.Config, configPath string,
) (status statuspage.Snapshot, err error) {
	// Establish that a protected matching transaction exists before touching
	// the broker lease path, then hold the lease and read it again.
	localUpgrade, localErr := readStoppedUpgrade(cfg, configPath)
	controllerUpgrade, controllerErr := readStoppedControllerUpgrade(cfg)
	if localErr != nil && !errors.Is(localErr, os.ErrNotExist) {
		return statuspage.Snapshot{}, localErr
	}
	if controllerErr != nil && !errors.Is(controllerErr, os.ErrNotExist) {
		return statuspage.Snapshot{}, controllerErr
	}
	if localErr != nil && controllerErr != nil {
		return statuspage.Snapshot{}, errors.Join(localErr, controllerErr)
	}
	lease, err := store.AcquireBrokerLease(cfg.Broker.StateFile)
	if err != nil {
		return statuspage.Snapshot{}, err
	}
	defer func() { err = errors.Join(err, lease.Close()) }()
	if localErr == nil {
		localUpgrade, localErr = readStoppedUpgrade(cfg, configPath)
		if localErr != nil {
			return statuspage.Snapshot{}, localErr
		}
	}
	if controllerErr == nil {
		controllerUpgrade, controllerErr = readStoppedControllerUpgrade(cfg)
		if controllerErr != nil {
			return statuspage.Snapshot{}, controllerErr
		}
	}
	status = statuspage.Snapshot{
		TransportStatus: cfg.Transport.Status(), Version: buildinfo.Version,
		ControllerID: cfg.ControllerID, ServiceRunning: false,
		Upgrade: toStatusPageUpgrade(localUpgrade),
	}
	if controllerErr == nil {
		status.ControllerUpgrade = toStatusPageControllerUpgrade(controllerUpgrade)
	}
	if cfg.EffectiveInstanceID() != delegationconfig.DefaultInstanceID {
		status.InstanceID = cfg.EffectiveInstanceID()
	}
	if err := status.Validate(); err != nil {
		return statuspage.Snapshot{}, fmt.Errorf("build stopped broker status: %w", err)
	}
	return status, nil
}

func readStoppedUpgrade(
	cfg delegationconfig.Config, configPath string,
) (*localbridge.UpgradeSnapshot, error) {
	home, err := delegationconfig.DefaultHome()
	if err != nil {
		return nil, err
	}
	home, err = filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	root, err := localupgrade.RootForConfig(home, cfg)
	if err != nil {
		return nil, err
	}
	transactionStore, err := localupgrade.OpenExistingStore(root)
	if err != nil {
		return nil, err
	}
	j, err := transactionStore.Load()
	if err != nil {
		return nil, err
	}
	if j.Role != cfg.Role || j.InstanceID != cfg.EffectiveInstanceID() ||
		j.ControllerID != cfg.ControllerID || j.DeviceID != cfg.DeviceID ||
		filepath.Clean(j.Invocation.ConfigPath) != filepath.Clean(configPath) {
		return nil, errors.New("upgrade journal does not match the requested service identity")
	}
	snapshot := bridgeUpgradeSnapshot(j)
	return &snapshot, nil
}

func readStoppedControllerUpgrade(
	cfg delegationconfig.Config,
) (localbridge.ControllerUpgradeSnapshot, error) {
	home, err := delegationconfig.DefaultHome()
	if err != nil {
		return localbridge.ControllerUpgradeSnapshot{}, err
	}
	home, err = filepath.Abs(home)
	if err != nil {
		return localbridge.ControllerUpgradeSnapshot{}, err
	}
	root, err := coordinatedupgrade.RootForBroker(home, cfg.EffectiveInstanceID())
	if err != nil {
		return localbridge.ControllerUpgradeSnapshot{}, err
	}
	transactionStore, err := coordinatedupgrade.OpenExistingStore(root)
	if err != nil {
		return localbridge.ControllerUpgradeSnapshot{}, err
	}
	journal, err := transactionStore.Load()
	if err != nil {
		return localbridge.ControllerUpgradeSnapshot{}, err
	}
	if journal.ControllerID != cfg.ControllerID ||
		journal.InstanceID != cfg.EffectiveInstanceID() {
		return localbridge.ControllerUpgradeSnapshot{}, errors.New(
			"controller upgrade journal does not match the requested broker identity",
		)
	}
	return controllerUpgradeSnapshot(journal), nil
}

func writePeerStatus(
	stdout io.Writer,
	stderr io.Writer,
	status localbridge.StatusSnapshot,
	jsonOutput bool,
) int {
	var output []byte
	if jsonOutput {
		var err error
		output, err = json.Marshal(status)
		if err != nil {
			return writeFixedStatusError(stderr, statusOutputError, 1)
		}
		output = append(output, '\n')
	} else {
		var rendered bytes.Buffer
		fmt.Fprintln(&rendered, "delegation peer status")
		fmt.Fprintf(&rendered, "version: %s\n", status.Version)
		fmt.Fprintf(&rendered, "transport: %s\n", status.Transport)
		if status.TailscaleHostname != "" {
			fmt.Fprintf(&rendered, "tailscale hostname: %s\n", status.TailscaleHostname)
		}
		fmt.Fprintf(&rendered, "device: %s\n", status.DeviceName)
		fmt.Fprintf(&rendered, "service running: %t\n", status.ServiceRunning)
		fmt.Fprintf(&rendered, "broker connection: %s\n", status.ConnectionState)
		if status.ConnectionErrorCode != "" {
			fmt.Fprintf(&rendered, "connection error: %s\n", status.ConnectionErrorCode)
		}
		fmt.Fprintf(&rendered, "connected: %t\n", status.Connected)
		fmt.Fprintf(&rendered, "worker sync ready: %t\n", status.WorkerSyncReady)
		fmt.Fprintf(&rendered, "worker ready: %t\n", status.WorkerReady)
		fmt.Fprintf(&rendered, "dispatchable: %t\n", status.Dispatchable)
		fmt.Fprintf(&rendered, "readiness epoch: %d\n", status.WorkerReadiness.Epoch)
		fmt.Fprintf(&rendered, "readiness state: %s\n", status.WorkerReadiness.State)
		fmt.Fprintf(&rendered, "readiness attempts: %d/%d\n", status.WorkerReadiness.AttemptCount, protocol.MaximumReadinessAttempts)
		fmt.Fprintf(&rendered, "readiness runtime digest: %s\n", status.WorkerReadiness.RuntimeDigest)
		fmt.Fprintf(&rendered, "readiness config digest: %s\n", status.WorkerReadiness.ConfigDigest)
		fmt.Fprintf(&rendered, "readiness epoch started at: %d\n", status.WorkerReadiness.EpochStartedAt)
		fmt.Fprintf(&rendered, "readiness next attempt at: %d\n", status.WorkerReadiness.NextAttemptAt)
		fmt.Fprintf(&rendered, "readiness last attempt at: %d\n", status.WorkerReadiness.LastAttemptAt)
		if status.WorkerReadiness.FailureCode != "" {
			fmt.Fprintf(&rendered, "readiness failure: %s\n", status.WorkerReadiness.FailureCode)
		}
		fmt.Fprintf(&rendered, "readiness updated at: %d\n", status.WorkerReadiness.UpdatedAt)
		fmt.Fprintf(&rendered, "registry revision: %d\n", status.RegistryRevision)
		fmt.Fprintf(&rendered, "worker revision: %d\n", status.WorkerRevision)
		fmt.Fprintf(&rendered, "broker worker revision: %d\n", status.BrokerWorkerRevision)
		if status.ConnectionState == localbridge.ConnectionStateRecoveryRequired {
			fmt.Fprintf(&rendered, "rejected peer worker revision: %d\n", status.RecoveryPeerWorkerRevision)
		}
		writeUpgradeStatus(&rendered, status.Upgrade)
		fmt.Fprintf(
			&rendered,
			"worker slots: %d/%d occupied\n",
			status.Workers.Occupied,
			status.MaxWorkerSlots,
		)
		fmt.Fprintln(&rendered, "workers:")
		fmt.Fprintf(&rendered, "  total: %d\n", status.Workers.Total)
		fmt.Fprintf(&rendered, "  reserved: %d\n", status.Workers.Reserved)
		fmt.Fprintf(&rendered, "  pending: %d\n", status.Workers.Pending)
		fmt.Fprintf(&rendered, "  starting: %d\n", status.Workers.Starting)
		fmt.Fprintf(&rendered, "  preflight: %d\n", status.Workers.Preflight)
		fmt.Fprintf(&rendered, "  ready: %d\n", status.Workers.Ready)
		fmt.Fprintf(&rendered, "  running: %d\n", status.Workers.Running)
		fmt.Fprintf(&rendered, "  finalizing: %d\n", status.Workers.Finalizing)
		fmt.Fprintf(&rendered, "  idle: %d\n", status.Workers.Idle)
		fmt.Fprintf(&rendered, "  interrupted: %d\n", status.Workers.Interrupted)
		fmt.Fprintf(&rendered, "  failed: %d\n", status.Workers.Failed)
		fmt.Fprintln(&rendered, "artifacts:")
		fmt.Fprintf(&rendered, "  capture pending: %d\n", status.Artifacts.CapturePending)
		fmt.Fprintf(&rendered, "  publish pending: %d\n", status.Artifacts.PublishPending)
		fmt.Fprintf(&rendered, "  retained: %d\n", status.Artifacts.Retained)
		fmt.Fprintf(&rendered, "  retained bytes: %d\n", status.Artifacts.RetainedBytes)
		fmt.Fprintln(&rendered, "results:")
		fmt.Fprintf(&rendered, "  outbox capture pending: %d\n", status.Results.OutboxCapturePending)
		fmt.Fprintf(&rendered, "  outbox publish pending: %d\n", status.Results.OutboxPublishPending)
		fmt.Fprintf(&rendered, "  outbox delivery pending: %d\n", status.Results.OutboxDeliveryPending)
		fmt.Fprintf(&rendered, "  outbox delivered: %d\n", status.Results.OutboxDelivered)
		fmt.Fprintf(&rendered, "  outbox release pending: %d\n", status.Results.OutboxReleasePending)
		fmt.Fprintf(&rendered, "  outbox retained bytes: %d\n", status.Results.OutboxRetainedBytes)
		fmt.Fprintf(&rendered, "  inbox receiving: %d\n", status.Results.InboxReceiving)
		fmt.Fprintf(&rendered, "  inbox available: %d\n", status.Results.InboxAvailable)
		fmt.Fprintf(&rendered, "  inbox eviction pending: %d\n", status.Results.InboxEvictionPending)
		fmt.Fprintf(&rendered, "  inbox evicted lifetime: %d\n", status.Results.InboxEvicted)
		fmt.Fprintf(&rendered, "  inbox retained bytes: %d\n", status.Results.InboxRetainedBytes)
		fmt.Fprintf(&rendered, "  rollout capture failed: %d\n", status.Results.RolloutCaptureFailed)
		fmt.Fprintf(&rendered, "  workspace capture failed: %d\n", status.Results.WorkspaceCaptureFailed)
		output = rendered.Bytes()
	}
	if len(output) == 0 || len(output) > maximumStatusOutput {
		return writeFixedStatusError(stderr, statusOutputError, 1)
	}
	if _, err := io.Copy(stdout, bytes.NewReader(output)); err != nil {
		return writeFixedStatusError(stderr, statusOutputError, 1)
	}
	return 0
}

func writeUpgradeStatus(rendered *bytes.Buffer, upgrade *localbridge.UpgradeSnapshot) {
	if upgrade == nil {
		return
	}
	fmt.Fprintln(rendered, "upgrade:")
	fmt.Fprintf(rendered, "  transaction: %s\n", upgrade.TransactionID)
	fmt.Fprintf(rendered, "  state: %s\n", upgrade.State)
	fmt.Fprintf(rendered, "  version: %s -> %s\n", upgrade.SourceVersion, upgrade.TargetVersion)
	fmt.Fprintf(rendered, "  commit authorized: %t\n", upgrade.CommitAuthorized)
	if upgrade.FailureCode != "" {
		fmt.Fprintf(rendered, "  failure: %s\n", upgrade.FailureCode)
	}
}

func writeFixedStatusError(stderr io.Writer, message string, code int) int {
	_, _ = io.WriteString(stderr, message)
	return code
}
