package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/localbridge"
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
}

type peerLocalStatusProvider struct {
	client         connectorStatusSource
	state          peerStatusStore
	controllerID   string
	deviceID       string
	deviceName     string
	maxWorkerSlots int
}

func (p peerLocalStatusProvider) LocalStatus(ctx context.Context) (localbridge.StatusSnapshot, error) {
	if p.client == nil || p.state == nil {
		return localbridge.StatusSnapshot{}, errors.New("peer status sources are unavailable")
	}
	durable, err := p.state.ReadPeerStatusSnapshot(ctx, p.controllerID, p.deviceID)
	if err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("read durable peer status: %w", err)
	}
	connected := p.client.Status()
	status := localbridge.StatusSnapshot{
		Version:              buildinfo.Version,
		ControllerID:         p.controllerID,
		DeviceID:             p.deviceID,
		DeviceName:           p.deviceName,
		Connected:            connected.Connected,
		RegistryRevision:     connected.RegistryRevision,
		WorkerRevision:       durable.WorkerRevision,
		BrokerWorkerRevision: connected.WorkerRevision,
		WorkerSyncReady:      connected.Connected && connected.WorkerRevision == durable.WorkerRevision,
		MaxWorkerSlots:       p.maxWorkerSlots,
		Workers: localbridge.WorkerCounts{
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
		},
		Artifacts: localbridge.ArtifactCounts{
			CapturePending: int64(durable.Artifacts.CaptureBacklog),
			PublishPending: int64(durable.Artifacts.PublishBacklog),
			Retained:       int64(durable.Artifacts.Retained),
			RetainedBytes:  durable.Artifacts.RetainedBytes,
		},
		Results: localbridge.ResultCounts{
			OutboxCapturePending:   int64(durable.Results.OutboxCapturePending),
			OutboxPublishPending:   int64(durable.Results.OutboxPublishPending),
			OutboxDeliveryPending:  int64(durable.Results.OutboxDeliveryPending),
			OutboxDelivered:        int64(durable.Results.OutboxDelivered),
			OutboxRetainedBytes:    durable.Results.OutboxRetainedBytes,
			InboxReceiving:         int64(durable.Results.InboxReceiving),
			InboxAvailable:         int64(durable.Results.InboxAvailable),
			InboxEvictionPending:   int64(durable.Results.InboxEvictionPending),
			InboxRetainedBytes:     durable.Results.InboxRetainedBytes,
			RolloutCaptureFailed:   int64(durable.Results.RolloutCaptureFailed),
			WorkspaceCaptureFailed: int64(durable.Results.WorkspaceCaptureFailed),
		},
	}
	if err := status.Validate(); err != nil {
		return localbridge.StatusSnapshot{}, fmt.Errorf("build peer status: %w", err)
	}
	return status, nil
}

type statusReader func(context.Context, string) (localbridge.StatusSnapshot, error)
type brokerStatusReader func(context.Context, string) (statuspage.Snapshot, error)

func runStatus(args []string, stdout, stderr io.Writer) int {
	return runStatusWithReaders(
		args, stdout, stderr, localbridge.ReadStatus, readBrokerStatus,
	)
}

func runStatusWithReader(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	read statusReader,
) int {
	return runStatusWithReaders(args, stdout, stderr, read, nil)
}

func runStatusWithReaders(
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	readPeer statusReader,
	readBroker brokerStatusReader,
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
	cfg, err := delegationconfig.Read(resolvedConfig)
	if err != nil {
		return writeError(stderr, err)
	}
	if cfg.Role == delegationconfig.RoleBroker {
		if cfg.Broker.StatusListen == "" || readBroker == nil {
			return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
		}
		ctx, cancel := context.WithTimeout(context.Background(), peerStatusReadTimeout)
		status, err := readBroker(ctx, cfg.Broker.StatusListen)
		cancel()
		if err != nil || status.Validate() != nil || status.ControllerID != cfg.ControllerID {
			return writeFixedStatusError(stderr, brokerStatusUnavailableError, exitUnavailable)
		}
		return writeBrokerStatus(stdout, stderr, status, *jsonOutput)
	}
	if cfg.Role != delegationconfig.RolePeer {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
	if err != nil || readPeer == nil {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	ctx, cancel := context.WithTimeout(context.Background(), peerStatusReadTimeout)
	status, err := readPeer(ctx, endpoint)
	cancel()
	if err != nil || status.Validate() != nil ||
		status.ControllerID != cfg.ControllerID || status.DeviceID != cfg.DeviceID {
		return writeFixedStatusError(stderr, peerStatusUnavailableError, exitUnavailable)
	}
	return writePeerStatus(stdout, stderr, status, *jsonOutput)
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
		fmt.Fprintf(&rendered, "device: %s\n", status.DeviceName)
		fmt.Fprintf(&rendered, "connected: %t\n", status.Connected)
		fmt.Fprintf(&rendered, "worker sync ready: %t\n", status.WorkerSyncReady)
		fmt.Fprintf(&rendered, "registry revision: %d\n", status.RegistryRevision)
		fmt.Fprintf(&rendered, "worker revision: %d\n", status.WorkerRevision)
		fmt.Fprintf(&rendered, "broker worker revision: %d\n", status.BrokerWorkerRevision)
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
		fmt.Fprintf(&rendered, "  outbox retained bytes: %d\n", status.Results.OutboxRetainedBytes)
		fmt.Fprintf(&rendered, "  inbox receiving: %d\n", status.Results.InboxReceiving)
		fmt.Fprintf(&rendered, "  inbox available: %d\n", status.Results.InboxAvailable)
		fmt.Fprintf(&rendered, "  inbox eviction pending: %d\n", status.Results.InboxEvictionPending)
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

func writeFixedStatusError(stderr io.Writer, message string, code int) int {
	_, _ = io.WriteString(stderr, message)
	return code
}
