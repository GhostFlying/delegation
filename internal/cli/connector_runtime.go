package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/connector"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

func runConnectorService(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
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
	lease, err := store.AcquirePeerLease(cfg.Peer.StateFile)
	if err != nil {
		return err
	}
	peerState, err := store.OpenPeer(ctx, cfg.Peer.StateFile)
	if err != nil {
		return errors.Join(err, lease.Close())
	}
	defer func() {
		resultErr = errors.Join(resultErr, peerState.Close(), lease.Close())
	}()
	var stderrMu sync.Mutex
	writeStderr := func(format string, args ...any) error {
		stderrMu.Lock()
		defer stderrMu.Unlock()
		_, err := fmt.Fprintf(stderr, format, args...)
		return err
	}
	client, err := connector.New(connector.Options{
		BrokerURL:                cfg.Broker.URL,
		AllowInsecureNonLoopback: cfg.Broker.AllowInsecureNonLoopback,
		ControllerID:             cfg.ControllerID,
		DeviceID:                 cfg.DeviceID,
		DeviceName:               cfg.DeviceName,
		AuthMode:                 cfg.Broker.Auth.Mode,
		Token:                    token,
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
	bridge, err := localbridge.ListenWithWorkerAuthorization(
		endpoint,
		localbridge.ServiceIdentity{
			ControllerID: cfg.ControllerID,
			DeviceID:     cfg.DeviceID,
		},
		client,
		peerWorkerAuthorizer{state: peerState, deviceID: cfg.DeviceID},
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

type peerWorkerAuthorizer struct {
	state    *store.PeerStore
	deviceID string
}

func (a peerWorkerAuthorizer) AuthorizeWorker(
	ctx context.Context,
	principal control.PrincipalIdentity,
) error {
	if a.state == nil || principal.ParentAgentID == "" || principal.DeviceID != a.deviceID {
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
	case store.WorkerStarting, store.WorkerReady, store.WorkerRunning, store.WorkerIdle:
		return nil
	case store.WorkerReserved, store.WorkerFailed:
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
	if err := pathguard.ValidatePeerAuthority(configPath, cfg.Peer.StateFile, cfg.Broker.Auth.TokenFile); err != nil {
		return nil, err
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
