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
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

func runConnectorService(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
	stderr io.Writer,
) error {
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
	role, err := connectorRole(cfg.Role)
	if err != nil {
		return err
	}
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
		Role:                     role,
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
	bridge, err := localbridge.Listen(endpoint, localbridge.ServiceIdentity{
		ControllerID: cfg.ControllerID,
		DeviceID:     cfg.DeviceID,
		Role:         role,
	}, client)
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

func loadConnectorAuthority(
	configPath string,
	cfg delegationconfig.Config,
) (*tokenfile.Token, error) {
	if cfg.Role != delegationconfig.RoleController && cfg.Role != delegationconfig.RoleDevice {
		return nil, errors.New("connector runtime requires a controller or device configuration")
	}
	if err := pathguard.ValidateConnectorAuthority(configPath, cfg.Broker.Auth.TokenFile); err != nil {
		return nil, err
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeNone {
		return nil, nil
	}
	token, err := tokenfile.Read(cfg.Broker.Auth.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read connector device token: %w", err)
	}
	return &token, nil
}

func connectorRole(role delegationconfig.Role) (control.DeviceRole, error) {
	switch role {
	case delegationconfig.RoleController:
		return control.DeviceRoleController, nil
	case delegationconfig.RoleDevice:
		return control.DeviceRoleWorker, nil
	default:
		return "", fmt.Errorf("unsupported connector role %q", role)
	}
}
