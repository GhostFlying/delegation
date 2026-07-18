package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
)

const (
	brokerReadHeaderTimeout = 10 * time.Second
	brokerIdleTimeout       = 60 * time.Second
	brokerMaxHeaderBytes    = 16 << 10
)

type brokerListenFunc func(context.Context, string, string) (net.Listener, error)

type brokerRuntimeOptions struct {
	listen      brokerListenFunc
	prepare     func(context.Context, *broker.Server) (store.PresenceTransition, error)
	reportError func(error)
}

type brokerRuntimeResources struct {
	listener   net.Listener
	httpServer *http.Server
	broker     *broker.Server
	registry   *store.Store
	serveDone  <-chan error
}

func runBrokerService(
	ctx context.Context,
	configPath string,
	cfg delegationconfig.Config,
	stderr io.Writer,
	options brokerRuntimeOptions,
) (runErr error) {
	masterToken, err := loadBrokerAuthority(configPath, cfg)
	if err != nil {
		return err
	}
	if err := writeInsecureTransportWarning(stderr, cfg); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	listen := options.listen
	if listen == nil {
		var listenConfig net.ListenConfig
		listen = listenConfig.Listen
	}
	resources := brokerRuntimeResources{}
	defer func() {
		runErr = errors.Join(runErr, resources.close())
	}()
	resources.listener, err = listen(ctx, "tcp", cfg.Broker.Listen)
	if err != nil {
		if cleanContextCancellation(ctx, err) {
			return nil
		}
		return fmt.Errorf("listen for broker connections on %s: %w", cfg.Broker.Listen, err)
	}
	if ctx.Err() != nil {
		return nil
	}
	resources.registry, err = store.Open(ctx, cfg.Broker.StateFile)
	if err != nil {
		if cleanContextCancellation(ctx, err) {
			return nil
		}
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	brokerServer, err := broker.New(broker.Options{
		ControllerID: cfg.ControllerID,
		AuthMode:     cfg.Broker.Auth.Mode,
		MasterToken:  masterToken,
		Registry:     resources.registry,
		ReportError:  options.reportError,
	})
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	resources.broker = brokerServer
	prepare := options.prepare
	if prepare == nil {
		prepare = func(ctx context.Context, server *broker.Server) (store.PresenceTransition, error) {
			return server.Prepare(ctx)
		}
	}
	if _, err := prepare(ctx, resources.broker); err != nil {
		if cleanContextCancellation(ctx, err) {
			return nil
		}
		return fmt.Errorf("prepare broker presence epoch: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	resources.httpServer = &http.Server{
		Handler:           resources.broker.Handler(),
		ReadHeaderTimeout: brokerReadHeaderTimeout,
		IdleTimeout:       brokerIdleTimeout,
		MaxHeaderBytes:    brokerMaxHeaderBytes,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	if _, err := fmt.Fprintf(stderr, "delegation: broker listening on %s\n", cfg.Broker.Listen); err != nil {
		return fmt.Errorf("write broker readiness: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	serveDone := make(chan error, 1)
	resources.serveDone = serveDone
	go func() {
		serveDone <- resources.httpServer.Serve(resources.listener)
	}()
	select {
	case err := <-serveDone:
		resources.serveDone = nil
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve broker HTTP: %w", err)
	case <-ctx.Done():
		return nil
	}
}

func loadBrokerAuthority(
	configPath string,
	cfg delegationconfig.Config,
) (*tokenfile.Token, error) {
	if cfg.Role != delegationconfig.RoleBroker {
		return nil, errors.New("broker runtime requires a broker configuration")
	}
	if err := pathguard.ValidateBrokerAuthority(
		configPath, cfg.Broker.StateFile, cfg.Broker.Auth.TokenFile,
	); err != nil {
		return nil, err
	}
	if err := store.ValidatePath(cfg.Broker.StateFile); err != nil {
		return nil, err
	}
	if cfg.Broker.Auth.Mode == delegationconfig.AuthModeNone {
		return nil, nil
	}
	masterToken, err := tokenfile.Read(cfg.Broker.Auth.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read broker master token: %w", err)
	}
	return &masterToken, nil
}

func (r *brokerRuntimeResources) close() error {
	var failures []error
	if r.listener != nil {
		if err := r.listener.Close(); !ignorableNetworkClose(err) {
			failures = append(failures, fmt.Errorf("close broker listener: %w", err))
		}
	}
	if r.httpServer != nil {
		if err := r.httpServer.Close(); !ignorableNetworkClose(err) {
			failures = append(failures, fmt.Errorf("close broker HTTP server: %w", err))
		}
	}
	if r.broker != nil {
		if err := r.broker.Close(context.Background()); err != nil {
			failures = append(failures, err)
		}
	}
	if r.serveDone != nil {
		if err := <-r.serveDone; !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			failures = append(failures, fmt.Errorf("stop broker HTTP: %w", err))
		}
	}
	if r.registry != nil {
		if err := r.registry.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close broker state: %w", err))
		}
	}
	return errors.Join(failures...)
}

func ignorableNetworkClose(err error) bool {
	return err == nil || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed)
}

func cleanContextCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}
