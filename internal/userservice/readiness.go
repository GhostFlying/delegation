package userservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
)

const (
	serviceReadinessTimeout       = 10 * time.Second
	serviceReadinessPoll          = 200 * time.Millisecond
	serviceReadinessConfirmations = 2
	maximumHealthBody             = 128
)

func waitForServiceReady(configPath string) error {
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceReadinessTimeout)
	defer cancel()
	confirmations := 0
	var lastErr error
	for {
		lastErr = probeService(ctx, cfg)
		if lastErr == nil {
			confirmations++
			if confirmations == serviceReadinessConfirmations {
				return nil
			}
		} else {
			confirmations = 0
		}
		timer := time.NewTimer(serviceReadinessPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func probeService(ctx context.Context, cfg delegationconfig.Config) error {
	switch cfg.Role {
	case delegationconfig.RolePeer:
		endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
		if err != nil {
			return err
		}
		return localbridge.Probe(ctx, endpoint, localbridge.ServiceIdentity{
			ControllerID: cfg.ControllerID,
			DeviceID:     cfg.DeviceID,
		})
	case delegationconfig.RoleBroker:
		host, port, err := net.SplitHostPort(cfg.Broker.Listen)
		if err != nil {
			return err
		}
		if host == "" {
			host = "127.0.0.1"
		} else if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			if ip.To4() != nil {
				host = "127.0.0.1"
			} else {
				host = "::1"
			}
		}
		healthURL := (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: "/healthz"}).String()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}
		client := newBrokerHealthClient()
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, maximumHealthBody+1))
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK || string(body) != "ok\n" ||
			response.Header.Get(broker.HealthServiceHeader) != "broker" ||
			response.Header.Get(broker.HealthControllerHeader) != cfg.ControllerID {
			return fmt.Errorf("broker health check did not match controller %s", cfg.ControllerID)
		}
		return nil
	default:
		return fmt.Errorf("unsupported service role %q", cfg.Role)
	}
}

func newBrokerHealthClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
