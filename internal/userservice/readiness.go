package userservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/runtimeconfig"
	"github.com/GhostFlying/delegation/internal/statuspage"
)

const (
	serviceReadinessTimeout       = 10 * time.Second
	serviceReadinessPoll          = 200 * time.Millisecond
	serviceReadinessConfirmations = 2
	maximumHealthBody             = 128
	maximumStatusBody             = 16 * 1024
)

func waitForServiceReady(configPath string) error {
	cfg, err := runtimeconfig.Read(configPath)
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
		endpoint, err := localbridge.EndpointForInstance(
			cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
		)
		if err != nil {
			return err
		}
		expectedIdentity := localbridge.ServiceIdentity{
			ControllerID: cfg.ControllerID,
			DeviceID:     cfg.DeviceID,
		}
		if cfg.EffectiveInstanceID() != delegationconfig.DefaultInstanceID {
			expectedIdentity.InstanceID = cfg.EffectiveInstanceID()
		}
		return localbridge.Probe(ctx, endpoint, expectedIdentity)
	case delegationconfig.RoleBroker:
		if cfg.Transport.Mode == delegationconfig.TransportModeTailscale {
			return probeTailscaleBrokerStatus(ctx, cfg)
		}
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
		responseInstanceID := response.Header.Get(broker.HealthInstanceHeader)
		if responseInstanceID == "" {
			responseInstanceID = delegationconfig.DefaultInstanceID
		}
		if response.StatusCode != http.StatusOK || string(body) != "ok\n" ||
			response.Header.Get(broker.HealthServiceHeader) != "broker" ||
			response.Header.Get(broker.HealthControllerHeader) != cfg.ControllerID ||
			responseInstanceID != cfg.EffectiveInstanceID() {
			return fmt.Errorf(
				"broker health check did not match controller %s instance %s",
				cfg.ControllerID,
				cfg.EffectiveInstanceID(),
			)
		}
		return nil
	default:
		return fmt.Errorf("unsupported service role %q", cfg.Role)
	}
}

func probeTailscaleBrokerStatus(ctx context.Context, cfg delegationconfig.Config) error {
	if cfg.Broker.StatusListen == "" {
		return errors.New("tailscale broker readiness requires a loopback status listener")
	}
	statusURL := (&url.URL{
		Scheme: "http",
		Host:   cfg.Broker.StatusListen,
		Path:   statuspage.JSONPath,
	}).String()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return err
	}
	response, err := newBrokerHealthClient().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maximumStatusBody+1))
	if readErr != nil {
		return readErr
	}
	if response.StatusCode != http.StatusOK || mediaErr != nil ||
		mediaType != "application/json" || len(body) > maximumStatusBody {
		return errors.New("tailscale broker status response is invalid")
	}
	var snapshot statuspage.Snapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("decode tailscale broker status: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("validate tailscale broker status: %w", err)
	}
	statusInstanceID := snapshot.InstanceID
	if statusInstanceID == "" {
		statusInstanceID = delegationconfig.DefaultInstanceID
	}
	if snapshot.ControllerID != cfg.ControllerID ||
		statusInstanceID != cfg.EffectiveInstanceID() {
		return fmt.Errorf(
			"tailscale broker status did not match controller %s instance %s",
			cfg.ControllerID,
			cfg.EffectiveInstanceID(),
		)
	}
	return nil
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
