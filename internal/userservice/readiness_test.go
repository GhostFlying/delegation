package userservice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/statuspage"
)

const (
	readinessControllerID = "123e4567-e89b-42d3-a456-426614174700"
	readinessDeviceID     = "123e4567-e89b-42d3-a456-426614174701"
)

type readinessBackend struct{}

func (*readinessBackend) Call(
	context.Context,
	string,
	string,
	*control.PrincipalIdentity,
	any,
	any,
) error {
	return errors.New("unexpected readiness backend call")
}

func TestConnectorReadinessRejectsWrongBridgeIdentity(t *testing.T) {
	temporaryRoot := ""
	if runtime.GOOS != "windows" {
		temporaryRoot = "/tmp"
	}
	home, err := os.MkdirTemp(temporaryRoot, "dr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	cfg := delegationconfig.Config{
		Role:         delegationconfig.RolePeer,
		ControllerID: readinessControllerID,
		DeviceID:     readinessDeviceID,
	}
	endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	server, err := localbridge.Listen(endpoint, localbridge.ServiceIdentity{
		ControllerID: "123e4567-e89b-42d3-a456-426614174799",
		DeviceID:     cfg.DeviceID,
	}, &readinessBackend{})
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveContext) }()
	t.Cleanup(func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close readiness bridge: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve readiness bridge: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("readiness bridge did not stop")
		}
	})
	probeContext, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	if err := probeService(probeContext, cfg); err == nil {
		t.Fatal("probeService accepted a connector bridge from another controller")
	}
}

func TestConnectorReadinessUsesInstanceEndpoint(t *testing.T) {
	temporaryRoot := ""
	if runtime.GOOS != "windows" {
		temporaryRoot = "/tmp"
	}
	home, err := os.MkdirTemp(temporaryRoot, "dr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	cfg := delegationconfig.Config{
		InstanceID:   "alpha",
		Role:         delegationconfig.RolePeer,
		ControllerID: readinessControllerID,
		DeviceID:     readinessDeviceID,
	}
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	server, err := localbridge.Listen(endpoint, localbridge.ServiceIdentity{
		ControllerID: cfg.ControllerID,
		DeviceID:     cfg.DeviceID,
		InstanceID:   cfg.InstanceID,
	}, &readinessBackend{})
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveContext) }()
	t.Cleanup(func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close readiness bridge: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve readiness bridge: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("readiness bridge did not stop")
		}
	})
	probeContext, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	if err := probeService(probeContext, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorReadinessRejectsWrongInstanceIdentity(t *testing.T) {
	temporaryRoot := ""
	if runtime.GOOS != "windows" {
		temporaryRoot = "/tmp"
	}
	home, err := os.MkdirTemp(temporaryRoot, "dr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	cfg := delegationconfig.Config{
		InstanceID:   "alpha",
		Role:         delegationconfig.RolePeer,
		ControllerID: readinessControllerID,
		DeviceID:     readinessDeviceID,
	}
	endpoint, err := localbridge.EndpointForInstance(
		cfg.EffectiveInstanceID(), cfg.ControllerID, cfg.DeviceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	server, err := localbridge.Listen(endpoint, localbridge.ServiceIdentity{
		ControllerID: cfg.ControllerID,
		DeviceID:     cfg.DeviceID,
		InstanceID:   "beta",
	}, &readinessBackend{})
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveContext) }()
	t.Cleanup(func() {
		cancel()
		if err := server.Close(); err != nil {
			t.Errorf("close readiness bridge: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve readiness bridge: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("readiness bridge did not stop")
		}
	})
	probeContext, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	if err := probeService(probeContext, cfg); err == nil {
		t.Fatal("probeService accepted a connector bridge from another instance")
	}
}

func TestBrokerReadinessRequiresDelegationIdentity(t *testing.T) {
	valid := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if valid {
			writer.Header().Set(broker.HealthServiceHeader, "broker")
			writer.Header().Set(broker.HealthControllerHeader, readinessControllerID)
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	}))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	if _, _, err := net.SplitHostPort(address); err != nil {
		t.Fatal(err)
	}
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          delegationconfig.RoleBroker,
		ControllerID:  readinessControllerID,
		Broker: delegationconfig.BrokerConfig{
			Listen: address,
			Auth:   delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
	if err := probeService(context.Background(), cfg); err == nil {
		t.Fatal("probeService accepted an unrelated HTTP health endpoint")
	}
	valid = true
	if err := probeService(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerReadinessRequiresNamedInstanceIdentity(t *testing.T) {
	responseInstanceID := "beta"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(broker.HealthServiceHeader, "broker")
		writer.Header().Set(broker.HealthControllerHeader, readinessControllerID)
		writer.Header().Set(broker.HealthInstanceHeader, responseInstanceID)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	}))
	defer server.Close()
	cfg := delegationconfig.Config{
		InstanceID:   "alpha",
		Role:         delegationconfig.RoleBroker,
		ControllerID: readinessControllerID,
		Broker: delegationconfig.BrokerConfig{
			Listen: strings.TrimPrefix(server.URL, "http://"),
			Auth:   delegationconfig.AuthConfig{Mode: delegationconfig.AuthModeNone},
		},
	}
	if err := probeService(context.Background(), cfg); err == nil {
		t.Fatal("probeService accepted broker health from another instance")
	}
	responseInstanceID = cfg.InstanceID
	if err := probeService(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestTailscaleBrokerReadinessUsesOnlyNativeStatusListener(t *testing.T) {
	primaryRequests := 0
	primary := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		primaryRequests++
		http.Error(writer, "tailnet listener must not be probed", http.StatusInternalServerError)
	}))
	defer primary.Close()
	_, primaryPort, err := net.SplitHostPort(strings.TrimPrefix(primary.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}

	snapshot := statuspage.Snapshot{
		TransportStatus: delegationconfig.TransportStatus{
			Transport:         "tailscale",
			TailscaleHostname: "alpha-broker",
		},
		Version:      "0.2.0-test",
		ControllerID: readinessControllerID,
		InstanceID:   "alpha",
	}
	status := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != statuspage.JSONPath {
			t.Errorf("status request path = %q, want %q", request.URL.Path, statuspage.JSONPath)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(snapshot); err != nil {
			t.Error(err)
		}
	}))
	defer status.Close()
	cfg := delegationconfig.Config{
		InstanceID:   "alpha",
		Role:         delegationconfig.RoleBroker,
		ControllerID: readinessControllerID,
		Transport: delegationconfig.TransportConfig{
			Mode: delegationconfig.TransportModeTailscale,
			Tailscale: &delegationconfig.TailscaleConfig{
				Hostname: "alpha-broker",
			},
		},
		Broker: delegationconfig.BrokerConfig{
			Listen:       ":" + primaryPort,
			StatusListen: strings.TrimPrefix(status.URL, "http://"),
		},
	}
	if err := probeService(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if primaryRequests != 0 {
		t.Fatalf("tailscale broker readiness probed the tailnet port %d times", primaryRequests)
	}

	snapshot.InstanceID = "beta"
	if err := probeService(context.Background(), cfg); err == nil {
		t.Fatal("tailscale broker readiness accepted another instance")
	}
	snapshot.InstanceID = cfg.InstanceID
	snapshot.ControllerID = "123e4567-e89b-42d3-a456-426614174799"
	if err := probeService(context.Background(), cfg); err == nil {
		t.Fatal("tailscale broker readiness accepted another controller")
	}
}

func TestBrokerReadinessIgnoresAmbientProxy(t *testing.T) {
	const helperEnvironment = "DELEGATION_TEST_READINESS_PROXY_POLICY"
	if os.Getenv(helperEnvironment) == "1" {
		client := newBrokerHealthClient()
		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("broker readiness transport = %T", client.Transport)
		}
		if transport.Proxy != nil {
			t.Fatal("broker readiness retained ambient HTTP proxy routing")
		}
		return
	}

	environment := make([]string, 0, len(os.Environ())+4)
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY":
			continue
		default:
			environment = append(environment, variable)
		}
	}
	environment = append(environment,
		helperEnvironment+"=1",
		"HTTP_PROXY=http://127.0.0.1:32767",
		"HTTPS_PROXY=http://127.0.0.1:32767",
		"NO_PROXY=",
	)
	command := exec.Command(os.Args[0], "-test.run=^TestBrokerReadinessIgnoresAmbientProxy$", "-test.count=1")
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proxy policy helper failed: %v\n%s", err, output)
	}
}
