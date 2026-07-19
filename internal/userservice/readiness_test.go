package userservice

import (
	"context"
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
		Role:         delegationconfig.RoleController,
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
		Role:         control.DeviceRoleController,
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
