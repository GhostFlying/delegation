package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/broker"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/rootmcp"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tokenfile"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	runtimeWorkerID = "123e4567-e89b-42d3-a456-426614174202"
	runtimeThreadID = "123e4567-e89b-42d3-a456-426614174203"
)

func TestPeerServicesRegisterDevicesAndServeRootBridge(t *testing.T) {
	registry, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state", "broker.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	brokerServer, err := broker.New(broker.Options{
		ControllerID:      runtimeControllerID,
		AuthMode:          delegationconfig.AuthModeNone,
		Registry:          registry,
		HeartbeatInterval: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := brokerServer.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(brokerServer.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := brokerServer.Close(closeContext); err != nil {
			t.Errorf("close broker: %v", err)
		}
		cancel()
		if err := registry.Close(); err != nil {
			t.Errorf("close registry: %v", err)
		}
	})
	brokerURL := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	firstPath, firstConfig := setupConnectorRuntimeTest(
		t, runtimeDeviceID, "peer-a", brokerURL,
	)
	secondPath, secondConfig := setupConnectorRuntimeTest(
		t, runtimeWorkerID, "peer-c", brokerURL,
	)

	controllerContext, cancelController := context.WithCancel(context.Background())
	deviceContext, cancelDevice := context.WithCancel(context.Background())
	controllerDone := make(chan error, 1)
	deviceDone := make(chan error, 1)
	var controllerLog bytes.Buffer
	var deviceLog bytes.Buffer
	go func() {
		controllerDone <- runConnectorService(
			controllerContext, firstPath, firstConfig, &controllerLog,
		)
	}()
	go func() {
		deviceDone <- runConnectorService(deviceContext, secondPath, secondConfig, &deviceLog)
	}()
	stopped := false
	t.Cleanup(func() {
		if stopped {
			return
		}
		cancelController()
		cancelDevice()
		<-controllerDone
		<-deviceDone
	})
	waitForRuntimeDevice(t, registry, runtimeDeviceID, true)
	waitForRuntimeDevice(t, registry, runtimeWorkerID, true)

	endpoint, err := localbridge.Endpoint(runtimeControllerID, runtimeDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	bridgeClient, err := localbridge.NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	callContext, cancelCall := context.WithTimeout(context.Background(), time.Second)
	var root protocol.EnsureRootTreeResult
	err = bridgeClient.Call(
		callContext,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: runtimeThreadID},
		&root,
	)
	cancelCall()
	if err != nil {
		t.Fatal(err)
	}
	source := root.Principal.Identity()
	callContext, cancelCall = context.WithTimeout(context.Background(), time.Second)
	var devices protocol.ListDevicesResult
	err = bridgeClient.Call(
		callContext,
		protocol.MethodListDevices,
		root.Tree.TreeID,
		&source,
		protocol.ListDevicesParams{Limit: 10},
		&devices,
	)
	cancelCall()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices.Devices) != 2 ||
		devices.Devices[0].DeviceID != runtimeDeviceID ||
		devices.Devices[1].DeviceID != runtimeWorkerID {
		t.Fatalf("devices through controller bridge = %#v", devices.Devices)
	}
	assertRootMCPDevices(t, firstPath)

	duplicateContext, cancelDuplicate := context.WithTimeout(context.Background(), time.Second)
	err = runConnectorService(duplicateContext, firstPath, firstConfig, &bytes.Buffer{})
	cancelDuplicate()
	if err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("duplicate connector service error = %v", err)
	}

	cancelController()
	cancelDevice()
	if err := waitConnectorRuntime(controllerDone); err != nil {
		t.Fatal(err)
	}
	if err := waitConnectorRuntime(deviceDone); err != nil {
		t.Fatal(err)
	}
	stopped = true
	waitForRuntimeDevice(t, registry, runtimeDeviceID, false)
	waitForRuntimeDevice(t, registry, runtimeWorkerID, false)
	if !strings.Contains(controllerLog.String(), "peer connector service started") ||
		!strings.Contains(deviceLog.String(), "peer connector service started") {
		t.Fatalf("connector logs = %q / %q", controllerLog.String(), deviceLog.String())
	}
}

func assertRootMCPDevices(t *testing.T, configPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, err := loadRootMCPServer(configPath)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		serverSession.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clientSession.Close(); err != nil {
			t.Errorf("close root MCP client: %v", err)
		}
		if err := serverSession.Close(); err != nil {
			t.Errorf("close root MCP server: %v", err)
		}
	})
	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Meta:      mcp.Meta{"threadId": runtimeThreadID},
		Name:      rootmcp.ToolListDevices,
		Arguments: map[string]any{"limit": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("list_devices through root MCP = %#v", result)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output rootmcp.ListDevicesOutput
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	if len(output.Devices) != 2 || output.Devices[0].DeviceID != runtimeDeviceID ||
		output.Devices[1].DeviceID != runtimeWorkerID {
		t.Fatalf("devices through root MCP = %#v", output.Devices)
	}
}

func TestConnectorAuthorityIsReadBeforeRuntimeSideEffects(t *testing.T) {
	root := privateTestDirectory(t)
	tokenPath := filepath.Join(root, "device.token")
	if _, err := tokenfile.WriteNew(tokenPath, tokenfile.Token{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--device-id", runtimeDeviceID,
		"--device-name", "controller",
		"--broker-url", "wss://broker.example.test",
		"--auth-mode", "token",
		"--token-file", tokenPath,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := tokenfile.Validate(tokenPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = runConnectorService(ctx, configPath, cfg, &stderr)
	if err == nil || !strings.Contains(err.Error(), "read peer token") {
		t.Fatalf("missing connector authority error = %v", err)
	}
}

func setupConnectorRuntimeTest(
	t *testing.T,
	deviceID, deviceName, brokerURL string,
) (string, delegationconfig.Config) {
	t.Helper()
	configPath := privateTestPath(t, deviceName+".json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run([]string{
		"setup", "peer",
		"--config", configPath,
		"--controller-id", runtimeControllerID,
		"--device-id", deviceID,
		"--device-name", deviceName,
		"--broker-url", brokerURL,
		"--auth-mode", "none",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("setup peer code = %d, stderr = %q", code, stderr.String())
	}
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		t.Fatal(err)
	}
	return configPath, cfg
}

func waitForRuntimeDevice(t *testing.T, registry *store.Store, deviceID string, online bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, err := registry.DescribeDevice(context.Background(), runtimeControllerID, deviceID)
		if err == nil && record.Device.Online == online {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("device %s online state did not become %v", deviceID, online)
}

func waitConnectorRuntime(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		return errors.New("connector runtime did not stop")
	}
}
