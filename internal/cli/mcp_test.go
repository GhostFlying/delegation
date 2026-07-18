package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/rootmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	mcpTestControllerID = "123e4567-e89b-42d3-a456-426614174600"
	mcpTestDeviceID     = "123e4567-e89b-42d3-a456-426614174601"
)

func TestRootMCPInitializesOfflineWithoutReadingDeviceToken(t *testing.T) {
	configPath := writeRootMCPConfig(t, delegationconfig.RoleController)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	runDone := make(chan error, 1)
	go func() {
		runDone <- runRootMCP(context.Background(), configPath, serverTransport)
	}()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 2 || tools.Tools[0].Name != rootmcp.ToolDescribeDevice ||
		tools.Tools[1].Name != rootmcp.ToolListDevices {
		t.Fatalf("root MCP tools = %#v", tools.Tools)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("root MCP did not stop after its client disconnected")
	}
}

func TestRootMCPRejectsWorkerConfiguration(t *testing.T) {
	_, err := loadRootMCPServer(writeRootMCPConfig(t, delegationconfig.RoleDevice))
	if err == nil || !strings.Contains(err.Error(), "controller configuration") {
		t.Fatalf("loadRootMCPServer() error = %v", err)
	}
}

func TestRootMCPStdioProcess(t *testing.T) {
	configPath := writeRootMCPConfig(t, delegationconfig.RoleController)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRootMCPStdioHelper$")
	command.Env = append(os.Environ(),
		"DELEGATION_TEST_MCP_HELPER=1",
		"DELEGATION_CONFIG="+configPath,
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	client := mcp.NewClient(&mcp.Implementation{Name: "process-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{
		Command: command, TerminateDuration: time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("connect root MCP process: %v; stderr = %q", err, stderr.String())
	}
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list root MCP process tools: %v; stderr = %q", err, stderr.String())
	}
	if len(tools.Tools) != 2 {
		t.Fatalf("root MCP process tools = %#v", tools.Tools)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("close root MCP process: %v; stderr = %q", err, stderr.String())
	}
}

func TestRootMCPStdioHelper(t *testing.T) {
	if os.Getenv("DELEGATION_TEST_MCP_HELPER") != "1" {
		return
	}
	os.Exit(Run([]string{"mcp", "root"}, os.Stdout, os.Stderr))
}

func writeRootMCPConfig(t *testing.T, role delegationconfig.Role) string {
	t.Helper()
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	cfg := delegationconfig.Config{
		SchemaVersion: delegationconfig.CurrentSchemaVersion,
		Role:          role,
		ControllerID:  mcpTestControllerID,
		DeviceID:      mcpTestDeviceID,
		DeviceName:    "controller",
		Broker: delegationconfig.BrokerConfig{
			URL: "ws://127.0.0.1:1",
			Auth: delegationconfig.AuthConfig{
				Mode:      delegationconfig.AuthModeToken,
				TokenFile: filepath.Join(directory, "missing-device-token"),
			},
		},
	}
	if role == delegationconfig.RoleDevice {
		cfg.DeviceName = "worker"
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return configPath
}
