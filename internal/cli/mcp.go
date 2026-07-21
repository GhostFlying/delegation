package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/rootmcp"
	"github.com/GhostFlying/delegation/internal/workermcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func runMCP(args []string, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: delegation mcp <root|worker> [options]")
		return exitUsage
	}
	switch args[0] {
	case "root":
		return runRootMCPCommand(args[1:], stderr)
	case "worker":
		return runWorkerMCPCommand(args[1:], stderr)
	default:
		fmt.Fprintln(stderr, "usage: delegation mcp <root|worker> [options]")
		return exitUsage
	}
}

func runRootMCPCommand(args []string, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPeerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation mcp root", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "peer configuration file path")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runRootMCP(ctx, resolvedConfig, &mcp.StdioTransport{}); err != nil &&
		!errors.Is(err, context.Canceled) {
		return writeError(stderr, err)
	}
	return 0
}

func runWorkerMCPCommand(args []string, stderr io.Writer) int {
	defaultPath, err := delegationconfig.DefaultPeerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation mcp worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "peer configuration file path")
	treeID := flags.String("tree-id", "", "managed worker tree UUID")
	agentID := flags.String("agent-id", "", "managed worker agent UUID")
	parentAgentID := flags.String("parent-agent-id", "", "managed worker parent agent UUID")
	if code := parseFlags(flags, args); code >= 0 {
		return code
	}
	for _, required := range []struct {
		name  string
		value string
	}{
		{name: "--tree-id", value: *treeID},
		{name: "--agent-id", value: *agentID},
		{name: "--parent-agent-id", value: *parentAgentID},
	} {
		if required.value == "" {
			return writeError(stderr, fmt.Errorf("%s is required", required.name))
		}
	}
	resolvedConfig, err := absolutePath(*configPath)
	if err != nil {
		return writeError(stderr, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	principal := control.PrincipalIdentity{
		TreeID:        *treeID,
		AgentID:       *agentID,
		ParentAgentID: *parentAgentID,
	}
	if err := runWorkerMCP(ctx, resolvedConfig, principal, &mcp.StdioTransport{}); err != nil &&
		!errors.Is(err, context.Canceled) {
		return writeError(stderr, err)
	}
	return 0
}

func runRootMCP(ctx context.Context, configPath string, transport mcp.Transport) error {
	server, err := loadRootMCPServer(configPath)
	if err != nil {
		return err
	}
	if err := server.Run(ctx, transport); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("run root MCP: %w", err)
	}
	return nil
}

func runWorkerMCP(
	ctx context.Context,
	configPath string,
	principal control.PrincipalIdentity,
	transport mcp.Transport,
) error {
	server, err := loadWorkerMCPServer(configPath, principal)
	if err != nil {
		return err
	}
	if err := server.Run(ctx, transport); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("run worker MCP: %w", err)
	}
	return nil
}

func loadRootMCPServer(configPath string) (*mcp.Server, error) {
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		return nil, err
	}
	if cfg.Role != delegationconfig.RolePeer {
		return nil, errors.New("root MCP requires a peer configuration")
	}
	if err := pathguard.ValidatePeerAuthority(configPath, cfg.Peer.StateFile, cfg.Broker.Auth.TokenFile); err != nil {
		return nil, err
	}
	endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
	if err != nil {
		return nil, err
	}
	backend, err := localbridge.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	return rootmcp.NewServer(backend, cfg.ControllerID, cfg.DeviceID)
}

func loadWorkerMCPServer(
	configPath string,
	principal control.PrincipalIdentity,
) (*mcp.Server, error) {
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		return nil, err
	}
	if cfg.Role != delegationconfig.RolePeer {
		return nil, errors.New("worker MCP requires a peer configuration")
	}
	if err := pathguard.ValidatePeerAuthority(configPath, cfg.Peer.StateFile, cfg.Broker.Auth.TokenFile); err != nil {
		return nil, err
	}
	principal.ControllerID = cfg.ControllerID
	principal.DeviceID = cfg.DeviceID
	if err := principal.Validate(); err != nil {
		return nil, fmt.Errorf("managed worker principal: %w", err)
	}
	if principal.ParentAgentID == "" {
		return nil, errors.New("managed worker parentAgentId is required")
	}
	endpoint, err := localbridge.Endpoint(cfg.ControllerID, cfg.DeviceID)
	if err != nil {
		return nil, err
	}
	backend, err := localbridge.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	return workermcp.NewServer(backend, principal)
}
