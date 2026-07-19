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
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/pathguard"
	"github.com/GhostFlying/delegation/internal/rootmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func runMCP(args []string, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "root" {
		fmt.Fprintln(stderr, "usage: delegation mcp root [--config path]")
		return exitUsage
	}
	defaultPath, err := delegationconfig.DefaultPeerPath()
	if err != nil {
		return writeError(stderr, err)
	}
	flags := flag.NewFlagSet("delegation mcp root", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultPath, "peer configuration file path")
	if code := parseFlags(flags, args[1:]); code >= 0 {
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

func loadRootMCPServer(configPath string) (*mcp.Server, error) {
	cfg, err := delegationconfig.Read(configPath)
	if err != nil {
		return nil, err
	}
	if cfg.Role != delegationconfig.RolePeer {
		return nil, errors.New("root MCP requires a peer configuration")
	}
	if err := pathguard.ValidateConnectorAuthority(configPath, cfg.Broker.Auth.TokenFile); err != nil {
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
