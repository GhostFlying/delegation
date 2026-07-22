package workerhost

import (
	"context"
	"fmt"
	"runtime"
	"slices"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	workerProfileVersion    = 1
	workerPermissionProfile = "delegation-worker"
	windowsWorkerProfile    = ":danger-full-access"
	rootPluginEnabledConfig = "plugins.delegation@delegation.enabled"
	workerServerName        = "delegation_worker"
	workerSource            = "delegation_worker"
	workerMCPTimeout        = 10
	maximumMCPPages         = 16
	mcpPageSize             = 100
)

var requiredWorkerTools = []string{"send_message", "wait_agent"}

func (h *Host) managedConfig(worker store.WorkerReservation) map[string]any {
	config := codexconfig.Clone(h.codexConfig)
	config["features.plugins"] = false
	config["features.multi_agent"] = false
	config["features.multi_agent_v2"] = false
	config["features.enable_fanout"] = false
	config[rootPluginEnabledConfig] = false
	filesystem := map[string]any{
		":minimal":         "read",
		":workspace_roots": map[string]any{".": "read"},
	}
	addCodexRuntimeFilesystemPermission(filesystem, h.codexBinary)
	if h.providerEnvironmentFile != "" {
		filesystem[h.providerEnvironmentFile] = "deny"
	}
	if runtime.GOOS == "windows" {
		// Codex requires the elevated Windows sandbox to enforce restricted reads.
		// M2 isolates worker capabilities, not same-user filesystem access, so keep
		// non-interactive worker commands usable until sandbox provisioning exists.
		config["default_permissions"] = windowsWorkerProfile
		// An explicit trust decision prevents app-server from persisting one in the
		// isolated CODEX_HOME without enabling workspace-local Codex configuration.
		config["projects"] = map[string]any{
			worker.WorkspacePath: map[string]any{"trust_level": "untrusted"},
		}
		delete(config, "permissions."+workerPermissionProfile)
	} else {
		config["default_permissions"] = workerPermissionProfile
		config["permissions."+workerPermissionProfile] = map[string]any{
			"filesystem": filesystem,
		}
	}
	config["shell_environment_policy"] = map[string]any{
		"inherit":                 "core",
		"ignore_default_excludes": false,
		"exclude":                 append([]string(nil), h.shellExcludedEnvironment...),
	}
	config["mcp_servers."+workerServerName] = map[string]any{
		"command": h.delegationBinary,
		"args": []string{
			"mcp", "worker",
			"--config", h.peerConfigPath,
			"--tree-id", worker.TreeID,
			"--agent-id", worker.AgentID,
			"--parent-agent-id", worker.ParentAgentID,
		},
		"required":            true,
		"startup_timeout_sec": workerMCPTimeout,
	}
	return config
}

func (h *Host) verifyWorkerMCP(
	ctx context.Context,
	client application,
	threadID string,
) error {
	var cursor *string
	seenCursors := map[string]struct{}{}
	servers := make([]mcpServerStatus, 0, 1)
	finished := false
	for range maximumMCPPages {
		var page mcpStatusPage
		if err := client.MCPServerStatusList(ctx, mcpStatusParams{
			Cursor: cursor, Limit: mcpPageSize, Detail: "full", ThreadID: threadID,
		}, &page); err != nil {
			return fmt.Errorf("list managed MCP servers: %w", err)
		}
		servers = append(servers, page.Data...)
		if len(servers) > mcpPageSize {
			return blockedMCPError("inventory exceeds its bound")
		}
		if page.NextCursor == nil {
			finished = true
			break
		}
		if *page.NextCursor == "" {
			return blockedMCPError("inventory returned an empty cursor")
		}
		if _, duplicate := seenCursors[*page.NextCursor]; duplicate {
			return blockedMCPError("inventory repeated a cursor")
		}
		seenCursors[*page.NextCursor] = struct{}{}
		cursor = page.NextCursor
	}
	if !finished {
		return blockedMCPError("inventory exceeds its page bound")
	}
	if len(servers) != 1 || servers[0].Name != workerServerName {
		return blockedMCPError("inventory contains unexpected servers: %v", serverNames(servers))
	}
	server := servers[0]
	if server.AuthStatus != "unsupported" {
		return blockedMCPError("auth status is %q", server.AuthStatus)
	}
	if len(server.Resources) != 0 || len(server.ResourceTemplates) != 0 {
		return blockedMCPError(
			"resource inventory is not empty: %d resources, %d templates",
			len(server.Resources),
			len(server.ResourceTemplates),
		)
	}
	tools := make([]string, 0, len(server.Tools))
	for name := range server.Tools {
		tools = append(tools, name)
	}
	slices.Sort(tools)
	if !slices.Equal(tools, requiredWorkerTools) {
		return blockedMCPError("tools are %v", tools)
	}
	return nil
}

func blockedMCPError(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrMCPInjectionBlocked}, args...)...)
}

func serverNames(servers []mcpServerStatus) []string {
	names := make([]string, len(servers))
	for index := range servers {
		names[index] = servers[index].Name
	}
	return names
}
