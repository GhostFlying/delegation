package rootmcp

import (
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const rootMCPWorkspaceID = "123e4567-e89b-42d3-a456-426614174409"

func TestRootMCPSyncWorkspaceUsesTrustedSandboxMetadata(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "repository", "nested")
	manifestHash := strings.Repeat("a", 64)
	backend := &fakeRootBackend{workspaceResult: &protocol.SyncWorkspaceResult{
		Outcome: protocol.WorkspacePrepareReady,
		Workspace: &protocol.WorkspaceSummary{
			WorkspaceID: rootMCPWorkspaceID, SourceDeviceID: rootMCPDeviceID,
			TargetDeviceID: rootMCPWorkerID,
			HeadOID:        strings.Repeat("b", 40), ObjectFormat: "sha1",
			WorkingDirectory: "nested", Strategy: protocol.WorkspaceStrategyDirect,
			ManifestHash: manifestHash, Warnings: []string{},
		},
		Warnings: []string{},
	}}
	ctx, client, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	capabilities := client.InitializeResult().Capabilities
	if capabilities == nil || capabilities.Experimental == nil {
		t.Fatal("root MCP did not advertise experimental capabilities")
	}
	if _, found := capabilities.Experimental[sandboxStateMetaCapability]; !found {
		t.Fatalf("root MCP capabilities = %#v", capabilities.Experimental)
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{
		Meta: mcp.Meta{
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": localFileURI(cwd)},
		},
		Name: ToolSyncWorkspace,
		Arguments: map[string]any{
			"sync_id": rootMCPWorkspaceID, "target_device_id": rootMCPWorkerID,
			"git_url": "ssh://git@example.invalid/repository.git",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("sync_workspace failed: %s", toolText(result))
	}
	var output SyncWorkspaceOutput
	decodeStructured(t, result.StructuredContent, &output)
	if output.WorkspaceID != rootMCPWorkspaceID || output.HeadOID != strings.Repeat("b", 40) ||
		output.Outcome != protocol.WorkspacePrepareReady {
		t.Fatalf("sync output = %#v", output)
	}
	calls := backend.snapshot()
	if len(calls) != 2 || calls[0].method != protocol.MethodEnsureRootTree ||
		calls[1].method != protocol.MethodSyncWorkspace {
		t.Fatalf("backend calls = %#v", calls)
	}
	params, ok := calls[1].params.(protocol.SyncWorkspaceParams)
	if !ok || params.SourcePath != cwd || params.SyncID != rootMCPWorkspaceID ||
		params.TargetDeviceID != rootMCPWorkerID {
		t.Fatalf("workspace params = %#v", calls[1].params)
	}
	if calls[1].treeID != rootMCPTreeID || calls[1].source == nil ||
		calls[1].source.AgentID != rootMCPAgentID || calls[1].source.DeviceID != rootMCPDeviceID {
		t.Fatalf("workspace authority = %#v", calls[1])
	}
}

func TestRootMCPSyncWorkspaceBoundsInvalidBackendErrors(t *testing.T) {
	oversized := strings.Repeat("sensitive-invalid-warning", 16*1024)
	backend := &fakeRootBackend{workspaceResult: &protocol.SyncWorkspaceResult{
		Outcome:  protocol.WorkspacePrepareReady,
		Warnings: []string{oversized},
	}}
	ctx, client, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{
		Meta: mcp.Meta{
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": localFileURI(t.TempDir())},
		},
		Name: ToolSyncWorkspace,
		Arguments: map[string]any{
			"sync_id": rootMCPWorkspaceID, "target_device_id": rootMCPWorkerID,
			"git_url": "ssh://git@example.invalid/repository.git",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := toolText(result)
	if !result.IsError || text != "delegation service returned an invalid workspace" ||
		strings.Contains(text, oversized[:64]) || len(text) > 256 {
		t.Fatalf("invalid backend result was not sanitized: error=%t bytes=%d text=%q", result.IsError, len(text), text)
	}
}

func TestRootMCPSyncWorkspaceRejectsUntrustedSandboxMetadata(t *testing.T) {
	cases := map[string]mcp.Meta{
		"missing": {"threadId": rootMCPThreadID},
		"wrong type": {
			"threadId": rootMCPThreadID, sandboxStateMetaCapability: "not an object",
		},
		"missing cwd": {
			"threadId": rootMCPThreadID, sandboxStateMetaCapability: map[string]any{},
		},
		"non file URI": {
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": "https://example.invalid/repo"},
		},
		"relative file URI": {
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": "file:relative"},
		},
	}
	if runtime.GOOS != "windows" {
		cases["remote authority"] = mcp.Meta{
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": "file://remote.invalid/repo"},
		}
	}
	for name, metadata := range cases {
		t.Run(name, func(t *testing.T) {
			backend := &fakeRootBackend{}
			ctx, client, closeSessions := connectRootMCP(t, backend)
			defer closeSessions()
			result, err := client.CallTool(ctx, &mcp.CallToolParams{
				Meta: metadata, Name: ToolSyncWorkspace,
				Arguments: map[string]any{
					"sync_id": rootMCPWorkspaceID, "target_device_id": rootMCPWorkerID,
					"git_url": "ssh://git@example.invalid/repository.git",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError || toolText(result) == "" {
				t.Fatalf("sync_workspace result = %#v", result)
			}
			if calls := backend.snapshot(); len(calls) != 0 {
				t.Fatalf("untrusted metadata reached backend: %#v", calls)
			}
		})
	}
}

func localFileURI(path string) string {
	slashPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	return (&url.URL{Scheme: "file", Path: slashPath}).String()
}
