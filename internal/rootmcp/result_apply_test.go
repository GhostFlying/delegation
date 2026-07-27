package rootmcp

import (
	"path/filepath"
	"testing"

	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	rootMCPApplyID   = "123e4567-e89b-42d3-a456-426614174410"
	rootMCPPackageID = "123e4567-e89b-42d3-a456-426614174411"
)

func TestRootMCPApplyAgentChangesUsesOnlyTrustedSandboxPath(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "repository", "nested")
	backend := &fakeRootBackend{applyResult: &localbridge.ApplyAgentChangesResult{
		ApplyID: rootMCPApplyID, PackageID: rootMCPPackageID,
		Outcome: localbridge.ApplyAgentChangesApplied,
	}}
	ctx, client, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{
		Meta: mcp.Meta{
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": localFileURI(cwd)},
		},
		Name: ToolApplyAgentChanges,
		Arguments: map[string]any{
			"apply_id": rootMCPApplyID, "package_id": rootMCPPackageID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("apply_agent_changes failed: %s", toolText(result))
	}
	var output ApplyAgentChangesOutput
	decodeStructured(t, result.StructuredContent, &output)
	if output != (ApplyAgentChangesOutput{
		ApplyID: rootMCPApplyID, PackageID: rootMCPPackageID, Outcome: resultApplyApplied,
	}) {
		t.Fatalf("apply output = %#v", output)
	}
	calls := backend.snapshot()
	if len(calls) != 2 || calls[0].method != protocol.MethodEnsureRootTree ||
		calls[1].method != localbridge.MethodApplyAgentChanges {
		t.Fatalf("backend calls = %#v", calls)
	}
	params, ok := calls[1].params.(localbridge.ApplyAgentChangesParams)
	if !ok || params.ApplyID != rootMCPApplyID || params.PackageID != rootMCPPackageID ||
		params.SourcePath != cwd {
		t.Fatalf("local apply params = %#v", calls[1].params)
	}
	if calls[1].treeID != rootMCPTreeID || calls[1].source == nil ||
		calls[1].source.AgentID != rootMCPAgentID || calls[1].source.DeviceID != rootMCPDeviceID {
		t.Fatalf("local apply authority = %#v", calls[1])
	}
}

func TestRootMCPApplyAgentChangesReturnsStructuredConflict(t *testing.T) {
	backend := &fakeRootBackend{applyResult: &localbridge.ApplyAgentChangesResult{
		ApplyID: rootMCPApplyID, PackageID: rootMCPPackageID,
		Outcome:     localbridge.ApplyAgentChangesNeedsResolution,
		FailureCode: "root_workspace_conflict",
	}}
	ctx, client, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{
		Meta: mcp.Meta{
			"threadId":                 rootMCPThreadID,
			sandboxStateMetaCapability: map[string]any{"sandboxCwd": localFileURI(t.TempDir())},
		},
		Name: ToolApplyAgentChanges,
		Arguments: map[string]any{
			"apply_id": rootMCPApplyID, "package_id": rootMCPPackageID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("needs-resolution result is an MCP error: %s", toolText(result))
	}
	var output ApplyAgentChangesOutput
	decodeStructured(t, result.StructuredContent, &output)
	if output.Outcome != resultApplyResolution || output.FailureCode != "root_workspace_conflict" {
		t.Fatalf("apply conflict output = %#v", output)
	}
}
