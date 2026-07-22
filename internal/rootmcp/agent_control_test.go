package rootmcp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestRootMCPControlsAgentsByIDAndTaskTarget(t *testing.T) {
	backend := &fakeRootBackend{}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()

	tests := []struct {
		tool        string
		arguments   map[string]any
		wantID      string
		wantAction  protocol.AgentOperationAction
		wantOutcome protocol.AgentOperationOutcome
	}{
		{
			tool: ToolSendMessage,
			arguments: map[string]any{
				"target": rootMCPWorkerID, "message_id": rootMCPMessageID,
				"message": "Send the current status.",
			},
			wantID: rootMCPMessageID, wantAction: protocol.AgentOperationSend,
			wantOutcome: protocol.AgentOperationOutcomeQueued,
		},
		{
			tool: ToolFollowupTask,
			arguments: map[string]any{
				"target": "windows_build", "operation_id": rootMCPFollowupID,
				"message": "Run the focused test again.",
			},
			wantID: rootMCPFollowupID, wantAction: protocol.AgentOperationFollowup,
			wantOutcome: protocol.AgentOperationOutcomeStarted,
		},
		{
			tool: ToolInterruptAgent,
			arguments: map[string]any{
				"target": "/root/windows_build", "operation_id": rootMCPInterruptID,
			},
			wantID: rootMCPInterruptID, wantAction: protocol.AgentOperationInterrupt,
			wantOutcome: protocol.AgentOperationOutcomeInterrupted,
		},
	}
	for _, test := range tests {
		t.Run(test.tool, func(t *testing.T) {
			result := callTool(t, ctx, clientSession, test.tool, rootMCPThreadID, test.arguments)
			if result.IsError {
				t.Fatalf("%s result = %#v", test.tool, result)
			}
			var output AgentOperationOutput
			decodeStructured(t, result.StructuredContent, &output)
			if output.OperationID != test.wantID || output.AgentID != rootMCPWorkerID ||
				output.Action != test.wantAction || output.Outcome != test.wantOutcome ||
				output.FailureCode != "" {
				t.Fatalf("%s output = %#v", test.tool, output)
			}
			data, err := json.Marshal(result.StructuredContent)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "previous_status") {
				t.Fatalf("%s output invented previous_status: %s", test.tool, data)
			}
		})
	}

	calls := backend.snapshot()
	if len(calls) != 8 || calls[1].method != protocol.MethodSendAgent ||
		calls[3].method != protocol.MethodListAgents ||
		calls[4].method != protocol.MethodFollowupAgent ||
		calls[6].method != protocol.MethodListAgents ||
		calls[7].method != protocol.MethodInterruptAgent {
		t.Fatalf("agent control calls = %#v", calls)
	}
	send := calls[1].params.(protocol.SendAgentParams)
	followup := calls[4].params.(protocol.FollowupAgentParams)
	interrupt := calls[7].params.(protocol.InterruptAgentParams)
	if send.AgentID != rootMCPWorkerID || send.MessageID != rootMCPMessageID ||
		followup.AgentID != rootMCPWorkerID || followup.OperationID != rootMCPFollowupID ||
		interrupt.AgentID != rootMCPWorkerID || interrupt.OperationID != rootMCPInterruptID {
		t.Fatalf("agent control idempotency parameters = %#v, %#v, %#v", send, followup, interrupt)
	}
}

func TestRootMCPRejectsUnsupportedAgentTargets(t *testing.T) {
	backend := &fakeRootBackend{}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	for _, target := range []string{"/root", "/root/child/grandchild", "child/grandchild", "/other/child"} {
		result := callTool(t, ctx, clientSession, ToolInterruptAgent, rootMCPThreadID, map[string]any{
			"target": target, "operation_id": rootMCPInterruptID,
		})
		if !result.IsError {
			t.Fatalf("target %q was accepted: %#v", target, result)
		}
	}
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("invalid targets reached backend: %#v", calls)
	}

	result := callTool(t, ctx, clientSession, ToolInterruptAgent, rootMCPThreadID, map[string]any{
		"target": "missing_agent", "operation_id": rootMCPInterruptID,
	})
	if !result.IsError || !strings.Contains(toolText(result), "was not found") {
		t.Fatalf("unknown target result = %#v", result)
	}
	calls := backend.snapshot()
	if len(calls) != 2 || calls[0].method != protocol.MethodEnsureRootTree ||
		calls[1].method != protocol.MethodListAgents {
		t.Fatalf("unknown target calls = %#v", calls)
	}
}

func TestRootMCPRejectsAmbiguousTaskTargetAndMismatchedResult(t *testing.T) {
	first := testAgent(rootMCPThreadID, rootMCPWorkerID, "duplicate", 1)
	second := testAgent(rootMCPMessageID, rootMCPWorkerID, "duplicate", 2)
	second.Principal.AgentID = rootMCPFollowupID
	backend := &fakeRootBackend{agentsResult: &protocol.ListAgentsResult{
		Agents: []protocol.AgentSummary{first, second},
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	result := callTool(t, ctx, clientSession, ToolInterruptAgent, rootMCPThreadID, map[string]any{
		"target": "duplicate", "operation_id": rootMCPInterruptID,
	})
	closeSessions()
	if !result.IsError || !strings.Contains(toolText(result), "ambiguous") {
		t.Fatalf("ambiguous target result = %#v", result)
	}

	backend = &fakeRootBackend{operationResult: &protocol.AgentOperationResult{
		OperationID: rootMCPInterruptID, AgentID: rootMCPWorkerID,
		Action: protocol.AgentOperationFollowup, Outcome: protocol.AgentOperationOutcomeStarted,
	}}
	ctx, clientSession, closeSessions = connectRootMCP(t, backend)
	defer closeSessions()
	result = callTool(t, ctx, clientSession, ToolInterruptAgent, rootMCPThreadID, map[string]any{
		"target": rootMCPWorkerID, "operation_id": rootMCPInterruptID,
	})
	if !result.IsError || !strings.Contains(toolText(result), "mismatched agent operation") {
		t.Fatalf("mismatched operation result = %#v", result)
	}
}

type deadlineRootBackend struct {
	delegate  *fakeRootBackend
	mu        sync.Mutex
	deadlines map[string]time.Duration
}

func (b *deadlineRootBackend) Call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	deadline, ok := ctx.Deadline()
	if ok {
		b.mu.Lock()
		b.deadlines[method] = time.Until(deadline)
		b.mu.Unlock()
	}
	return b.delegate.Call(ctx, method, treeID, source, params, result)
}

func TestRootMCPAgentControlsUseLongCallDeadline(t *testing.T) {
	backend := &deadlineRootBackend{
		delegate: &fakeRootBackend{}, deadlines: make(map[string]time.Duration),
	}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, ctx, clientSession, ToolSendMessage, rootMCPThreadID, map[string]any{
		"target": rootMCPWorkerID, "message_id": rootMCPMessageID, "message": "status",
	})
	if result.IsError {
		t.Fatalf("send_message result = %#v", result)
	}
	backend.mu.Lock()
	ensureRemaining := backend.deadlines[protocol.MethodEnsureRootTree]
	operationRemaining := backend.deadlines[protocol.MethodSendAgent]
	backend.mu.Unlock()
	if ensureRemaining <= 0 || ensureRemaining > bridgeCallTimeout ||
		operationRemaining <= 2*time.Minute || operationRemaining > agentCallTimeout {
		t.Fatalf("root MCP deadlines = ensure %s, operation %s", ensureRemaining, operationRemaining)
	}
}
