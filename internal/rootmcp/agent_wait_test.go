package rootmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type serialWaitBackend struct {
	started chan struct{}
	release chan struct{}
	waits   atomic.Int32
}

type repollWaitBackend struct {
	waits atomic.Int32
}

func (b *repollWaitBackend) Call(
	_ context.Context,
	method string,
	_ string,
	_ *control.PrincipalIdentity,
	params, result any,
) error {
	switch method {
	case protocol.MethodEnsureRootTree:
		input := params.(protocol.EnsureRootTreeParams)
		*result.(*protocol.EnsureRootTreeResult) = rootResult(input.ExternalThreadID)
	case protocol.MethodWaitAgent:
		sequence := b.waits.Add(1)
		input := params.(protocol.WaitAgentParams)
		response := protocol.WaitAgentResult{
			Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
			NextMailboxCursor: input.MailboxCursor, NextLifecycleCursor: input.LifecycleCursor,
		}
		if sequence == 3 {
			response.Activities = []protocol.AgentLifecycleActivity{{
				AgentID: rootMCPWorkerID, TargetDeviceID: rootMCPDeviceID,
				TargetRevision: 1, Phase: protocol.WorkerLifecycleIdle,
				Sequence: 1, ObservedAt: 1,
			}}
			response.NextLifecycleCursor = 1
		}
		*result.(*protocol.WaitAgentResult) = response
	}
	return nil
}

func (b *serialWaitBackend) Call(
	ctx context.Context,
	method string,
	_ string,
	_ *control.PrincipalIdentity,
	params, result any,
) error {
	switch method {
	case protocol.MethodEnsureRootTree:
		input := params.(protocol.EnsureRootTreeParams)
		*result.(*protocol.EnsureRootTreeResult) = rootResult(input.ExternalThreadID)
		return nil
	case protocol.MethodWaitAgent:
		sequence := b.waits.Add(1)
		if sequence == 1 {
			close(b.started)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-b.release:
			}
		}
		input := params.(protocol.WaitAgentParams)
		*result.(*protocol.WaitAgentResult) = protocol.WaitAgentResult{
			Messages: []protocol.MailboxMessage{},
			Activities: []protocol.AgentLifecycleActivity{{
				AgentID: rootMCPWorkerID, TargetDeviceID: rootMCPDeviceID,
				TargetRevision: uint64(sequence), Phase: protocol.WorkerLifecycleIdle,
				Sequence: uint64(sequence), ObservedAt: int64(sequence),
			}},
			NextMailboxCursor: input.MailboxCursor, NextLifecycleCursor: uint64(sequence),
		}
		return nil
	default:
		return nil
	}
}

func TestWaitAgentKeepsIndependentCursorsOutOfModelInput(t *testing.T) {
	worker := control.NewWorkerPrincipal(
		rootMCPControllerID, rootMCPTreeID, rootMCPWorkerID, rootMCPAgentID, rootMCPDeviceID,
	).Identity()
	backend := &fakeRootBackend{waitResults: []protocol.WaitAgentResult{
		{
			Messages: []protocol.MailboxMessage{{
				MessageID: rootMCPMessageID, Sequence: 1, Source: worker,
				Message: "worker result", CreatedAt: 10,
			}},
			Activities:          []protocol.AgentLifecycleActivity{},
			NextMailboxCursor:   1,
			NextLifecycleCursor: 0,
			MoreMessages:        true,
		},
		{
			Messages: []protocol.MailboxMessage{},
			Activities: []protocol.AgentLifecycleActivity{{
				AgentID: rootMCPWorkerID, TargetDeviceID: rootMCPDeviceID,
				TargetRevision: 3, Phase: protocol.WorkerLifecycleIdle,
				Sequence: 1, ObservedAt: 11,
			}},
			NextMailboxCursor:   1,
			NextLifecycleCursor: 1,
		},
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()

	first := callTool(t, ctx, clientSession, ToolWaitAgent, rootMCPThreadID, map[string]any{
		"timeout_seconds": 1,
	})
	if first.IsError {
		t.Fatalf("first wait_agent result = %#v", first)
	}
	var firstOutput WaitAgentOutput
	decodeStructured(t, first.StructuredContent, &firstOutput)
	if len(firstOutput.Messages) != 1 || len(firstOutput.Activities) != 0 ||
		firstOutput.Messages[0].SourceAgentID != rootMCPWorkerID ||
		firstOutput.Messages[0].Message != "worker result" || !firstOutput.HasMore {
		t.Fatalf("first wait_agent output = %#v", firstOutput)
	}

	second := callTool(t, ctx, clientSession, ToolWaitAgent, rootMCPThreadID, map[string]any{
		"timeout_seconds": 1,
	})
	if second.IsError {
		t.Fatalf("second wait_agent result = %#v", second)
	}
	var secondOutput WaitAgentOutput
	decodeStructured(t, second.StructuredContent, &secondOutput)
	if len(secondOutput.Messages) != 0 || len(secondOutput.Activities) != 1 ||
		secondOutput.Activities[0].AgentID != rootMCPWorkerID ||
		secondOutput.Activities[0].Phase != protocol.WorkerLifecycleIdle || secondOutput.HasMore {
		t.Fatalf("second wait_agent output = %#v", secondOutput)
	}

	var waits []protocol.WaitAgentParams
	for _, call := range backend.snapshot() {
		if call.method == protocol.MethodWaitAgent {
			waits = append(waits, call.params.(protocol.WaitAgentParams))
		}
	}
	if len(waits) != 2 || waits[0].MailboxCursor != 0 || waits[0].LifecycleCursor != 0 ||
		waits[1].MailboxCursor != 1 || waits[1].LifecycleCursor != 0 ||
		waits[0].MessageLimit != agentWaitMessageLimit ||
		waits[0].ActivityLimit != agentWaitActivityLimit {
		t.Fatalf("wait_agent broker params = %#v", waits)
	}
}

func TestWaitAgentDoesNotAdvanceCursorForInvalidResult(t *testing.T) {
	worker := control.NewWorkerPrincipal(
		rootMCPControllerID, rootMCPTreeID, rootMCPWorkerID, rootMCPAgentID, rootMCPDeviceID,
	).Identity()
	message := protocol.MailboxMessage{
		MessageID: rootMCPMessageID, Sequence: 1, Source: worker,
		Message: "retry result", CreatedAt: 10,
	}
	backend := &fakeRootBackend{waitResults: []protocol.WaitAgentResult{
		{
			Messages: []protocol.MailboxMessage{message}, Activities: []protocol.AgentLifecycleActivity{},
			NextMailboxCursor: 2,
		},
		{
			Messages: []protocol.MailboxMessage{message}, Activities: []protocol.AgentLifecycleActivity{},
			NextMailboxCursor: 1,
		},
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()

	invalid := callTool(t, ctx, clientSession, ToolWaitAgent, rootMCPThreadID, map[string]any{
		"timeout_seconds": 1,
	})
	if !invalid.IsError {
		t.Fatalf("invalid wait_agent result was accepted: %#v", invalid)
	}
	retried := callTool(t, ctx, clientSession, ToolWaitAgent, rootMCPThreadID, map[string]any{
		"timeout_seconds": 1,
	})
	if retried.IsError {
		t.Fatalf("valid wait_agent retry = %#v", retried)
	}

	var waits []protocol.WaitAgentParams
	for _, call := range backend.snapshot() {
		if call.method == protocol.MethodWaitAgent {
			waits = append(waits, call.params.(protocol.WaitAgentParams))
		}
	}
	if len(waits) != 2 || waits[0].MailboxCursor != 0 || waits[1].MailboxCursor != 0 {
		t.Fatalf("wait_agent cursor advanced after invalid result: %#v", waits)
	}
}

func TestWaitAgentSerializesConcurrentCallsForOneTask(t *testing.T) {
	backend := &serialWaitBackend{started: make(chan struct{}), release: make(chan struct{})}
	root := &Root{
		backend: backend, controllerID: rootMCPControllerID, deviceID: rootMCPDeviceID,
		waitStates: make(map[string]*agentWaitState),
	}
	request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Meta: mcp.Meta{"threadId": rootMCPThreadID}, Name: ToolWaitAgent,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		_, _, err := root.waitAgent(ctx, request, WaitAgentInput{TimeoutSeconds: 1})
		results <- err
	}()
	select {
	case <-backend.started:
	case <-ctx.Done():
		t.Fatal("first wait_agent call did not reach backend")
	}
	go func() {
		_, _, err := root.waitAgent(ctx, request, WaitAgentInput{TimeoutSeconds: 1})
		results <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if calls := backend.waits.Load(); calls != 1 {
		t.Fatalf("concurrent wait_agent calls reached backend %d times before cursor update", calls)
	}
	close(backend.release)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("serialized wait_agent calls did not finish")
		}
	}
	if calls := backend.waits.Load(); calls != 2 {
		t.Fatalf("serialized wait_agent backend calls = %d", calls)
	}
}

func TestMaximumWaitAgentPageFitsOutputLimit(t *testing.T) {
	worker := control.NewWorkerPrincipal(
		rootMCPControllerID, rootMCPTreeID, rootMCPWorkerID, rootMCPAgentID, rootMCPDeviceID,
	).Identity()
	result := protocol.WaitAgentResult{
		Messages:   make([]protocol.MailboxMessage, 0, agentWaitMessageLimit),
		Activities: make([]protocol.AgentLifecycleActivity, 0, agentWaitActivityLimit),
	}
	for index := range agentWaitMessageLimit {
		result.Messages = append(result.Messages, protocol.MailboxMessage{
			MessageID: rootMCPMessageID, Sequence: uint64(index + 1), Source: worker,
			Message: strings.Repeat("\x01", protocol.MaximumMailboxMessageBytes), CreatedAt: 1,
		})
	}
	for index := range agentWaitActivityLimit {
		result.Activities = append(result.Activities, protocol.AgentLifecycleActivity{
			AgentID: rootMCPWorkerID, TargetDeviceID: rootMCPDeviceID,
			TargetRevision: uint64(index + 1), Phase: protocol.WorkerLifecycleFailed,
			FailureCode: strings.Repeat("a", protocol.MaximumFailureCodeBytes),
			Sequence:    uint64(index + 1), ObservedAt: 1,
		})
	}
	result.NextMailboxCursor = uint64(len(result.Messages))
	result.NextLifecycleCursor = uint64(len(result.Activities))
	if err := validateWaitAgentResult(result, protocol.WaitAgentParams{
		MessageLimit: agentWaitMessageLimit, ActivityLimit: agentWaitActivityLimit,
	}, rootResult(rootMCPThreadID).Principal); err != nil {
		t.Fatal(err)
	}
	output := waitAgentOutput(result)
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 4*1024 {
		t.Fatalf("worst-case agent wait output did not exercise JSON expansion: %d bytes", len(data))
	}
	if err := enforceOutputLimit(output, maximumAgentWaitBytes); err != nil {
		t.Fatalf("maximum valid agent wait page is %d bytes: %v", len(data), err)
	}
}

func TestWaitStateEvictsIdleLeastRecentlyUsedThread(t *testing.T) {
	root := &Root{waitStates: make(map[string]*agentWaitState)}
	for index := range maximumAgentWaitStates {
		_, release, err := root.waitState(fmt.Sprintf("thread-%03d", index))
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	_, release, err := root.waitState("thread-new")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if len(root.waitStates) != maximumAgentWaitStates {
		t.Fatalf("wait state count = %d", len(root.waitStates))
	}
	if _, found := root.waitStates["thread-000"]; found {
		t.Fatal("least recently used idle wait state was not evicted")
	}
	if _, found := root.waitStates["thread-new"]; !found {
		t.Fatal("new wait state was not retained")
	}
}

func TestWaitStateRejectsOnlyWhenEveryStateIsActive(t *testing.T) {
	root := &Root{waitStates: make(map[string]*agentWaitState)}
	releases := make([]func(), 0, maximumAgentWaitStates)
	for index := range maximumAgentWaitStates {
		_, release, err := root.waitState(fmt.Sprintf("active-%03d", index))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, _, err := root.waitState("active-overflow"); err == nil {
		t.Fatal("wait state capacity accepted a new thread while every state was active")
	}
	for _, release := range releases {
		release()
	}
}

func TestWaitAgentBacksOffAfterEarlyEmptyResponses(t *testing.T) {
	backend := &repollWaitBackend{}
	root := &Root{
		backend: backend, controllerID: rootMCPControllerID, deviceID: rootMCPDeviceID,
		waitStates: make(map[string]*agentWaitState),
	}
	request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Meta: mcp.Meta{"threadId": rootMCPThreadID}, Name: ToolWaitAgent,
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	_, output, err := root.waitAgent(ctx, request, WaitAgentInput{TimeoutSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Activities) != 1 || output.Activities[0].AgentID != rootMCPWorkerID {
		t.Fatalf("repoll output = %#v", output)
	}
	if waits := backend.waits.Load(); waits != 3 {
		t.Fatalf("early empty wait_agent backend calls = %d", waits)
	}
	if elapsed := time.Since(started); elapsed < 2*minimumAgentRepollDelay {
		t.Fatalf("two agent wait backoffs completed in %s", elapsed)
	}
}
