package rootmcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	rootMCPControllerID = "123e4567-e89b-42d3-a456-426614174400"
	rootMCPDeviceID     = "123e4567-e89b-42d3-a456-426614174401"
	rootMCPThreadID     = "123e4567-e89b-42d3-a456-426614174402"
	rootMCPTreeID       = "123e4567-e89b-42d3-a456-426614174403"
	rootMCPAgentID      = "123e4567-e89b-42d3-a456-426614174404"
	rootMCPWorkerID     = "123e4567-e89b-42d3-a456-426614174405"
	rootMCPMessageID    = "123e4567-e89b-42d3-a456-426614174406"
	rootMCPFollowupID   = "123e4567-e89b-42d3-a456-426614174407"
	rootMCPInterruptID  = "123e4567-e89b-42d3-a456-426614174408"
)

type rootMCPCall struct {
	method string
	treeID string
	source *control.PrincipalIdentity
	params any
}

type fakeRootBackend struct {
	mu              sync.Mutex
	calls           []rootMCPCall
	err             error
	ensureResult    *protocol.EnsureRootTreeResult
	listResult      *protocol.ListDevicesResult
	describeResult  *protocol.DescribeDeviceResult
	spawnResult     *protocol.SpawnAgentResult
	agentsResult    *protocol.ListAgentsResult
	operationResult *protocol.AgentOperationResult
	waitResults     []protocol.WaitAgentResult
}

type cancelRootBackend struct {
	started  chan struct{}
	canceled chan error
}

func (b *cancelRootBackend) Call(
	ctx context.Context,
	_ string,
	_ string,
	_ *control.PrincipalIdentity,
	_ any,
	_ any,
) error {
	close(b.started)
	<-ctx.Done()
	b.canceled <- ctx.Err()
	return ctx.Err()
}

func (b *fakeRootBackend) Call(
	_ context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	call := rootMCPCall{method: method, treeID: treeID, params: params}
	if source != nil {
		copy := *source
		call.source = &copy
	}
	b.mu.Lock()
	b.calls = append(b.calls, call)
	err := b.err
	ensureResult := b.ensureResult
	listResult := b.listResult
	describeResult := b.describeResult
	spawnResult := b.spawnResult
	agentsResult := b.agentsResult
	operationResult := b.operationResult
	var waitResult *protocol.WaitAgentResult
	if method == protocol.MethodWaitAgent && len(b.waitResults) != 0 {
		copy := b.waitResults[0]
		b.waitResults = b.waitResults[1:]
		waitResult = &copy
	}
	b.mu.Unlock()
	if err != nil {
		return err
	}
	switch method {
	case protocol.MethodEnsureRootTree:
		if ensureResult != nil {
			*result.(*protocol.EnsureRootTreeResult) = *ensureResult
			break
		}
		input := params.(protocol.EnsureRootTreeParams)
		*result.(*protocol.EnsureRootTreeResult) = rootResult(input.ExternalThreadID)
	case protocol.MethodListDevices:
		if listResult != nil {
			*result.(*protocol.ListDevicesResult) = *listResult
			break
		}
		input := params.(protocol.ListDevicesParams)
		page := protocol.ListDevicesResult{
			Revision: 7,
			Devices: []control.Device{
				testDevice(rootMCPDeviceID, 24),
				testDevice(rootMCPWorkerID, 2),
			},
			NextCursor: rootMCPWorkerID,
		}
		if input.AfterDeviceID != "" {
			page.Devices = []control.Device{}
			page.NextCursor = ""
		}
		*result.(*protocol.ListDevicesResult) = page
	case protocol.MethodDescribeDevice:
		if describeResult != nil {
			*result.(*protocol.DescribeDeviceResult) = *describeResult
			break
		}
		*result.(*protocol.DescribeDeviceResult) = protocol.DescribeDeviceResult{
			Revision: 8,
			Device:   testDevice(params.(protocol.DescribeDeviceParams).DeviceID, 24),
		}
	case protocol.MethodSpawnAgent:
		if spawnResult != nil {
			*result.(*protocol.SpawnAgentResult) = *spawnResult
			break
		}
		input := params.(protocol.SpawnAgentParams)
		*result.(*protocol.SpawnAgentResult) = protocol.SpawnAgentResult{
			Agent: testAgent(input.SpawnID, input.TargetDeviceID, input.TaskName, 1),
		}
	case protocol.MethodListAgents:
		if agentsResult != nil {
			*result.(*protocol.ListAgentsResult) = *agentsResult
			break
		}
		input := params.(protocol.ListAgentsParams)
		*result.(*protocol.ListAgentsResult) = protocol.ListAgentsResult{
			Agents: []protocol.AgentSummary{
				testAgent(rootMCPThreadID, rootMCPWorkerID, "windows_build", input.AfterSequence+1),
			},
		}
	case protocol.MethodSendAgent:
		if operationResult != nil {
			*result.(*protocol.AgentOperationResult) = *operationResult
			break
		}
		input := params.(protocol.SendAgentParams)
		*result.(*protocol.AgentOperationResult) = protocol.AgentOperationResult{
			OperationID: input.MessageID, AgentID: input.AgentID,
			Action: protocol.AgentOperationSend, Outcome: protocol.AgentOperationOutcomeQueued,
		}
	case protocol.MethodFollowupAgent:
		if operationResult != nil {
			*result.(*protocol.AgentOperationResult) = *operationResult
			break
		}
		input := params.(protocol.FollowupAgentParams)
		*result.(*protocol.AgentOperationResult) = protocol.AgentOperationResult{
			OperationID: input.OperationID, AgentID: input.AgentID,
			Action: protocol.AgentOperationFollowup, Outcome: protocol.AgentOperationOutcomeStarted,
		}
	case protocol.MethodInterruptAgent:
		if operationResult != nil {
			*result.(*protocol.AgentOperationResult) = *operationResult
			break
		}
		input := params.(protocol.InterruptAgentParams)
		*result.(*protocol.AgentOperationResult) = protocol.AgentOperationResult{
			OperationID: input.OperationID, AgentID: input.AgentID,
			Action: protocol.AgentOperationInterrupt, Outcome: protocol.AgentOperationOutcomeInterrupted,
		}
	case protocol.MethodWaitAgent:
		input := params.(protocol.WaitAgentParams)
		if waitResult != nil {
			*result.(*protocol.WaitAgentResult) = *waitResult
			break
		}
		*result.(*protocol.WaitAgentResult) = protocol.WaitAgentResult{
			Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
			NextMailboxCursor: input.MailboxCursor, NextLifecycleCursor: input.LifecycleCursor,
		}
	default:
		return fmt.Errorf("unexpected method %q", method)
	}
	return nil
}

func (b *fakeRootBackend) snapshot() []rootMCPCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]rootMCPCall(nil), b.calls...)
}

func TestRootMCPListsStaticToolsAndBindsThread(t *testing.T) {
	backend := &fakeRootBackend{}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 8 ||
		tools.Tools[0].Name != ToolDescribeDevice || tools.Tools[1].Name != ToolFollowupTask ||
		tools.Tools[2].Name != ToolInterruptAgent || tools.Tools[3].Name != ToolListAgents ||
		tools.Tools[4].Name != ToolListDevices || tools.Tools[5].Name != ToolSendMessage ||
		tools.Tools[6].Name != ToolSpawnAgent || tools.Tools[7].Name != ToolWaitAgent {
		t.Fatalf("root tools = %#v", tools.Tools)
	}
	for _, tool := range tools.Tools {
		mutating := tool.Name == ToolSpawnAgent || tool.Name == ToolSendMessage ||
			tool.Name == ToolFollowupTask || tool.Name == ToolInterruptAgent
		consuming := tool.Name == ToolWaitAgent
		if tool.Annotations == nil ||
			tool.Annotations.IdempotentHint != !consuming ||
			tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint ||
			tool.Annotations.OpenWorldHint == nil ||
			(mutating && (tool.Annotations.ReadOnlyHint || !*tool.Annotations.OpenWorldHint)) ||
			(consuming && (tool.Annotations.ReadOnlyHint || *tool.Annotations.OpenWorldHint)) ||
			(!mutating && !consuming &&
				(!tool.Annotations.ReadOnlyHint || *tool.Annotations.OpenWorldHint)) {
			t.Fatalf("tool annotations for %s = %#v", tool.Name, tool.Annotations)
		}
		assertToolSchema(t, tool)
	}

	first := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{"limit": 2})
	if first.IsError {
		t.Fatalf("list_devices result = %#v", first)
	}
	var firstPage ListDevicesOutput
	decodeStructured(t, first.StructuredContent, &firstPage)
	firstJSON, err := json.Marshal(first.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(firstJSON, []byte(`"role"`)) || !bytes.Contains(firstJSON, []byte(`"isCurrentDevice"`)) {
		t.Fatalf("peer summary wire shape = %s", firstJSON)
	}
	if firstPage.Revision != 7 || len(firstPage.Devices) != 2 || firstPage.NextCursor == "" ||
		len(firstPage.Devices[0].Features) != 0 ||
		!firstPage.Devices[0].FeaturesTruncated || !firstPage.Devices[1].FeaturesTruncated ||
		firstPage.Devices[0].ProtocolVersion != protocol.Version ||
		!firstPage.Devices[0].IsCurrentDevice || firstPage.Devices[1].IsCurrentDevice {
		t.Fatalf("first MCP device page = %#v", firstPage)
	}
	second := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"cursor": firstPage.NextCursor, "limit": 2,
	})
	if second.IsError {
		t.Fatalf("second list_devices result = %#v", second)
	}
	described := callTool(t, ctx, clientSession, ToolDescribeDevice, rootMCPThreadID, map[string]any{
		"deviceId": rootMCPWorkerID,
	})
	if described.IsError {
		t.Fatalf("describe_device result = %#v", described)
	}
	var description DescribeDeviceOutput
	decodeStructured(t, described.StructuredContent, &description)
	if description.Revision != 8 || description.Device.DeviceID != rootMCPWorkerID ||
		description.Device.ProtocolVersion != protocol.Version ||
		len(description.Device.Features) != 24 || description.Device.FeaturesTruncated ||
		description.Device.IsCurrentDevice {
		t.Fatalf("MCP device description = %#v", description)
	}

	calls := backend.snapshot()
	if len(calls) != 6 {
		t.Fatalf("backend calls = %#v", calls)
	}
	for index := 0; index < len(calls); index += 2 {
		if calls[index].method != protocol.MethodEnsureRootTree ||
			calls[index].params.(protocol.EnsureRootTreeParams).ExternalThreadID != rootMCPThreadID {
			t.Fatalf("root binding call %d = %#v", index, calls[index])
		}
	}
	pageParams := calls[3].params.(protocol.ListDevicesParams)
	if pageParams.AfterDeviceID != rootMCPWorkerID || pageParams.ExpectedRevision == nil ||
		*pageParams.ExpectedRevision != 7 {
		t.Fatalf("second page params = %#v", pageParams)
	}
	for _, index := range []int{1, 3, 5} {
		if calls[index].treeID != rootMCPTreeID || calls[index].source == nil ||
			*calls[index].source != rootResult(rootMCPThreadID).Principal.Identity() {
			t.Fatalf("authorized call %d = %#v", index, calls[index])
		}
	}
}

func TestRootMCPSpawnsAndListsDurableAgents(t *testing.T) {
	backend := &fakeRootBackend{}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	spawned := callTool(t, ctx, clientSession, ToolSpawnAgent, rootMCPThreadID, map[string]any{
		"spawn_id":         rootMCPThreadID,
		"target_device_id": rootMCPWorkerID,
		"task_name":        "windows_build",
		"message":          "Run the Windows build and report the exact result.",
	})
	if spawned.IsError {
		t.Fatalf("spawn_agent result = %#v", spawned)
	}
	var agent AgentOutput
	decodeStructured(t, spawned.StructuredContent, &agent)
	if agent.SpawnID != rootMCPThreadID || agent.AgentID != rootMCPWorkerID ||
		agent.ParentAgentID != rootMCPAgentID || agent.TargetDeviceID != rootMCPWorkerID ||
		agent.TaskName != "windows_build" || agent.Status != protocol.AgentSpawnStarted {
		t.Fatalf("spawn_agent output = %#v", agent)
	}
	listed := callTool(t, ctx, clientSession, ToolListAgents, rootMCPThreadID, map[string]any{"limit": 1})
	if listed.IsError {
		t.Fatalf("list_agents result = %#v", listed)
	}
	var page ListAgentsOutput
	decodeStructured(t, listed.StructuredContent, &page)
	if len(page.Agents) != 1 || page.Agents[0].TaskName != "windows_build" {
		t.Fatalf("list_agents output = %#v", page)
	}
	calls := backend.snapshot()
	if len(calls) != 4 || calls[1].method != protocol.MethodSpawnAgent ||
		calls[3].method != protocol.MethodListAgents {
		t.Fatalf("agent backend calls = %#v", calls)
	}
	spawnParams := calls[1].params.(protocol.SpawnAgentParams)
	if spawnParams.Message != "Run the Windows build and report the exact result." ||
		calls[1].source == nil || *calls[1].source != rootResult(rootMCPThreadID).Principal.Identity() {
		t.Fatalf("spawn backend call = %#v", calls[1])
	}
}

func TestRootMCPFailsClosedWithoutValidThreadMetadata(t *testing.T) {
	backend := &fakeRootBackend{}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	for name, meta := range map[string]mcp.Meta{
		"missing":      nil,
		"wrong type":   {"threadId": 42},
		"invalid UUID": {"threadId": "not-a-uuid"},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
				Meta: meta, Name: ToolListDevices, Arguments: map[string]any{},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError || !strings.Contains(toolText(result), "threadId") {
				t.Fatalf("invalid metadata result = %#v", result)
			}
		})
	}
	if len(backend.snapshot()) != 0 {
		t.Fatal("invalid thread metadata reached the bridge")
	}
}

func TestRootMCPFailsClosedOnMismatchedRootBinding(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.EnsureRootTreeResult)
	}{
		{name: "external thread", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Tree.ExternalThreadID = rootMCPWorkerID
		}},
		{name: "controller", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.ControllerID = rootMCPWorkerID
		}},
		{name: "configured controller", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Tree.ControllerID = rootMCPWorkerID
			result.Principal.ControllerID = rootMCPWorkerID
		}},
		{name: "tree", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.TreeID = rootMCPWorkerID
		}},
		{name: "root agent", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.AgentID = rootMCPWorkerID
		}},
		{name: "root device", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.DeviceID = rootMCPWorkerID
		}},
		{name: "configured root device", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Tree.RootDeviceID = rootMCPWorkerID
			result.Principal.DeviceID = rootMCPWorkerID
		}},
		{name: "parent", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.ParentAgentID = rootMCPWorkerID
			result.Principal.Capabilities = control.WorkerCapabilities()
		}},
		{name: "capabilities", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.Capabilities = []control.Capability{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := rootResult(rootMCPThreadID)
			test.mutate(&binding)
			backend := &fakeRootBackend{ensureResult: &binding}
			ctx, clientSession, closeSessions := connectRootMCP(t, backend)
			defer closeSessions()
			result := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
			if !result.IsError {
				t.Fatalf("mismatched root binding result = %#v", result)
			}
			calls := backend.snapshot()
			if len(calls) != 1 || calls[0].method != protocol.MethodEnsureRootTree {
				t.Fatalf("mismatched root binding reached registry: %#v", calls)
			}
		})
	}
}

func TestRootMCPReturnsRecoverableBridgeErrors(t *testing.T) {
	backend := &fakeRootBackend{err: &localbridge.RPCError{
		Code: protocol.ErrorUnavailable, Message: "broker unavailable",
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
	if !result.IsError || !strings.Contains(toolText(result), "connector is offline") {
		t.Fatalf("offline bridge result = %#v", result)
	}
	backend.mu.Lock()
	backend.err = nil
	backend.mu.Unlock()
	invalidCursor := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"cursor": "invalid!",
	})
	if !invalidCursor.IsError || !strings.Contains(toolText(invalidCursor), "cursor") {
		t.Fatalf("invalid cursor result = %#v", invalidCursor)
	}
}

func TestRootMCPExplainsRootBindingConflict(t *testing.T) {
	backend := &fakeRootBackend{err: &localbridge.RPCError{
		Code: protocol.ErrorConflict, Message: "root device mismatch",
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
	text := toolText(result)
	if !result.IsError || !strings.Contains(text, "bound to another delegation root device") ||
		strings.Contains(text, "without a cursor") {
		t.Fatalf("root conflict result = %#v", result)
	}
}

func TestRootMCPCancellationReachesBackend(t *testing.T) {
	backend := &cancelRootBackend{started: make(chan struct{}), canceled: make(chan error, 1)}
	_, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	callDone := make(chan error, 1)
	go func() {
		_, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
			Meta:      mcp.Meta{"threadId": rootMCPThreadID},
			Name:      ToolListDevices,
			Arguments: map[string]any{},
		})
		callDone <- err
	}()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("root MCP backend call did not start")
	}
	select {
	case err := <-callDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled MCP call error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MCP call did not honor its deadline")
	}
	cancel()
	select {
	case err := <-backend.canceled:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("backend cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MCP cancellation did not reach the backend")
	}
}

func TestRootMCPRejectsInvalidRegistryResults(t *testing.T) {
	device := testDevice(rootMCPWorkerID, 2)
	device.Name = strings.Repeat("n", 129)
	backend := &fakeRootBackend{listResult: &protocol.ListDevicesResult{
		Revision: 7,
		Devices:  []control.Device{device},
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
	if !result.IsError || !strings.Contains(toolText(result), "invalid device") {
		t.Fatalf("invalid registry result = %#v", result)
	}

	backend.mu.Lock()
	valid := testDevice(rootMCPWorkerID, 2)
	backend.listResult = nil
	backend.describeResult = &protocol.DescribeDeviceResult{Revision: 8, Device: valid}
	backend.describeResult.Device.ControllerID = "123e4567-e89b-42d3-a456-426614174499"
	backend.mu.Unlock()
	result = callTool(t, ctx, clientSession, ToolDescribeDevice, rootMCPThreadID, map[string]any{
		"deviceId": rootMCPWorkerID,
	})
	if !result.IsError || !strings.Contains(toolText(result), "mismatched device") {
		t.Fatalf("mismatched registry result = %#v", result)
	}
}

func TestValidateListResultRejectsRevisionSkew(t *testing.T) {
	expected := uint64(7)
	device := testDevice(rootMCPWorkerID, 2)
	for _, test := range []struct {
		name   string
		result protocol.ListDevicesResult
		params protocol.ListDevicesParams
	}{
		{
			name:   "cursor revision mismatch",
			result: protocol.ListDevicesResult{Revision: 8},
			params: protocol.ListDevicesParams{Limit: 1, ExpectedRevision: &expected},
		},
		{
			name:   "device newer than registry",
			result: protocol.ListDevicesResult{Revision: 7, Devices: []control.Device{device}},
			params: protocol.ListDevicesParams{Limit: 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "device newer than registry" {
				test.result.Devices[0].Revision = 8
			}
			if err := validateListResult(test.result, test.params, rootMCPControllerID); err == nil {
				t.Fatal("validateListResult accepted revision skew")
			}
		})
	}
}

func TestRootMCPOutputIsBounded(t *testing.T) {
	if maximumDevicePage > 4 || listFeatureLimit != 0 || maximumListBytes > 4*1024 ||
		maximumDetailBytes > 8*1024 || maximumAgentListBytes > 16*1024 || len(serverInstructions) > 1024 {
		t.Fatalf(
			"root MCP bounds = page %d, features %d, list %d bytes, detail %d bytes, instructions %d bytes",
			maximumDevicePage, listFeatureLimit, maximumListBytes, maximumDetailBytes,
			len(serverInstructions),
		)
	}
	features := make([]string, 64)
	for index := range features {
		features[index] = fmt.Sprintf("F%02d%s", index, strings.Repeat("x", 61))
	}
	devices := make([]control.Device, maximumDevicePage)
	for index := range devices {
		devices[index] = control.Device{
			ControllerID:    rootMCPControllerID,
			DeviceID:        fmt.Sprintf("123e4567-e89b-42d3-a456-%012x", index+1),
			Name:            strings.Repeat(`"`, 128),
			OS:              strings.Repeat(`\`, 32),
			Arch:            strings.Repeat(`"`, 32),
			RuntimeVersion:  strings.Repeat(`\`, 64),
			ProtocolVersion: protocol.Version,
			Features:        features,
			Online:          true,
			LastSeenAt:      int64(^uint64(0) >> 1),
			Revision:        1,
		}
	}
	backend := &fakeRootBackend{listResult: &protocol.ListDevicesResult{
		Revision: ^uint64(0),
		Devices:  devices,
	}}
	ctx, clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, ctx, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"limit": maximumDevicePage,
	})
	if result.IsError {
		t.Fatalf("maximum list_devices result = %#v", result)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maximumListBytes {
		t.Fatalf("maximum list_devices output = %d bytes, want at most %d", len(data), maximumListBytes)
	}
	if strings.Contains(string(data), `"features":`) {
		t.Fatalf("list_devices output includes full feature data: %s", data)
	}

	summary := summarizeDevice(control.Device{
		DeviceID:       rootMCPWorkerID,
		Name:           strings.Repeat("n", 128),
		OS:             strings.Repeat("o", 32),
		Arch:           strings.Repeat("a", 32),
		RuntimeVersion: strings.Repeat("v", 64),
		Features:       features,
		Online:         true,
		LastSeenAt:     int64(^uint64(0) >> 1),
	}, listFeatureLimit, rootMCPDeviceID)
	if len(summary.Features) != 0 || !summary.FeaturesTruncated {
		t.Fatalf("bounded summary = %#v", summary)
	}
}

func assertToolSchema(t *testing.T, tool *mcp.Tool) {
	t.Helper()
	data, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Minimum   *float64 `json:"minimum"`
			Maximum   *float64 `json:"maximum"`
			MinLength *int     `json:"minLength"`
			MaxLength *int     `json:"maxLength"`
			Pattern   string   `json:"pattern"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	switch tool.Name {
	case ToolListDevices:
		limit := schema.Properties["limit"]
		cursor := schema.Properties["cursor"]
		if limit.Minimum == nil || *limit.Minimum != 1 || limit.Maximum == nil ||
			*limit.Maximum != maximumDevicePage || cursor.MaxLength == nil ||
			*cursor.MaxLength != base64.RawURLEncoding.EncodedLen(maximumCursorBytes) || cursor.Pattern == "" {
			t.Fatalf("list_devices input schema = %s", data)
		}
	case ToolDescribeDevice:
		deviceID := schema.Properties["deviceId"]
		if deviceID.MinLength == nil || *deviceID.MinLength != 36 || deviceID.MaxLength == nil ||
			*deviceID.MaxLength != 36 || deviceID.Pattern != uuidPattern {
			t.Fatalf("describe_device input schema = %s", data)
		}
		if matched, err := regexp.MatchString(deviceID.Pattern, strings.ToUpper(rootMCPDeviceID)); err != nil || matched {
			t.Fatalf("describe_device schema accepted uppercase UUID: %s", data)
		}
	case ToolListAgents:
		limit := schema.Properties["limit"]
		cursor := schema.Properties["cursor"]
		if limit.Minimum == nil || *limit.Minimum != 1 || limit.Maximum == nil ||
			*limit.Maximum != protocol.MaximumAgentPage || cursor.MaxLength == nil ||
			*cursor.MaxLength != base64.RawURLEncoding.EncodedLen(maximumCursorBytes) || cursor.Pattern == "" {
			t.Fatalf("list_agents input schema = %s", data)
		}
	case ToolSpawnAgent:
		spawnID := schema.Properties["spawn_id"]
		targetID := schema.Properties["target_device_id"]
		taskName := schema.Properties["task_name"]
		message := schema.Properties["message"]
		if spawnID.MinLength == nil || *spawnID.MinLength != 36 || spawnID.MaxLength == nil ||
			*spawnID.MaxLength != 36 || spawnID.Pattern != uuidPattern ||
			targetID.Pattern != uuidPattern || taskName.MaxLength == nil ||
			*taskName.MaxLength != protocol.MaximumAgentTaskNameBytes || taskName.Pattern == "" ||
			message.MaxLength == nil || *message.MaxLength != protocol.MaximumAgentPromptBytes {
			t.Fatalf("spawn_agent input schema = %s", data)
		}
	case ToolSendMessage, ToolFollowupTask, ToolInterruptAgent:
		target := schema.Properties["target"]
		idName := "operation_id"
		if tool.Name == ToolSendMessage {
			idName = "message_id"
		}
		id := schema.Properties[idName]
		if target.MinLength == nil || *target.MinLength != 1 || target.MaxLength == nil ||
			*target.MaxLength != maximumAgentTargetBytes || target.Pattern != agentTargetPattern ||
			id.MinLength == nil || *id.MinLength != 36 || id.MaxLength == nil ||
			*id.MaxLength != 36 || id.Pattern != uuidPattern {
			t.Fatalf("%s input schema = %s", tool.Name, data)
		}
		if tool.Name != ToolInterruptAgent {
			message := schema.Properties["message"]
			wantMaximum := protocol.MaximumAgentPromptBytes
			if tool.Name == ToolSendMessage {
				wantMaximum = protocol.MaximumMailboxMessageBytes
			}
			if message.MinLength == nil || *message.MinLength != 1 ||
				message.MaxLength == nil || *message.MaxLength != wantMaximum {
				t.Fatalf("%s input schema = %s", tool.Name, data)
			}
		}
	case ToolWaitAgent:
		timeout := schema.Properties["timeout_seconds"]
		if timeout.Minimum == nil || *timeout.Minimum != 1 || timeout.Maximum == nil ||
			*timeout.Maximum != maximumAgentWaitSeconds {
			t.Fatalf("wait_agent input schema = %s", data)
		}
	default:
		t.Fatalf("unexpected tool %q", tool.Name)
	}
}

func connectRootMCP(t *testing.T, backend Backend) (context.Context, *mcp.ClientSession, func()) {
	t.Helper()
	server, err := NewServer(backend, rootMCPControllerID, rootMCPDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		serverSession.Close()
		cancel()
		t.Fatal(err)
	}
	return ctx, clientSession, func() {
		defer cancel()
		if err := clientSession.Close(); err != nil {
			t.Errorf("close MCP client: %v", err)
		}
		if err := serverSession.Close(); err != nil {
			t.Errorf("close MCP server: %v", err)
		}
	}
}

func callTool(
	t *testing.T,
	ctx context.Context,
	client *mcp.ClientSession,
	name, threadID string,
	arguments any,
) *mcp.CallToolResult {
	t.Helper()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{
		Meta: mcp.Meta{"threadId": threadID}, Name: name, Arguments: arguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func decodeStructured(t *testing.T, value any, target any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}

func toolText(result *mcp.CallToolResult) string {
	var text []string
	for _, content := range result.Content {
		if current, ok := content.(*mcp.TextContent); ok {
			text = append(text, current.Text)
		}
	}
	return strings.Join(text, "\n")
}

func rootResult(threadID string) protocol.EnsureRootTreeResult {
	tree := control.Tree{
		ControllerID:     rootMCPControllerID,
		TreeID:           rootMCPTreeID,
		ExternalThreadID: threadID,
		RootAgentID:      rootMCPAgentID,
		RootDeviceID:     rootMCPDeviceID,
		CreatedAt:        1,
	}
	return protocol.EnsureRootTreeResult{
		Tree: tree,
		Principal: control.NewRootPrincipal(
			rootMCPControllerID, rootMCPTreeID, rootMCPAgentID, rootMCPDeviceID,
		),
	}
}

func testDevice(deviceID string, featureCount int) control.Device {
	features := make([]string, featureCount)
	for index := range features {
		features[index] = fmt.Sprintf("feature%02d", index)
	}
	return control.Device{
		ControllerID:    rootMCPControllerID,
		DeviceID:        deviceID,
		Name:            "builder",
		OS:              "windows",
		Arch:            "amd64",
		RuntimeVersion:  "0.1.0-alpha.0.m1.1",
		ProtocolVersion: protocol.Version,
		Features:        features,
		Online:          true,
		LastSeenAt:      time.Unix(1, 0).Unix(),
		Revision:        1,
	}
}

func testAgent(spawnID, deviceID, taskName string, sequence uint64) protocol.AgentSummary {
	return protocol.AgentSummary{
		SpawnID: spawnID,
		Principal: control.NewWorkerPrincipal(
			rootMCPControllerID, rootMCPTreeID, rootMCPWorkerID, rootMCPAgentID, deviceID,
		).Identity(),
		TaskName: taskName,
		Status:   protocol.AgentSpawnStarted,
		Sequence: sequence,
	}
}
