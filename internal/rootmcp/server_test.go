package rootmcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
)

type rootMCPCall struct {
	method string
	treeID string
	source *control.PrincipalIdentity
	params any
}

type fakeRootBackend struct {
	mu             sync.Mutex
	calls          []rootMCPCall
	err            error
	ensureResult   *protocol.EnsureRootTreeResult
	listResult     *protocol.ListDevicesResult
	describeResult *protocol.DescribeDeviceResult
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
				testDevice(rootMCPDeviceID, control.DeviceRoleController, 24),
				testDevice(rootMCPWorkerID, control.DeviceRoleWorker, 2),
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
			Device:   testDevice(params.(protocol.DescribeDeviceParams).DeviceID, control.DeviceRoleWorker, 24),
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

func TestRootMCPListsStaticReadOnlyToolsAndBindsThread(t *testing.T) {
	backend := &fakeRootBackend{}
	clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	tools, err := clientSession.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 2 ||
		tools.Tools[0].Name != ToolDescribeDevice || tools.Tools[1].Name != ToolListDevices {
		t.Fatalf("root tools = %#v", tools.Tools)
	}
	for _, tool := range tools.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint ||
			tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint ||
			tool.Annotations.OpenWorldHint == nil || *tool.Annotations.OpenWorldHint {
			t.Fatalf("tool annotations for %s = %#v", tool.Name, tool.Annotations)
		}
		assertToolSchema(t, tool)
	}

	first := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{"limit": 2})
	if first.IsError {
		t.Fatalf("list_devices result = %#v", first)
	}
	var firstPage ListDevicesOutput
	decodeStructured(t, first.StructuredContent, &firstPage)
	if firstPage.Revision != 7 || len(firstPage.Devices) != 2 || firstPage.NextCursor == "" ||
		len(firstPage.Devices[0].Features) != listFeatureLimit ||
		!firstPage.Devices[0].FeaturesTruncated || firstPage.Devices[1].FeaturesTruncated {
		t.Fatalf("first MCP device page = %#v", firstPage)
	}
	second := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"cursor": firstPage.NextCursor, "limit": 2,
	})
	if second.IsError {
		t.Fatalf("second list_devices result = %#v", second)
	}
	described := callTool(t, clientSession, ToolDescribeDevice, rootMCPThreadID, map[string]any{
		"deviceId": rootMCPWorkerID,
	})
	if described.IsError {
		t.Fatalf("describe_device result = %#v", described)
	}
	var description DescribeDeviceOutput
	decodeStructured(t, described.StructuredContent, &description)
	if description.Revision != 8 || description.Device.DeviceID != rootMCPWorkerID ||
		len(description.Device.Features) != 24 || description.Device.FeaturesTruncated {
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

func TestRootMCPFailsClosedWithoutValidThreadMetadata(t *testing.T) {
	backend := &fakeRootBackend{}
	clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	for name, meta := range map[string]mcp.Meta{
		"missing":      nil,
		"wrong type":   {"threadId": 42},
		"invalid UUID": {"threadId": "not-a-uuid"},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := clientSession.CallTool(context.Background(), &mcp.CallToolParams{
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
		{name: "tree", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.TreeID = rootMCPWorkerID
		}},
		{name: "root agent", mutate: func(result *protocol.EnsureRootTreeResult) {
			result.Principal.AgentID = rootMCPWorkerID
		}},
		{name: "root device", mutate: func(result *protocol.EnsureRootTreeResult) {
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
			clientSession, closeSessions := connectRootMCP(t, backend)
			defer closeSessions()
			result := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
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
	clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
	if !result.IsError || !strings.Contains(toolText(result), "connector is offline") {
		t.Fatalf("offline bridge result = %#v", result)
	}
	backend.mu.Lock()
	backend.err = nil
	backend.mu.Unlock()
	invalidCursor := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"cursor": "invalid!",
	})
	if !invalidCursor.IsError || !strings.Contains(toolText(invalidCursor), "cursor") {
		t.Fatalf("invalid cursor result = %#v", invalidCursor)
	}
}

func TestRootMCPRejectsInvalidRegistryResults(t *testing.T) {
	device := testDevice(rootMCPWorkerID, control.DeviceRoleWorker, 2)
	device.Name = strings.Repeat("n", 129)
	backend := &fakeRootBackend{listResult: &protocol.ListDevicesResult{
		Revision: 7,
		Devices:  []control.Device{device},
	}}
	clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{})
	if !result.IsError || !strings.Contains(toolText(result), "invalid device") {
		t.Fatalf("invalid registry result = %#v", result)
	}

	backend.mu.Lock()
	valid := testDevice(rootMCPWorkerID, control.DeviceRoleWorker, 2)
	backend.listResult = nil
	backend.describeResult = &protocol.DescribeDeviceResult{Revision: 8, Device: valid}
	backend.describeResult.Device.ControllerID = "123e4567-e89b-42d3-a456-426614174499"
	backend.mu.Unlock()
	result = callTool(t, clientSession, ToolDescribeDevice, rootMCPThreadID, map[string]any{
		"deviceId": rootMCPWorkerID,
	})
	if !result.IsError || !strings.Contains(toolText(result), "mismatched device") {
		t.Fatalf("mismatched registry result = %#v", result)
	}
}

func TestRootMCPOutputIsBounded(t *testing.T) {
	if maximumDevicePage > 16 || listFeatureLimit > 16 || len(serverInstructions) > 512 {
		t.Fatalf(
			"root MCP bounds = page %d, features %d, instructions %d bytes",
			maximumDevicePage, listFeatureLimit, len(serverInstructions),
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
			Role:            control.DeviceRoleWorker,
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
	clientSession, closeSessions := connectRootMCP(t, backend)
	defer closeSessions()
	result := callTool(t, clientSession, ToolListDevices, rootMCPThreadID, map[string]any{
		"limit": maximumDevicePage,
	})
	if result.IsError {
		t.Fatalf("maximum list_devices result = %#v", result)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maximumOutputBytes {
		t.Fatalf("maximum list_devices output = %d bytes, want at most %d", len(data), maximumOutputBytes)
	}

	summary := summarizeDevice(control.Device{
		DeviceID:       rootMCPWorkerID,
		Name:           strings.Repeat("n", 128),
		Role:           control.DeviceRoleWorker,
		OS:             strings.Repeat("o", 32),
		Arch:           strings.Repeat("a", 32),
		RuntimeVersion: strings.Repeat("v", 64),
		Features:       features,
		Online:         true,
		LastSeenAt:     int64(^uint64(0) >> 1),
	}, listFeatureLimit)
	if len(summary.Features) != listFeatureLimit || !summary.FeaturesTruncated {
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
	default:
		t.Fatalf("unexpected tool %q", tool.Name)
	}
}

func connectRootMCP(t *testing.T, backend Backend) (*mcp.ClientSession, func()) {
	t.Helper()
	server, err := NewServer(backend)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		serverSession.Close()
		t.Fatal(err)
	}
	return clientSession, func() {
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
	client *mcp.ClientSession,
	name, threadID string,
	arguments any,
) *mcp.CallToolResult {
	t.Helper()
	result, err := client.CallTool(context.Background(), &mcp.CallToolParams{
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

func testDevice(deviceID string, role control.DeviceRole, featureCount int) control.Device {
	features := make([]string, featureCount)
	for index := range features {
		features[index] = fmt.Sprintf("feature%02d", index)
	}
	return control.Device{
		ControllerID:    rootMCPControllerID,
		DeviceID:        deviceID,
		Name:            "builder",
		Role:            role,
		OS:              "windows",
		Arch:            "amd64",
		RuntimeVersion:  "0.1.0-alpha.0",
		ProtocolVersion: protocol.Version,
		Features:        features,
		Online:          true,
		LastSeenAt:      time.Unix(1, 0).Unix(),
		Revision:        1,
	}
}
