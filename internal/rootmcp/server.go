package rootmcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ToolListDevices    = "list_devices"
	ToolDescribeDevice = "describe_device"
	ToolSpawnAgent     = "spawn_agent"
	ToolListAgents     = "list_agents"
	ToolSendMessage    = "send_message"
	ToolFollowupTask   = "followup_task"
	ToolInterruptAgent = "interrupt_agent"
	ToolWaitAgent      = "wait_agent"
	ToolSyncWorkspace  = "sync_workspace"
	maximumDevicePage  = 4
	listFeatureLimit   = 0
	maximumListBytes   = 4 * 1024
	maximumDetailBytes = 8 * 1024
	bridgeCallTimeout  = 15 * time.Second
	agentCallTimeout   = 135 * time.Second
	uuidPattern        = `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	serverInstructions = "Delegation exposes live peers, synchronized Git workspaces, and managed agents. Use list_devices for bounded summaries and describe_device before choosing an online peer; isCurrentDevice allows self-targeting. For repository work, call sync_workspace with a fresh sync_id, an explicit Git URL, and the selected target; then pass the returned workspace_id to spawn_agent. Call spawn_agent with a fresh spawn_id and self-contained task. busy means no target slot; indeterminate means no definitive target result was confirmed or recorded and the worker may have started. Retry either with the same ID and exact arguments. Use list_agents for durable receipts and wait_agent for lifecycle activity or worker messages; call wait_agent again while has_more is true. Targets accept an agent UUID, task_name, or /root/task_name. Use a fresh message_id or operation_id for each logical action; retry uncertain results with the same ID and arguments. send_message delivers to running or idle agents, followup_task starts idle agents, and interrupt_agent stops active turns."
)

type Backend interface {
	Call(context.Context, string, string, *control.PrincipalIdentity, any, any) error
}

type Root struct {
	backend      Backend
	controllerID string
	deviceID     string
	waitMu       sync.Mutex
	waitStates   map[string]*agentWaitState
	waitUse      uint64
}

type ListDevicesInput struct {
	Cursor string `json:"cursor,omitempty" jsonschema:"opaque cursor returned by a previous list_devices call"`
	Limit  int    `json:"limit,omitempty" jsonschema:"maximum devices to return, from 1 through 4; defaults to 4"`
}

type ListDevicesOutput struct {
	Revision   uint64          `json:"revision"`
	Devices    []DeviceSummary `json:"devices"`
	NextCursor string          `json:"nextCursor,omitempty"`
}

type DescribeDeviceInput struct {
	DeviceID string `json:"deviceId" jsonschema:"stable device UUID returned by list_devices"`
}

type DescribeDeviceOutput struct {
	Revision uint64        `json:"revision"`
	Device   DeviceSummary `json:"device"`
}

type DeviceSummary struct {
	DeviceID          string   `json:"deviceId"`
	Name              string   `json:"name"`
	IsCurrentDevice   bool     `json:"isCurrentDevice"`
	OS                string   `json:"os"`
	Arch              string   `json:"arch"`
	RuntimeVersion    string   `json:"runtimeVersion"`
	ProtocolVersion   int      `json:"protocolVersion"`
	Features          []string `json:"features,omitempty"`
	FeaturesTruncated bool     `json:"featuresTruncated,omitempty"`
	Online            bool     `json:"online"`
	LastSeenAt        int64    `json:"lastSeenAt"`
}

func NewServer(backend Backend, controllerID, deviceID string) (*mcp.Server, error) {
	if backend == nil {
		return nil, errors.New("root MCP backend is required")
	}
	if err := identity.ValidateID(controllerID); err != nil {
		return nil, fmt.Errorf("root MCP controllerId %w", err)
	}
	if err := identity.ValidateID(deviceID); err != nil {
		return nil, fmt.Errorf("root MCP deviceId %w", err)
	}
	listInputSchema, describeInputSchema, err := inputSchemas()
	if err != nil {
		return nil, err
	}
	spawnInputSchema, listAgentsInputSchema, err := agentInputSchemas()
	if err != nil {
		return nil, err
	}
	sendInputSchema, followupInputSchema, interruptInputSchema, err := agentControlInputSchemas()
	if err != nil {
		return nil, err
	}
	waitInputSchema, err := agentWaitInputSchema()
	if err != nil {
		return nil, err
	}
	workspaceSchema, err := workspaceInputSchema()
	if err != nil {
		return nil, err
	}
	root := &Root{
		backend: backend, controllerID: controllerID, deviceID: deviceID,
		waitStates: make(map[string]*agentWaitState),
	}
	server := mcp.NewServer(
		&mcp.Implementation{
			Name: "delegation", Title: "Delegation", Version: buildinfo.Version,
		},
		&mcp.ServerOptions{
			Instructions: serverInstructions,
			Capabilities: &mcp.ServerCapabilities{Experimental: map[string]any{
				sandboxStateMetaCapability: map[string]any{},
			}},
		},
	)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolSyncWorkspace,
		Title:       "Synchronize Git workspace",
		Description: "Prepare the current trusted Git worktree on an explicitly selected peer using an explicit remote URL.",
		Annotations: mutatingAnnotations(),
		InputSchema: workspaceSchema,
	}, root.syncWorkspace)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolListDevices,
		Title:       "List delegation devices",
		Description: "List bounded summaries of live delegation peers with revision-bound pagination. Use describe_device for features.",
		Annotations: readOnlyAnnotations(),
		InputSchema: listInputSchema,
	}, root.listDevices)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolDescribeDevice,
		Title:       "Describe delegation device",
		Description: "Return current details for one delegation device UUID.",
		Annotations: readOnlyAnnotations(),
		InputSchema: describeInputSchema,
	}, root.describeDevice)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolSpawnAgent,
		Title:       "Spawn delegation agent",
		Description: "Start one managed Codex agent on an explicitly selected peer.",
		Annotations: mutatingAnnotations(),
		InputSchema: spawnInputSchema,
	}, root.spawnAgent)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolListAgents,
		Title:       "List delegation agents",
		Description: "List durable managed-agent dispatch receipts for this root task.",
		Annotations: readOnlyAnnotations(),
		InputSchema: listAgentsInputSchema,
	}, root.listAgents)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolSendMessage,
		Title:       "Send message to delegation agent",
		Description: "Send or steer one message to a managed agent in this root task.",
		Annotations: mutatingAnnotations(),
		InputSchema: sendInputSchema,
	}, root.sendMessage)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolFollowupTask,
		Title:       "Follow up delegation task",
		Description: "Start a new turn for an idle managed agent in this root task.",
		Annotations: mutatingAnnotations(),
		InputSchema: followupInputSchema,
	}, root.followupTask)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolInterruptAgent,
		Title:       "Interrupt delegation agent",
		Description: "Interrupt the active turn of a managed agent in this root task.",
		Annotations: mutatingAnnotations(),
		InputSchema: interruptInputSchema,
	}, root.interruptAgent)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolWaitAgent,
		Title:       "Wait for delegation agents",
		Description: "Wait for new managed-agent lifecycle activity or worker messages in this root task.",
		Annotations: consumingAnnotations(),
		InputSchema: waitInputSchema,
	}, root.waitAgent)
	return server, nil
}

func (r *Root) listDevices(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input ListDevicesInput,
) (*mcp.CallToolResult, ListDevicesOutput, error) {
	threadID, err := threadID(request)
	if err != nil {
		return nil, ListDevicesOutput{}, err
	}
	limit := input.Limit
	if limit == 0 {
		limit = maximumDevicePage
	}
	if limit < 1 || limit > maximumDevicePage {
		return nil, ListDevicesOutput{}, fmt.Errorf("limit must be from 1 through %d", maximumDevicePage)
	}
	afterDeviceID, expectedRevision, err := decodeCursor(input.Cursor)
	if err != nil {
		return nil, ListDevicesOutput{}, err
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return nil, ListDevicesOutput{}, err
	}
	params := protocol.ListDevicesParams{
		AfterDeviceID: afterDeviceID, Limit: limit, ExpectedRevision: expectedRevision,
	}
	source := principal.Identity()
	var result protocol.ListDevicesResult
	if err := r.call(ctx, protocol.MethodListDevices, tree.TreeID, &source, params, &result); err != nil {
		return nil, ListDevicesOutput{}, explainBridgeError(err)
	}
	if err := validateListResult(result, params, principal.ControllerID); err != nil {
		return nil, ListDevicesOutput{}, err
	}
	output := ListDevicesOutput{
		Revision: result.Revision,
		Devices:  make([]DeviceSummary, 0, len(result.Devices)),
	}
	for _, device := range result.Devices {
		output.Devices = append(output.Devices, summarizeDevice(device, listFeatureLimit, r.deviceID))
	}
	if result.NextCursor != "" {
		output.NextCursor, err = encodeCursor(result.Revision, result.NextCursor)
		if err != nil {
			return nil, ListDevicesOutput{}, err
		}
	}
	if err := enforceOutputLimit(output, maximumListBytes); err != nil {
		return nil, ListDevicesOutput{}, err
	}
	return nil, output, nil
}

func (r *Root) describeDevice(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input DescribeDeviceInput,
) (*mcp.CallToolResult, DescribeDeviceOutput, error) {
	threadID, err := threadID(request)
	if err != nil {
		return nil, DescribeDeviceOutput{}, err
	}
	if err := identity.ValidateID(input.DeviceID); err != nil {
		return nil, DescribeDeviceOutput{}, fmt.Errorf("deviceId %w", err)
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return nil, DescribeDeviceOutput{}, err
	}
	source := principal.Identity()
	var result protocol.DescribeDeviceResult
	if err := r.call(
		ctx,
		protocol.MethodDescribeDevice,
		tree.TreeID,
		&source,
		protocol.DescribeDeviceParams{DeviceID: input.DeviceID},
		&result,
	); err != nil {
		return nil, DescribeDeviceOutput{}, explainBridgeError(err)
	}
	if err := validateDescribeResult(result, input.DeviceID, principal.ControllerID); err != nil {
		return nil, DescribeDeviceOutput{}, err
	}
	output := DescribeDeviceOutput{
		Revision: result.Revision,
		Device:   summarizeDevice(result.Device, len(result.Device.Features), r.deviceID),
	}
	if err := enforceOutputLimit(output, maximumDetailBytes); err != nil {
		return nil, DescribeDeviceOutput{}, err
	}
	return nil, output, nil
}

func inputSchemas() (*jsonschema.Schema, *jsonschema.Schema, error) {
	list, err := jsonschema.For[ListDevicesInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build list_devices input schema: %w", err)
	}
	limit, found := list.Properties["limit"]
	if !found {
		return nil, nil, errors.New("list_devices input schema is missing limit")
	}
	limit.Minimum = jsonschema.Ptr(1.0)
	limit.Maximum = jsonschema.Ptr(float64(maximumDevicePage))
	cursor, found := list.Properties["cursor"]
	if !found {
		return nil, nil, errors.New("list_devices input schema is missing cursor")
	}
	cursor.MaxLength = jsonschema.Ptr(base64.RawURLEncoding.EncodedLen(maximumCursorBytes))
	cursor.Pattern = `^(?:[A-Za-z0-9_-]+)?$`

	describe, err := jsonschema.For[DescribeDeviceInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build describe_device input schema: %w", err)
	}
	deviceID, found := describe.Properties["deviceId"]
	if !found {
		return nil, nil, errors.New("describe_device input schema is missing deviceId")
	}
	deviceID.MinLength = jsonschema.Ptr(36)
	deviceID.MaxLength = jsonschema.Ptr(36)
	deviceID.Pattern = uuidPattern
	return list, describe, nil
}

func enforceOutputLimit(output any, maximumBytes int) error {
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("encode delegation tool output: %w", err)
	}
	if len(data) > maximumBytes {
		return fmt.Errorf("delegation tool output exceeds %d bytes", maximumBytes)
	}
	return nil
}

func validateListResult(
	result protocol.ListDevicesResult,
	params protocol.ListDevicesParams,
	controllerID string,
) error {
	if len(result.Devices) > params.Limit {
		return errors.New("delegation service returned too many devices")
	}
	if len(result.Devices) > 0 && result.Revision == 0 {
		return errors.New("delegation service returned devices without a registry revision")
	}
	if params.ExpectedRevision != nil && result.Revision != *params.ExpectedRevision {
		return errors.New("delegation service returned a device page from another registry revision")
	}
	previous := params.AfterDeviceID
	for _, device := range result.Devices {
		if err := device.Validate(); err != nil {
			return fmt.Errorf("delegation service returned an invalid device: %w", err)
		}
		if device.ControllerID != controllerID {
			return errors.New("delegation service returned a device from another controller")
		}
		if device.Revision > result.Revision {
			return errors.New("delegation service returned a device newer than its registry revision")
		}
		if previous != "" && device.DeviceID <= previous {
			return errors.New("delegation service returned devices out of order")
		}
		previous = device.DeviceID
	}
	if result.NextCursor != "" {
		if len(result.Devices) != params.Limit ||
			result.NextCursor != result.Devices[len(result.Devices)-1].DeviceID {
			return errors.New("delegation service returned an invalid device cursor")
		}
	}
	return nil
}

func validateDescribeResult(
	result protocol.DescribeDeviceResult,
	deviceID, controllerID string,
) error {
	if err := result.Device.Validate(); err != nil {
		return fmt.Errorf("delegation service returned an invalid device: %w", err)
	}
	if result.Revision == 0 || result.Revision < result.Device.Revision {
		return errors.New("delegation service returned an invalid registry revision")
	}
	if result.Device.ControllerID != controllerID || result.Device.DeviceID != deviceID {
		return errors.New("delegation service returned a mismatched device")
	}
	return nil
}

func (r *Root) ensureRoot(
	ctx context.Context,
	threadID string,
) (control.Tree, control.Principal, error) {
	var result protocol.EnsureRootTreeResult
	if err := r.call(
		ctx,
		protocol.MethodEnsureRootTree,
		"",
		nil,
		protocol.EnsureRootTreeParams{ExternalThreadID: threadID},
		&result,
	); err != nil {
		return control.Tree{}, control.Principal{}, explainEnsureRootError(err)
	}
	if err := result.Tree.Validate(); err != nil {
		return control.Tree{}, control.Principal{}, fmt.Errorf("delegation service returned an invalid tree: %w", err)
	}
	if err := result.Principal.Validate(); err != nil {
		return control.Tree{}, control.Principal{}, fmt.Errorf("delegation service returned an invalid principal: %w", err)
	}
	if result.Tree.ExternalThreadID != threadID ||
		result.Tree.ControllerID != r.controllerID ||
		result.Tree.RootDeviceID != r.deviceID ||
		result.Tree.ControllerID != result.Principal.ControllerID ||
		result.Tree.TreeID != result.Principal.TreeID ||
		result.Tree.RootAgentID != result.Principal.AgentID ||
		result.Tree.RootDeviceID != result.Principal.DeviceID ||
		result.Principal.ParentAgentID != "" {
		return control.Tree{}, control.Principal{}, errors.New("delegation service returned a mismatched root binding")
	}
	return result.Tree, result.Principal, nil
}

func (r *Root) call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	callContext, cancel := context.WithTimeout(ctx, bridgeCallTimeout)
	if isAgentCall(method) {
		cancel()
		callContext, cancel = context.WithTimeout(ctx, agentCallTimeout)
	}
	defer cancel()
	return r.backend.Call(callContext, method, treeID, source, params, result)
}

func isAgentCall(method string) bool {
	switch method {
	case protocol.MethodSpawnAgent, protocol.MethodSendAgent,
		protocol.MethodFollowupAgent, protocol.MethodInterruptAgent,
		protocol.MethodWaitAgent:
		return true
	default:
		return false
	}
}

func summarizeDevice(device control.Device, featureLimit int, currentDeviceID string) DeviceSummary {
	limit := min(len(device.Features), featureLimit)
	return DeviceSummary{
		DeviceID:          device.DeviceID,
		Name:              device.Name,
		IsCurrentDevice:   device.DeviceID == currentDeviceID,
		OS:                device.OS,
		Arch:              device.Arch,
		RuntimeVersion:    device.RuntimeVersion,
		ProtocolVersion:   device.ProtocolVersion,
		Features:          slices.Clone(device.Features[:limit]),
		FeaturesTruncated: limit != len(device.Features),
		Online:            device.Online,
		LastSeenAt:        device.LastSeenAt,
	}
}

func readOnlyAnnotations() *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{
		ReadOnlyHint:    true,
		IdempotentHint:  true,
		DestructiveHint: &no,
		OpenWorldHint:   &no,
	}
}

func mutatingAnnotations() *mcp.ToolAnnotations {
	yes := true
	no := false
	return &mcp.ToolAnnotations{
		ReadOnlyHint:    false,
		IdempotentHint:  true,
		DestructiveHint: &no,
		OpenWorldHint:   &yes,
	}
}

func consumingAnnotations() *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{
		ReadOnlyHint:    false,
		IdempotentHint:  false,
		DestructiveHint: &no,
		OpenWorldHint:   &no,
	}
}
