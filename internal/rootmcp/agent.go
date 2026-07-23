package rootmcp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const maximumAgentListBytes = 16 * 1024

type SpawnAgentInput struct {
	SpawnID        string `json:"spawn_id" jsonschema:"fresh UUID used to retry this exact dispatch safely"`
	TargetDeviceID string `json:"target_device_id" jsonschema:"target peer UUID returned by list_devices"`
	TaskName       string `json:"task_name" jsonschema:"short lowercase task identifier"`
	Message        string `json:"message" jsonschema:"self-contained task for the managed worker"`
	WorkspaceID    string `json:"workspace_id,omitempty" jsonschema:"prepared workspace UUID returned by sync_workspace"`
}

type AgentOutput struct {
	SpawnID        string                    `json:"spawn_id"`
	AgentID        string                    `json:"agent_id"`
	ParentAgentID  string                    `json:"parent_agent_id"`
	TargetDeviceID string                    `json:"target_device_id"`
	TaskName       string                    `json:"task_name"`
	Status         protocol.AgentSpawnStatus `json:"status"`
	FailureCode    string                    `json:"failure_code,omitempty"`
	WorkspaceID    string                    `json:"workspace_id,omitempty"`
}

type SpawnAgentOutput struct {
	SpawnID        string                     `json:"spawn_id"`
	AgentID        string                     `json:"agent_id"`
	ParentAgentID  string                     `json:"parent_agent_id"`
	TargetDeviceID string                     `json:"target_device_id"`
	TaskName       string                     `json:"task_name"`
	Status         protocol.AgentSpawnStatus  `json:"status"`
	Outcome        protocol.AgentSpawnOutcome `json:"outcome"`
	FailureCode    string                     `json:"failure_code,omitempty"`
	WorkspaceID    string                     `json:"workspace_id,omitempty"`
}

type ListAgentsInput struct {
	Cursor string `json:"cursor,omitempty" jsonschema:"opaque cursor returned by a previous list_agents call"`
	Limit  int    `json:"limit,omitempty" jsonschema:"maximum agents to return, from 1 through 32; defaults to 32"`
}

type ListAgentsOutput struct {
	Agents     []AgentOutput `json:"agents"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

func (r *Root) spawnAgent(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input SpawnAgentInput,
) (*mcp.CallToolResult, SpawnAgentOutput, error) {
	threadID, err := threadID(request)
	if err != nil {
		return nil, SpawnAgentOutput{}, err
	}
	params := protocol.SpawnAgentParams{
		SpawnID: input.SpawnID, TargetDeviceID: input.TargetDeviceID,
		TaskName: input.TaskName, Message: input.Message, WorkspaceID: input.WorkspaceID,
	}
	if err := params.Validate(); err != nil {
		return nil, SpawnAgentOutput{}, err
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return nil, SpawnAgentOutput{}, err
	}
	source := principal.Identity()
	var result protocol.SpawnAgentResult
	if err := r.call(
		ctx, protocol.MethodSpawnAgent, tree.TreeID, &source, params, &result,
	); err != nil {
		return nil, SpawnAgentOutput{}, explainAgentError(err)
	}
	if err := validateSpawnAgentResult(result, params, principal.ControllerID, tree.TreeID, principal.AgentID); err != nil {
		return nil, SpawnAgentOutput{}, err
	}
	output := spawnAgentOutput(result)
	if err := enforceOutputLimit(output, maximumDetailBytes); err != nil {
		return nil, SpawnAgentOutput{}, err
	}
	return nil, output, nil
}

func (r *Root) listAgents(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input ListAgentsInput,
) (*mcp.CallToolResult, ListAgentsOutput, error) {
	threadID, err := threadID(request)
	if err != nil {
		return nil, ListAgentsOutput{}, err
	}
	limit := input.Limit
	if limit == 0 {
		limit = protocol.MaximumAgentPage
	}
	if limit < 1 || limit > protocol.MaximumAgentPage {
		return nil, ListAgentsOutput{}, fmt.Errorf(
			"limit must be from 1 through %d", protocol.MaximumAgentPage,
		)
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return nil, ListAgentsOutput{}, err
	}
	afterSequence, err := decodeAgentCursor(input.Cursor, tree.TreeID)
	if err != nil {
		return nil, ListAgentsOutput{}, err
	}
	params := protocol.ListAgentsParams{AfterSequence: afterSequence, Limit: limit}
	source := principal.Identity()
	var result protocol.ListAgentsResult
	if err := r.call(
		ctx, protocol.MethodListAgents, tree.TreeID, &source, params, &result,
	); err != nil {
		return nil, ListAgentsOutput{}, explainAgentError(err)
	}
	if err := validateListAgentsResult(
		result, params, principal.ControllerID, tree.TreeID, principal.AgentID,
	); err != nil {
		return nil, ListAgentsOutput{}, err
	}
	output := ListAgentsOutput{Agents: make([]AgentOutput, 0, len(result.Agents))}
	for _, agent := range result.Agents {
		output.Agents = append(output.Agents, agentOutput(agent))
	}
	if result.NextSequence != 0 {
		output.NextCursor, err = encodeAgentCursor(tree.TreeID, result.NextSequence)
		if err != nil {
			return nil, ListAgentsOutput{}, err
		}
	}
	if err := enforceOutputLimit(output, maximumAgentListBytes); err != nil {
		return nil, ListAgentsOutput{}, err
	}
	return nil, output, nil
}

func agentInputSchemas() (*jsonschema.Schema, *jsonschema.Schema, error) {
	spawn, err := jsonschema.For[SpawnAgentInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build spawn_agent input schema: %w", err)
	}
	for _, name := range []string{"spawn_id", "target_device_id"} {
		property, found := spawn.Properties[name]
		if !found {
			return nil, nil, fmt.Errorf("spawn_agent input schema is missing %s", name)
		}
		property.MinLength = jsonschema.Ptr(36)
		property.MaxLength = jsonschema.Ptr(36)
		property.Pattern = uuidPattern
	}
	workspaceID, found := spawn.Properties["workspace_id"]
	if !found {
		return nil, nil, errors.New("spawn_agent input schema is missing workspace_id")
	}
	workspaceID.MaxLength = jsonschema.Ptr(36)
	workspaceID.Pattern = `^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?$`
	taskName, found := spawn.Properties["task_name"]
	if !found {
		return nil, nil, errors.New("spawn_agent input schema is missing task_name")
	}
	taskName.MinLength = jsonschema.Ptr(1)
	taskName.MaxLength = jsonschema.Ptr(protocol.MaximumAgentTaskNameBytes)
	taskName.Pattern = `^[a-z0-9_]+$`
	message, found := spawn.Properties["message"]
	if !found {
		return nil, nil, errors.New("spawn_agent input schema is missing message")
	}
	message.MinLength = jsonschema.Ptr(1)
	message.MaxLength = jsonschema.Ptr(protocol.MaximumAgentPromptBytes)

	list, err := jsonschema.For[ListAgentsInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build list_agents input schema: %w", err)
	}
	limit, found := list.Properties["limit"]
	if !found {
		return nil, nil, errors.New("list_agents input schema is missing limit")
	}
	limit.Minimum = jsonschema.Ptr(1.0)
	limit.Maximum = jsonschema.Ptr(float64(protocol.MaximumAgentPage))
	cursor, found := list.Properties["cursor"]
	if !found {
		return nil, nil, errors.New("list_agents input schema is missing cursor")
	}
	cursor.MaxLength = jsonschema.Ptr(base64.RawURLEncoding.EncodedLen(maximumCursorBytes))
	cursor.Pattern = `^(?:[A-Za-z0-9_-]+)?$`
	return spawn, list, nil
}

func validateSpawnAgentResult(
	result protocol.SpawnAgentResult,
	params protocol.SpawnAgentParams,
	controllerID, treeID, parentAgentID string,
) error {
	if err := result.Validate(); err != nil {
		return fmt.Errorf("delegation service returned an invalid agent: %w", err)
	}
	if result.Agent.SpawnID != params.SpawnID ||
		result.Agent.Principal.ControllerID != controllerID ||
		result.Agent.Principal.TreeID != treeID ||
		result.Agent.Principal.ParentAgentID != parentAgentID ||
		result.Agent.Principal.DeviceID != params.TargetDeviceID ||
		result.Agent.TaskName != params.TaskName || result.Agent.WorkspaceID != params.WorkspaceID {
		return errors.New("delegation service returned a mismatched agent")
	}
	return nil
}

func validateListAgentsResult(
	result protocol.ListAgentsResult,
	params protocol.ListAgentsParams,
	controllerID, treeID, parentAgentID string,
) error {
	if len(result.Agents) > params.Limit {
		return errors.New("delegation service returned too many agents")
	}
	previous := params.AfterSequence
	for _, agent := range result.Agents {
		if err := agent.Validate(); err != nil {
			return fmt.Errorf("delegation service returned an invalid agent: %w", err)
		}
		if agent.Principal.ControllerID != controllerID || agent.Principal.TreeID != treeID ||
			agent.Principal.ParentAgentID != parentAgentID || agent.Sequence <= previous {
			return errors.New("delegation service returned mismatched or unordered agents")
		}
		previous = agent.Sequence
	}
	if result.NextSequence != 0 {
		if len(result.Agents) != params.Limit || result.NextSequence != previous {
			return errors.New("delegation service returned an invalid agent cursor")
		}
	}
	return nil
}

func agentOutput(agent protocol.AgentSummary) AgentOutput {
	return AgentOutput{
		SpawnID:        agent.SpawnID,
		AgentID:        agent.Principal.AgentID,
		ParentAgentID:  agent.Principal.ParentAgentID,
		TargetDeviceID: agent.Principal.DeviceID,
		TaskName:       agent.TaskName,
		Status:         agent.Status,
		FailureCode:    agent.FailureCode,
		WorkspaceID:    agent.WorkspaceID,
	}
}

func spawnAgentOutput(result protocol.SpawnAgentResult) SpawnAgentOutput {
	agent := result.Agent
	return SpawnAgentOutput{
		SpawnID:        agent.SpawnID,
		AgentID:        agent.Principal.AgentID,
		ParentAgentID:  agent.Principal.ParentAgentID,
		TargetDeviceID: agent.Principal.DeviceID,
		TaskName:       agent.TaskName,
		Status:         agent.Status,
		Outcome:        result.Outcome,
		FailureCode:    agent.FailureCode,
		WorkspaceID:    agent.WorkspaceID,
	}
}
