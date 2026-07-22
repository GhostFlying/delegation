package rootmcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maximumAgentTargetBytes = len("/root/") + protocol.MaximumAgentTaskNameBytes
	agentTargetPattern      = `^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|(?:/root/)?[a-z0-9_]{1,64})$`
)

type SendMessageInput struct {
	Target    string `json:"target" jsonschema:"agent UUID, task_name, or /root/task_name in the current tree"`
	MessageID string `json:"message_id" jsonschema:"fresh UUID used to retry this exact logical message safely"`
	Message   string `json:"message" jsonschema:"message to deliver or steer to the managed agent"`
}

type FollowupTaskInput struct {
	Target      string `json:"target" jsonschema:"agent UUID, task_name, or /root/task_name in the current tree"`
	OperationID string `json:"operation_id" jsonschema:"fresh UUID used to retry this exact follow-up safely"`
	Message     string `json:"message" jsonschema:"self-contained follow-up task for the managed agent"`
}

type InterruptAgentInput struct {
	Target      string `json:"target" jsonschema:"agent UUID, task_name, or /root/task_name in the current tree"`
	OperationID string `json:"operation_id" jsonschema:"fresh UUID used to retry this exact interrupt safely"`
}

type AgentOperationOutput struct {
	OperationID string                         `json:"operation_id"`
	AgentID     string                         `json:"agent_id"`
	Action      protocol.AgentOperationAction  `json:"action"`
	Outcome     protocol.AgentOperationOutcome `json:"outcome"`
	FailureCode string                         `json:"failure_code,omitempty"`
}

type parsedAgentTarget struct {
	agentID  string
	taskName string
}

func (r *Root) sendMessage(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input SendMessageInput,
) (*mcp.CallToolResult, AgentOperationOutput, error) {
	if err := identity.ValidateID(input.MessageID); err != nil {
		return nil, AgentOperationOutput{}, fmt.Errorf("message_id %w", err)
	}
	if err := protocol.ValidateMailboxMessage(input.Message); err != nil {
		return nil, AgentOperationOutput{}, err
	}
	tree, principal, agentID, err := r.resolveOperationTarget(ctx, request, input.Target)
	if err != nil {
		return nil, AgentOperationOutput{}, err
	}
	params := protocol.SendAgentParams{
		AgentID: agentID, MessageID: input.MessageID, Message: input.Message,
	}
	return r.runAgentOperation(
		ctx, tree, principal, protocol.MethodSendAgent, protocol.AgentOperationSend,
		input.MessageID, agentID, params,
	)
}

func (r *Root) followupTask(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input FollowupTaskInput,
) (*mcp.CallToolResult, AgentOperationOutput, error) {
	if err := identity.ValidateID(input.OperationID); err != nil {
		return nil, AgentOperationOutput{}, fmt.Errorf("operation_id %w", err)
	}
	if err := protocol.ValidateAgentMessage(input.Message); err != nil {
		return nil, AgentOperationOutput{}, err
	}
	tree, principal, agentID, err := r.resolveOperationTarget(ctx, request, input.Target)
	if err != nil {
		return nil, AgentOperationOutput{}, err
	}
	params := protocol.FollowupAgentParams{
		OperationID: input.OperationID, AgentID: agentID, Message: input.Message,
	}
	return r.runAgentOperation(
		ctx, tree, principal, protocol.MethodFollowupAgent, protocol.AgentOperationFollowup,
		input.OperationID, agentID, params,
	)
}

func (r *Root) interruptAgent(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input InterruptAgentInput,
) (*mcp.CallToolResult, AgentOperationOutput, error) {
	if err := identity.ValidateID(input.OperationID); err != nil {
		return nil, AgentOperationOutput{}, fmt.Errorf("operation_id %w", err)
	}
	tree, principal, agentID, err := r.resolveOperationTarget(ctx, request, input.Target)
	if err != nil {
		return nil, AgentOperationOutput{}, err
	}
	params := protocol.InterruptAgentParams{OperationID: input.OperationID, AgentID: agentID}
	return r.runAgentOperation(
		ctx, tree, principal, protocol.MethodInterruptAgent, protocol.AgentOperationInterrupt,
		input.OperationID, agentID, params,
	)
}

func (r *Root) resolveOperationTarget(
	ctx context.Context,
	request *mcp.CallToolRequest,
	value string,
) (control.Tree, control.Principal, string, error) {
	target, err := parseAgentTarget(value)
	if err != nil {
		return control.Tree{}, control.Principal{}, "", err
	}
	threadID, err := threadID(request)
	if err != nil {
		return control.Tree{}, control.Principal{}, "", err
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return control.Tree{}, control.Principal{}, "", err
	}
	if target.agentID != "" {
		return tree, principal, target.agentID, nil
	}
	agentID, err := r.resolveTaskName(ctx, tree, principal, target.taskName)
	if err != nil {
		return control.Tree{}, control.Principal{}, "", err
	}
	return tree, principal, agentID, nil
}

func (r *Root) resolveTaskName(
	ctx context.Context,
	tree control.Tree,
	principal control.Principal,
	taskName string,
) (string, error) {
	var match string
	var afterSequence uint64
	for {
		params := protocol.ListAgentsParams{
			AfterSequence: afterSequence,
			Limit:         protocol.MaximumAgentPage,
		}
		source := principal.Identity()
		var result protocol.ListAgentsResult
		if err := r.call(
			ctx, protocol.MethodListAgents, tree.TreeID, &source, params, &result,
		); err != nil {
			return "", explainAgentError(err)
		}
		if err := validateListAgentsResult(
			result, params, principal.ControllerID, tree.TreeID, principal.AgentID,
		); err != nil {
			return "", err
		}
		for _, agent := range result.Agents {
			if agent.TaskName != taskName {
				continue
			}
			if match != "" && match != agent.Principal.AgentID {
				return "", fmt.Errorf("delegation agent target %q is ambiguous", taskName)
			}
			match = agent.Principal.AgentID
		}
		if result.NextSequence == 0 {
			break
		}
		afterSequence = result.NextSequence
	}
	if match == "" {
		return "", fmt.Errorf("delegation agent target %q was not found; call list_agents to refresh", taskName)
	}
	return match, nil
}

func (r *Root) runAgentOperation(
	ctx context.Context,
	tree control.Tree,
	principal control.Principal,
	method string,
	action protocol.AgentOperationAction,
	operationID, agentID string,
	params any,
) (*mcp.CallToolResult, AgentOperationOutput, error) {
	source := principal.Identity()
	var result protocol.AgentOperationResult
	if err := r.call(ctx, method, tree.TreeID, &source, params, &result); err != nil {
		return nil, AgentOperationOutput{}, explainAgentError(err)
	}
	if err := validateAgentOperationResult(result, operationID, agentID, action); err != nil {
		return nil, AgentOperationOutput{}, err
	}
	output := AgentOperationOutput{
		OperationID: result.OperationID,
		AgentID:     result.AgentID,
		Action:      result.Action,
		Outcome:     result.Outcome,
		FailureCode: result.FailureCode,
	}
	if err := enforceOutputLimit(output, maximumDetailBytes); err != nil {
		return nil, AgentOperationOutput{}, err
	}
	return nil, output, nil
}

func parseAgentTarget(value string) (parsedAgentTarget, error) {
	if err := identity.ValidateID(value); err == nil {
		return parsedAgentTarget{agentID: value}, nil
	}
	taskName := value
	if strings.HasPrefix(value, "/") {
		if value == "/root" {
			return parsedAgentTarget{}, errors.New("target /root names the root agent, not a managed agent")
		}
		const prefix = "/root/"
		if !strings.HasPrefix(value, prefix) {
			return parsedAgentTarget{}, errors.New("agent target paths must use /root/task_name")
		}
		taskName = strings.TrimPrefix(value, prefix)
	}
	if strings.Contains(taskName, "/") {
		return parsedAgentTarget{}, errors.New("nested agent target paths are not supported")
	}
	if err := protocol.ValidateAgentTaskName(taskName); err != nil {
		return parsedAgentTarget{}, fmt.Errorf("target: %w", err)
	}
	return parsedAgentTarget{taskName: taskName}, nil
}

func validateAgentOperationResult(
	result protocol.AgentOperationResult,
	operationID, agentID string,
	action protocol.AgentOperationAction,
) error {
	if err := result.Validate(); err != nil {
		return fmt.Errorf("delegation service returned an invalid agent operation: %w", err)
	}
	if result.OperationID != operationID || result.AgentID != agentID || result.Action != action {
		return errors.New("delegation service returned a mismatched agent operation")
	}
	return nil
}

func agentControlInputSchemas() (*jsonschema.Schema, *jsonschema.Schema, *jsonschema.Schema, error) {
	send, err := jsonschema.For[SendMessageInput](nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build send_message input schema: %w", err)
	}
	if err := configureAgentControlSchema(
		send, ToolSendMessage, "message_id", protocol.MaximumMailboxMessageBytes,
	); err != nil {
		return nil, nil, nil, err
	}

	followup, err := jsonschema.For[FollowupTaskInput](nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build followup_task input schema: %w", err)
	}
	if err := configureAgentControlSchema(
		followup, ToolFollowupTask, "operation_id", protocol.MaximumAgentPromptBytes,
	); err != nil {
		return nil, nil, nil, err
	}

	interrupt, err := jsonschema.For[InterruptAgentInput](nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build interrupt_agent input schema: %w", err)
	}
	if err := configureAgentTargetAndID(interrupt, ToolInterruptAgent, "operation_id"); err != nil {
		return nil, nil, nil, err
	}
	return send, followup, interrupt, nil
}

func configureAgentControlSchema(
	schema *jsonschema.Schema,
	toolName, idName string,
	maximumMessageBytes int,
) error {
	if err := configureAgentTargetAndID(schema, toolName, idName); err != nil {
		return err
	}
	message, found := schema.Properties["message"]
	if !found {
		return fmt.Errorf("%s input schema is missing message", toolName)
	}
	message.MinLength = jsonschema.Ptr(1)
	message.MaxLength = jsonschema.Ptr(maximumMessageBytes)
	return nil
}

func configureAgentTargetAndID(schema *jsonschema.Schema, toolName, idName string) error {
	target, found := schema.Properties["target"]
	if !found {
		return fmt.Errorf("%s input schema is missing target", toolName)
	}
	target.MinLength = jsonschema.Ptr(1)
	target.MaxLength = jsonschema.Ptr(maximumAgentTargetBytes)
	target.Pattern = agentTargetPattern
	id, found := schema.Properties[idName]
	if !found {
		return fmt.Errorf("%s input schema is missing %s", toolName, idName)
	}
	id.MinLength = jsonschema.Ptr(36)
	id.MaxLength = jsonschema.Ptr(36)
	id.Pattern = uuidPattern
	return nil
}
