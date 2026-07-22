package workermcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ToolSendMessage    = "send_message"
	ToolWaitAgent      = "wait_agent"
	maximumWaitSeconds = 25
	maximumMailboxPage = 1
	// A page contains one message capped at 1 KiB. This larger encoded bound
	// only accommodates worst-case JSON escaping of that same bounded text.
	maximumWaitOutput    = 8 * 1024
	bridgeCallTimeout    = 30 * time.Second
	lowercaseUUIDPattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
	serverInstructions   = "You are a managed Delegation worker. Use send_message to report useful progress or questions to your parent or root agent. Create one lowercase UUID messageId for each logical message. If send_message is unavailable or its outcome is ambiguous, retry promptly with exactly the same messageId, recipient, and message; recent identical retries resolve to the same delivery while the receipt is retained. Never create a new messageId for that logical message. Use wait_agent only when you need a reply. This worker has no device registry, workspace sync, spawn, or agent-management authority."
)

type Backend interface {
	Call(context.Context, string, string, *control.PrincipalIdentity, any, any) error
}

type Worker struct {
	backend   Backend
	principal control.PrincipalIdentity
}

type SendMessageInput struct {
	MessageID string `json:"messageId" jsonschema:"lowercase UUID chosen once for this logical message; reuse it with identical arguments after an unavailable or ambiguous result"`
	Recipient string `json:"recipient,omitempty" jsonschema:"recipient agent: parent or root; defaults to parent"`
	Message   string `json:"message" jsonschema:"message body to deliver; must contain from 1 through 1024 UTF-8 bytes"`
}

type SendMessageOutput struct {
	MessageID string `json:"messageId"`
	Sequence  uint64 `json:"sequence"`
}

type WaitAgentInput struct {
	Cursor         uint64 `json:"cursor,omitempty" jsonschema:"last consumed mailbox sequence; defaults to zero"`
	TimeoutSeconds int    `json:"timeoutSeconds,omitempty" jsonschema:"long-poll timeout from 1 through 25 seconds; defaults to 25"`
}

type WaitAgentOutput struct {
	Messages   []protocol.MailboxMessage `json:"messages"`
	NextCursor uint64                    `json:"nextCursor"`
}

func NewServer(backend Backend, principal control.PrincipalIdentity) (*mcp.Server, error) {
	if backend == nil {
		return nil, errors.New("worker MCP backend is required")
	}
	if err := principal.Validate(); err != nil {
		return nil, fmt.Errorf("worker MCP principal: %w", err)
	}
	if principal.ParentAgentID == "" {
		return nil, errors.New("worker MCP requires a managed worker principal")
	}
	sendSchema, waitSchema, err := inputSchemas()
	if err != nil {
		return nil, err
	}
	worker := &Worker{backend: backend, principal: principal}
	server := mcp.NewServer(
		&mcp.Implementation{
			Name: "delegation_worker", Title: "Delegation Worker", Version: buildinfo.Version,
		},
		&mcp.ServerOptions{
			Instructions: serverInstructions,
			Capabilities: &mcp.ServerCapabilities{},
		},
	)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolSendMessage,
		Title:       "Send delegation message",
		Description: "Send a message to this worker's parent or root agent. Choose one lowercase UUID messageId per logical message; promptly retry unavailable or ambiguous recent calls with every argument unchanged so a retained receipt resolves to the same delivery.",
		Annotations: mutatingAnnotations(),
		InputSchema: sendSchema,
	}, worker.sendMessage)
	mcp.AddTool(server, &mcp.Tool{
		Name:        ToolWaitAgent,
		Title:       "Wait for delegation messages",
		Description: "Wait for messages addressed to this managed worker and advance its mailbox cursor.",
		Annotations: readOnlyAnnotations(),
		InputSchema: waitSchema,
	}, worker.waitAgent)
	return server, nil
}

func (w *Worker) sendMessage(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input SendMessageInput,
) (*mcp.CallToolResult, SendMessageOutput, error) {
	params := protocol.SendMessageParams{
		MessageID: input.MessageID,
		Target:    protocol.MessageTarget{Kind: protocol.MessageTargetParent},
		Message:   input.Message,
	}
	if err := protocol.ValidateMailboxMessage(input.Message); err != nil {
		return nil, SendMessageOutput{}, err
	}
	recipient := protocol.MessageTargetParent
	switch input.Recipient {
	case "", string(protocol.MessageTargetParent):
	case string(protocol.MessageTargetRoot):
		recipient = protocol.MessageTargetRoot
	default:
		return nil, SendMessageOutput{}, errors.New("recipient must be parent or root")
	}
	params.Target.Kind = recipient
	if err := params.Validate(); err != nil {
		return nil, SendMessageOutput{}, err
	}
	var result protocol.SendMessageResult
	if err := w.call(ctx, protocol.MethodSendMessage, params, &result); err != nil {
		return nil, SendMessageOutput{}, workerBackendError(protocol.MethodSendMessage, params.MessageID, err)
	}
	if result.MessageID != params.MessageID || result.Sequence == 0 {
		return nil, SendMessageOutput{}, errors.New("delegation service returned an invalid message receipt")
	}
	return nil, SendMessageOutput(result), nil
}

func (w *Worker) waitAgent(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	input WaitAgentInput,
) (*mcp.CallToolResult, WaitAgentOutput, error) {
	timeout := input.TimeoutSeconds
	if timeout == 0 {
		timeout = maximumWaitSeconds
	}
	if timeout < 1 || timeout > maximumWaitSeconds {
		return nil, WaitAgentOutput{}, fmt.Errorf(
			"timeoutSeconds must be from 1 through %d",
			maximumWaitSeconds,
		)
	}
	var result protocol.WaitMailboxResult
	if err := w.call(ctx, protocol.MethodWaitMailbox, protocol.WaitMailboxParams{
		Cursor:        input.Cursor,
		TimeoutMillis: timeout * 1000,
		Limit:         maximumMailboxPage,
	}, &result); err != nil {
		return nil, WaitAgentOutput{}, workerBackendError(protocol.MethodWaitMailbox, "", err)
	}
	if err := validateMailboxResult(result, input.Cursor, w.principal); err != nil {
		return nil, WaitAgentOutput{}, err
	}
	output := WaitAgentOutput(result)
	data, err := json.Marshal(output)
	if err != nil {
		return nil, WaitAgentOutput{}, fmt.Errorf("encode worker mailbox output: %w", err)
	}
	if len(data) > maximumWaitOutput {
		return nil, WaitAgentOutput{}, fmt.Errorf("worker mailbox output exceeds %d bytes", maximumWaitOutput)
	}
	return nil, output, nil
}

func workerBackendError(method, messageID string, err error) error {
	var rpcError *localbridge.RPCError
	if errors.As(err, &rpcError) {
		switch rpcError.Code {
		case protocol.ErrorInvalidParams:
			return errors.New("delegation request was rejected")
		case protocol.ErrorForbidden:
			return errors.New("delegation worker is no longer authorized")
		case protocol.ErrorNotFound:
			return errors.New("delegation message recipient is unavailable")
		case protocol.ErrorConflict:
			if method == protocol.MethodSendMessage {
				return fmt.Errorf("delegation messageId %s is bound to different arguments; reuse the original complete arguments for this logical message, or use a new lowercase UUID only for a genuinely new message", messageID)
			}
			return errors.New("delegation mailbox cursor is stale; retry wait_agent with cursor 0")
		}
	}
	if method == protocol.MethodSendMessage {
		return fmt.Errorf("delegation service unavailable; promptly retry send_message with messageId %s and the exact same recipient and message", messageID)
	}
	return errors.New("delegation service unavailable")
}

func validateMailboxResult(
	result protocol.WaitMailboxResult,
	cursor uint64,
	principal control.PrincipalIdentity,
) error {
	if len(result.Messages) > maximumMailboxPage {
		return errors.New("delegation service returned too many mailbox messages")
	}
	if len(result.Messages) == 0 {
		if result.NextCursor != cursor {
			return errors.New("delegation service advanced an empty mailbox cursor")
		}
		return nil
	}
	previous := cursor
	for _, message := range result.Messages {
		if err := message.Validate(); err != nil {
			return errors.New("delegation service returned invalid mailbox message content")
		}
		if message.Sequence <= previous {
			return errors.New("delegation service returned mailbox messages out of order")
		}
		if err := message.Source.Validate(); err != nil ||
			message.Source.ControllerID != principal.ControllerID ||
			message.Source.TreeID != principal.TreeID {
			return errors.New("delegation service returned a mailbox message from another tree")
		}
		previous = message.Sequence
	}
	if result.NextCursor != previous {
		return errors.New("delegation service returned an invalid mailbox cursor")
	}
	return nil
}

func (w *Worker) call(ctx context.Context, method string, params, result any) error {
	callContext, cancel := context.WithTimeout(ctx, bridgeCallTimeout)
	defer cancel()
	source := w.principal
	return w.backend.Call(callContext, method, source.TreeID, &source, params, result)
}

func inputSchemas() (*jsonschema.Schema, *jsonschema.Schema, error) {
	send, err := jsonschema.For[SendMessageInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build send_message input schema: %w", err)
	}
	recipient := send.Properties["recipient"]
	recipient.Enum = []any{"parent", "root"}
	messageID := send.Properties["messageId"]
	messageID.Pattern = lowercaseUUIDPattern
	message := send.Properties["message"]
	message.MinLength = jsonschema.Ptr(1)

	wait, err := jsonschema.For[WaitAgentInput](nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build wait_agent input schema: %w", err)
	}
	timeout := wait.Properties["timeoutSeconds"]
	timeout.Minimum = jsonschema.Ptr(1.0)
	timeout.Maximum = jsonschema.Ptr(float64(maximumWaitSeconds))
	return send, wait, nil
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
	no := false
	return &mcp.ToolAnnotations{
		ReadOnlyHint:    false,
		IdempotentHint:  true,
		DestructiveHint: &no,
		OpenWorldHint:   &no,
	}
}
