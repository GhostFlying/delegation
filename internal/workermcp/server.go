package workermcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/buildinfo"
	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ToolSendMessage    = "send_message"
	ToolWaitAgent      = "wait_agent"
	maximumMessageSize = 16 * 1024
	maximumWaitSeconds = 25
	maximumMailboxPage = 16
	bridgeCallTimeout  = 30 * time.Second
	serverInstructions = "You are a managed Delegation worker. Use send_message to report useful progress or questions to your parent or root agent. Use wait_agent only when you need a reply. This worker has no device registry, workspace sync, spawn, or agent-management authority."
)

type Backend interface {
	Call(context.Context, string, string, *control.PrincipalIdentity, any, any) error
}

type Worker struct {
	backend   Backend
	principal control.PrincipalIdentity
}

type SendMessageInput struct {
	Recipient string `json:"recipient,omitempty" jsonschema:"recipient agent: parent or root; defaults to parent"`
	Message   string `json:"message" jsonschema:"message body to deliver"`
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
		Description: "Send a reliable message to this worker's parent or root agent.",
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
	if strings.TrimSpace(input.Message) == "" || len(input.Message) > maximumMessageSize ||
		!utf8.ValidString(input.Message) || strings.ContainsRune(input.Message, '\x00') {
		return nil, SendMessageOutput{}, fmt.Errorf(
			"message must contain from 1 through %d bytes of valid text",
			maximumMessageSize,
		)
	}
	recipient := protocol.MessageTargetParent
	switch input.Recipient {
	case "", string(protocol.MessageTargetParent):
	case string(protocol.MessageTargetRoot):
		recipient = protocol.MessageTargetRoot
	default:
		return nil, SendMessageOutput{}, errors.New("recipient must be parent or root")
	}
	var result protocol.SendMessageResult
	if err := w.call(ctx, protocol.MethodSendMessage, protocol.SendMessageParams{
		Target:  protocol.MessageTarget{Kind: recipient},
		Message: input.Message,
	}, &result); err != nil {
		return nil, SendMessageOutput{}, err
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
		return nil, WaitAgentOutput{}, err
	}
	return nil, WaitAgentOutput(result), nil
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
	message := send.Properties["message"]
	message.MinLength = jsonschema.Ptr(1)
	message.MaxLength = jsonschema.Ptr(maximumMessageSize)

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
		IdempotentHint:  false,
		DestructiveHint: &no,
		OpenWorldHint:   &no,
	}
}
