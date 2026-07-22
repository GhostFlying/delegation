package workermcp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	workerTestControllerID   = "123e4567-e89b-42d3-a456-426614174700"
	workerTestTreeID         = "123e4567-e89b-42d3-a456-426614174701"
	workerTestAgentID        = "123e4567-e89b-42d3-a456-426614174702"
	workerTestParentID       = "123e4567-e89b-42d3-a456-426614174703"
	workerTestDeviceID       = "123e4567-e89b-42d3-a456-426614174704"
	workerTestMessageID      = "123e4567-e89b-42d3-a456-426614174705"
	workerTestOtherMessageID = "123e4567-e89b-42d3-a456-426614174706"
)

type recordedWorkerCall struct {
	method string
	treeID string
	source control.PrincipalIdentity
	params any
}

type workerBackend struct {
	mu         sync.Mutex
	calls      []recordedWorkerCall
	sendResult *protocol.SendMessageResult
	waitResult *protocol.WaitMailboxResult
	err        error
}

func (b *workerBackend) Call(
	_ context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	if source == nil {
		return errors.New("source is required")
	}
	b.mu.Lock()
	b.calls = append(b.calls, recordedWorkerCall{
		method: method, treeID: treeID, source: *source, params: params,
	})
	b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	switch method {
	case protocol.MethodSendMessage:
		send := params.(protocol.SendMessageParams)
		receipt := protocol.SendMessageResult{
			MessageID: send.MessageID,
			Sequence:  7,
		}
		if b.sendResult != nil {
			receipt = *b.sendResult
		}
		*result.(*protocol.SendMessageResult) = receipt
	case protocol.MethodWaitMailbox:
		mailbox := protocol.WaitMailboxResult{
			Messages:   []protocol.MailboxMessage{},
			NextCursor: params.(protocol.WaitMailboxParams).Cursor,
		}
		if b.waitResult != nil {
			mailbox = *b.waitResult
		}
		*result.(*protocol.WaitMailboxResult) = mailbox
	default:
		return errors.New("unexpected method")
	}
	return nil
}

func (b *workerBackend) snapshot() []recordedWorkerCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recordedWorkerCall(nil), b.calls...)
}

func TestWorkerMCPExposesOnlyMessageToolsAndBindsPrincipal(t *testing.T) {
	backend := &workerBackend{}
	principal := workerTestPrincipal()
	server, err := NewServer(backend, principal)
	if err != nil {
		t.Fatal(err)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "worker-test", Version: "1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		serverSession.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
	})

	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 2 || tools.Tools[0].Name != ToolSendMessage || tools.Tools[1].Name != ToolWaitAgent {
		t.Fatalf("worker MCP tools = %#v", tools.Tools)
	}
	if _, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: ToolSendMessage,
		Arguments: map[string]any{
			"messageId": workerTestMessageID,
			"recipient": "root",
			"message":   "status update",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name: ToolWaitAgent,
		Arguments: map[string]any{
			"cursor":         4,
			"timeoutSeconds": 3,
		},
	}); err != nil {
		t.Fatal(err)
	}

	calls := backend.snapshot()
	if len(calls) != 2 {
		t.Fatalf("worker backend calls = %#v", calls)
	}
	sendParams := calls[0].params.(protocol.SendMessageParams)
	if sendParams.MessageID != workerTestMessageID {
		t.Fatalf("send_message messageId = %q, want %q", sendParams.MessageID, workerTestMessageID)
	}
	want := []recordedWorkerCall{
		{
			method: protocol.MethodSendMessage,
			treeID: workerTestTreeID,
			source: principal,
			params: protocol.SendMessageParams{
				MessageID: workerTestMessageID,
				Target:    protocol.MessageTarget{Kind: protocol.MessageTargetRoot},
				Message:   "status update",
			},
		},
		{
			method: protocol.MethodWaitMailbox,
			treeID: workerTestTreeID,
			source: principal,
			params: protocol.WaitMailboxParams{Cursor: 4, TimeoutMillis: 3000, Limit: 1},
		},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("worker backend calls = %#v, want %#v", calls, want)
	}
}

func TestWorkerMCPRejectsRootIdentityAndInvalidInputs(t *testing.T) {
	backend := &workerBackend{}
	root := workerTestPrincipal()
	root.ParentAgentID = ""
	if _, err := NewServer(backend, root); err == nil {
		t.Fatal("NewServer accepted a root principal")
	}

	worker := &Worker{backend: backend, principal: workerTestPrincipal()}
	for name, input := range map[string]SendMessageInput{
		"missing messageId": {Message: "status update"},
		"uppercase messageId": {
			MessageID: strings.ToUpper(workerTestMessageID),
			Message:   "status update",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := worker.sendMessage(context.Background(), nil, input); err == nil {
				t.Fatal("send_message accepted an invalid messageId")
			}
		})
	}
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		MessageID: workerTestMessageID,
		Recipient: "agent",
		Message:   "invalid target",
	}); err == nil {
		t.Fatal("send_message accepted an arbitrary recipient")
	}
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		MessageID: workerTestMessageID,
		Message:   string([]byte{0xff}),
	}); err == nil {
		t.Fatal("send_message accepted invalid UTF-8")
	}
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		MessageID: workerTestMessageID,
		Message:   strings.Repeat("x", protocol.MaximumMailboxMessageBytes+1),
	}); err == nil {
		t.Fatal("send_message accepted an oversized message")
	}
	if _, _, err := worker.waitAgent(context.Background(), nil, WaitAgentInput{
		TimeoutSeconds: maximumWaitSeconds + 1,
	}); err == nil {
		t.Fatal("wait_agent accepted an excessive timeout")
	}
	if calls := backend.snapshot(); len(calls) != 0 {
		data, _ := json.Marshal(calls)
		t.Fatalf("invalid tools reached backend: %s", data)
	}
}

func TestWorkerMCPValidatesMailboxResultsAndOutputBound(t *testing.T) {
	valid := mailboxResult("status update")
	tests := map[string]func(*protocol.WaitMailboxResult){
		"too many messages": func(result *protocol.WaitMailboxResult) {
			result.Messages = append(result.Messages, result.Messages[0])
		},
		"cross-tree source": func(result *protocol.WaitMailboxResult) {
			result.Messages[0].Source.TreeID = "123e4567-e89b-42d3-a456-426614174706"
		},
		"sequence regression": func(result *protocol.WaitMailboxResult) {
			result.Messages[0].Sequence = 4
			result.NextCursor = 4
		},
		"cursor mismatch": func(result *protocol.WaitMailboxResult) {
			result.NextCursor++
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := valid
			result.Messages = append([]protocol.MailboxMessage(nil), valid.Messages...)
			mutate(&result)
			worker := &Worker{
				backend:   &workerBackend{waitResult: &result},
				principal: workerTestPrincipal(),
			}
			if _, _, err := worker.waitAgent(context.Background(), nil, WaitAgentInput{
				Cursor: 4, TimeoutSeconds: 1,
			}); err == nil {
				t.Fatal("wait_agent accepted an invalid mailbox result")
			}
		})
	}

	worker := &Worker{
		backend:   &workerBackend{waitResult: &valid},
		principal: workerTestPrincipal(),
	}
	_, output, err := worker.waitAgent(context.Background(), nil, WaitAgentInput{
		Cursor: 4, TimeoutSeconds: 1,
	})
	if err != nil || !reflect.DeepEqual(output, WaitAgentOutput(valid)) {
		t.Fatalf("valid wait_agent output = %#v, %v", output, err)
	}
}

func TestWorkerMCPAcceptsWorstCaseEncodedMessageWithinOutputBound(t *testing.T) {
	message := strings.Repeat("\x01", protocol.MaximumMailboxMessageBytes)
	result := mailboxResult(message)
	data, err := json.Marshal(WaitAgentOutput(result))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maximumWaitOutput {
		t.Fatalf("maximum legal mailbox output = %d bytes, limit %d", len(data), maximumWaitOutput)
	}
	worker := &Worker{
		backend:   &workerBackend{waitResult: &result},
		principal: workerTestPrincipal(),
	}
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		MessageID: workerTestMessageID,
		Message:   message,
	}); err != nil {
		t.Fatalf("send_message rejected a maximum legal message: %v", err)
	}
	_, output, err := worker.waitAgent(context.Background(), nil, WaitAgentInput{
		Cursor: 4, TimeoutSeconds: 1,
	})
	if err != nil || !reflect.DeepEqual(output, WaitAgentOutput(result)) {
		t.Fatalf("wait_agent maximum legal output = %#v, %v", output, err)
	}
}

func TestWorkerMCPRejectsInvalidMessageReceipt(t *testing.T) {
	tests := map[string]protocol.SendMessageResult{
		"missing": {},
		"mismatched": {
			MessageID: workerTestOtherMessageID,
			Sequence:  1,
		},
	}
	for name, receipt := range tests {
		t.Run(name, func(t *testing.T) {
			worker := &Worker{
				backend:   &workerBackend{sendResult: &receipt},
				principal: workerTestPrincipal(),
			}
			if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
				MessageID: workerTestMessageID,
				Message:   "status update",
			}); err == nil {
				t.Fatal("send_message accepted an invalid receipt")
			}
		})
	}
}

func TestWorkerMCPSendMessageUsesUTF8ByteBoundWithoutMisleadingSchemaLimit(t *testing.T) {
	send, _, err := inputSchemas()
	if err != nil {
		t.Fatal(err)
	}
	messageSchema := send.Properties["message"]
	if messageSchema.MaxLength != nil || !strings.Contains(messageSchema.Description, "1024 UTF-8 bytes") {
		t.Fatalf("send_message message schema = %#v", messageSchema)
	}
	messageIDSchema := send.Properties["messageId"]
	if messageIDSchema.Pattern != lowercaseUUIDPattern ||
		!strings.Contains(messageIDSchema.Description, "reuse it with identical arguments") ||
		!containsString(send.Required, "messageId") {
		t.Fatalf("send_message messageId schema = %#v, required = %#v", messageIDSchema, send.Required)
	}
	if !strings.Contains(serverInstructions, "exactly the same messageId, recipient, and message") ||
		!strings.Contains(serverInstructions, "while the receipt is retained") {
		t.Fatalf("worker instructions do not explain bounded retry semantics: %q", serverInstructions)
	}

	worker := &Worker{backend: &workerBackend{}, principal: workerTestPrincipal()}
	boundary := strings.Repeat("界", 341) + "x"
	if len(boundary) != protocol.MaximumMailboxMessageBytes {
		t.Fatalf("non-ASCII boundary fixture = %d bytes", len(boundary))
	}
	if _, _, err := worker.sendMessage(
		context.Background(), nil, SendMessageInput{MessageID: workerTestMessageID, Message: boundary},
	); err != nil {
		t.Fatalf("send_message rejected %d-byte non-ASCII message: %v", len(boundary), err)
	}
	oversized := boundary + "x"
	if _, _, err := worker.sendMessage(
		context.Background(), nil, SendMessageInput{MessageID: workerTestMessageID, Message: oversized},
	); err == nil {
		t.Fatalf("send_message accepted %d-byte non-ASCII message", len(oversized))
	}
}

func TestWorkerMCPDoesNotExposeBackendTransportDetails(t *testing.T) {
	const transportDetail = "/private/home/peer/runtime.sock"
	worker := &Worker{
		backend:   &workerBackend{err: errors.New(transportDetail)},
		principal: workerTestPrincipal(),
	}
	_, _, sendErr := worker.sendMessage(
		context.Background(),
		nil,
		SendMessageInput{MessageID: workerTestMessageID, Message: "status update"},
	)
	_, _, waitErr := worker.waitAgent(
		context.Background(),
		nil,
		WaitAgentInput{TimeoutSeconds: 1},
	)
	if sendErr == nil || strings.Contains(sendErr.Error(), transportDetail) ||
		sendErr.Error() != "delegation service unavailable; promptly retry send_message with messageId "+workerTestMessageID+" and the exact same recipient and message" {
		t.Fatalf("worker send transport error = %v", sendErr)
	}
	if waitErr == nil || strings.Contains(waitErr.Error(), transportDetail) ||
		waitErr.Error() != "delegation service unavailable" {
		t.Fatalf("worker wait transport error = %v", waitErr)
	}
}

func TestWorkerMCPExplainsPermanentBrokerErrorsWithoutLeakingDetails(t *testing.T) {
	tests := []struct {
		code int
		want string
	}{
		{code: protocol.ErrorInvalidParams, want: "delegation request was rejected"},
		{code: protocol.ErrorForbidden, want: "delegation worker is no longer authorized"},
		{code: protocol.ErrorNotFound, want: "delegation message recipient is unavailable"},
		{code: protocol.ErrorConflict, want: "delegation mailbox cursor is stale; retry wait_agent with cursor 0"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			const brokerDetail = "/private/broker/state.sqlite3"
			worker := &Worker{
				backend: &workerBackend{err: &localbridge.RPCError{
					Code: test.code, Message: brokerDetail,
				}},
				principal: workerTestPrincipal(),
			}
			_, _, err := worker.waitAgent(
				context.Background(), nil, WaitAgentInput{TimeoutSeconds: 1},
			)
			if err == nil || err.Error() != test.want || strings.Contains(err.Error(), brokerDetail) {
				t.Fatalf("worker broker error = %v, want %q", err, test.want)
			}
		})
	}

	const brokerDetail = "/private/broker/state.sqlite3"
	worker := &Worker{
		backend: &workerBackend{err: &localbridge.RPCError{
			Code: protocol.ErrorConflict, Message: brokerDetail,
		}},
		principal: workerTestPrincipal(),
	}
	_, _, err := worker.sendMessage(
		context.Background(), nil, SendMessageInput{
			MessageID: workerTestMessageID,
			Message:   "status update",
		},
	)
	if err == nil || err.Error() != "delegation messageId "+workerTestMessageID+" is bound to different arguments; reuse the original complete arguments for this logical message, or use a new lowercase UUID only for a genuinely new message" ||
		strings.Contains(err.Error(), brokerDetail) {
		t.Fatalf("worker send conflict error = %v", err)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func mailboxResult(message string) protocol.WaitMailboxResult {
	return protocol.WaitMailboxResult{
		Messages: []protocol.MailboxMessage{{
			MessageID: "123e4567-e89b-42d3-a456-426614174707",
			Sequence:  5,
			Source: control.PrincipalIdentity{
				ControllerID: workerTestControllerID,
				TreeID:       workerTestTreeID,
				AgentID:      workerTestParentID,
				DeviceID:     workerTestDeviceID,
			},
			Message: message, CreatedAt: 1,
		}},
		NextCursor: 5,
	}
}

func workerTestPrincipal() control.PrincipalIdentity {
	return control.PrincipalIdentity{
		ControllerID:  workerTestControllerID,
		TreeID:        workerTestTreeID,
		AgentID:       workerTestAgentID,
		ParentAgentID: workerTestParentID,
		DeviceID:      workerTestDeviceID,
	}
}
