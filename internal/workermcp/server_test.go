package workermcp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	workerTestControllerID = "123e4567-e89b-42d3-a456-426614174700"
	workerTestTreeID       = "123e4567-e89b-42d3-a456-426614174701"
	workerTestAgentID      = "123e4567-e89b-42d3-a456-426614174702"
	workerTestParentID     = "123e4567-e89b-42d3-a456-426614174703"
	workerTestDeviceID     = "123e4567-e89b-42d3-a456-426614174704"
)

type recordedWorkerCall struct {
	method string
	treeID string
	source control.PrincipalIdentity
	params any
}

type workerBackend struct {
	mu    sync.Mutex
	calls []recordedWorkerCall
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
	switch method {
	case protocol.MethodSendMessage:
		*result.(*protocol.SendMessageResult) = protocol.SendMessageResult{
			MessageID: "123e4567-e89b-42d3-a456-426614174705",
			Sequence:  7,
		}
	case protocol.MethodWaitMailbox:
		*result.(*protocol.WaitMailboxResult) = protocol.WaitMailboxResult{
			Messages:   []protocol.MailboxMessage{},
			NextCursor: 9,
		}
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
	want := []recordedWorkerCall{
		{
			method: protocol.MethodSendMessage,
			treeID: workerTestTreeID,
			source: principal,
			params: protocol.SendMessageParams{
				Target:  protocol.MessageTarget{Kind: protocol.MessageTargetRoot},
				Message: "status update",
			},
		},
		{
			method: protocol.MethodWaitMailbox,
			treeID: workerTestTreeID,
			source: principal,
			params: protocol.WaitMailboxParams{Cursor: 4, TimeoutMillis: 3000, Limit: 16},
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
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		Recipient: "agent",
		Message:   "invalid target",
	}); err == nil {
		t.Fatal("send_message accepted an arbitrary recipient")
	}
	if _, _, err := worker.sendMessage(context.Background(), nil, SendMessageInput{
		Message: string([]byte{0xff}),
	}); err == nil {
		t.Fatal("send_message accepted invalid UTF-8")
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

func workerTestPrincipal() control.PrincipalIdentity {
	return control.PrincipalIdentity{
		ControllerID:  workerTestControllerID,
		TreeID:        workerTestTreeID,
		AgentID:       workerTestAgentID,
		ParentAgentID: workerTestParentID,
		DeviceID:      workerTestDeviceID,
	}
}
