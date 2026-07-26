package rootmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	rootMCPArtifactID          = "123e4567-e89b-42d3-a456-426614174409"
	rootMCPTurnID              = "123e4567-e89b-42d3-a456-426614174410"
	rootMCPArtifactWorkspaceID = "123e4567-e89b-42d3-a456-426614174411"
	rootMCPOtherTreeID         = "123e4567-e89b-42d3-a456-426614174412"
	rootMCPResultPackageID     = "123e4567-e89b-42d3-a456-426614174413"
	rootMCPManagedThreadID     = "123e4567-e89b-42d3-a456-426614174414"
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
			Artifacts: []protocol.ChangesArtifactMetadata{}, Results: []protocol.ResultPackageHandle{},
			NextMailboxCursor: input.MailboxCursor, NextLifecycleCursor: input.LifecycleCursor,
			NextArtifactCursor: input.ArtifactCursor, NextResultCursor: input.ResultCursor,
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
			Artifacts: []protocol.ChangesArtifactMetadata{}, Results: []protocol.ResultPackageHandle{},
			NextArtifactCursor: input.ArtifactCursor, NextResultCursor: input.ResultCursor,
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
	handle := rootMCPResultHandle(protocol.ResultPackageAvailable)
	backend := &fakeRootBackend{waitResults: []protocol.WaitAgentResult{
		{
			Messages: []protocol.MailboxMessage{{
				MessageID: rootMCPMessageID, Sequence: 1, Source: worker,
				Message: "worker result", CreatedAt: 10,
			}},
			Activities: []protocol.AgentLifecycleActivity{},
			Artifacts: []protocol.ChangesArtifactMetadata{
				rootMCPChangesArtifact(protocol.ChangesArtifactAvailable),
			},
			Results:             []protocol.ResultPackageHandle{handle},
			NextMailboxCursor:   1,
			NextLifecycleCursor: 0,
			NextArtifactCursor:  1,
			NextResultCursor:    handle.Sequence,
			MoreMessages:        true,
			MoreResults:         true,
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
			Artifacts:           []protocol.ChangesArtifactMetadata{},
			NextArtifactCursor:  1,
			Results:             []protocol.ResultPackageHandle{},
			NextResultCursor:    handle.Sequence,
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
		len(firstOutput.Artifacts) != 1 || len(firstOutput.Results) != 1 ||
		firstOutput.Messages[0].SourceAgentID != rootMCPWorkerID ||
		firstOutput.Artifacts[0].ArtifactID != rootMCPArtifactID ||
		firstOutput.Results[0].PackageID != rootMCPResultPackageID ||
		firstOutput.Results[0].Availability != protocol.ResultPackageAvailable ||
		firstOutput.Results[0].ManagedThreadID != rootMCPManagedThreadID ||
		firstOutput.Results[0].TurnID != rootMCPTurnID ||
		firstOutput.Messages[0].Message != "worker result" || !firstOutput.HasMore {
		t.Fatalf("first wait_agent output = %#v", firstOutput)
	}
	encodedResult, err := json.Marshal(firstOutput.Results[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"controller_id", "tree_id", "manifest", "rawSha256", "workspaceId"} {
		if strings.Contains(string(encodedResult), forbidden) {
			t.Fatalf("root result handle leaked %q routing data: %s", forbidden, encodedResult)
		}
	}
	for _, required := range []string{"raw_sha256", "base_warnings", "result_warnings"} {
		if !strings.Contains(string(encodedResult), required) {
			t.Fatalf("root result handle omitted %q summary data: %s", required, encodedResult)
		}
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
		len(secondOutput.Artifacts) != 0 || len(secondOutput.Results) != 0 ||
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
		waits[0].ArtifactCursor != 0 || waits[0].ResultCursor != 0 || waits[1].MailboxCursor != 1 ||
		waits[1].LifecycleCursor != 0 || waits[1].ArtifactCursor != 1 ||
		waits[1].ResultCursor != handle.Sequence ||
		waits[0].MessageLimit != agentWaitMessageLimit ||
		waits[0].ActivityLimit != agentWaitActivityLimit ||
		waits[0].ArtifactLimit != agentWaitArtifactLimit ||
		waits[0].ResultLimit != agentWaitResultLimit {
		t.Fatalf("wait_agent broker params = %#v", waits)
	}
}

func TestWaitAgentMapsChangesArtifactShapesAndContinuation(t *testing.T) {
	tests := []struct {
		name    string
		status  protocol.ChangesArtifactStatus
		hasMore bool
	}{
		{name: "available", status: protocol.ChangesArtifactAvailable, hasMore: true},
		{name: "dirty unchanged", status: protocol.ChangesArtifactUnchanged},
		{name: "capture failed", status: protocol.ChangesArtifactCaptureFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := rootMCPChangesArtifact(test.status)
			backend := &fakeRootBackend{waitResults: []protocol.WaitAgentResult{{
				Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
				Artifacts:          []protocol.ChangesArtifactMetadata{artifact},
				NextArtifactCursor: artifact.Sequence, MoreArtifacts: test.hasMore,
			}}}
			ctx, clientSession, closeSessions := connectRootMCP(t, backend)
			defer closeSessions()

			result := callTool(t, ctx, clientSession, ToolWaitAgent, rootMCPThreadID, map[string]any{
				"timeout_seconds": 1,
			})
			if result.IsError {
				t.Fatalf("wait_agent result = %#v", result)
			}
			var output WaitAgentOutput
			decodeStructured(t, result.StructuredContent, &output)
			if len(output.Messages) != 0 || len(output.Activities) != 0 ||
				len(output.Artifacts) != 1 || output.HasMore != test.hasMore {
				t.Fatalf("wait_agent output = %#v", output)
			}
			want := agentArtifactOutput(artifact)
			if !reflect.DeepEqual(output.Artifacts[0], want) {
				t.Fatalf("artifact output = %#v, want %#v", output.Artifacts[0], want)
			}
			encoded, err := json.Marshal(output.Artifacts[0])
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "tree_id") {
				t.Fatalf("root output leaked broker routing metadata: %s", encoded)
			}
			switch test.status {
			case protocol.ChangesArtifactAvailable:
				if len(output.Artifacts[0].Parts) != 2 || output.Artifacts[0].ResultClean {
					t.Fatalf("available artifact shape = %#v", output.Artifacts[0])
				}
			case protocol.ChangesArtifactUnchanged:
				if output.Artifacts[0].BaseClean || output.Artifacts[0].ResultClean ||
					len(output.Artifacts[0].Parts) != 0 || output.Artifacts[0].FailureCode != "" {
					t.Fatalf("dirty unchanged artifact shape = %#v", output.Artifacts[0])
				}
			case protocol.ChangesArtifactCaptureFailed:
				if output.Artifacts[0].ResultHeadOID != "" ||
					output.Artifacts[0].ResultSnapshotHash != "" || output.Artifacts[0].ResultClean ||
					len(output.Artifacts[0].Parts) != 0 ||
					output.Artifacts[0].FailureCode != "changes_capture_failed" {
					t.Fatalf("failed artifact shape = %#v", output.Artifacts[0])
				}
			}
		})
	}
}

func TestWaitAgentRejectsMismatchedArtifactIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ChangesArtifactMetadata)
	}{
		{name: "tree", mutate: func(artifact *protocol.ChangesArtifactMetadata) {
			artifact.TreeID = rootMCPOtherTreeID
		}},
		{name: "invalid source agent", mutate: func(artifact *protocol.ChangesArtifactMetadata) {
			artifact.SourceAgentID = "not-an-id"
		}},
		{name: "invalid source device", mutate: func(artifact *protocol.ChangesArtifactMetadata) {
			artifact.SourceDeviceID = "not-an-id"
		}},
		{name: "wrong workspace source device", mutate: func(artifact *protocol.ChangesArtifactMetadata) {
			artifact.WorkspaceSourceDeviceID = rootMCPOtherTreeID
		}},
		{name: "root as source", mutate: func(artifact *protocol.ChangesArtifactMetadata) {
			artifact.SourceAgentID = rootMCPAgentID
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := rootMCPChangesArtifact(protocol.ChangesArtifactAvailable)
			test.mutate(&artifact)
			result := protocol.WaitAgentResult{
				Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
				Artifacts: []protocol.ChangesArtifactMetadata{artifact}, NextArtifactCursor: 1,
			}
			if err := validateWaitAgentResult(result, protocol.WaitAgentParams{
				MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
			}, rootResult(rootMCPThreadID).Principal); err == nil {
				t.Fatalf("mismatched artifact was accepted: %#v", artifact)
			}
		})
	}
}

func TestWaitAgentBoundsInvalidArtifactValidationErrors(t *testing.T) {
	marker := strings.Repeat("untrusted-artifact-status", 8*1024)
	artifact := rootMCPChangesArtifact(protocol.ChangesArtifactAvailable)
	artifact.Status = protocol.ChangesArtifactStatus(marker)
	result := protocol.WaitAgentResult{
		Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
		Artifacts: []protocol.ChangesArtifactMetadata{artifact}, NextArtifactCursor: 1,
	}
	err := validateWaitAgentResult(result, protocol.WaitAgentParams{
		MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
	}, rootResult(rootMCPThreadID).Principal)
	if err == nil || err.Error() != "delegation service returned an invalid changes artifact" {
		t.Fatalf("invalid artifact error = %v", err)
	}
	if strings.Contains(err.Error(), "untrusted-artifact-status") || len(err.Error()) > 128 {
		t.Fatalf("invalid artifact error reflected backend input: %q", err)
	}
}

func TestWaitAgentRejectsUnverifiedOrMismatchedResultPackages(t *testing.T) {
	root := rootResult(rootMCPThreadID).Principal
	tests := []struct {
		name         string
		cursor       uint64
		mutateHandle func(*protocol.ResultPackageHandle)
	}{
		{name: "unverified", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Availability = protocol.ResultPackageUnverified
		}},
		{name: "controller", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Manifest.ControllerID = rootMCPOtherTreeID
		}},
		{name: "tree", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Manifest.TreeID = rootMCPOtherTreeID
		}},
		{name: "root as source", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Manifest.SourceAgentID = rootMCPAgentID
		}},
		{name: "wrong workspace source", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Manifest = rootMCPManagedResultHandle(protocol.ResultPackageAvailable).Manifest
			handle.Manifest.Workspace.SourceDeviceID = rootMCPOtherTreeID
		}},
		{name: "invalid manifest", mutateHandle: func(handle *protocol.ResultPackageHandle) {
			handle.Manifest.PackageID = "not-an-id"
		}},
		{name: "unordered", cursor: 1, mutateHandle: func(*protocol.ResultPackageHandle) {}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handle := rootMCPResultHandle(protocol.ResultPackageAvailable)
			test.mutateHandle(&handle)
			result := protocol.WaitAgentResult{
				Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
				Artifacts: []protocol.ChangesArtifactMetadata{}, Results: []protocol.ResultPackageHandle{handle},
				NextResultCursor: handle.Sequence,
			}
			err := validateWaitAgentResult(result, protocol.WaitAgentParams{
				ResultCursor: test.cursor, MessageLimit: 1, ActivityLimit: 1,
				ArtifactLimit: 1, ResultLimit: 1,
			}, root)
			if err == nil {
				t.Fatalf("mismatched result package was accepted: %#v", handle)
			}
		})
	}

	evicted := rootMCPResultHandle(protocol.ResultPackageEvicted)
	if err := validateWaitAgentResult(protocol.WaitAgentResult{
		Messages: []protocol.MailboxMessage{}, Activities: []protocol.AgentLifecycleActivity{},
		Artifacts: []protocol.ChangesArtifactMetadata{}, Results: []protocol.ResultPackageHandle{evicted},
		NextResultCursor: evicted.Sequence,
	}, protocol.WaitAgentParams{
		MessageLimit: 1, ActivityLimit: 1, ArtifactLimit: 1, ResultLimit: 1,
	}, root); err != nil {
		t.Fatalf("evicted result package was rejected: %v", err)
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
		Artifacts:  []protocol.ChangesArtifactMetadata{rootMCPChangesArtifact(protocol.ChangesArtifactAvailable)},
		Results:    []protocol.ResultPackageHandle{rootMCPManagedResultHandle(protocol.ResultPackageAvailable)},
	}
	for range agentWaitMessageLimit {
		result.Messages = append(result.Messages, protocol.MailboxMessage{
			MessageID: rootMCPMessageID, Sequence: uint64(math.MaxInt64), Source: worker,
			Message:   strings.Repeat("\x01", protocol.MaximumMailboxMessageBytes),
			CreatedAt: math.MaxInt64,
		})
	}
	for index := range agentWaitActivityLimit {
		result.Activities = append(result.Activities, protocol.AgentLifecycleActivity{
			AgentID: rootMCPWorkerID, TargetDeviceID: rootMCPDeviceID,
			TargetRevision: math.MaxInt64, Phase: protocol.WorkerLifecycleFailed,
			FailureCode: strings.Repeat("a", protocol.MaximumFailureCodeBytes),
			Sequence:    uint64(math.MaxInt64 - agentWaitActivityLimit + 1 + index),
			ObservedAt:  math.MaxInt64,
		})
	}
	result.NextMailboxCursor = result.Messages[len(result.Messages)-1].Sequence
	result.NextLifecycleCursor = result.Activities[len(result.Activities)-1].Sequence
	warnings := make([]string, 0, protocol.MaximumWorkspaceWarnings)
	for index := range protocol.MaximumWorkspaceWarnings {
		warnings = append(warnings, fmt.Sprintf("warning_%02d_%s", index, strings.Repeat("x", 53)))
	}
	result.Artifacts[0].BaseWarnings = warnings
	result.Artifacts[0].ResultWarnings = append([]string{}, warnings...)
	result.Artifacts[0].ObjectFormat = "sha256"
	result.Artifacts[0].BaseHeadOID = strings.Repeat("a", 64)
	result.Artifacts[0].BaseClean = false
	result.Artifacts[0].ResultHeadOID = strings.Repeat("d", 64)
	result.Artifacts[0].Sequence = math.MaxInt64
	result.Artifacts[0].ObservedAt = math.MaxInt64
	result.NextArtifactCursor = math.MaxInt64
	result.Artifacts[0].Parts = []protocol.WorkspaceArtifactDescriptor{
		{
			Kind: protocol.WorkspaceArtifactBundle, Size: protocol.MaximumWorkspaceArtifactBytes,
			SHA256: strings.Repeat("f", 64),
		},
		{
			Kind: protocol.WorkspaceArtifactOverlay, Size: protocol.MaximumWorkspaceArtifactBytes,
			SHA256: strings.Repeat("9", 64),
		},
	}
	result.Results[0].Manifest.LifecycleRevision = math.MaxInt64
	result.Results[0].Manifest.CapturedAt = math.MaxInt64
	result.Results[0].Manifest.Rollout.RawSize = protocol.MaximumResultRolloutRawBytes
	result.Results[0].Manifest.Workspace.ObjectFormat = "sha256"
	result.Results[0].Manifest.Workspace.BaseHeadOID = strings.Repeat("1", 64)
	result.Results[0].Manifest.Workspace.ResultHeadOID = strings.Repeat("4", 64)
	result.Results[0].Manifest.Workspace.BaseWarnings = append([]string{}, warnings...)
	result.Results[0].Manifest.Workspace.ResultWarnings = append([]string{}, warnings...)
	result.Results[0].Manifest.Parts[0].Size = protocol.MaximumResultChangesBundleBytes
	result.Results[0].Manifest.Parts[1].Size = protocol.MaximumResultRolloutBytes
	result.Results[0].Sequence = math.MaxInt64
	result.Results[0].DeliveredAt = math.MaxInt64
	result.NextResultCursor = math.MaxInt64
	if err := validateWaitAgentResult(result, protocol.WaitAgentParams{
		MessageLimit: agentWaitMessageLimit, ActivityLimit: agentWaitActivityLimit,
		ArtifactLimit: agentWaitArtifactLimit, ResultLimit: agentWaitResultLimit,
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

func rootMCPChangesArtifact(status protocol.ChangesArtifactStatus) protocol.ChangesArtifactMetadata {
	artifact := protocol.ChangesArtifactMetadata{
		TreeID: rootMCPTreeID, ArtifactID: rootMCPArtifactID, TurnID: rootMCPTurnID,
		WorkspaceID: rootMCPArtifactWorkspaceID, Status: status,
		SourceAgentID: rootMCPWorkerID, SourceDeviceID: rootMCPDeviceID,
		WorkspaceSourceDeviceID: rootMCPDeviceID,
		WorkspaceTargetDeviceID: rootMCPDeviceID,
		ObjectFormat:            "sha1", BaseHeadOID: strings.Repeat("a", 40),
		BaseManifestHash: strings.Repeat("b", 64), BaseSnapshotHash: strings.Repeat("c", 64),
		BaseClean: true, Sequence: 1, ObservedAt: 11,
		BaseWarnings: []string{}, ResultWarnings: []string{},
	}
	switch status {
	case protocol.ChangesArtifactAvailable:
		artifact.ResultHeadOID = strings.Repeat("d", 40)
		artifact.ResultSnapshotHash = strings.Repeat("e", 64)
		artifact.Parts = []protocol.WorkspaceArtifactDescriptor{
			{Kind: protocol.WorkspaceArtifactBundle, Size: 32, SHA256: strings.Repeat("f", 64)},
			{Kind: protocol.WorkspaceArtifactOverlay, Size: 48, SHA256: strings.Repeat("9", 64)},
		}
		artifact.BaseWarnings = []string{protocol.WorkspaceWarningFullHistoryFallback}
		artifact.ResultWarnings = []string{
			protocol.WorkspaceWarningLFSPayloadNotTransferred,
			protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred,
		}
	case protocol.ChangesArtifactUnchanged:
		artifact.BaseClean = false
		artifact.ResultHeadOID = artifact.BaseHeadOID
		artifact.ResultSnapshotHash = artifact.BaseSnapshotHash
		artifact.Parts = []protocol.WorkspaceArtifactDescriptor{}
	case protocol.ChangesArtifactCaptureFailed:
		artifact.Parts = []protocol.WorkspaceArtifactDescriptor{}
		artifact.BaseWarnings = []string{protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred}
		artifact.FailureCode = "changes_capture_failed"
	}
	return artifact
}

func rootMCPResultHandle(availability protocol.ResultPackageAvailability) protocol.ResultPackageHandle {
	return protocol.ResultPackageHandle{
		Manifest: protocol.ResultManifest{
			Version: protocol.ResultManifestVersion, PackageID: rootMCPResultPackageID,
			ControllerID: rootMCPControllerID, TreeID: rootMCPTreeID,
			SourceAgentID: rootMCPWorkerID, SourceDeviceID: rootMCPDeviceID,
			ManagedThreadID: rootMCPManagedThreadID, TurnID: rootMCPTurnID,
			LifecycleRevision: 2,
			Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
			CapturedAt:        12,
			Rollout: protocol.ResultRolloutComponent{
				Status: protocol.ResultRolloutAvailable, RawSize: 42,
				RawSHA256: strings.Repeat("a", 64),
			},
			Workspace: protocol.ResultWorkspaceComponent{
				Status:       protocol.ResultWorkspaceNotManaged,
				BaseWarnings: []string{}, ResultWarnings: []string{},
			},
			Parts: []protocol.ResultPackagePartDescriptor{{
				Kind: protocol.ResultPackagePartRollout, Size: 21,
				SHA256: strings.Repeat("b", 64),
			}},
		},
		Availability: availability, Sequence: 1, DeliveredAt: 13,
	}
}

func rootMCPManagedResultHandle(availability protocol.ResultPackageAvailability) protocol.ResultPackageHandle {
	handle := rootMCPResultHandle(availability)
	handle.Manifest.Workspace = protocol.ResultWorkspaceComponent{
		Status: protocol.ResultWorkspaceChanged, WorkspaceID: rootMCPArtifactWorkspaceID,
		SourceDeviceID: rootMCPDeviceID, TargetDeviceID: rootMCPDeviceID,
		ObjectFormat: "sha1", BaseHeadOID: strings.Repeat("1", 40),
		BaseManifestHash: strings.Repeat("2", 64), BaseSnapshotHash: strings.Repeat("3", 64),
		BaseClean: true, ResultHeadOID: strings.Repeat("4", 40),
		ResultSnapshotHash: strings.Repeat("5", 64), ResultClean: true,
		BaseWarnings: []string{}, ResultWarnings: []string{},
	}
	handle.Manifest.Parts = []protocol.ResultPackagePartDescriptor{
		{Kind: protocol.ResultPackagePartChangesBundle, Size: 32, SHA256: strings.Repeat("6", 64)},
		{Kind: protocol.ResultPackagePartRollout, Size: 21, SHA256: strings.Repeat("b", 64)},
	}
	return handle
}

func agentArtifactOutput(artifact protocol.ChangesArtifactMetadata) AgentArtifactOutput {
	parts := make([]AgentArtifactPartOutput, 0, len(artifact.Parts))
	for _, part := range artifact.Parts {
		parts = append(parts, AgentArtifactPartOutput{
			Kind: part.Kind, Size: part.Size, SHA256: part.SHA256,
		})
	}
	return AgentArtifactOutput{
		ArtifactID: artifact.ArtifactID, TurnID: artifact.TurnID,
		WorkspaceID: artifact.WorkspaceID, Status: artifact.Status,
		SourceAgentID: artifact.SourceAgentID, SourceDeviceID: artifact.SourceDeviceID,
		WorkspaceSourceDeviceID: artifact.WorkspaceSourceDeviceID,
		WorkspaceTargetDeviceID: artifact.WorkspaceTargetDeviceID,
		ObjectFormat:            artifact.ObjectFormat, BaseHeadOID: artifact.BaseHeadOID,
		BaseManifestHash: artifact.BaseManifestHash, BaseSnapshotHash: artifact.BaseSnapshotHash,
		BaseClean: artifact.BaseClean, ResultHeadOID: artifact.ResultHeadOID,
		ResultSnapshotHash: artifact.ResultSnapshotHash, ResultClean: artifact.ResultClean,
		Parts: parts, BaseWarnings: append([]string{}, artifact.BaseWarnings...),
		ResultWarnings: append([]string{}, artifact.ResultWarnings...),
		FailureCode:    artifact.FailureCode, Sequence: artifact.Sequence, ObservedAt: artifact.ObservedAt,
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
