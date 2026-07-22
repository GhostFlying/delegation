package rootmcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultAgentWaitSeconds = 30
	maximumAgentWaitSeconds = 300
	maximumAgentWaitStates  = 128
	maximumAgentWaitBytes   = 16 * 1024
	// A model-visible page contains at most one 1 KiB worker message plus four
	// lifecycle records. The worst valid JSON expansion is covered by a test.
	agentWaitMessageLimit   = 1
	agentWaitActivityLimit  = 4
	minimumAgentRepollDelay = 10 * time.Millisecond
)

type WaitAgentInput struct {
	TimeoutSeconds int `json:"timeout_seconds,omitempty" jsonschema:"seconds to wait, from 1 through 300; defaults to 30"`
}

type AgentMessageOutput struct {
	MessageID      string `json:"message_id"`
	Sequence       uint64 `json:"sequence"`
	SourceAgentID  string `json:"source_agent_id"`
	SourceDeviceID string `json:"source_device_id"`
	Message        string `json:"message"`
	CreatedAt      int64  `json:"created_at"`
}

type AgentActivityOutput struct {
	AgentID        string                        `json:"agent_id"`
	TargetDeviceID string                        `json:"target_device_id"`
	TargetRevision uint64                        `json:"target_revision"`
	Phase          protocol.WorkerLifecyclePhase `json:"phase"`
	FailureCode    string                        `json:"failure_code,omitempty"`
	Sequence       uint64                        `json:"sequence"`
	ObservedAt     int64                         `json:"observed_at"`
}

type WaitAgentOutput struct {
	Messages   []AgentMessageOutput  `json:"messages"`
	Activities []AgentActivityOutput `json:"activities"`
	HasMore    bool                  `json:"has_more"`
}

type agentWaitState struct {
	gate            chan struct{}
	treeID          string
	mailboxCursor   uint64
	lifecycleCursor uint64
	users           int
	lastUsed        uint64
}

func newAgentWaitState() *agentWaitState {
	state := &agentWaitState{gate: make(chan struct{}, 1)}
	state.gate <- struct{}{}
	return state
}

func (r *Root) waitAgent(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input WaitAgentInput,
) (*mcp.CallToolResult, WaitAgentOutput, error) {
	threadID, err := threadID(request)
	if err != nil {
		return nil, WaitAgentOutput{}, err
	}
	timeoutSeconds := input.TimeoutSeconds
	if timeoutSeconds == 0 {
		timeoutSeconds = defaultAgentWaitSeconds
	}
	if timeoutSeconds < 1 || timeoutSeconds > maximumAgentWaitSeconds {
		return nil, WaitAgentOutput{}, fmt.Errorf(
			"timeout_seconds must be from 1 through %d", maximumAgentWaitSeconds,
		)
	}
	tree, principal, err := r.ensureRoot(ctx, threadID)
	if err != nil {
		return nil, WaitAgentOutput{}, err
	}
	state, releaseState, err := r.waitState(threadID)
	if err != nil {
		return nil, WaitAgentOutput{}, err
	}
	defer releaseState()
	select {
	case <-ctx.Done():
		return nil, WaitAgentOutput{}, ctx.Err()
	case <-state.gate:
	}
	defer func() { state.gate <- struct{}{} }()
	if state.treeID != tree.TreeID {
		state.treeID = tree.TreeID
		state.mailboxCursor = 0
		state.lifecycleCursor = 0
	}

	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		params := protocol.WaitAgentParams{
			MailboxCursor: state.mailboxCursor, LifecycleCursor: state.lifecycleCursor,
			TimeoutMillis: int(min(remaining, time.Duration(protocol.MaximumAgentWaitMillis)*time.Millisecond).Milliseconds()),
			MessageLimit:  agentWaitMessageLimit, ActivityLimit: agentWaitActivityLimit,
		}
		source := principal.Identity()
		var result protocol.WaitAgentResult
		if err := r.call(
			ctx, protocol.MethodWaitAgent, tree.TreeID, &source, params, &result,
		); err != nil {
			return nil, WaitAgentOutput{}, explainAgentError(err)
		}
		if err := validateWaitAgentResult(result, params, principal); err != nil {
			return nil, WaitAgentOutput{}, err
		}
		output := waitAgentOutput(result)
		if err := enforceOutputLimit(output, maximumAgentWaitBytes); err != nil {
			return nil, WaitAgentOutput{}, err
		}
		state.mailboxCursor = result.NextMailboxCursor
		state.lifecycleCursor = result.NextLifecycleCursor
		if len(output.Messages) != 0 || len(output.Activities) != 0 || time.Until(deadline) <= 0 {
			return nil, output, nil
		}
		delay := min(time.Until(deadline), minimumAgentRepollDelay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, WaitAgentOutput{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Root) waitState(threadID string) (*agentWaitState, func(), error) {
	r.waitMu.Lock()
	defer r.waitMu.Unlock()
	if state := r.waitStates[threadID]; state != nil {
		r.waitUse++
		state.users++
		state.lastUsed = r.waitUse
		return state, func() { r.releaseWaitState(state) }, nil
	}
	if len(r.waitStates) >= maximumAgentWaitStates {
		var (
			evictThread string
			evictUse    uint64
		)
		for candidateThread, candidate := range r.waitStates {
			if candidate.users != 0 || evictThread != "" && candidate.lastUsed >= evictUse {
				continue
			}
			evictThread = candidateThread
			evictUse = candidate.lastUsed
		}
		if evictThread == "" {
			return nil, nil, errors.New("delegation wait state capacity is exhausted; retry after another wait finishes")
		}
		delete(r.waitStates, evictThread)
	}
	state := newAgentWaitState()
	r.waitUse++
	state.users = 1
	state.lastUsed = r.waitUse
	r.waitStates[threadID] = state
	return state, func() { r.releaseWaitState(state) }, nil
}

func (r *Root) releaseWaitState(state *agentWaitState) {
	r.waitMu.Lock()
	defer r.waitMu.Unlock()
	state.users--
	r.waitUse++
	state.lastUsed = r.waitUse
}

func validateWaitAgentResult(
	result protocol.WaitAgentResult,
	params protocol.WaitAgentParams,
	root control.Principal,
) error {
	if len(result.Messages) > params.MessageLimit || len(result.Activities) > params.ActivityLimit {
		return errors.New("delegation service returned too much agent activity")
	}
	if result.MoreMessages && len(result.Messages) != params.MessageLimit ||
		result.MoreActivities && len(result.Activities) != params.ActivityLimit {
		return errors.New("delegation service returned invalid agent continuation state")
	}
	mailboxCursor := params.MailboxCursor
	for _, message := range result.Messages {
		if err := message.Validate(); err != nil {
			return fmt.Errorf("delegation service returned an invalid agent message: %w", err)
		}
		if message.Sequence <= mailboxCursor || message.Source.ControllerID != root.ControllerID ||
			message.Source.TreeID != root.TreeID || message.Source.ParentAgentID != root.AgentID {
			return errors.New("delegation service returned a mismatched or unordered agent message")
		}
		mailboxCursor = message.Sequence
	}
	if result.NextMailboxCursor != mailboxCursor {
		return errors.New("delegation service returned an invalid mailbox cursor")
	}
	lifecycleCursor := params.LifecycleCursor
	for _, activity := range result.Activities {
		if err := activity.Validate(); err != nil {
			return fmt.Errorf("delegation service returned invalid agent lifecycle activity: %w", err)
		}
		if activity.Sequence <= lifecycleCursor {
			return errors.New("delegation service returned unordered agent lifecycle activity")
		}
		lifecycleCursor = activity.Sequence
	}
	if result.NextLifecycleCursor != lifecycleCursor {
		return errors.New("delegation service returned an invalid lifecycle cursor")
	}
	return nil
}

func waitAgentOutput(result protocol.WaitAgentResult) WaitAgentOutput {
	output := WaitAgentOutput{
		Messages:   make([]AgentMessageOutput, 0, len(result.Messages)),
		Activities: make([]AgentActivityOutput, 0, len(result.Activities)),
		HasMore:    result.MoreMessages || result.MoreActivities,
	}
	for _, message := range result.Messages {
		output.Messages = append(output.Messages, AgentMessageOutput{
			MessageID: message.MessageID, Sequence: message.Sequence,
			SourceAgentID: message.Source.AgentID, SourceDeviceID: message.Source.DeviceID,
			Message: message.Message, CreatedAt: message.CreatedAt,
		})
	}
	for _, activity := range result.Activities {
		output.Activities = append(output.Activities, AgentActivityOutput{
			AgentID: activity.AgentID, TargetDeviceID: activity.TargetDeviceID,
			TargetRevision: activity.TargetRevision, Phase: activity.Phase,
			FailureCode: activity.FailureCode, Sequence: activity.Sequence,
			ObservedAt: activity.ObservedAt,
		})
	}
	return output
}

func agentWaitInputSchema() (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[WaitAgentInput](nil)
	if err != nil {
		return nil, fmt.Errorf("build wait_agent input schema: %w", err)
	}
	timeout, found := schema.Properties["timeout_seconds"]
	if !found {
		return nil, errors.New("wait_agent input schema is missing timeout_seconds")
	}
	timeout.Minimum = jsonschema.Ptr(1.0)
	timeout.Maximum = jsonschema.Ptr(float64(maximumAgentWaitSeconds))
	return schema, nil
}
