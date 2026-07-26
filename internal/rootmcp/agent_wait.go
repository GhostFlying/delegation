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
	// A model-visible page contains at most one 1 KiB worker message, four
	// lifecycle records, one legacy changes artifact, and one verified result
	// package handle. The worst valid JSON expansion is covered by a test.
	agentWaitMessageLimit   = 1
	agentWaitActivityLimit  = 4
	agentWaitArtifactLimit  = 1
	agentWaitResultLimit    = 1
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

type AgentArtifactPartOutput struct {
	Kind   protocol.WorkspaceArtifactKind `json:"kind"`
	Size   int64                          `json:"size"`
	SHA256 string                         `json:"sha256"`
}

type AgentArtifactOutput struct {
	ArtifactID              string                         `json:"artifact_id"`
	TurnID                  string                         `json:"turn_id"`
	WorkspaceID             string                         `json:"workspace_id"`
	Status                  protocol.ChangesArtifactStatus `json:"status"`
	SourceAgentID           string                         `json:"source_agent_id"`
	SourceDeviceID          string                         `json:"source_device_id"`
	WorkspaceSourceDeviceID string                         `json:"workspace_source_device_id"`
	WorkspaceTargetDeviceID string                         `json:"workspace_target_device_id"`
	ObjectFormat            string                         `json:"object_format"`
	BaseHeadOID             string                         `json:"base_head_oid"`
	BaseManifestHash        string                         `json:"base_manifest_hash"`
	BaseSnapshotHash        string                         `json:"base_snapshot_hash"`
	BaseClean               bool                           `json:"base_clean"`
	ResultHeadOID           string                         `json:"result_head_oid,omitempty"`
	ResultSnapshotHash      string                         `json:"result_snapshot_hash,omitempty"`
	ResultClean             bool                           `json:"result_clean"`
	Parts                   []AgentArtifactPartOutput      `json:"parts"`
	BaseWarnings            []string                       `json:"base_warnings"`
	ResultWarnings          []string                       `json:"result_warnings"`
	FailureCode             string                         `json:"failure_code,omitempty"`
	Sequence                uint64                         `json:"sequence"`
	ObservedAt              int64                          `json:"observed_at"`
}

type AgentResultPartOutput struct {
	Kind   protocol.ResultPackagePartKind `json:"kind"`
	Size   int64                          `json:"size"`
	SHA256 string                         `json:"sha256"`
}

type AgentResultTerminalOutput struct {
	Outcome     protocol.ResultTerminalOutcome `json:"outcome"`
	FailureCode string                         `json:"failure_code,omitempty"`
}

type AgentResultRolloutOutput struct {
	Status      protocol.ResultRolloutStatus `json:"status"`
	RawSize     int64                        `json:"raw_size"`
	RawSHA256   string                       `json:"raw_sha256,omitempty"`
	FailureCode string                       `json:"failure_code,omitempty"`
}

type AgentResultWorkspaceOutput struct {
	Status             protocol.ResultWorkspaceStatus `json:"status"`
	WorkspaceID        string                         `json:"workspace_id,omitempty"`
	SourceDeviceID     string                         `json:"source_device_id,omitempty"`
	TargetDeviceID     string                         `json:"target_device_id,omitempty"`
	ObjectFormat       string                         `json:"object_format,omitempty"`
	BaseHeadOID        string                         `json:"base_head_oid,omitempty"`
	BaseManifestHash   string                         `json:"base_manifest_hash,omitempty"`
	BaseSnapshotHash   string                         `json:"base_snapshot_hash,omitempty"`
	BaseClean          bool                           `json:"base_clean"`
	ResultHeadOID      string                         `json:"result_head_oid,omitempty"`
	ResultSnapshotHash string                         `json:"result_snapshot_hash,omitempty"`
	ResultClean        bool                           `json:"result_clean"`
	BaseWarnings       []string                       `json:"base_warnings"`
	ResultWarnings     []string                       `json:"result_warnings"`
	FailureCode        string                         `json:"failure_code,omitempty"`
}

type AgentResultOutput struct {
	PackageID         string                             `json:"package_id"`
	SourceAgentID     string                             `json:"source_agent_id"`
	SourceDeviceID    string                             `json:"source_device_id"`
	ManagedThreadID   string                             `json:"managed_thread_id"`
	TurnID            string                             `json:"turn_id"`
	LifecycleRevision uint64                             `json:"lifecycle_revision"`
	Terminal          AgentResultTerminalOutput          `json:"terminal"`
	CapturedAt        int64                              `json:"captured_at"`
	Rollout           AgentResultRolloutOutput           `json:"rollout"`
	Workspace         AgentResultWorkspaceOutput         `json:"workspace"`
	Parts             []AgentResultPartOutput            `json:"parts"`
	Availability      protocol.ResultPackageAvailability `json:"availability"`
	Sequence          uint64                             `json:"sequence"`
	DeliveredAt       int64                              `json:"delivered_at"`
}

type WaitAgentOutput struct {
	Messages   []AgentMessageOutput  `json:"messages"`
	Activities []AgentActivityOutput `json:"activities"`
	Artifacts  []AgentArtifactOutput `json:"artifacts"`
	Results    []AgentResultOutput   `json:"results"`
	HasMore    bool                  `json:"has_more"`
}

type agentWaitState struct {
	gate            chan struct{}
	treeID          string
	mailboxCursor   uint64
	lifecycleCursor uint64
	artifactCursor  uint64
	resultCursor    uint64
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
		state.artifactCursor = 0
		state.resultCursor = 0
	}

	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		params := protocol.WaitAgentParams{
			MailboxCursor: state.mailboxCursor, LifecycleCursor: state.lifecycleCursor,
			ArtifactCursor: state.artifactCursor, ResultCursor: state.resultCursor,
			TimeoutMillis: int(min(remaining, time.Duration(protocol.MaximumAgentWaitMillis)*time.Millisecond).Milliseconds()),
			MessageLimit:  agentWaitMessageLimit, ActivityLimit: agentWaitActivityLimit,
			ArtifactLimit: agentWaitArtifactLimit, ResultLimit: agentWaitResultLimit,
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
		state.artifactCursor = result.NextArtifactCursor
		state.resultCursor = result.NextResultCursor
		if len(output.Messages) != 0 || len(output.Activities) != 0 ||
			len(output.Artifacts) != 0 || len(output.Results) != 0 || time.Until(deadline) <= 0 {
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
	if len(result.Messages) > params.MessageLimit || len(result.Activities) > params.ActivityLimit ||
		len(result.Artifacts) > params.ArtifactLimit || len(result.Results) > params.ResultLimit {
		return errors.New("delegation service returned too much agent activity")
	}
	if result.MoreMessages && len(result.Messages) != params.MessageLimit ||
		result.MoreActivities && len(result.Activities) != params.ActivityLimit ||
		result.MoreArtifacts && len(result.Artifacts) != params.ArtifactLimit ||
		result.MoreResults && len(result.Results) != params.ResultLimit {
		return errors.New("delegation service returned invalid agent continuation state")
	}
	mailboxCursor := params.MailboxCursor
	for _, message := range result.Messages {
		if err := message.Validate(); err != nil {
			return errors.New("delegation service returned an invalid agent message")
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
			return errors.New("delegation service returned invalid agent lifecycle activity")
		}
		if activity.Sequence <= lifecycleCursor {
			return errors.New("delegation service returned unordered agent lifecycle activity")
		}
		lifecycleCursor = activity.Sequence
	}
	if result.NextLifecycleCursor != lifecycleCursor {
		return errors.New("delegation service returned an invalid lifecycle cursor")
	}
	artifactCursor := params.ArtifactCursor
	for _, artifact := range result.Artifacts {
		if err := artifact.Validate(); err != nil {
			return errors.New("delegation service returned an invalid changes artifact")
		}
		if artifact.Sequence <= artifactCursor || artifact.TreeID != root.TreeID ||
			artifact.SourceAgentID == root.AgentID ||
			artifact.WorkspaceSourceDeviceID != root.DeviceID {
			return errors.New("delegation service returned a mismatched or unordered changes artifact")
		}
		artifactCursor = artifact.Sequence
	}
	if result.NextArtifactCursor != artifactCursor {
		return errors.New("delegation service returned an invalid artifact cursor")
	}
	resultCursor := params.ResultCursor
	for _, handle := range result.Results {
		if err := handle.Validate(); err != nil {
			return errors.New("delegation service returned an invalid result package")
		}
		manifest := handle.Manifest
		if handle.Sequence <= resultCursor || manifest.ControllerID != root.ControllerID ||
			manifest.TreeID != root.TreeID || manifest.SourceAgentID == root.AgentID ||
			handle.Availability == protocol.ResultPackageUnverified ||
			(manifest.Workspace.Status != protocol.ResultWorkspaceNotManaged &&
				manifest.Workspace.SourceDeviceID != root.DeviceID) {
			return errors.New("delegation service returned a mismatched, unavailable, or unordered result package")
		}
		resultCursor = handle.Sequence
	}
	if result.NextResultCursor != resultCursor {
		return errors.New("delegation service returned an invalid result package cursor")
	}
	return nil
}

func waitAgentOutput(result protocol.WaitAgentResult) WaitAgentOutput {
	output := WaitAgentOutput{
		Messages:   make([]AgentMessageOutput, 0, len(result.Messages)),
		Activities: make([]AgentActivityOutput, 0, len(result.Activities)),
		Artifacts:  make([]AgentArtifactOutput, 0, len(result.Artifacts)),
		Results:    make([]AgentResultOutput, 0, len(result.Results)),
		HasMore: result.MoreMessages || result.MoreActivities || result.MoreArtifacts ||
			result.MoreResults,
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
	for _, artifact := range result.Artifacts {
		parts := make([]AgentArtifactPartOutput, 0, len(artifact.Parts))
		for _, part := range artifact.Parts {
			parts = append(parts, AgentArtifactPartOutput{
				Kind: part.Kind, Size: part.Size, SHA256: part.SHA256,
			})
		}
		output.Artifacts = append(output.Artifacts, AgentArtifactOutput{
			ArtifactID: artifact.ArtifactID, TurnID: artifact.TurnID,
			WorkspaceID: artifact.WorkspaceID, Status: artifact.Status,
			SourceAgentID: artifact.SourceAgentID, SourceDeviceID: artifact.SourceDeviceID,
			WorkspaceSourceDeviceID: artifact.WorkspaceSourceDeviceID,
			WorkspaceTargetDeviceID: artifact.WorkspaceTargetDeviceID,
			ObjectFormat:            artifact.ObjectFormat, BaseHeadOID: artifact.BaseHeadOID,
			BaseManifestHash: artifact.BaseManifestHash,
			BaseSnapshotHash: artifact.BaseSnapshotHash, BaseClean: artifact.BaseClean,
			ResultHeadOID:      artifact.ResultHeadOID,
			ResultSnapshotHash: artifact.ResultSnapshotHash, ResultClean: artifact.ResultClean,
			Parts:          parts,
			BaseWarnings:   append([]string{}, artifact.BaseWarnings...),
			ResultWarnings: append([]string{}, artifact.ResultWarnings...),
			FailureCode:    artifact.FailureCode, Sequence: artifact.Sequence,
			ObservedAt: artifact.ObservedAt,
		})
	}
	for _, handle := range result.Results {
		parts := make([]AgentResultPartOutput, 0, len(handle.Manifest.Parts))
		for _, part := range handle.Manifest.Parts {
			parts = append(parts, AgentResultPartOutput{
				Kind: part.Kind, Size: part.Size, SHA256: part.SHA256,
			})
		}
		manifest := handle.Manifest
		workspace := manifest.Workspace
		output.Results = append(output.Results, AgentResultOutput{
			PackageID: manifest.PackageID, SourceAgentID: manifest.SourceAgentID,
			SourceDeviceID: manifest.SourceDeviceID, ManagedThreadID: manifest.ManagedThreadID,
			TurnID: manifest.TurnID, LifecycleRevision: manifest.LifecycleRevision,
			Terminal: AgentResultTerminalOutput{
				Outcome: manifest.Terminal.Outcome, FailureCode: manifest.Terminal.FailureCode,
			},
			CapturedAt: manifest.CapturedAt,
			Rollout: AgentResultRolloutOutput{
				Status: manifest.Rollout.Status, RawSize: manifest.Rollout.RawSize,
				RawSHA256: manifest.Rollout.RawSHA256, FailureCode: manifest.Rollout.FailureCode,
			},
			Workspace: AgentResultWorkspaceOutput{
				Status: workspace.Status, WorkspaceID: workspace.WorkspaceID,
				SourceDeviceID: workspace.SourceDeviceID, TargetDeviceID: workspace.TargetDeviceID,
				ObjectFormat: workspace.ObjectFormat, BaseHeadOID: workspace.BaseHeadOID,
				BaseManifestHash: workspace.BaseManifestHash,
				BaseSnapshotHash: workspace.BaseSnapshotHash, BaseClean: workspace.BaseClean,
				ResultHeadOID:      workspace.ResultHeadOID,
				ResultSnapshotHash: workspace.ResultSnapshotHash, ResultClean: workspace.ResultClean,
				BaseWarnings:   append([]string{}, workspace.BaseWarnings...),
				ResultWarnings: append([]string{}, workspace.ResultWarnings...),
				FailureCode:    workspace.FailureCode,
			},
			Parts: parts, Availability: handle.Availability,
			Sequence: handle.Sequence, DeliveredAt: handle.DeliveredAt,
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
