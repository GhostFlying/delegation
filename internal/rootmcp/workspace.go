package rootmcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	workspaceCallTimeout        = 30*time.Minute + 10*time.Second
	maximumWorkspaceOutputBytes = 16 * 1024
)

type SyncWorkspaceInput struct {
	SyncID         string `json:"sync_id" jsonschema:"fresh UUID used to retry this exact workspace synchronization safely"`
	TargetDeviceID string `json:"target_device_id" jsonschema:"target peer UUID returned by list_devices"`
	GitURL         string `json:"git_url" jsonschema:"explicit SSH or HTTPS Git remote URL available to the target when possible"`
}

type SyncWorkspaceOutput struct {
	WorkspaceID      string                           `json:"workspace_id,omitempty"`
	SourceDeviceID   string                           `json:"source_device_id"`
	TargetDeviceID   string                           `json:"target_device_id"`
	HeadOID          string                           `json:"head_oid,omitempty"`
	ObjectFormat     string                           `json:"object_format,omitempty"`
	WorkingDirectory string                           `json:"working_directory,omitempty"`
	Strategy         protocol.WorkspaceStrategy       `json:"strategy,omitempty"`
	Outcome          protocol.WorkspacePrepareOutcome `json:"outcome"`
	Warnings         []string                         `json:"warnings"`
}

func (r *Root) syncWorkspace(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input SyncWorkspaceInput,
) (*mcp.CallToolResult, SyncWorkspaceOutput, error) {
	metadata, err := toolMetadata(request, true)
	if err != nil {
		return nil, SyncWorkspaceOutput{}, err
	}
	params := protocol.SyncWorkspaceParams{
		SyncID: input.SyncID, TargetDeviceID: input.TargetDeviceID,
		GitURL: input.GitURL, SourcePath: metadata.CWD,
	}
	if err := params.Validate(); err != nil {
		return nil, SyncWorkspaceOutput{}, err
	}
	tree, principal, err := r.ensureRoot(ctx, metadata.ThreadID)
	if err != nil {
		return nil, SyncWorkspaceOutput{}, err
	}
	callContext, cancel := context.WithTimeout(ctx, workspaceCallTimeout)
	defer cancel()
	source := principal.Identity()
	var result protocol.SyncWorkspaceResult
	if err := r.backend.Call(
		callContext, protocol.MethodSyncWorkspace, tree.TreeID, &source, params, &result,
	); err != nil {
		return nil, SyncWorkspaceOutput{}, explainAgentError(err)
	}
	if err := result.Validate(); err != nil {
		return nil, SyncWorkspaceOutput{}, errors.New("delegation service returned an invalid workspace")
	}
	output := SyncWorkspaceOutput{
		SourceDeviceID: principal.DeviceID, TargetDeviceID: input.TargetDeviceID,
		Outcome: result.Outcome, Warnings: append([]string(nil), result.Warnings...),
	}
	if result.Workspace != nil {
		workspace := result.Workspace
		if workspace.SourceDeviceID != principal.DeviceID || workspace.TargetDeviceID != input.TargetDeviceID ||
			workspace.WorkspaceID != input.SyncID {
			return nil, SyncWorkspaceOutput{}, errors.New("delegation service returned a mismatched workspace")
		}
		output.WorkspaceID = workspace.WorkspaceID
		output.HeadOID = workspace.HeadOID
		output.ObjectFormat = workspace.ObjectFormat
		output.WorkingDirectory = workspace.WorkingDirectory
		output.Strategy = workspace.Strategy
		output.Warnings = append([]string(nil), workspace.Warnings...)
	}
	if err := enforceOutputLimit(output, maximumWorkspaceOutputBytes); err != nil {
		return nil, SyncWorkspaceOutput{}, err
	}
	return nil, output, nil
}

func workspaceInputSchema() (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[SyncWorkspaceInput](nil)
	if err != nil {
		return nil, fmt.Errorf("build sync_workspace input schema: %w", err)
	}
	for _, name := range []string{"sync_id", "target_device_id"} {
		property, found := schema.Properties[name]
		if !found {
			return nil, fmt.Errorf("sync_workspace input schema is missing %s", name)
		}
		property.MinLength = jsonschema.Ptr(36)
		property.MaxLength = jsonschema.Ptr(36)
		property.Pattern = uuidPattern
	}
	gitURL, found := schema.Properties["git_url"]
	if !found {
		return nil, errors.New("sync_workspace input schema is missing git_url")
	}
	gitURL.MinLength = jsonschema.Ptr(1)
	gitURL.MaxLength = jsonschema.Ptr(protocol.MaximumGitURLBytes)
	return schema, nil
}
