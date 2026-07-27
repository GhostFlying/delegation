package rootmcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	resultApplyCallTimeout = 30*time.Minute + 10*time.Second
	resultApplyApplied     = "applied"
	resultApplyUnchanged   = "unchanged"
	resultApplyResolution  = "needs_resolution"
)

type ApplyAgentChangesInput struct {
	ApplyID   string `json:"apply_id" jsonschema:"fresh UUID used to retry this exact local apply safely"`
	PackageID string `json:"package_id" jsonschema:"delivered result package UUID returned by wait_agent"`
}

type ApplyAgentChangesOutput struct {
	ApplyID     string `json:"apply_id"`
	PackageID   string `json:"package_id"`
	Outcome     string `json:"outcome"`
	FailureCode string `json:"failure_code,omitempty"`
}

func (r *Root) applyAgentChanges(
	ctx context.Context,
	request *mcp.CallToolRequest,
	input ApplyAgentChangesInput,
) (*mcp.CallToolResult, ApplyAgentChangesOutput, error) {
	metadata, err := toolMetadata(request, true)
	if err != nil {
		return nil, ApplyAgentChangesOutput{}, err
	}
	params := localbridge.ApplyAgentChangesParams{
		ApplyID: input.ApplyID, PackageID: input.PackageID, SourcePath: metadata.CWD,
	}
	if err := params.Validate(); err != nil {
		return nil, ApplyAgentChangesOutput{}, err
	}
	tree, principal, err := r.ensureRoot(ctx, metadata.ThreadID)
	if err != nil {
		return nil, ApplyAgentChangesOutput{}, err
	}
	callContext, cancel := context.WithTimeout(ctx, resultApplyCallTimeout)
	defer cancel()
	source := principal.Identity()
	var result localbridge.ApplyAgentChangesResult
	if err := r.backend.Call(
		callContext, localbridge.MethodApplyAgentChanges, tree.TreeID, &source, params, &result,
	); err != nil {
		return nil, ApplyAgentChangesOutput{}, explainResultApplyError(err)
	}
	if err := result.Validate(); err != nil || result.ApplyID != input.ApplyID ||
		result.PackageID != input.PackageID {
		return nil, ApplyAgentChangesOutput{}, errors.New("delegation service returned an invalid apply result")
	}
	outcome := resultApplyApplied
	switch result.Outcome {
	case localbridge.ApplyAgentChangesApplied:
	case localbridge.ApplyAgentChangesUnchanged:
		outcome = resultApplyUnchanged
	case localbridge.ApplyAgentChangesNeedsResolution:
		outcome = resultApplyResolution
	default:
		return nil, ApplyAgentChangesOutput{}, errors.New("delegation service returned an invalid apply result")
	}
	return nil, ApplyAgentChangesOutput{
		ApplyID: input.ApplyID, PackageID: input.PackageID,
		Outcome: outcome, FailureCode: result.FailureCode,
	}, nil
}

func resultApplyInputSchema() (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[ApplyAgentChangesInput](nil)
	if err != nil {
		return nil, fmt.Errorf("build apply_agent_changes input schema: %w", err)
	}
	for _, name := range []string{"apply_id", "package_id"} {
		property, found := schema.Properties[name]
		if !found {
			return nil, fmt.Errorf("apply_agent_changes input schema is missing %s", name)
		}
		property.MinLength = jsonschema.Ptr(36)
		property.MaxLength = jsonschema.Ptr(36)
		property.Pattern = uuidPattern
	}
	return schema, nil
}
