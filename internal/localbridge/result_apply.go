package localbridge

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const MethodApplyAgentChanges = "artifact.result.apply.local"

var (
	ErrApplyPackageEvicted     = errors.New("result package evicted")
	ErrApplyPackageUnavailable = errors.New("result package unavailable")
	ErrApplyRequestConflict    = errors.New("result apply request conflict")
	ErrApplyRecoveryRequired   = errors.New("root workspace apply recovery required")
	ErrApplyBacklog            = errors.New("root workspace apply backlog is full")
)

type ApplyAgentChangesParams struct {
	ApplyID    string `json:"applyId"`
	PackageID  string `json:"packageId"`
	SourcePath string `json:"sourcePath"`
}

func (p ApplyAgentChangesParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "applyId", value: p.ApplyID},
		{name: "packageId", value: p.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if !filepath.IsAbs(p.SourcePath) || filepath.Clean(p.SourcePath) != p.SourcePath ||
		len(p.SourcePath) > protocol.MaximumSourcePathBytes || strings.ContainsRune(p.SourcePath, '\x00') {
		return errors.New("sourcePath must be a normalized absolute local path")
	}
	return nil
}

type ApplyAgentChangesOutcome string

const (
	ApplyAgentChangesApplied         ApplyAgentChangesOutcome = "applied"
	ApplyAgentChangesUnchanged       ApplyAgentChangesOutcome = "unchanged"
	ApplyAgentChangesNeedsResolution ApplyAgentChangesOutcome = "needsResolution"
)

type ApplyAgentChangesResult struct {
	ApplyID     string                   `json:"applyId"`
	PackageID   string                   `json:"packageId"`
	Outcome     ApplyAgentChangesOutcome `json:"outcome"`
	FailureCode string                   `json:"failureCode"`
}

func (r ApplyAgentChangesResult) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "applyId", value: r.ApplyID},
		{name: "packageId", value: r.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	switch r.Outcome {
	case ApplyAgentChangesApplied, ApplyAgentChangesUnchanged:
		if r.FailureCode != "" {
			return errors.New("successful result apply must not contain failureCode")
		}
	case ApplyAgentChangesNeedsResolution:
		if err := protocol.ValidateFailureCode(r.FailureCode); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported result apply outcome %q", r.Outcome)
	}
	return nil
}

type ResultApplyRequest struct {
	Root   control.PrincipalIdentity
	Params ApplyAgentChangesParams
}

type ResultApplyPreparation struct {
	Completed     *ApplyAgentChangesResult
	Authorization *protocol.AuthorizeResultApplyParams
}

func (p ResultApplyPreparation) Validate(request ResultApplyRequest) error {
	if (p.Completed == nil) == (p.Authorization == nil) {
		return errors.New("result apply preparation must be completed or require authorization")
	}
	if p.Completed != nil {
		if err := p.Completed.Validate(); err != nil {
			return err
		}
		if p.Completed.ApplyID != request.Params.ApplyID ||
			p.Completed.PackageID != request.Params.PackageID {
			return errors.New("completed result apply differs from its request")
		}
		return nil
	}
	if err := p.Authorization.Validate(); err != nil {
		return err
	}
	if p.Authorization.ApplyID != request.Params.ApplyID ||
		p.Authorization.PackageID != request.Params.PackageID {
		return errors.New("result apply authorization differs from its request")
	}
	return nil
}

type ResultApplyProvider interface {
	PrepareResultApply(context.Context, ResultApplyRequest) (ResultApplyPreparation, error)
	ApplyAuthorizedResult(
		context.Context,
		ResultApplyRequest,
		protocol.AuthorizeResultApplyResult,
	) (ApplyAgentChangesResult, error)
}
