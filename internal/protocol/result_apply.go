package protocol

import (
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

// AuthorizeResultApplyParams binds one root-local apply attempt to immutable
// broker authority. SourcePathSHA256 must be computed from trusted Codex cwd
// metadata; the raw local path must never cross the broker protocol.
type AuthorizeResultApplyParams struct {
	ApplyID          string `json:"applyId"`
	PackageID        string `json:"packageId"`
	SourcePathSHA256 string `json:"sourcePathSha256"`
	GitURL           string `json:"gitUrl"`
}

func (p AuthorizeResultApplyParams) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "applyId", value: p.ApplyID},
		{name: "packageId", value: p.PackageID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if !sha256DigestPattern.MatchString(p.SourcePathSHA256) {
		return errors.New("sourcePathSha256 must be a lowercase SHA-256 digest")
	}
	return validateWorkspaceText("gitUrl", p.GitURL, MaximumGitURLBytes)
}

func SameAuthorizeResultApplyParams(left, right AuthorizeResultApplyParams) bool {
	return left == right
}

// AuthorizeResultApplyResult deliberately contains only immutable identifiers
// and digests. Local paths, manifest bytes, Git URLs, rollouts, and payload
// locations remain confined to the root peer.
type AuthorizeResultApplyResult struct {
	ApplyID          string `json:"applyId"`
	PackageID        string `json:"packageId"`
	ManifestSHA256   string `json:"manifestSha256"`
	WorkspaceID      string `json:"workspaceId"`
	BaseManifestHash string `json:"baseManifestHash"`
}

func (r AuthorizeResultApplyResult) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "applyId", value: r.ApplyID},
		{name: "packageId", value: r.PackageID},
		{name: "workspaceId", value: r.WorkspaceID},
	} {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if !sha256DigestPattern.MatchString(r.ManifestSHA256) {
		return errors.New("manifestSha256 must be a lowercase SHA-256 digest")
	}
	if !sha256DigestPattern.MatchString(r.BaseManifestHash) {
		return errors.New("baseManifestHash must be a lowercase SHA-256 digest")
	}
	return nil
}
