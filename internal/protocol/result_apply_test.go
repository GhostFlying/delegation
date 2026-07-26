package protocol

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestResultApplyAuthorizationContractIsBoundedAndExactlyComparable(t *testing.T) {
	params := AuthorizeResultApplyParams{
		ApplyID:          testResultAttemptID,
		PackageID:        testResultPackageID,
		SourcePathSHA256: strings.Repeat("a", 64),
		GitURL:           "ssh://git@example.invalid/repository.git",
	}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}
	if !SameAuthorizeResultApplyParams(params, params) {
		t.Fatal("identical result apply parameters did not compare equal")
	}
	changed := params
	changed.GitURL = "https://example.invalid/other.git"
	if SameAuthorizeResultApplyParams(params, changed) {
		t.Fatal("different result apply parameters compared equal")
	}
	result := AuthorizeResultApplyResult{
		ApplyID: params.ApplyID, PackageID: params.PackageID,
		ManifestSHA256: strings.Repeat("b", 64), WorkspaceID: testWorkspaceID,
		BaseManifestHash: strings.Repeat("c", 64),
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	wantFields := []string{
		"applyId", "baseManifestHash", "manifestSha256", "packageId", "workspaceId",
	}
	gotFields := make([]string, 0, len(fields))
	for field := range fields {
		gotFields = append(gotFields, field)
	}
	slices.Sort(gotFields)
	if !slices.Equal(gotFields, wantFields) {
		t.Fatalf("result apply response fields = %v, want %v", gotFields, wantFields)
	}

	invalid := params
	invalid.SourcePathSHA256 = strings.Repeat("A", 64)
	if err := invalid.Validate(); err == nil {
		t.Fatal("result apply request accepted a non-canonical source path digest")
	}
	invalid = params
	invalid.GitURL = strings.Repeat("x", MaximumGitURLBytes+1)
	if err := invalid.Validate(); err == nil {
		t.Fatal("result apply request accepted an oversized Git URL")
	}
}
