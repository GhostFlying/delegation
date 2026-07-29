package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var actionReferencePattern = regexp.MustCompile(`uses:\s+[^@\s]+@([^\s]+)`)

func TestReleaseCandidateWorkflowUsesProtectedNativeSigningAndProvenance(t *testing.T) {
	workflow := readWorkflow(t, "release-candidate.yml")
	for _, required := range []string{
		"runs-on: ubuntu-latest",
		"runs-on: macos-15",
		"runs-on: windows-latest",
		"environment: release-signing",
		"if: ${{ vars.DELEGATION_ENABLE_SIGNED_RELEASE == 'true' }}",
		"codesign --force --options runtime --timestamp",
		"codesign --verify --strict",
		"xcrun notarytool submit",
		"signtool verify /pa /all /v",
		"/tr $env:WINDOWS_TIMESTAMP_URL /td SHA256",
		"actions/attest@a1948c3f048ba23858d222213b7c278aabede763",
		"subject-checksums: dist/release-artifacts.sha256",
		"delegation-release-candidate-${{ github.run_id }}",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("release candidate workflow is missing %q", required)
		}
	}
	if strings.Contains(workflow, "pull_request:") || strings.Contains(workflow, "\n  push:") {
		t.Fatal("release candidate workflow must not expose signing jobs to pull requests or pushes")
	}
	if got := strings.Count(workflow, "environment: release-signing"); got != 2 {
		t.Fatalf("release-signing environment references = %d, want 2", got)
	}
	if got := strings.Count(workflow, "secrets."); got != 7 {
		t.Fatalf("candidate signing secret references = %d, want 7", got)
	}
	assertPinnedActions(t, workflow)
}

func TestReleaseWorkflowPublishesVerifiedUnsignedArtifacts(t *testing.T) {
	workflow := readWorkflow(t, "release.yml")
	for _, required := range []string{
		"RELEASE_TAG: ${{ inputs.tag }}",
		"test \"$GITHUB_REF\" = \"refs/heads/$DEFAULT_BRANCH\"",
		"test \"$RELEASE_TAG\" = \"v$version\"",
		"[[ \"$version_core\" == *-* ]]",
		"git merge-base --is-ancestor",
		"go run ./cmd/releasepack -out dist",
		"artifact-ids: ${{ needs.build.outputs.artifact_id }}",
		"source/plugins/delegation/release-artifacts.sha256",
		"sha256sum --strict -c release-artifacts.sha256",
		"environment: github-release",
		"test \"$actual_commit\" = \"$EXPECTED_COMMIT\"",
		"gh release create \"$RELEASE_TAG\"",
		"--verify-tag",
		"sha256sum --strict -c release-artifacts.sha256",
		"--notes \"$notes\"",
		"--prerelease",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("release workflow is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"candidate_run_id",
		"candidate_artifact_id",
		"verify-candidate",
		"verify-promotion",
		"actions/attest@",
		"environment: release-signing",
		"secrets.",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("unsigned prerelease workflow contains signed promotion behavior %q", forbidden)
		}
	}
	if got := strings.Count(
		workflow,
		"source/plugins/delegation/release-artifacts.sha256",
	); got != 2 {
		t.Fatalf("tracked manifest verification count = %d, want 2", got)
	}
	if got := strings.Count(
		workflow,
		"sha256sum --strict -c release-artifacts.sha256",
	); got != 3 {
		t.Fatalf("artifact checksum command reference count = %d, want 3", got)
	}
	if got := strings.Count(workflow, "contents: write"); got != 1 {
		t.Fatalf("write-scoped contents permission count = %d, want 1", got)
	}
	assertPinnedActions(t, workflow)
}

func TestOrdinaryCIValidatesUnsignedPackagesWithoutRebindingPublishedManifest(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	if !strings.Contains(workflow, "diff -r dist-first dist-second") {
		t.Fatal("ordinary CI does not compare deterministic unsigned package bytes")
	}
	if strings.Contains(
		workflow,
		"diff -u plugins/delegation/release-artifacts.sha256 dist-first/release-artifacts.sha256",
	) {
		t.Fatal("ordinary CI rebinds the published manifest to post-release source")
	}
	assertPinnedActions(t, workflow)
}

func assertPinnedActions(t *testing.T, workflow string) {
	t.Helper()
	matches := actionReferencePattern.FindAllStringSubmatch(workflow, -1)
	if len(matches) == 0 {
		t.Fatal("workflow contains no action references")
	}
	for _, match := range matches {
		if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(match[1]) {
			t.Errorf("action reference is not pinned to a full commit: %q", match[0])
		}
	}
}

func readWorkflow(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
