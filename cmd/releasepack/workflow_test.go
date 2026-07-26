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

func TestPromotionWorkflowPublishesCandidateWithoutRuntimeRebuild(t *testing.T) {
	workflow := readWorkflow(t, "release.yml")
	for _, required := range []string{
		"test \"$GITHUB_REF_TYPE\" = tag",
		"verify-github-metadata",
		"verify-candidate",
		"verify-promotion",
		"artifact-ids: ${{ inputs.candidate_artifact_id }}",
		"run-id: ${{ inputs.candidate_run_id }}",
		"--signer-workflow \"$GITHUB_REPOSITORY/.github/workflows/release-candidate.yml\"",
		"--source-digest \"$SOURCE_COMMIT\"",
		"predicate-type: https://github.com/GhostFlying/delegation/attestations/release-promotion/v1",
		"actions/attest@a1948c3f048ba23858d222213b7c278aabede763",
		"environment: github-release",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("promotion workflow is missing %q", required)
		}
	}
	if strings.Contains(workflow, "secrets.") {
		t.Fatal("promotion workflow must not receive platform signing credentials")
	}
	if strings.Contains(workflow, "build-target") || strings.Contains(workflow, "package-target") ||
		strings.Contains(workflow, "go build") {
		t.Fatal("promotion workflow rebuilds or repackages runtime bytes")
	}
	if got := strings.Count(workflow, "verify-github-metadata"); got != 2 {
		t.Fatalf("GitHub metadata verification count = %d, want 2", got)
	}
	assertPinnedActions(t, workflow)
}

func TestOrdinaryCIValidatesUnsignedPackagesWithoutBindingTheReleaseManifest(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	if !strings.Contains(workflow, "diff -r dist-first dist-second") {
		t.Fatal("ordinary CI does not compare deterministic unsigned package bytes")
	}
	if strings.Contains(
		workflow,
		"diff -u plugins/delegation/release-artifacts.sha256 dist-first/release-artifacts.sha256",
	) {
		t.Fatal("ordinary CI binds the signed release manifest to unsigned package bytes")
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
