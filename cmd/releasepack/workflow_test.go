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

func TestOrdinaryCIValidatesUnsignedPackagesAgainstTrackedManifest(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	if !strings.Contains(workflow, "diff -r dist-first dist-second") {
		t.Fatal("ordinary CI does not compare deterministic unsigned package bytes")
	}
	if !strings.Contains(
		workflow,
		"diff -u \\\n            plugins/delegation/release-artifacts.sha256 \\\n            dist-first/release-artifacts.sha256",
	) {
		t.Fatal("ordinary CI does not bind the tracked manifest to the fresh deterministic build")
	}
	assertPinnedActions(t, workflow)
}

func TestDirectGoBuildTestAndVetPathsUsePrivacyTag(t *testing.T) {
	workflowPaths, err := filepath.Glob(
		filepath.Join("..", "..", ".github", "workflows", "*.yml"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(workflowPaths) == 0 {
		t.Fatal("no GitHub workflows found")
	}
	paths := append(workflowPaths,
		filepath.Join("..", "..", "tests", "posix_plugin_test.sh"),
		filepath.Join("..", "..", "tests", "windows_plugin_test.ps1"),
	)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for lineNumber, line := range strings.Split(string(data), "\n") {
			if !directGoCommandPattern.MatchString(line) {
				continue
			}
			if !strings.Contains(line, "ts_omit_logtail") {
				t.Errorf("%s:%d direct Go command lacks ts_omit_logtail: %s", path, lineNumber+1, line)
			}
		}
	}

	workflow := readWorkflow(t, "ci.yml")
	for _, combined := range []string{
		"-tags='ts_omit_logtail,integration'",
		"-tags='ts_omit_logtail,integration,live'",
	} {
		if !strings.Contains(workflow, combined) {
			t.Errorf("CI workflow is missing combined build tags %q", combined)
		}
	}
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

var directGoCommandPattern = regexp.MustCompile(
	`^\s*(?:&\s+)?(?:go|"\$go_bin")(?:\s+-C\s+\S+)?\s+(?:build|test|vet)\b`,
)

func readWorkflow(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	return normalizeTestLineEndings(string(data))
}

func normalizeTestLineEndings(text string) string {
	return strings.ReplaceAll(text, "\r\n", "\n")
}

func TestNormalizeTestLineEndings(t *testing.T) {
	const input = "first\r\nsecond\r\n"
	const want = "first\nsecond\n"
	if got := normalizeTestLineEndings(input); got != want {
		t.Fatalf("normalizeTestLineEndings() = %q, want %q", got, want)
	}
}
