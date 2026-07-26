package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitHubCandidateMetadataBindsRunArtifactAndSource(t *testing.T) {
	root := t.TempDir()
	runPath, artifactPath := writeGitHubMetadataFixture(t, root)
	candidate, err := verifyGitHubCandidateMetadata(
		runPath,
		artifactPath,
		"GhostFlying/delegation",
		"main",
		"123456",
		"654321",
		".github/workflows/release-candidate.yml",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := verifiedGitHubCandidate{
		SourceCommit:   strings.Repeat("d", 40),
		WorkflowRunID:  "123456",
		ArtifactID:     "654321",
		ArtifactName:   "delegation-release-candidate-123456",
		ArtifactDigest: strings.Repeat("e", 64),
		SourceRef:      "refs/heads/main",
	}
	if candidate != want {
		t.Fatalf("candidate = %#v, want %#v", candidate, want)
	}
}

func TestGitHubCandidateMetadataRejectsCrossRunArtifact(t *testing.T) {
	root := t.TempDir()
	runPath, artifactPath := writeGitHubMetadataFixture(t, root)
	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	var artifact githubArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatal(err)
	}
	artifact.WorkflowRun.ID++
	writeJSONFixture(t, artifactPath, artifact)
	_, err = verifyGitHubCandidateMetadata(
		runPath,
		artifactPath,
		"GhostFlying/delegation",
		"main",
		"123456",
		"654321",
		".github/workflows/release-candidate.yml",
	)
	if err == nil || !strings.Contains(err.Error(), "not bound to the selected workflow run") {
		t.Fatalf("verifyGitHubCandidateMetadata() error = %v", err)
	}
}

func writeGitHubMetadataFixture(t *testing.T, root string) (string, string) {
	t.Helper()
	var run githubWorkflowRun
	run.ID = 123456
	run.HeadBranch = "main"
	run.HeadSHA = strings.Repeat("d", 40)
	run.Event = "workflow_dispatch"
	run.Status = "completed"
	run.Conclusion = "success"
	run.Path = ".github/workflows/release-candidate.yml"
	run.Repository.FullName = "GhostFlying/delegation"
	var artifact githubArtifact
	artifact.ID = 654321
	artifact.Name = "delegation-release-candidate-123456"
	artifact.SizeInBytes = 4096
	artifact.Digest = "sha256:" + strings.Repeat("e", 64)
	artifact.WorkflowRun.ID = run.ID
	artifact.WorkflowRun.HeadSHA = run.HeadSHA
	runPath := filepath.Join(root, "run.json")
	artifactPath := filepath.Join(root, "artifact.json")
	writeJSONFixture(t, runPath, run)
	writeJSONFixture(t, artifactPath, artifact)
	return runPath, artifactPath
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
