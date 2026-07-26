package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const maxGitHubMetadataSize = 2 << 20

type githubWorkflowRun struct {
	ID         int64  `json:"id"`
	HeadBranch string `json:"head_branch"`
	HeadSHA    string `json:"head_sha"`
	Event      string `json:"event"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Path       string `json:"path"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type githubArtifact struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	SizeInBytes int64  `json:"size_in_bytes"`
	Expired     bool   `json:"expired"`
	Digest      string `json:"digest"`
	WorkflowRun struct {
		ID      int64  `json:"id"`
		HeadSHA string `json:"head_sha"`
	} `json:"workflow_run"`
}

type verifiedGitHubCandidate struct {
	SourceCommit   string
	WorkflowRunID  string
	ArtifactID     string
	ArtifactName   string
	ArtifactDigest string
	SourceRef      string
}

func verifyGitHubCandidateMetadata(
	runPath,
	artifactPath,
	repository,
	defaultBranch,
	workflowRunID,
	artifactID,
	workflowPath string,
) (verifiedGitHubCandidate, error) {
	if !repositoryPattern.MatchString(repository) {
		return verifiedGitHubCandidate{}, fmt.Errorf("invalid GitHub repository %q", repository)
	}
	if defaultBranch == "" || strings.ContainsAny(defaultBranch, "\r\n") {
		return verifiedGitHubCandidate{}, errors.New("default branch must be a non-empty single line")
	}
	expectedRunID, err := parsePositiveID(workflowRunID, "workflow run ID")
	if err != nil {
		return verifiedGitHubCandidate{}, err
	}
	expectedArtifactID, err := parsePositiveID(artifactID, "artifact ID")
	if err != nil {
		return verifiedGitHubCandidate{}, err
	}
	if workflowPath != ".github/workflows/release-candidate.yml" {
		return verifiedGitHubCandidate{}, fmt.Errorf("untrusted candidate workflow path %q", workflowPath)
	}
	var run githubWorkflowRun
	if err := decodeGitHubAPIFile(runPath, &run); err != nil {
		return verifiedGitHubCandidate{}, fmt.Errorf("decode workflow run metadata: %w", err)
	}
	if run.ID != expectedRunID || run.Repository.FullName != repository {
		return verifiedGitHubCandidate{}, errors.New("workflow run does not belong to the selected repository and run ID")
	}
	if run.Path != workflowPath || run.Event != "workflow_dispatch" || run.Status != "completed" || run.Conclusion != "success" {
		return verifiedGitHubCandidate{}, errors.New("workflow run is not a successful trusted release candidate run")
	}
	if run.HeadBranch != defaultBranch || !commitPattern.MatchString(run.HeadSHA) {
		return verifiedGitHubCandidate{}, errors.New("workflow run did not build an exact default-branch commit")
	}
	var artifact githubArtifact
	if err := decodeGitHubAPIFile(artifactPath, &artifact); err != nil {
		return verifiedGitHubCandidate{}, fmt.Errorf("decode artifact metadata: %w", err)
	}
	if artifact.ID != expectedArtifactID || artifact.Expired || artifact.SizeInBytes <= 0 {
		return verifiedGitHubCandidate{}, errors.New("candidate artifact is missing, expired, empty, or has the wrong ID")
	}
	if artifact.WorkflowRun.ID != expectedRunID || artifact.WorkflowRun.HeadSHA != run.HeadSHA {
		return verifiedGitHubCandidate{}, errors.New("candidate artifact is not bound to the selected workflow run and source commit")
	}
	expectedName := "delegation-release-candidate-" + workflowRunID
	if artifact.Name != expectedName {
		return verifiedGitHubCandidate{}, fmt.Errorf("candidate artifact name %q does not match %q", artifact.Name, expectedName)
	}
	digest, ok := strings.CutPrefix(artifact.Digest, "sha256:")
	if !ok || !lowercaseDigestPattern.MatchString(digest) {
		return verifiedGitHubCandidate{}, errors.New("candidate artifact does not have a valid GitHub SHA-256 digest")
	}
	return verifiedGitHubCandidate{
		SourceCommit:   run.HeadSHA,
		WorkflowRunID:  workflowRunID,
		ArtifactID:     artifactID,
		ArtifactName:   artifact.Name,
		ArtifactDigest: digest,
		SourceRef:      "refs/heads/" + defaultBranch,
	}, nil
}

func decodeGitHubAPIFile(path string, value any) error {
	data, err := readBoundedRegularFile(path, maxGitHubMetadataSize)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("metadata contains more than one JSON value")
		}
		return err
	}
	return nil
}

func writeGitHubOutput(path string, candidate verifiedGitHubCandidate) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(
		file,
		"source_commit=%s\nworkflow_run_id=%s\nartifact_id=%s\nartifact_name=%s\nartifact_digest=%s\nsource_ref=%s\n",
		candidate.SourceCommit,
		candidate.WorkflowRunID,
		candidate.ArtifactID,
		candidate.ArtifactName,
		candidate.ArtifactDigest,
		candidate.SourceRef,
	)
	return errors.Join(writeErr, file.Close())
}
