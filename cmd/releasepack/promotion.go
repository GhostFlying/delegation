package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const promotionPredicateType = "https://github.com/GhostFlying/delegation/attestations/release-promotion/v1"

type promotionContract struct {
	Repository     string
	RuntimeVersion string
	SourceCommit   string
	Tag            string
	TagCommit      string
	WorkflowRunID  string
	ArtifactName   string
	PayloadSHA256  string
}

type promotionPredicate struct {
	SchemaVersion  int                         `json:"schemaVersion"`
	Repository     string                      `json:"repository"`
	RuntimeVersion string                      `json:"runtimeVersion"`
	SourceCommit   string                      `json:"sourceCommit"`
	Tag            string                      `json:"tag"`
	TagCommit      string                      `json:"tagCommit"`
	Candidate      promotionCandidateReference `json:"candidate"`
}

type promotionCandidateReference struct {
	WorkflowRunID  string `json:"workflowRunId"`
	ArtifactID     string `json:"artifactId"`
	ArtifactName   string `json:"artifactName"`
	ArtifactSHA256 string `json:"artifactSha256"`
	PayloadSHA256  string `json:"payloadSha256"`
}

func verifyPromotion(
	repoRoot,
	candidateRoot,
	repository,
	tag,
	sourceCommit,
	workflowRunID,
	candidateName string,
) (promotionContract, error) {
	repoRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return promotionContract{}, fmt.Errorf("resolve repository root: %w", err)
	}
	notice, err := readReleaseNotice(repoRoot)
	if err != nil {
		return promotionContract{}, err
	}
	descriptor, err := verifyCandidate(candidateRoot, candidateExpectations{
		Repository:        repository,
		SourceCommit:      sourceCommit,
		WorkflowRunID:     workflowRunID,
		CandidateName:     candidateName,
		RequireProvenance: true,
	}, notice)
	if err != nil {
		return promotionContract{}, err
	}
	expectedTag := "v" + descriptor.RuntimeVersion
	if tag != expectedTag {
		return promotionContract{}, fmt.Errorf("release tag %q does not match %q", tag, expectedTag)
	}
	tagCommit, err := gitOutput(repoRoot, "rev-parse", "--verify", "refs/tags/"+tag+"^{commit}")
	if err != nil {
		return promotionContract{}, err
	}
	if !commitPattern.MatchString(tagCommit) {
		return promotionContract{}, fmt.Errorf("tag resolves to invalid commit %q", tagCommit)
	}
	headCommit, err := gitOutput(repoRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return promotionContract{}, err
	}
	if headCommit != tagCommit {
		return promotionContract{}, errors.New("checked-out commit is not the immutable release tag commit")
	}
	parents, err := gitOutput(repoRoot, "rev-list", "--parents", "-n", "1", tagCommit)
	if err != nil {
		return promotionContract{}, err
	}
	parentFields := strings.Fields(parents)
	if len(parentFields) != 2 || parentFields[0] != tagCommit || parentFields[1] != sourceCommit {
		return promotionContract{}, errors.New("release manifest commit must have the candidate source as its only parent")
	}
	diff, err := gitOutput(repoRoot, "diff", "--name-status", "--no-renames", sourceCommit, tagCommit)
	if err != nil {
		return promotionContract{}, err
	}
	if diff != "M\tplugins/delegation/release-artifacts.sha256" {
		return promotionContract{}, errors.New("candidate source to release tag must change only the checksum manifest")
	}
	sourceVersion, err := gitOutput(repoRoot, "show", sourceCommit+":plugins/delegation/VERSION")
	if err != nil {
		return promotionContract{}, err
	}
	if sourceVersion != descriptor.RuntimeVersion {
		return promotionContract{}, errors.New("candidate source version does not match the candidate descriptor")
	}
	tagVersion, err := gitOutput(repoRoot, "show", tagCommit+":plugins/delegation/VERSION")
	if err != nil {
		return promotionContract{}, err
	}
	if tagVersion != descriptor.RuntimeVersion {
		return promotionContract{}, errors.New("tag version does not match the candidate descriptor")
	}
	trackedManifest, err := os.ReadFile(filepath.Join(repoRoot, "plugins", "delegation", candidateManifestName))
	if err != nil {
		return promotionContract{}, fmt.Errorf("read tracked release manifest: %w", err)
	}
	candidateManifest, err := readBoundedRegularFile(filepath.Join(candidateRoot, candidateManifestName), maxEvidenceSize)
	if err != nil {
		return promotionContract{}, fmt.Errorf("read candidate release manifest: %w", err)
	}
	if !bytes.Equal(trackedManifest, candidateManifest) {
		return promotionContract{}, errors.New("tracked checksum manifest does not match the immutable candidate")
	}
	return promotionContract{
		Repository:     repository,
		RuntimeVersion: descriptor.RuntimeVersion,
		SourceCommit:   sourceCommit,
		Tag:            tag,
		TagCommit:      tagCommit,
		WorkflowRunID:  workflowRunID,
		ArtifactName:   candidateName,
		PayloadSHA256:  descriptor.CandidateArtifact.PayloadSHA256,
	}, nil
}

func writePromotionPredicate(
	path,
	artifactID,
	artifactDigest string,
	contract promotionContract,
) error {
	if _, err := parsePositiveID(artifactID, "artifact ID"); err != nil {
		return err
	}
	if !lowercaseDigestPattern.MatchString(artifactDigest) {
		return fmt.Errorf("invalid candidate artifact digest %q", artifactDigest)
	}
	predicate := promotionPredicate{
		SchemaVersion:  candidateSchemaVersion,
		Repository:     contract.Repository,
		RuntimeVersion: contract.RuntimeVersion,
		SourceCommit:   contract.SourceCommit,
		Tag:            contract.Tag,
		TagCommit:      contract.TagCommit,
		Candidate: promotionCandidateReference{
			WorkflowRunID:  contract.WorkflowRunID,
			ArtifactID:     artifactID,
			ArtifactName:   contract.ArtifactName,
			ArtifactSHA256: artifactDigest,
			PayloadSHA256:  contract.PayloadSHA256,
		},
	}
	data, err := canonicalJSON(predicate)
	if err != nil {
		return err
	}
	if err := writeNewFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write promotion predicate: %w", err)
	}
	return nil
}

func gitOutput(root string, args ...string) (string, error) {
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}
