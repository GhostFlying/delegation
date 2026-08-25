package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func runCommand(name string, args []string, stdout, stderr io.Writer) int {
	var err error
	switch name {
	case "build-target":
		err = runBuildTarget(args, stderr)
	case "package-target":
		err = runPackageTarget(args, stderr)
	case "write-evidence":
		err = runWriteEvidence(args, stderr)
	case "assemble":
		err = runAssemble(args, stderr)
	case "verify-candidate":
		err = runVerifyCandidate(args, stdout, stderr)
	case "verify-release":
		err = runVerifyRelease(args, stderr)
	case "verify-github-metadata":
		err = runVerifyGitHubMetadata(args, stderr)
	case "verify-promotion":
		err = runVerifyPromotion(args, stdout, stderr)
	case "write-promotion-predicate":
		err = runWritePromotionPredicate(args, stderr)
	default:
		fmt.Fprintf(stderr, "releasepack: unknown command %q\n", name)
		return 2
	}
	if err == nil {
		return 0
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintf(stderr, "releasepack %s: %v\n", name, err)
	return 1
}

func runBuildTarget(args []string, stderr io.Writer) error {
	flags := newCommandFlags("build-target", stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	targetID := flags.String("target", "", "release target operating-system-architecture")
	output := flags.String("out", "", "new binary path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	t, err := parseTarget(*targetID)
	if err != nil {
		return err
	}
	if *output == "" {
		return errors.New("--out is required")
	}
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	if _, err := readVersion(root); err != nil {
		return err
	}
	if err := verifyGoToolchain(root); err != nil {
		return err
	}
	outputPath, err := filepath.Abs(*output)
	if err != nil {
		return fmt.Errorf("resolve binary output: %w", err)
	}
	if err := requireNewPath(outputPath); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create binary output directory: %w", err)
	}
	if err := buildTarget(root, outputPath, t); err != nil {
		return err
	}
	file, info, err := openRegularFile(outputPath, maxArchiveSize)
	if err != nil {
		return fmt.Errorf("verify built binary: %w", err)
	}
	closeErr := file.Close()
	if info.Size() == 0 {
		return errors.Join(errors.New("built binary is empty"), closeErr)
	}
	return closeErr
}

func runPackageTarget(args []string, stderr io.Writer) error {
	flags := newCommandFlags("package-target", stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	targetID := flags.String("target", "", "release target operating-system-architecture")
	binary := flags.String("binary", "", "signed native binary path")
	output := flags.String("out", "", "output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	t, err := parseTarget(*targetID)
	if err != nil {
		return err
	}
	if *binary == "" || *output == "" {
		return errors.New("--binary and --out are required")
	}
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	version, err := readVersion(root)
	if err != nil {
		return err
	}
	notice, err := readReleaseNotice(root)
	if err != nil {
		return err
	}
	binaryPath, err := filepath.Abs(*binary)
	if err != nil {
		return fmt.Errorf("resolve binary: %w", err)
	}
	file, info, err := openRegularFile(binaryPath, maxArchiveSize)
	if err != nil {
		return fmt.Errorf("open signed binary: %w", err)
	}
	closeErr := file.Close()
	if info.Size() == 0 {
		return errors.Join(errors.New("signed binary is empty"), closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	outputRoot, err := prepareOutputDirectory(*output)
	if err != nil {
		return err
	}
	archivePath := filepath.Join(outputRoot, t.archiveName(version))
	if t.archive == "zip" {
		err = writeZip(archivePath, binaryPath, t.binaryName(), notice)
	} else {
		err = writeTarGzip(archivePath, binaryPath, t.binaryName(), notice)
	}
	if err != nil {
		return fmt.Errorf("package signed target: %w", err)
	}
	return verifyRuntimeArchive(archivePath, t, notice)
}

func runWriteEvidence(args []string, stderr io.Writer) error {
	flags := newCommandFlags("write-evidence", stderr)
	targetID := flags.String("target", "", "release target operating-system-architecture")
	output := flags.String("out", "", "output directory")
	notarizationID := flags.String("notarization-request-id", "", "accepted Apple notarization request UUID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	t, err := parseTarget(*targetID)
	if err != nil {
		return err
	}
	if *output == "" {
		return errors.New("--out is required")
	}
	outputRoot, err := prepareOutputDirectory(*output)
	if err != nil {
		return err
	}
	return writeEvidence(filepath.Join(outputRoot, t.evidenceName()), t, *notarizationID)
}

func runAssemble(args []string, stderr io.Writer) error {
	flags := newCommandFlags("assemble", stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	input := flags.String("input", "", "flat directory containing six archives and evidence files")
	output := flags.String("out", "", "new candidate directory")
	repository := flags.String("repository", "", "GitHub owner/repository")
	sourceCommit := flags.String("source-commit", "", "candidate source commit")
	workflowRunID := flags.String("workflow-run-id", "", "candidate workflow run ID")
	candidateName := flags.String("candidate-name", "", "immutable GitHub artifact name")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	if *input == "" || *output == "" {
		return errors.New("--input and --out are required")
	}
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	version, err := readVersion(root)
	if err != nil {
		return err
	}
	notice, err := readReleaseNotice(root)
	if err != nil {
		return err
	}
	return assembleCandidate(
		*input,
		*output,
		*repository,
		version,
		*sourceCommit,
		*workflowRunID,
		*candidateName,
		notice,
	)
}

func runVerifyCandidate(args []string, stdout, stderr io.Writer) error {
	flags := newCommandFlags("verify-candidate", stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	candidateRoot := flags.String("candidate", "", "candidate directory")
	repository := flags.String("repository", "", "expected GitHub owner/repository")
	sourceCommit := flags.String("source-commit", "", "expected candidate source commit")
	workflowRunID := flags.String("workflow-run-id", "", "expected candidate workflow run ID")
	candidateName := flags.String("candidate-name", "", "expected immutable GitHub artifact name")
	requireProvenance := flags.Bool("require-provenance", true, "require the candidate Sigstore bundle")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	if *candidateRoot == "" {
		return errors.New("--candidate is required")
	}
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return fmt.Errorf("resolve repository root: %w", err)
	}
	notice, err := readReleaseNotice(root)
	if err != nil {
		return err
	}
	descriptor, err := verifyCandidate(*candidateRoot, candidateExpectations{
		Repository:        *repository,
		SourceCommit:      *sourceCommit,
		WorkflowRunID:     *workflowRunID,
		CandidateName:     *candidateName,
		RequireProvenance: *requireProvenance,
	}, notice)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "version=%s\npayload_sha256=%s\n", descriptor.RuntimeVersion, descriptor.CandidateArtifact.PayloadSHA256)
	return err
}

func runVerifyRelease(args []string, stderr io.Writer) error {
	flags := newCommandFlags("verify-release", stderr)
	repoRoot := flags.String("repo", ".", "repository root")
	releaseRoot := flags.String("release", "", "release directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	if *releaseRoot == "" {
		return errors.New("--release is required")
	}
	return verifyRelease(*repoRoot, *releaseRoot)
}

func runVerifyGitHubMetadata(args []string, stderr io.Writer) error {
	flags := newCommandFlags("verify-github-metadata", stderr)
	runJSON := flags.String("run-json", "", "GitHub workflow run API response")
	artifactJSON := flags.String("artifact-json", "", "GitHub artifact API response")
	repository := flags.String("repository", "", "expected GitHub owner/repository")
	defaultBranch := flags.String("default-branch", "", "expected source branch")
	workflowRunID := flags.String("workflow-run-id", "", "selected workflow run ID")
	artifactID := flags.String("artifact-id", "", "selected artifact ID")
	workflowPath := flags.String("workflow-path", ".github/workflows/release-candidate.yml", "trusted workflow path")
	githubOutput := flags.String("github-output", "", "GitHub Actions output file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	if *runJSON == "" || *artifactJSON == "" || *githubOutput == "" {
		return errors.New("--run-json, --artifact-json, and --github-output are required")
	}
	candidate, err := verifyGitHubCandidateMetadata(
		*runJSON,
		*artifactJSON,
		*repository,
		*defaultBranch,
		*workflowRunID,
		*artifactID,
		*workflowPath,
	)
	if err != nil {
		return err
	}
	return writeGitHubOutput(*githubOutput, candidate)
}

func runVerifyPromotion(args []string, stdout, stderr io.Writer) error {
	flags, values := promotionFlags("verify-promotion", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	contract, err := verifyPromotionFromFlags(values)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		stdout,
		"version=%s\ntag_commit=%s\npayload_sha256=%s\n",
		contract.RuntimeVersion,
		contract.TagCommit,
		contract.PayloadSHA256,
	)
	return err
}

func runWritePromotionPredicate(args []string, stderr io.Writer) error {
	flags, values := promotionFlags("write-promotion-predicate", stderr)
	artifactID := flags.String("artifact-id", "", "selected candidate artifact ID")
	artifactDigest := flags.String("artifact-digest", "", "GitHub candidate artifact SHA-256")
	output := flags.String("out", "", "new canonical predicate path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := requireNoArguments(flags); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("--out is required")
	}
	contract, err := verifyPromotionFromFlags(values)
	if err != nil {
		return err
	}
	return writePromotionPredicate(*output, *artifactID, *artifactDigest, contract)
}

type promotionFlagValues struct {
	repoRoot      *string
	candidateRoot *string
	repository    *string
	tag           *string
	sourceCommit  *string
	workflowRunID *string
	candidateName *string
}

func promotionFlags(name string, stderr io.Writer) (*flag.FlagSet, promotionFlagValues) {
	flags := newCommandFlags(name, stderr)
	values := promotionFlagValues{
		repoRoot:      flags.String("repo", ".", "checked-out repository root"),
		candidateRoot: flags.String("candidate", "", "verified candidate directory"),
		repository:    flags.String("repository", "", "expected GitHub owner/repository"),
		tag:           flags.String("tag", "", "immutable v<VERSION> tag"),
		sourceCommit:  flags.String("source-commit", "", "candidate source commit"),
		workflowRunID: flags.String("workflow-run-id", "", "candidate workflow run ID"),
		candidateName: flags.String("candidate-name", "", "candidate artifact name"),
	}
	return flags, values
}

func verifyPromotionFromFlags(values promotionFlagValues) (promotionContract, error) {
	if *values.candidateRoot == "" {
		return promotionContract{}, errors.New("--candidate is required")
	}
	return verifyPromotion(
		*values.repoRoot,
		*values.candidateRoot,
		*values.repository,
		*values.tag,
		*values.sourceCommit,
		*values.workflowRunID,
		*values.candidateName,
	)
}

func newCommandFlags(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet("releasepack "+name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func requireNoArguments(flags *flag.FlagSet) error {
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return nil
}

func prepareOutputDirectory(path string) (string, error) {
	root, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("output must be a directory, not a link")
	}
	return root, nil
}
