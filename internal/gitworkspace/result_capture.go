package gitworkspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	ChangesBundleName  = "changes.bundle"
	ChangesOverlayName = "changes-overlay.tar.zst"
)

type ResultArtifact struct {
	Kind   protocol.WorkspaceArtifactKind
	Name   string
	Path   string
	Size   int64
	SHA256 string
}

type ResultCapture struct {
	ArtifactDirectory  string
	ResultHeadOID      string
	ObjectFormat       string
	WorkingDirectory   string
	ResultClean        bool
	ResultSnapshotHash string
	ResultWarnings     []string
	Unchanged          bool
	Bundle             *ResultArtifact
	Overlay            *ResultArtifact
}

// CaptureResult snapshots a managed worker repository without modifying it.
// The artifact directory must not exist and is removed if capture fails.
func (r Runner) CaptureResult(
	ctx context.Context,
	repositoryPath, artifactDirectory string,
	base protocol.WorkspaceManifest,
) (ResultCapture, error) {
	return r.captureResult(
		ctx, repositoryPath, artifactDirectory, base,
		protocol.MaximumWorkspaceArtifactBytes,
	)
}

func (r Runner) captureResult(
	ctx context.Context,
	repositoryPath, artifactDirectory string,
	base protocol.WorkspaceManifest,
	maximumArtifactBytes int64,
) (capture ResultCapture, returnErr error) {
	if err := base.Validate(); err != nil {
		return ResultCapture{}, fmt.Errorf("validate prepared workspace manifest: %w", err)
	}
	if !filepath.IsAbs(repositoryPath) || !filepath.IsAbs(artifactDirectory) {
		return ResultCapture{}, errors.New("result capture paths must be absolute")
	}
	if filepath.Clean(repositoryPath) != repositoryPath || filepath.Clean(artifactDirectory) != artifactDirectory {
		return ResultCapture{}, errors.New("result capture paths must be normalized")
	}
	if maximumArtifactBytes < 1 || maximumArtifactBytes > protocol.MaximumWorkspaceArtifactBytes {
		return ResultCapture{}, errors.New("result capture artifact byte limit is invalid")
	}
	resolvedRepository, err := filepath.EvalSymlinks(repositoryPath)
	if err != nil {
		return ResultCapture{}, fmt.Errorf("resolve managed workspace: %w", err)
	}
	r = r.forIsolatedTarget()
	if err := r.validateManagedResultRepository(ctx, resolvedRepository); err != nil {
		return ResultCapture{}, err
	}
	if err := r.ensureSafeSourceConfig(ctx, resolvedRepository); err != nil {
		return ResultCapture{}, err
	}
	resolvedArtifactParent, err := filepath.EvalSymlinks(filepath.Dir(artifactDirectory))
	if err != nil {
		return ResultCapture{}, fmt.Errorf("resolve result artifact parent: %w", err)
	}
	resolvedArtifactDirectory := filepath.Join(resolvedArtifactParent, filepath.Base(artifactDirectory))
	if pathWithin(resolvedRepository, resolvedArtifactDirectory) {
		return ResultCapture{}, errors.New("result artifact directory must be outside the managed workspace")
	}
	if err := os.Mkdir(resolvedArtifactDirectory, 0o700); err != nil {
		return ResultCapture{}, fmt.Errorf("create result artifact directory: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			returnErr = errors.Join(returnErr, os.RemoveAll(resolvedArtifactDirectory))
		}
	}()

	resultHead, err := r.resultHead(ctx, resolvedRepository, base)
	if err != nil {
		return ResultCapture{}, err
	}
	initial, err := r.captureOverlay(
		ctx, resolvedRepository, resultHead, base.ObjectFormat, base.WorkingDirectory, "",
	)
	if err != nil {
		return ResultCapture{}, fmt.Errorf("inspect worker result snapshot: %w", err)
	}
	resultManifest := base
	resultManifest.HeadOID = resultHead
	resultManifest.Clean = len(initial.manifest.Entries) == 0
	resultManifest.SourceSnapshotHash = initial.manifest.SourceSnapshotHash
	if err := resultManifest.Validate(); err != nil {
		return ResultCapture{}, fmt.Errorf("validate worker result manifest: %w", err)
	}
	resultWarnings, err := r.contentWarnings(ctx, resolvedRepository)
	if err != nil {
		return ResultCapture{}, fmt.Errorf("inspect worker result warnings: %w", err)
	}

	capture = ResultCapture{
		ArtifactDirectory:  resolvedArtifactDirectory,
		ResultHeadOID:      resultHead,
		ObjectFormat:       base.ObjectFormat,
		WorkingDirectory:   base.WorkingDirectory,
		ResultClean:        resultManifest.Clean,
		ResultSnapshotHash: resultManifest.SourceSnapshotHash,
		ResultWarnings:     resultWarnings,
		Unchanged: resultHead == base.HeadOID &&
			resultManifest.SourceSnapshotHash == base.SourceSnapshotHash,
	}
	if !capture.Unchanged && resultHead != base.HeadOID {
		name := filepath.Join(resolvedArtifactDirectory, ChangesBundleName)
		if err := r.createResultBundle(
			ctx, resolvedRepository, name, base.HeadOID, resultHead, maximumArtifactBytes,
		); err != nil {
			return ResultCapture{}, err
		}
		artifact, err := describeResultArtifact(
			name, ChangesBundleName, protocol.WorkspaceArtifactBundle, maximumArtifactBytes,
		)
		if err != nil {
			return ResultCapture{}, err
		}
		capture.Bundle = &artifact
	}
	if !capture.Unchanged && !resultManifest.Clean {
		name := filepath.Join(resolvedArtifactDirectory, ChangesOverlayName)
		if err := r.CreateOverlay(ctx, resolvedRepository, name, resultManifest); err != nil {
			return ResultCapture{}, fmt.Errorf("create worker result overlay: %w", err)
		}
		artifact, err := describeResultArtifact(
			name, ChangesOverlayName, protocol.WorkspaceArtifactOverlay, maximumArtifactBytes,
		)
		if err != nil {
			return ResultCapture{}, err
		}
		capture.Overlay = &artifact
	}

	verified, err := r.captureOverlay(
		ctx, resolvedRepository, resultHead, base.ObjectFormat, base.WorkingDirectory, "",
	)
	if err != nil {
		return ResultCapture{}, fmt.Errorf("verify worker result snapshot: %w", err)
	}
	if verified.manifest.SourceSnapshotHash != resultManifest.SourceSnapshotHash ||
		(len(verified.manifest.Entries) == 0) != resultManifest.Clean {
		return ResultCapture{}, errors.New("worker result changed while its artifacts were captured")
	}
	verifiedWarnings, err := r.contentWarnings(ctx, resolvedRepository)
	if err != nil {
		return ResultCapture{}, fmt.Errorf("verify worker result warnings: %w", err)
	}
	if !slices.Equal(verifiedWarnings, resultWarnings) {
		return ResultCapture{}, errors.New("worker result warnings changed while its artifacts were captured")
	}
	if err := r.validateManagedResultRepository(ctx, resolvedRepository); err != nil {
		return ResultCapture{}, err
	}
	if err := r.ensureSafeSourceConfig(ctx, resolvedRepository); err != nil {
		return ResultCapture{}, err
	}
	var artifactBytes int64
	for _, artifact := range []*ResultArtifact{capture.Bundle, capture.Overlay} {
		if artifact != nil {
			artifactBytes += artifact.Size
		}
	}
	if artifactBytes > protocol.MaximumWorkspaceTransferBytes {
		return ResultCapture{}, fmt.Errorf(
			"worker result artifacts exceed %d bytes", protocol.MaximumWorkspaceTransferBytes,
		)
	}
	if err := syncResultArtifactDirectories(resolvedArtifactDirectory, resolvedArtifactParent); err != nil {
		return ResultCapture{}, err
	}
	keep = true
	return capture, nil
}

func (r Runner) resultHead(
	ctx context.Context,
	repositoryPath string,
	base protocol.WorkspaceManifest,
) (string, error) {
	format, err := r.output(ctx, repositoryPath, "rev-parse", "--show-object-format")
	if err != nil || strings.TrimSpace(string(format)) != base.ObjectFormat {
		return "", preserveContextError(err, errors.New("worker result Git object format changed"))
	}
	if err := r.run(
		ctx, repositoryPath, "--no-replace-objects", "cat-file", "-e", base.HeadOID+"^{commit}",
	); err != nil {
		return "", preserveContextError(err, errors.New("prepared base commit is unavailable"))
	}
	headOutput, err := r.output(
		ctx, repositoryPath, "--no-replace-objects", "rev-parse", "--verify", "HEAD^{commit}",
	)
	if err != nil {
		return "", preserveContextError(err, errors.New("worker result has no commit at HEAD"))
	}
	head := strings.TrimSpace(string(headOutput))
	if !gitObjectID(head) || len(head) != len(base.HeadOID) {
		return "", errors.New("worker result HEAD is not a valid object ID")
	}
	if head == base.HeadOID {
		return head, nil
	}
	err = r.run(
		ctx, repositoryPath, "-c", "core.commitGraph=false", "--no-replace-objects",
		"merge-base", "--is-ancestor", base.HeadOID, head,
	)
	if err == nil {
		return head, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return "", errors.New("worker result HEAD does not descend from the prepared base")
	}
	return "", preserveContextError(err, errors.New("verify worker result ancestry"))
}

func (r Runner) createResultBundle(
	ctx context.Context,
	repositoryPath, destination, baseHead, resultHead string,
	maximumBytes int64,
) error {
	if err := r.runDiscardingOutput(
		ctx, repositoryPath,
		"-c", "core.commitGraph=false", "--no-replace-objects",
		"rev-list", "--objects", "--missing=error", "--no-object-names",
		resultHead, "^"+baseHead,
	); err != nil {
		return preserveContextError(err, errors.New("verify worker result objects"))
	}
	args := []string{
		"-c", "core.commitGraph=false", "--no-replace-objects",
		"bundle", "create", "--quiet", "-", "HEAD", "^" + baseHead,
	}
	if err := r.createBundleFile(ctx, repositoryPath, destination, maximumBytes, args); err != nil {
		if errors.Is(err, errWorkspaceArtifactTooLarge) {
			return err
		}
		return preserveContextError(err, errors.New("create worker result Git bundle"))
	}
	if err := r.run(ctx, repositoryPath, "bundle", "verify", destination); err != nil {
		return preserveContextError(err, errors.New("verify worker result Git bundle"))
	}
	heads, err := r.output(ctx, repositoryPath, "bundle", "list-heads", destination, "HEAD")
	if err != nil {
		return preserveContextError(err, errors.New("inspect worker result Git bundle head"))
	}
	fields := strings.Fields(string(heads))
	if len(fields) != 2 || fields[0] != resultHead || fields[1] != "HEAD" {
		return errors.New("worker result Git bundle does not advertise the pinned HEAD")
	}
	actualHead, err := r.output(
		ctx, repositoryPath, "--no-replace-objects", "rev-parse", "--verify", "HEAD^{commit}",
	)
	if err != nil || strings.TrimSpace(string(actualHead)) != resultHead {
		return preserveContextError(err, errors.New("worker result HEAD changed during bundle creation"))
	}
	return nil
}

func describeResultArtifact(
	path, name string,
	kind protocol.WorkspaceArtifactKind,
	maximumBytes int64,
) (ResultArtifact, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return ResultArtifact{}, fmt.Errorf("inspect worker result artifact: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maximumBytes {
		return ResultArtifact{}, fmt.Errorf("worker result artifact must contain from 1 through %d bytes", maximumBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return ResultArtifact{}, fmt.Errorf("open worker result artifact: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	written, err := io.Copy(digest, io.LimitReader(file, maximumBytes+1))
	if err != nil {
		return ResultArtifact{}, fmt.Errorf("hash worker result artifact: %w", err)
	}
	finalInfo, err := file.Stat()
	if err != nil {
		return ResultArtifact{}, fmt.Errorf("reinspect worker result artifact: %w", err)
	}
	if written != info.Size() || written > maximumBytes || !finalInfo.Mode().IsRegular() ||
		finalInfo.Size() != info.Size() {
		return ResultArtifact{}, errors.New("worker result artifact changed while it was hashed")
	}
	return ResultArtifact{
		Kind: kind, Name: name, Path: path, Size: written,
		SHA256: hex.EncodeToString(digest.Sum(nil)),
	}, nil
}

func pathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
