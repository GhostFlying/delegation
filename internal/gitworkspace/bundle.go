package gitworkspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const maximumBundleBasisCandidates = 32

var errWorkspaceArtifactTooLarge = errors.New("source Git bundle exceeded its byte limit")

func (r Runner) bundleBasisOIDs(ctx context.Context, repositoryPath string) ([]string, error) {
	output, err := r.output(
		ctx,
		repositoryPath,
		"rev-list",
		fmt.Sprintf("--max-count=%d", maximumBundleBasisCandidates),
		"--remotes=origin",
		"--tags",
	)
	if err != nil {
		return nil, preserveContextError(err, errors.New("enumerate target Git bundle prerequisites"))
	}
	basis := strings.Fields(string(output))
	for _, oid := range basis {
		if !gitObjectID(oid) {
			return nil, errors.New("target Git returned an invalid bundle prerequisite")
		}
	}
	slices.Sort(basis)
	basis = slices.Compact(basis)
	if len(basis) > protocol.MaximumWorkspaceBasisOIDs {
		basis = basis[:protocol.MaximumWorkspaceBasisOIDs]
	}
	return basis, nil
}

func (r Runner) CreateBundle(
	ctx context.Context,
	repositoryPath, destination string,
	manifest protocol.WorkspaceManifest,
	basisCandidates []string,
) (protocol.WorkspaceStrategy, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	if !manifest.Clean {
		return "", errors.New("clean bundle transport cannot export a dirty source")
	}
	if !filepath.IsAbs(repositoryPath) || !filepath.IsAbs(destination) {
		return "", errors.New("bundle paths must be absolute")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return "", errors.New("bundle destination already exists")
		}
		return "", fmt.Errorf("inspect bundle destination: %w", err)
	}
	actualHead, err := r.output(ctx, repositoryPath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(string(actualHead)) != manifest.HeadOID {
		return "", preserveContextError(err, errors.New("source HEAD changed before bundle creation"))
	}
	basis, err := r.chooseBundleBasis(ctx, repositoryPath, manifest.HeadOID, basisCandidates)
	if err != nil {
		return "", err
	}
	shallow, err := r.output(ctx, repositoryPath, "rev-parse", "--is-shallow-repository")
	if err != nil {
		return "", preserveContextError(err, errors.New("inspect source repository depth"))
	}
	if strings.TrimSpace(string(shallow)) != "false" {
		return "", errors.New("Git bundle transport does not support shallow source repositories")
	}
	objects, err := r.output(
		ctx, repositoryPath,
		"--no-replace-objects", "rev-list", "--objects", "--missing=print", "--no-object-names", "HEAD",
	)
	if err != nil {
		return "", preserveContextError(err, errors.New("verify source repository objects"))
	}
	for _, object := range strings.Fields(string(objects)) {
		if strings.HasPrefix(object, "?") {
			return "", errors.New("Git bundle transport does not support missing source objects")
		}
	}
	strategy := protocol.WorkspaceStrategyFull
	args := []string{"--no-replace-objects", "bundle", "create", "--quiet", "-", "HEAD"}
	if basis != "" {
		strategy = protocol.WorkspaceStrategyThin
		args = append(args, "^"+basis)
	}
	if err := r.createBundleFile(
		ctx, repositoryPath, destination, protocol.MaximumWorkspaceArtifactBytes, args,
	); err != nil {
		if errors.Is(err, errWorkspaceArtifactTooLarge) {
			return "", err
		}
		return "", preserveContextError(err, errors.New("create source Git bundle"))
	}
	info, err := os.Stat(destination)
	if err != nil {
		return "", fmt.Errorf("inspect source Git bundle: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > protocol.MaximumWorkspaceArtifactBytes {
		return "", fmt.Errorf("source Git bundle must contain from 1 through %d bytes", protocol.MaximumWorkspaceArtifactBytes)
	}
	if err := r.run(ctx, repositoryPath, "bundle", "verify", destination); err != nil {
		return "", preserveContextError(err, errors.New("verify source Git bundle"))
	}
	heads, err := r.output(ctx, repositoryPath, "bundle", "list-heads", destination, "HEAD")
	if err != nil {
		return "", preserveContextError(err, errors.New("inspect source Git bundle head"))
	}
	fields := strings.Fields(string(heads))
	if len(fields) != 2 || fields[0] != manifest.HeadOID || fields[1] != "HEAD" {
		return "", errors.New("source Git bundle does not advertise the pinned HEAD")
	}
	actualHead, err = r.output(ctx, repositoryPath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(string(actualHead)) != manifest.HeadOID {
		return "", preserveContextError(err, errors.New("source HEAD changed during bundle creation"))
	}
	return strategy, nil
}

func (r Runner) createBundleFile(
	ctx context.Context,
	repositoryPath, destination string,
	maximumBytes int64,
	args []string,
) (returnErr error) {
	if maximumBytes < 1 {
		return errors.New("Git bundle byte limit must be positive")
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create source Git bundle file: %w", err)
	}
	keep := false
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
		if !keep {
			returnErr = errors.Join(returnErr, os.Remove(destination))
		}
	}()

	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	command := exec.CommandContext(commandContext, r.Binary, safeGitArgs(args)...)
	command.WaitDelay = commandWaitDelay
	command.Dir = repositoryPath
	command.Env = hardenedEnvironment(r.excludedEnvironment)
	output := boundedFileWriter{file: file, remaining: maximumBytes}
	var stderr limitedBuffer
	command.Stdout = &output
	command.Stderr = &stderr
	runErr := command.Run()
	if output.exceeded {
		return fmt.Errorf("%w of %d bytes", errWorkspaceArtifactTooLarge, maximumBytes)
	}
	if commandContext.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errCommandTimeout
	}
	if runErr != nil {
		return runErr
	}
	if stderr.full {
		return errors.New("Git bundle command output exceeded the configured limit")
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync source Git bundle: %w", err)
	}
	keep = true
	return nil
}

type boundedFileWriter struct {
	file      *os.File
	remaining int64
	exceeded  bool
}

func (w *boundedFileWriter) Write(data []byte) (int, error) {
	if int64(len(data)) <= w.remaining {
		written, err := w.file.Write(data)
		w.remaining -= int64(written)
		return written, err
	}
	allowed := int(w.remaining)
	written := 0
	if allowed > 0 {
		var err error
		written, err = w.file.Write(data[:allowed])
		w.remaining -= int64(written)
		if err != nil {
			return written, err
		}
	}
	w.exceeded = true
	return written, errWorkspaceArtifactTooLarge
}

func (r Runner) chooseBundleBasis(
	ctx context.Context,
	repositoryPath, headOID string,
	candidates []string,
) (string, error) {
	if len(candidates) > protocol.MaximumWorkspaceBasisOIDs || !slices.IsSorted(candidates) ||
		len(slices.Compact(slices.Clone(candidates))) != len(candidates) {
		return "", errors.New("bundle prerequisite candidates must be bounded, sorted, and unique")
	}
	bestOID := ""
	var bestDistance uint64
	for _, candidate := range candidates {
		if !gitObjectID(candidate) {
			return "", errors.New("bundle prerequisite candidate is invalid")
		}
		if err := r.run(ctx, repositoryPath, "--no-replace-objects", "cat-file", "-e", candidate+"^{commit}"); err != nil {
			if isContextError(err) {
				return "", err
			}
			continue
		}
		output, err := r.output(
			ctx, repositoryPath, "--no-replace-objects", "merge-base", "--all", headOID, candidate,
		)
		if err != nil {
			var exitError *exec.ExitError
			if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
				continue
			}
			return "", preserveContextError(err, errors.New("select Git bundle prerequisite"))
		}
		for _, common := range strings.Fields(string(output)) {
			if !gitObjectID(common) {
				return "", errors.New("Git returned an invalid common ancestor")
			}
			countOutput, err := r.output(
				ctx, repositoryPath, "--no-replace-objects", "rev-list", "--count", common+".."+headOID,
			)
			if err != nil {
				return "", preserveContextError(err, errors.New("measure Git bundle prerequisite"))
			}
			distance, err := strconv.ParseUint(strings.TrimSpace(string(countOutput)), 10, 64)
			if err != nil {
				return "", errors.New("Git returned an invalid prerequisite distance")
			}
			if bestOID == "" || distance < bestDistance || distance == bestDistance && common < bestOID {
				bestOID = common
				bestDistance = distance
			}
		}
	}
	return bestOID, nil
}

func (r Runner) ApplyBundle(
	ctx context.Context,
	repositoryPath, bundlePath string,
	manifest protocol.WorkspaceManifest,
) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if !manifest.Clean {
		return errors.New("clean bundle transport cannot apply a dirty source")
	}
	if !filepath.IsAbs(repositoryPath) || !filepath.IsAbs(bundlePath) {
		return errors.New("bundle paths must be absolute")
	}
	createdRepository := false
	succeeded := false
	defer func() {
		if createdRepository && !succeeded {
			_ = os.RemoveAll(repositoryPath)
		}
	}()
	if _, err := os.Lstat(repositoryPath); errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(repositoryPath, 0o700); err != nil {
			return fmt.Errorf("create bundle repository: %w", err)
		}
		createdRepository = true
		if err := r.run(ctx, repositoryPath, "init", "--object-format="+manifest.ObjectFormat); err != nil {
			return preserveContextError(err, errors.New("initialize bundle repository"))
		}
		if err := r.run(ctx, repositoryPath, "config", "remote.origin.url", manifest.GitURL); err != nil {
			return preserveContextError(err, errors.New("configure explicit source Git URL"))
		}
	} else if err != nil {
		return fmt.Errorf("inspect bundle repository: %w", err)
	}
	if err := r.run(ctx, repositoryPath, "config", "core.hooksPath", disabledHooksPath()); err != nil {
		return preserveContextError(err, errors.New("disable target Git hooks"))
	}
	actualFormat, err := r.output(ctx, repositoryPath, "rev-parse", "--show-object-format")
	if err != nil || strings.TrimSpace(string(actualFormat)) != manifest.ObjectFormat {
		return preserveContextError(err, errors.New("bundle repository object format does not match source"))
	}
	if err := r.run(ctx, repositoryPath, "bundle", "verify", bundlePath); err != nil {
		return preserveContextError(err, errors.New("verify target Git bundle"))
	}
	heads, err := r.output(ctx, repositoryPath, "bundle", "list-heads", bundlePath, "HEAD")
	if err != nil {
		return preserveContextError(err, errors.New("inspect target Git bundle head"))
	}
	fields := strings.Fields(string(heads))
	if len(fields) != 2 || fields[0] != manifest.HeadOID || fields[1] != "HEAD" {
		return errors.New("target Git bundle does not advertise the pinned HEAD")
	}
	if err := r.run(
		ctx,
		repositoryPath,
		"-c", "protocol.allow=never",
		"-c", "protocol.file.allow=always",
		"fetch", "--no-tags", "--no-write-fetch-head", "--", bundlePath, "HEAD",
	); err != nil {
		return preserveContextError(err, errors.New("import target Git bundle"))
	}
	if err := r.run(ctx, repositoryPath, "checkout", "--detach", "--force", manifest.HeadOID); err != nil {
		return preserveContextError(err, errors.New("check out bundled source HEAD"))
	}
	if err := r.VerifyDirect(ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat); err != nil {
		return err
	}
	if err := validateWorkingDirectory(repositoryPath, manifest.WorkingDirectory); err != nil {
		return err
	}
	succeeded = true
	return nil
}
