package gitworkspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/workspaceoverlay"
)

// ErrRootApplyConflict reports a root state that cannot be overwritten safely.
var ErrRootApplyConflict = errors.New("root workspace conflicts with the result snapshot")

// InspectApplySource derives the explicit origin URL from the trusted cwd and
// rejects repository state that would make a root-local apply ambiguous.
func (r Runner) InspectApplySource(ctx context.Context, sourcePath string) (Repository, error) {
	if !filepath.IsAbs(sourcePath) || filepath.Clean(sourcePath) != sourcePath {
		return Repository{}, errors.New("root apply source path must be absolute")
	}
	layout, err := r.inspectApplyLayout(ctx, sourcePath)
	if err != nil {
		return Repository{}, err
	}
	worktreeConfig, err := r.preflightApplyState(ctx, layout)
	if err != nil {
		return Repository{}, err
	}
	origins, err := r.explicitConfigValues(
		ctx, layout.Root, layout.ConfigPath, "--get-all", "remote.origin.url",
	)
	if err != nil {
		return Repository{}, preserveContextError(err, errors.New("inspect root Git origin URL"))
	}
	if worktreeConfig != "" {
		worktreeOrigins, err := r.explicitConfigValues(
			ctx, layout.Root, worktreeConfig, "--get-all", "remote.origin.url",
		)
		if err != nil {
			return Repository{}, preserveContextError(err, errors.New("inspect root worktree Git origin URL"))
		}
		if len(worktreeOrigins) != 0 {
			origins = worktreeOrigins
		}
	}
	if len(origins) != 1 {
		return Repository{}, errors.New("root Git origin URL is unavailable")
	}
	gitURL := origins[0]
	if strings.ContainsRune(gitURL, '\n') || ValidateRemoteURL(gitURL) != nil {
		return Repository{}, errors.New("root Git origin URL is invalid")
	}
	repository, err := r.forIsolatedTarget().Inspect(ctx, sourcePath, gitURL)
	if err != nil {
		return Repository{}, err
	}
	if !sameFilePath(repository.Root, layout.Root) {
		return Repository{}, errors.New("root Git worktree changed during apply inspection")
	}
	if _, err := r.preflightApplyState(ctx, layout); err != nil {
		return Repository{}, err
	}
	return repository, nil
}

type applyRepositoryLayout struct {
	Root       string
	GitDir     string
	CommonDir  string
	ConfigPath string
}

// CreateApplyBundle captures root history without consulting system or global
// Git configuration.
func (r Runner) CreateApplyBundle(
	ctx context.Context,
	repositoryPath, destination string,
	manifest protocol.WorkspaceManifest,
) (protocol.WorkspaceStrategy, error) {
	return r.forIsolatedTarget().CreateBundle(ctx, repositoryPath, destination, manifest, nil)
}

// CreateApplyOverlay captures a private staging snapshot without consulting
// system or global Git configuration.
func (r Runner) CreateApplyOverlay(
	ctx context.Context,
	repositoryPath, destination string,
	manifest protocol.WorkspaceManifest,
) error {
	return r.forIsolatedTarget().CreateOverlay(ctx, repositoryPath, destination, manifest)
}

// InspectApplyStaging inspects a connector-owned private reconstruction while
// excluding host Git configuration. The staging checkout itself is not a
// user-owned root and therefore does not use root preflight policy.
func (r Runner) InspectApplyStaging(
	ctx context.Context,
	sourcePath, gitURL string,
) (Repository, error) {
	return r.forIsolatedTarget().Inspect(ctx, sourcePath, gitURL)
}

func (r Runner) inspectApplyLayout(
	ctx context.Context,
	sourcePath string,
) (applyRepositoryLayout, error) {
	output, err := r.forIsolatedTarget().output(
		ctx, sourcePath,
		"rev-parse", "--path-format=absolute", "--show-toplevel", "--absolute-git-dir", "--git-common-dir",
	)
	if err != nil {
		return applyRepositoryLayout{}, preserveContextError(
			err, errors.New("root apply source is not a readable Git worktree"),
		)
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) != 3 {
		return applyRepositoryLayout{}, errors.New("Git returned invalid root apply paths")
	}
	paths := make([]string, len(lines))
	for index, path := range lines {
		if !filepath.IsAbs(path) {
			return applyRepositoryLayout{}, errors.New("Git returned non-absolute root apply paths")
		}
		resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
		if err != nil {
			return applyRepositoryLayout{}, fmt.Errorf("resolve root apply Git path: %w", err)
		}
		info, err := os.Lstat(resolved)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return applyRepositoryLayout{}, errors.New("root apply Git paths must be real directories")
		}
		paths[index] = resolved
	}
	resolvedSource, err := filepath.EvalSymlinks(sourcePath)
	if err != nil || !pathWithin(paths[0], resolvedSource) || !pathWithin(paths[2], paths[1]) {
		return applyRepositoryLayout{}, errors.New("root apply Git paths cross repository authority")
	}
	configPath := filepath.Join(paths[2], "config")
	if err := requireRealApplyFile(configPath, true); err != nil {
		return applyRepositoryLayout{}, err
	}
	return applyRepositoryLayout{
		Root: paths[0], GitDir: paths[1], CommonDir: paths[2], ConfigPath: configPath,
	}, nil
}

func (r Runner) preflightApplyState(
	ctx context.Context,
	layout applyRepositoryLayout,
) (string, error) {
	if err := r.rejectUnsafeApplyConfig(ctx, layout.Root, layout.ConfigPath); err != nil {
		return "", err
	}
	worktreeMode, err := r.explicitConfigValues(
		ctx, layout.Root, layout.ConfigPath, "--bool", "--get-all", "extensions.worktreeConfig",
	)
	if err != nil || len(worktreeMode) > 1 {
		return "", preserveContextError(err, errors.New("inspect root Git worktree config mode"))
	}
	worktreeConfig := ""
	if len(worktreeMode) == 1 && worktreeMode[0] == "true" {
		candidate := filepath.Join(layout.GitDir, "config.worktree")
		if err := requireRealApplyFile(candidate, false); err != nil {
			return "", err
		}
		if _, err := os.Lstat(candidate); err == nil {
			worktreeConfig = candidate
			if err := r.rejectUnsafeApplyConfig(ctx, layout.Root, candidate); err != nil {
				return "", err
			}
		}
	}
	for _, candidate := range []string{
		filepath.Join(layout.CommonDir, "shallow"),
		filepath.Join(layout.GitDir, "shallow"),
	} {
		if _, err := os.Lstat(candidate); err == nil {
			return "", errors.New("root Git repository must not be shallow")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("inspect root Git shallow state: %w", err)
		}
	}
	for _, name := range []string{
		"MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "BISECT_LOG", "rebase-apply",
		"rebase-merge", "sequencer", "index.lock",
	} {
		candidate := filepath.Join(layout.GitDir, name)
		_, err := os.Lstat(filepath.Clean(candidate))
		if err == nil {
			return "", fmt.Errorf("root Git operation state %s is active", name)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("inspect root Git operation state %s: %w", name, err)
		}
	}
	replacements, err := r.forIsolatedTarget().outputWithLimit(
		ctx, layout.Root, maximumGitPathOutput,
		"--no-replace-objects", "for-each-ref", "--format=%(refname)", "refs/replace",
	)
	if err != nil {
		return "", preserveContextError(err, errors.New("inspect root Git replacement refs"))
	}
	if len(replacements) != 0 {
		return "", errors.New("root Git operation state refs/replace is active")
	}
	return worktreeConfig, nil
}

func (r Runner) rejectUnsafeApplyConfig(ctx context.Context, root, configPath string) error {
	keys, err := r.explicitConfigValues(ctx, root, configPath, "--name-only", "--get-regexp", ".*")
	if err != nil {
		return preserveContextError(err, errors.New("inspect root Git apply configuration"))
	}
	for _, key := range keys {
		lower := strings.ToLower(key)
		include := lower == "include.path" ||
			strings.HasPrefix(lower, "includeif.") && strings.HasSuffix(lower, ".path")
		filter := strings.HasPrefix(lower, "filter.") &&
			(strings.HasSuffix(lower, ".clean") || strings.HasSuffix(lower, ".smudge") ||
				strings.HasSuffix(lower, ".process"))
		if include || filter || lower == "core.fsmonitor" || lower == "core.worktree" {
			return errors.New("root Git configuration contains apply-unsafe filters, monitors, or includes")
		}
	}
	return nil
}

func (r Runner) explicitConfigValues(
	ctx context.Context,
	root, configPath string,
	args ...string,
) ([]string, error) {
	commandArgs := []string{"config", "--file", configPath, "--no-includes", "--null"}
	commandArgs = append(commandArgs, args...)
	output, err := r.forIsolatedTarget().output(ctx, root, commandArgs...)
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
			return nil, nil
		}
		return nil, err
	}
	records := strings.Split(string(output), "\x00")
	if len(records) != 0 && records[len(records)-1] == "" {
		records = records[:len(records)-1]
	}
	return records, nil
}

func requireRealApplyFile(path string, required bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) && !required {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("root Git configuration must be a real regular file")
	}
	return nil
}

func sameFilePath(left, right string) bool {
	leftInfo, leftErr := os.Stat(left)
	rightInfo, rightErr := os.Stat(right)
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}

func (r Runner) rootApplyUntrackedPaths(ctx context.Context, repositoryPath string) ([]string, error) {
	output, err := r.forIsolatedTarget().outputWithLimit(
		ctx, repositoryPath, maximumGitPathOutput,
		"ls-files", "--others", "--exclude-standard", "-z", "--",
	)
	if err != nil {
		return nil, preserveContextError(err, errors.New("inspect root untracked paths"))
	}
	paths, err := parseNULPaths(output)
	if err != nil {
		return nil, fmt.Errorf("inspect root untracked paths: %w", err)
	}
	for _, path := range paths {
		if err := workspaceoverlay.ValidatePath(path); err != nil {
			return nil, fmt.Errorf("invalid root untracked path %q: %w", path, err)
		}
	}
	return paths, nil
}

type rootApplyPlan struct {
	untrackedPaths  []string
	transitionPaths []string
}

func (r Runner) prepareRootApplyPlan(
	ctx context.Context,
	repositoryPath string,
	base protocol.WorkspaceManifest,
	desiredEntries []workspaceoverlay.Entry,
) (rootApplyPlan, error) {
	captured, err := r.captureOverlay(
		ctx, repositoryPath, base.HeadOID, base.ObjectFormat, base.WorkingDirectory, "",
	)
	if err != nil || matchCapturedManifest(captured.manifest, base) != nil {
		return rootApplyPlan{}, ErrRootApplyConflict
	}
	transitionPaths := make([]string, 0, len(captured.manifest.Entries)+len(desiredEntries))
	for _, entry := range captured.manifest.Entries {
		transitionPaths = append(transitionPaths, entry.Path)
	}
	for _, entry := range desiredEntries {
		transitionPaths = append(transitionPaths, entry.Path)
	}
	if err := r.rejectIgnoredRootApplyCollisions(ctx, repositoryPath, transitionPaths); err != nil {
		return rootApplyPlan{}, err
	}
	untrackedPaths, err := r.rootApplyUntrackedPaths(ctx, repositoryPath)
	if err != nil {
		return rootApplyPlan{}, err
	}
	return rootApplyPlan{
		untrackedPaths: untrackedPaths, transitionPaths: transitionPaths,
	}, nil
}

func (r Runner) rejectIgnoredRootApplyCollisions(
	ctx context.Context,
	repositoryPath string,
	transitionPaths []string,
) error {
	limit := workspaceoverlay.MaximumTotalPathBytes + workspaceoverlay.MaximumEntries*2
	output, err := r.forIsolatedTarget().outputWithLimit(
		ctx, repositoryPath, limit,
		"ls-files", "--others", "--ignored", "--exclude-standard", "--directory", "-z", "--",
	)
	if err != nil {
		return errors.Join(
			ErrRootApplyConflict,
			preserveContextError(err, errors.New("inspect ignored root paths before apply")),
		)
	}
	ignoredPaths, err := parseNULPaths(output)
	if err != nil || len(ignoredPaths) > workspaceoverlay.MaximumEntries {
		return ErrRootApplyConflict
	}
	transitionKeys := make([]string, 0, len(transitionPaths))
	for _, candidate := range transitionPaths {
		key, err := workspaceoverlay.PortablePathKey(candidate)
		if err != nil {
			return ErrRootApplyConflict
		}
		transitionKeys = append(transitionKeys, key)
	}
	pathBytes := 0
	for _, ignoredPath := range ignoredPaths {
		ignoredPath = strings.TrimSuffix(ignoredPath, "/")
		pathBytes += len(ignoredPath)
		if pathBytes > workspaceoverlay.MaximumTotalPathBytes {
			return ErrRootApplyConflict
		}
		ignoredKey, err := workspaceoverlay.PortablePathKey(ignoredPath)
		if err != nil {
			return ErrRootApplyConflict
		}
		for _, transitionKey := range transitionKeys {
			if pathsOverlap(ignoredKey, transitionKey) {
				return ErrRootApplyConflict
			}
		}
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func removeRootApplyUntracked(repositoryPath string, paths []string) error {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open root worktree for apply cleanup: %w", err)
	}
	defer root.Close()
	for _, path := range paths {
		if err := root.Remove(filepath.FromSlash(path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove retained root untracked path %q: %w", path, err)
		}
	}
	return nil
}

// FlattenStagingResult moves only the private staging HEAD back to base. The
// resulting index keeps worker commits as staged changes while preserving the
// worker's existing staged, unstaged, and untracked states.
func (r Runner) FlattenStagingResult(
	ctx context.Context,
	repositoryPath string,
	base protocol.WorkspaceManifest,
) (Repository, error) {
	if err := base.Validate(); err != nil {
		return Repository{}, err
	}
	r = r.forIsolatedTarget()
	if err := r.run(ctx, repositoryPath, "reset", "--soft", base.HeadOID); err != nil {
		return Repository{}, preserveContextError(err, errors.New("flatten worker result commits"))
	}
	if err := ensureWorkingDirectory(repositoryPath, base.WorkingDirectory); err != nil {
		return Repository{}, fmt.Errorf("preserve root apply working directory: %w", err)
	}
	inspectionPath := filepath.Join(repositoryPath, filepath.FromSlash(base.WorkingDirectory))
	flattened, err := r.Inspect(ctx, inspectionPath, base.GitURL)
	if err != nil {
		return Repository{}, fmt.Errorf("inspect flattened worker result: %w", err)
	}
	if flattened.Manifest.HeadOID != base.HeadOID ||
		flattened.Manifest.ObjectFormat != base.ObjectFormat ||
		flattened.Manifest.WorkingDirectory != base.WorkingDirectory {
		return Repository{}, errors.New("flattened worker result changed base repository authority")
	}
	return flattened, nil
}

// ApplyCleanSnapshotPreservingConfig restores a clean HEAD while leaving
// ignored files and repository-local configuration untouched.
func (r Runner) ApplyCleanSnapshotPreservingConfig(
	ctx context.Context,
	repositoryPath string,
	base, desired protocol.WorkspaceManifest,
	beforeMutation func() error,
) error {
	if err := desired.Validate(); err != nil {
		return err
	}
	if !desired.Clean {
		return errors.New("clean root snapshot must be marked clean")
	}
	r = r.forIsolatedTarget()
	plan, err := r.prepareRootApplyPlan(ctx, repositoryPath, base, nil)
	if err != nil {
		return err
	}
	if err := r.resetOverlayBase(
		ctx, repositoryPath, desired, false, &plan, beforeMutation,
	); err != nil {
		return err
	}
	return ensureWorkingDirectory(repositoryPath, desired.WorkingDirectory)
}

// ApplyRootReconstructionBundle checks out a bundle into private apply staging
// while retaining a real directory for the root task's original cwd.
func (r Runner) ApplyRootReconstructionBundle(
	ctx context.Context,
	repositoryPath, bundlePath string,
	manifest protocol.WorkspaceManifest,
) error {
	return r.applyBundle(
		ctx, repositoryPath, bundlePath, manifest, bundlePreserveWorkingDirectory,
	)
}
