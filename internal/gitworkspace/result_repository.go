package gitworkspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const maximumManagedGitObjectEntries = 1_000_000

func (r Runner) validateManagedResultRepository(ctx context.Context, repositoryPath string) error {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open managed workspace: %w", err)
	}
	defer root.Close()
	gitInfo, err := root.Lstat(".git")
	if err != nil || !gitInfo.IsDir() || gitInfo.Mode()&os.ModeSymlink != 0 {
		return preserveContextError(
			err, errors.New("managed workspace must use a standalone real .git directory"),
		)
	}
	gitRoot, err := root.OpenRoot(".git")
	if err != nil {
		return fmt.Errorf("open managed workspace Git directory: %w", err)
	}
	defer gitRoot.Close()
	openedGitDirectory, err := gitRoot.Open(".")
	if err != nil {
		return fmt.Errorf("reopen managed workspace Git directory: %w", err)
	}
	openedGitInfo, statErr := openedGitDirectory.Stat()
	closeErr := openedGitDirectory.Close()
	if statErr != nil {
		return fmt.Errorf("inspect reopened managed workspace Git directory: %w", statErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close reopened managed workspace Git directory: %w", closeErr)
	}
	if !os.SameFile(gitInfo, openedGitInfo) {
		return errors.New("managed workspace Git directory changed while it was opened")
	}
	if err := validateManagedGitMetadata(gitRoot); err != nil {
		return err
	}
	if err := r.ensureManagedResultConfig(ctx, repositoryPath); err != nil {
		return err
	}
	if err := requireRepositoryRoot(ctx, r, repositoryPath); err != nil {
		return err
	}
	for _, check := range []struct {
		argument string
		path     string
		label    string
	}{
		{argument: "--git-dir", path: filepath.Join(repositoryPath, ".git"), label: "Git directory"},
		{argument: "--git-common-dir", path: filepath.Join(repositoryPath, ".git"), label: "Git common directory"},
		{argument: "--git-path", path: filepath.Join(repositoryPath, ".git", "objects"), label: "Git object directory"},
	} {
		args := []string{"rev-parse", check.argument}
		if check.argument == "--git-path" {
			args = append(args, "objects")
		}
		if err := requireReportedGitPath(ctx, r, repositoryPath, check.path, check.label, args...); err != nil {
			return err
		}
	}
	shallow, err := r.output(ctx, repositoryPath, "rev-parse", "--is-shallow-repository")
	if err != nil {
		return preserveContextError(err, errors.New("inspect managed workspace shallow state"))
	}
	if strings.TrimSpace(string(shallow)) != "false" {
		return errors.New("managed workspace must not use shallow Git history")
	}
	return nil
}

func validateManagedGitMetadata(root *os.Root) error {
	for _, entry := range []struct {
		name      string
		directory bool
		required  bool
	}{
		{name: "config", required: true},
		{name: "HEAD", required: true},
		{name: "index", required: true},
		{name: "objects", directory: true, required: true},
		{name: "refs", directory: true, required: true},
		{name: "info", directory: true},
		{name: "objects/info", directory: true},
		{name: "objects/pack", directory: true},
		{name: "config.worktree"},
		{name: "packed-refs"},
	} {
		info, err := root.Lstat(filepath.FromSlash(entry.name))
		if errors.Is(err, os.ErrNotExist) && !entry.required {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect managed Git metadata %s: %w", entry.name, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || entry.directory != info.IsDir() ||
			!entry.directory && !info.Mode().IsRegular() {
			return fmt.Errorf("managed Git metadata %s must be a real %s", entry.name, metadataKind(entry.directory))
		}
	}
	for _, name := range []string{
		"gitdir",
		"commondir",
		"shallow",
		"info/grafts",
		"objects/info/alternates",
		"objects/info/http-alternates",
	} {
		if _, err := root.Lstat(filepath.FromSlash(name)); err == nil {
			return fmt.Errorf("managed Git metadata must not contain %s", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect managed Git metadata %s: %w", name, err)
		}
	}
	if err := validateManagedGitObjectStorage(root); err != nil {
		return err
	}
	return nil
}

func validateManagedGitObjectStorage(root *os.Root) error {
	objects, err := root.Open("objects")
	if err != nil {
		return fmt.Errorf("open managed Git object directory: %w", err)
	}
	defer objects.Close()
	entries, err := objects.ReadDir(259)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("inspect managed Git object directory: %w", err)
	}
	if len(entries) > 258 {
		return errors.New("managed Git object directory contains too many root entries")
	}
	total := 0
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || !validGitObjectDirectoryName(entry.Name()) {
			return fmt.Errorf("managed Git object directory contains invalid entry %s", entry.Name())
		}
		directory := filepath.Join("objects", entry.Name())
		if err := validateManagedGitObjectStorageDirectory(
			root, directory, entry.Name(), entry.Name() == "info", &total,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateManagedGitObjectStorageDirectory(
	root *os.Root,
	directory, label string,
	allowSplitCommitGraphs bool,
	total *int,
) error {
	files, err := root.Open(directory)
	if err != nil {
		return fmt.Errorf("open managed Git object storage %s: %w", label, err)
	}
	for {
		batch, readErr := files.ReadDir(256)
		for _, file := range batch {
			*total = *total + 1
			if *total > maximumManagedGitObjectEntries {
				_ = files.Close()
				return fmt.Errorf(
					"managed Git object storage exceeds %d entries", maximumManagedGitObjectEntries,
				)
			}
			info, infoErr := file.Info()
			if infoErr != nil {
				_ = files.Close()
				return fmt.Errorf("inspect managed Git object storage %s entry: %w", label, infoErr)
			}
			if allowSplitCommitGraphs && file.Name() == "commit-graphs" &&
				info.Mode()&os.ModeSymlink == 0 && info.IsDir() {
				if err := validateManagedGitObjectStorageDirectory(
					root, filepath.Join(directory, file.Name()), label+"/"+file.Name(), false, total,
				); err != nil {
					_ = files.Close()
					return err
				}
				continue
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				_ = files.Close()
				return fmt.Errorf(
					"managed Git object storage %s must contain only real regular files", label,
				)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = files.Close()
			return fmt.Errorf("inspect managed Git object storage %s: %w", label, readErr)
		}
	}
	if err := files.Close(); err != nil {
		return fmt.Errorf("close managed Git object storage %s: %w", label, err)
	}
	return nil
}

func validGitObjectDirectoryName(name string) bool {
	if name == "info" || name == "pack" {
		return true
	}
	if len(name) != 2 {
		return false
	}
	for _, character := range name {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func metadataKind(directory bool) string {
	if directory {
		return "directory"
	}
	return "regular file"
}

func (r Runner) ensureManagedResultConfig(ctx context.Context, root string) error {
	scopes := []string{"--local"}
	worktreeConfig, found, err := r.configValueWithoutIncludes(
		ctx, root, "--local", "--bool", "--get", "extensions.worktreeConfig",
	)
	if err != nil {
		return preserveContextError(err, errors.New("inspect managed Git worktree config mode"))
	}
	if found && strings.TrimSpace(string(worktreeConfig)) == "true" {
		scopes = append(scopes, "--worktree")
	}
	for _, scope := range scopes {
		includes, _, err := r.configValueWithoutIncludes(
			ctx, root, scope, "--null", "--name-only", "--get-regexp", `^include.*\.path$`,
		)
		if err != nil {
			return preserveContextError(err, errors.New("inspect managed Git config includes"))
		}
		if len(includes) != 0 {
			return errors.New("managed Git config must not include external config files")
		}
	}
	for _, setting := range deterministicTargetGitSettings() {
		value, found, err := r.configValueWithoutIncludes(
			ctx, root, "--local", "--get-all", setting.key,
		)
		if err != nil {
			return preserveContextError(err, fmt.Errorf("inspect managed Git config %s", setting.key))
		}
		if !found || string(value) != setting.value+"\n" {
			return fmt.Errorf("managed Git checkout config drifted at %s", setting.key)
		}
		if len(scopes) == 2 {
			_, overridden, err := r.configValueWithoutIncludes(
				ctx, root, "--worktree", "--get-all", setting.key,
			)
			if err != nil {
				return preserveContextError(err, fmt.Errorf("inspect managed worktree Git config %s", setting.key))
			}
			if overridden {
				return fmt.Errorf("managed Git worktree config must not override %s", setting.key)
			}
		}
	}
	for _, scope := range scopes {
		promisor, _, err := r.configValueWithoutIncludes(
			ctx, root, scope, "--null", "--name-only", "--get-regexp",
			`^(extensions\.partialclone|remote\..*\.(promisor|partialclonefilter))$`,
		)
		if err != nil {
			return preserveContextError(err, errors.New("inspect managed Git partial clone config"))
		}
		if len(promisor) != 0 {
			return errors.New("managed workspace must not use partial clone object fetching")
		}
	}
	return nil
}

func (r Runner) configValueWithoutIncludes(
	ctx context.Context,
	root string,
	args ...string,
) ([]byte, bool, error) {
	commandArgs := append([]string{"config", "--no-includes"}, args...)
	output, err := r.output(ctx, root, commandArgs...)
	if err == nil {
		return output, true, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return nil, false, nil
	}
	return nil, false, err
}

func requireRepositoryRoot(ctx context.Context, r Runner, repositoryPath string) error {
	rootOutput, err := r.output(ctx, repositoryPath, "rev-parse", "--show-toplevel")
	if err != nil {
		return preserveContextError(err, errors.New("managed workspace is not a readable Git worktree"))
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(rootOutput)))
	if err != nil {
		return fmt.Errorf("resolve managed Git worktree root: %w", err)
	}
	wantInfo, err := os.Stat(repositoryPath)
	if err != nil {
		return fmt.Errorf("inspect managed workspace: %w", err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("inspect managed Git worktree root: %w", err)
	}
	if !os.SameFile(wantInfo, rootInfo) {
		return errors.New("managed workspace path must be its Git worktree root")
	}
	return nil
}

func requireReportedGitPath(
	ctx context.Context,
	r Runner,
	repositoryPath, expectedPath, label string,
	args ...string,
) error {
	output, err := r.output(ctx, repositoryPath, args...)
	if err != nil {
		return preserveContextError(err, fmt.Errorf("inspect managed workspace %s", label))
	}
	reportedPath := strings.TrimSpace(string(output))
	if !filepath.IsAbs(reportedPath) {
		reportedPath = filepath.Join(repositoryPath, reportedPath)
	}
	expectedInfo, err := os.Stat(expectedPath)
	if err != nil {
		return fmt.Errorf("inspect expected managed workspace %s: %w", label, err)
	}
	reportedInfo, err := os.Stat(filepath.Clean(reportedPath))
	if err != nil {
		return fmt.Errorf("inspect reported managed workspace %s: %w", label, err)
	}
	if !os.SameFile(expectedInfo, reportedInfo) {
		return fmt.Errorf("managed workspace %s must remain inside its standalone .git directory", label)
	}
	return nil
}
