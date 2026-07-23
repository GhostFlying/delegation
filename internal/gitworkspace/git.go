package gitworkspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

const (
	commandTimeout      = 2 * time.Minute
	cloneCommandTimeout = 4 * time.Minute
	commandWaitDelay    = 5 * time.Second
	maximumOutput       = 4 * 1024 * 1024
)

var scpURLPattern = regexp.MustCompile(`^(?:[A-Za-z0-9._][A-Za-z0-9._-]*@)?[A-Za-z0-9][A-Za-z0-9.-]*:[^:\\].*$`)
var windowsDrivePathPattern = regexp.MustCompile(`^[A-Za-z]:`)

var (
	ErrBundleRequired = errors.New("workspace requires Git bundle transport")
	errCommandTimeout = errors.New("Git command exceeded its time limit")
)

type Repository struct {
	Root     string
	Manifest protocol.WorkspaceManifest
}

type BasePreparation struct {
	BundleRequired  bool
	OverlayRequired bool
	BasisOIDs       []string
}

type Runner struct {
	Binary              string
	excludedEnvironment []string
}

func NewRunner(binary string, excludedEnvironment ...string) (Runner, error) {
	if !filepath.IsAbs(binary) {
		return Runner{}, errors.New("Git binary must be an absolute path")
	}
	info, err := os.Stat(binary)
	if err != nil {
		return Runner{}, fmt.Errorf("inspect Git binary: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Runner{}, errors.New("Git binary must be a regular file")
	}
	resolved, err := filepath.EvalSymlinks(binary)
	if err != nil {
		return Runner{}, fmt.Errorf("resolve Git binary: %w", err)
	}
	return Runner{
		Binary:              resolved,
		excludedEnvironment: append([]string(nil), excludedEnvironment...),
	}, nil
}

func ValidateRemoteURL(raw string) error {
	if len(raw) == 0 || len(raw) > protocol.MaximumGitURLBytes ||
		strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "\x00\r\n") {
		return errors.New("Git URL must be bounded text without surrounding whitespace")
	}
	if windowsDrivePathPattern.MatchString(raw) {
		return errors.New("Git URL must not be a Windows drive path")
	}
	if scpURLPattern.MatchString(raw) && !strings.Contains(raw, "://") {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Opaque != "" || parsed.Hostname() == "" {
		return errors.New("Git URL must be an absolute SSH or HTTPS URL")
	}
	if strings.HasPrefix(parsed.Hostname(), "-") {
		return errors.New("Git URL host must not begin with a dash")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("Git URL must not contain a query or fragment")
	}
	switch parsed.Scheme {
	case "https":
		if parsed.User != nil {
			return errors.New("HTTPS Git URL must not contain credentials")
		}
	case "ssh":
		if parsed.User != nil {
			if _, hasPassword := parsed.User.Password(); hasPassword {
				return errors.New("SSH Git URL must not contain a password")
			}
			if strings.HasPrefix(parsed.User.Username(), "-") {
				return errors.New("SSH Git username must not begin with a dash")
			}
		}
	default:
		return errors.New("Git URL must use ssh://, SCP-style SSH, or https://")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return errors.New("Git URL must identify a repository path")
	}
	return nil
}

func (r Runner) Inspect(ctx context.Context, sourcePath, gitURL string) (Repository, error) {
	if err := ValidateRemoteURL(gitURL); err != nil {
		return Repository{}, err
	}
	if !filepath.IsAbs(sourcePath) {
		return Repository{}, errors.New("source workspace path must be absolute")
	}
	resolvedSource, err := filepath.EvalSymlinks(sourcePath)
	if err != nil {
		return Repository{}, fmt.Errorf("resolve source workspace: %w", err)
	}
	rootOutput, err := r.output(ctx, sourcePath, "rev-parse", "--show-toplevel")
	if err != nil {
		return Repository{}, preserveContextError(
			err, errors.New("source workspace is not a readable Git worktree"),
		)
	}
	root := strings.TrimSpace(string(rootOutput))
	if !filepath.IsAbs(root) {
		return Repository{}, errors.New("Git returned a non-absolute worktree root")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Repository{}, fmt.Errorf("resolve Git worktree root: %w", err)
	}
	relative, err := filepath.Rel(resolvedRoot, resolvedSource)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return Repository{}, errors.New("source cwd is outside its Git worktree")
	}
	if relative == "." {
		relative = ""
	}
	head, err := r.output(ctx, resolvedRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return Repository{}, preserveContextError(err, errors.New("source workspace has no commit at HEAD"))
	}
	headOID := strings.TrimSpace(string(head))
	objectFormatOutput, err := r.output(ctx, resolvedRoot, "rev-parse", "--show-object-format")
	if err != nil {
		return Repository{}, preserveContextError(
			err, errors.New("source Git does not report its object format"),
		)
	}
	objectFormat := strings.TrimSpace(string(objectFormatOutput))
	if err := r.ensureSafeSourceConfig(ctx, resolvedRoot); err != nil {
		return Repository{}, err
	}
	status, err := r.output(
		ctx, resolvedRoot, "status", "--porcelain=v2", "-z",
		"--untracked-files=normal", "--ignore-submodules=all",
	)
	if err != nil {
		return Repository{}, preserveContextError(err, errors.New("inspect source Git status"))
	}
	clean := len(status) == 0
	warnings := make([]string, 0, 2)
	if hasSubmodules, checkErr := r.hasSubmodules(ctx, resolvedRoot); checkErr != nil {
		return Repository{}, checkErr
	} else if hasSubmodules {
		warnings = append(warnings, "submodule_repository_not_transferred")
	}
	if hasLFS, checkErr := r.hasLFS(ctx, resolvedRoot); checkErr != nil {
		return Repository{}, checkErr
	} else if hasLFS {
		warnings = append(warnings, "lfs_payload_not_transferred")
	}
	slices.Sort(warnings)
	snapshot := sha256.Sum256([]byte(strings.Join([]string{
		headOID, objectFormat, filepath.ToSlash(relative), fmt.Sprintf("clean=%t", clean),
	}, "\x00")))
	manifest := protocol.WorkspaceManifest{
		GitURL: gitURL, HeadOID: headOID, ObjectFormat: objectFormat,
		WorkingDirectory: filepath.ToSlash(relative), Clean: clean,
		SourceSnapshotHash: hex.EncodeToString(snapshot[:]), Warnings: warnings,
	}
	if err := manifest.Validate(); err != nil {
		return Repository{}, fmt.Errorf("source workspace manifest: %w", err)
	}
	return Repository{Root: resolvedRoot, Manifest: manifest}, nil
}

func (r Runner) CloneDirect(ctx context.Context, destination string, manifest protocol.WorkspaceManifest) error {
	prepared, err := r.PrepareBase(ctx, destination, manifest)
	if err != nil {
		return err
	}
	if prepared.BundleRequired || prepared.OverlayRequired {
		_ = os.RemoveAll(destination)
		return ErrBundleRequired
	}
	return nil
}

func (r Runner) PrepareBase(
	ctx context.Context,
	destination string,
	manifest protocol.WorkspaceManifest,
) (BasePreparation, error) {
	if err := manifest.Validate(); err != nil {
		return BasePreparation{}, err
	}
	if err := ValidateRemoteURL(manifest.GitURL); err != nil {
		return BasePreparation{}, err
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return BasePreparation{}, errors.New("clone destination already exists")
		}
		return BasePreparation{}, fmt.Errorf("inspect clone destination: %w", err)
	}
	parent := filepath.Dir(destination)
	if err := os.Mkdir(destination, 0o700); err != nil {
		return BasePreparation{}, fmt.Errorf("create clone destination: %w", err)
	}
	remove := true
	defer func() {
		if remove {
			_ = os.RemoveAll(destination)
		}
	}()
	args := []string{
		"-c", "protocol.allow=never",
		"-c", "protocol.https.allow=always",
		"-c", "protocol.ssh.allow=always",
		"clone", "--no-checkout", "--no-recurse-submodules", "--", manifest.GitURL, destination,
	}
	if err := r.runWithTimeout(ctx, cloneCommandTimeout, parent, args...); err != nil {
		if isContextError(err) {
			return BasePreparation{}, err
		}
		return BasePreparation{
			BundleRequired: true, OverlayRequired: !manifest.Clean,
		}, nil
	}
	if err := r.run(ctx, destination, "config", "core.hooksPath", disabledHooksPath()); err != nil {
		return BasePreparation{}, preserveContextError(err, errors.New("disable target Git hooks"))
	}
	actualFormat, err := r.output(ctx, destination, "rev-parse", "--show-object-format")
	if err != nil {
		return BasePreparation{}, preserveContextError(err, errors.New("inspect target Git object format"))
	}
	if strings.TrimSpace(string(actualFormat)) != manifest.ObjectFormat {
		return BasePreparation{
			BundleRequired: true, OverlayRequired: !manifest.Clean,
		}, nil
	}
	if err := r.run(ctx, destination, "cat-file", "-e", manifest.HeadOID+"^{commit}"); err != nil {
		if isContextError(err) {
			return BasePreparation{}, err
		}
		basisOIDs, basisErr := r.bundleBasisOIDs(ctx, destination)
		if basisErr != nil {
			return BasePreparation{}, basisErr
		}
		remove = false
		return BasePreparation{
			BundleRequired: true, OverlayRequired: !manifest.Clean, BasisOIDs: basisOIDs,
		}, nil
	}
	if err := r.run(ctx, destination, "checkout", "--detach", "--force", manifest.HeadOID); err != nil {
		return BasePreparation{}, preserveContextError(err, errors.New("check out exact source HEAD on target"))
	}
	actualHead, err := r.output(ctx, destination, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return BasePreparation{}, preserveContextError(err, errors.New("target checkout does not match the exact source HEAD"))
	}
	if strings.TrimSpace(string(actualHead)) != manifest.HeadOID {
		return BasePreparation{}, errors.New("target checkout does not match the exact source HEAD")
	}
	status, err := r.output(ctx, destination, "status", "--porcelain=v2", "-z", "--untracked-files=normal")
	if err != nil {
		return BasePreparation{}, preserveContextError(err, errors.New("target direct clone is not clean"))
	}
	if len(status) != 0 {
		return BasePreparation{}, errors.New("target direct clone is not clean")
	}
	if err := validateWorkingDirectory(destination, manifest.WorkingDirectory); errors.Is(err, os.ErrNotExist) {
		if manifest.Clean {
			return BasePreparation{}, errors.New("target working directory is absent from the exact source HEAD")
		}
	} else if err != nil {
		return BasePreparation{}, err
	}
	remove = false
	return BasePreparation{OverlayRequired: !manifest.Clean}, nil
}

func (r Runner) VerifyDirect(
	ctx context.Context,
	repositoryPath, headOID, objectFormat string,
) error {
	if !filepath.IsAbs(repositoryPath) || !gitObjectID(headOID) {
		return errors.New("direct workspace verification input is invalid")
	}
	actualFormat, err := r.output(ctx, repositoryPath, "rev-parse", "--show-object-format")
	if err != nil {
		return preserveContextError(err, errors.New("prepared workspace object format changed"))
	}
	if strings.TrimSpace(string(actualFormat)) != objectFormat {
		return errors.New("prepared workspace object format changed")
	}
	actualHead, err := r.output(ctx, repositoryPath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return preserveContextError(err, errors.New("prepared workspace HEAD changed"))
	}
	if strings.TrimSpace(string(actualHead)) != headOID {
		return errors.New("prepared workspace HEAD changed")
	}
	status, err := r.output(ctx, repositoryPath, "status", "--porcelain=v2", "-z", "--untracked-files=normal")
	if err != nil {
		return preserveContextError(err, errors.New("prepared workspace is no longer clean"))
	}
	if len(status) != 0 {
		return errors.New("prepared workspace is no longer clean")
	}
	return nil
}

func ManifestHash(manifest protocol.WorkspaceManifest) (string, error) {
	return protocol.WorkspaceManifestHash(manifest)
}

func (r Runner) hasSubmodules(ctx context.Context, root string) (bool, error) {
	output, err := r.output(ctx, root, "ls-files", "--stage")
	if err != nil {
		return false, preserveContextError(err, errors.New("inspect source Git index modes"))
	}
	return bytes.Contains(output, []byte("160000 ")), nil
}

func (r Runner) hasLFS(ctx context.Context, root string) (bool, error) {
	output, err := r.output(ctx, root, "ls-files", "-z")
	if err != nil {
		return false, preserveContextError(err, errors.New("list source Git index"))
	}
	if len(output) == 0 {
		return false, nil
	}
	attributes, err := r.outputWithInput(
		ctx, root, output, "check-attr", "--cached", "-z", "--stdin", "filter",
	)
	if err != nil {
		return false, preserveContextError(err, errors.New("inspect source Git LFS attributes"))
	}
	fields := bytes.Split(bytes.TrimSuffix(attributes, []byte{0}), []byte{0})
	for index := 2; index < len(fields); index += 3 {
		if bytes.Equal(fields[index], []byte("lfs")) {
			return true, nil
		}
	}
	return false, nil
}

func (r Runner) run(ctx context.Context, directory string, args ...string) error {
	_, err := r.command(ctx, commandTimeout, directory, nil, args...)
	return err
}

func (r Runner) runWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	directory string,
	args ...string,
) error {
	_, err := r.command(ctx, timeout, directory, nil, args...)
	return err
}

func (r Runner) runDiscardingOutput(ctx context.Context, directory string, args ...string) error {
	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	command := exec.CommandContext(commandContext, r.Binary, safeGitArgs(args)...)
	command.WaitDelay = commandWaitDelay
	command.Dir = directory
	command.Env = hardenedEnvironment(r.excludedEnvironment)
	command.Stdout = io.Discard
	var stderr limitedBuffer
	command.Stderr = &stderr
	err := command.Run()
	if commandContext.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errCommandTimeout
	}
	if err != nil {
		return err
	}
	if stderr.full {
		return errors.New("Git command error output exceeded the configured limit")
	}
	return nil
}

func (r Runner) output(ctx context.Context, directory string, args ...string) ([]byte, error) {
	return r.command(ctx, commandTimeout, directory, nil, args...)
}

func (r Runner) outputWithInput(
	ctx context.Context,
	directory string,
	input []byte,
	args ...string,
) ([]byte, error) {
	return r.command(ctx, commandTimeout, directory, input, args...)
}

func (r Runner) command(
	ctx context.Context,
	timeout time.Duration,
	directory string,
	input []byte,
	args ...string,
) ([]byte, error) {
	commandContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(commandContext, r.Binary, safeGitArgs(args)...)
	command.WaitDelay = commandWaitDelay
	command.Dir = directory
	command.Env = hardenedEnvironment(r.excludedEnvironment)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	var output limitedBuffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	if commandContext.Err() != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errCommandTimeout
	}
	if err != nil {
		return nil, err
	}
	if output.full {
		return nil, errors.New("Git command output exceeded the configured limit")
	}
	return output.Bytes(), nil
}

func safeGitArgs(args []string) []string {
	safeArgs := []string{
		"-c", "core.fsmonitor=",
		"-c", "core.hooksPath=" + disabledHooksPath(),
	}
	if runtime.GOOS == "windows" {
		safeArgs = append(safeArgs, "-c", "core.longpaths=true")
	}
	return append(safeArgs, args...)
}

func (r Runner) ensureSafeSourceConfig(ctx context.Context, root string) error {
	scopes := []string{"--local"}
	worktreeConfig, found, err := r.configValue(
		ctx, root, "--local", "--bool", "--get", "extensions.worktreeConfig",
	)
	if err != nil {
		return preserveContextError(err, errors.New("inspect source Git worktree config mode"))
	}
	if found && strings.TrimSpace(string(worktreeConfig)) == "true" {
		scopes = append(scopes, "--worktree")
	}
	for _, scope := range scopes {
		filters, _, err := r.configValue(
			ctx, root, scope, "--null", "--name-only", "--get-regexp",
			`^filter\..*\.(clean|process)$`,
		)
		if err != nil {
			return preserveContextError(
				err, errors.New("inspect source repository executable filters"),
			)
		}
		if len(filters) != 0 {
			return errors.New("source repository config must not define executable clean or process filters")
		}
	}
	return nil
}

func (r Runner) configValue(
	ctx context.Context,
	root, scope string,
	args ...string,
) ([]byte, bool, error) {
	commandArgs := append([]string{"config", scope, "--includes"}, args...)
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

type limitedBuffer struct {
	buffer bytes.Buffer
	full   bool
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	remaining := maximumOutput - b.buffer.Len()
	if remaining <= 0 {
		b.full = true
		return original, nil
	}
	if len(data) > remaining {
		data = data[:remaining]
		b.full = true
	}
	_, _ = b.buffer.Write(data)
	return original, nil
}

func (b *limitedBuffer) Bytes() []byte {
	return b.buffer.Bytes()
}

func hardenedEnvironment(excluded []string) []string {
	environment := make([]string, 0, len(os.Environ())+4)
	for _, variable := range os.Environ() {
		name, _, _ := strings.Cut(variable, "=")
		if strings.HasPrefix(strings.ToUpper(name), "GIT_") ||
			strings.EqualFold(name, "GCM_INTERACTIVE") || environmentNameListed(name, excluded) {
			continue
		}
		environment = append(environment, variable)
	}
	environment = setEnvironment(environment, "GIT_TERMINAL_PROMPT", "0")
	environment = setEnvironment(environment, "GCM_INTERACTIVE", "Never")
	environment = setEnvironment(environment, "GIT_LFS_SKIP_SMUDGE", "1")
	environment = setEnvironment(environment, "GIT_SSH_COMMAND", "ssh -o BatchMode=yes")
	return environment
}

func environmentNameListed(name string, names []string) bool {
	for _, candidate := range names {
		if name == candidate || runtime.GOOS == "windows" && strings.EqualFold(name, candidate) {
			return true
		}
	}
	return false
}

func validateWorkingDirectory(repositoryPath, workingDirectory string) error {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open target repository: %w", err)
	}
	defer root.Close()
	partial := ""
	for _, component := range strings.Split(workingDirectory, "/") {
		if component == "" {
			continue
		}
		partial = filepath.Join(partial, component)
		info, err := root.Lstat(partial)
		if err != nil {
			return fmt.Errorf("inspect target working directory: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("target working directory must contain only real directories")
		}
	}
	return nil
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func preserveContextError(err, fallback error) error {
	if isContextError(err) {
		return err
	}
	return fallback
}

func setEnvironment(environment []string, name, value string) []string {
	for index := len(environment) - 1; index >= 0; index-- {
		currentName, _, _ := strings.Cut(environment[index], "=")
		if currentName == name || runtime.GOOS == "windows" && strings.EqualFold(currentName, name) {
			environment = append(environment[:index], environment[index+1:]...)
		}
	}
	return append(environment, name+"="+value)
}

func gitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' && char < 'a' || char > 'f' {
			return false
		}
	}
	return true
}

func disabledHooksPath() string {
	if runtime.GOOS == "windows" {
		return "NUL"
	}
	return "/dev/null"
}
