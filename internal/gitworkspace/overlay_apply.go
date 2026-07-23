package gitworkspace

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/workspaceoverlay"
)

const temporaryIntentToAddSymlinkTarget = ".delegation-intent-to-add"

func (r Runner) ApplyOverlay(
	ctx context.Context,
	repositoryPath, archivePath string,
	manifest protocol.WorkspaceManifest,
) error {
	r = r.forIsolatedTarget()
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := ValidateRemoteURL(manifest.GitURL); err != nil {
		return err
	}
	if manifest.Clean {
		return errors.New("workspace overlay cannot apply to a clean source manifest")
	}
	if !filepath.IsAbs(repositoryPath) || !filepath.IsAbs(archivePath) {
		return errors.New("workspace overlay paths must be absolute")
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open workspace overlay archive: %w", err)
	}
	archiveInfo, err := archive.Stat()
	if err != nil || !archiveInfo.Mode().IsRegular() || archiveInfo.Size() < 1 ||
		archiveInfo.Size() > protocol.MaximumWorkspaceArtifactBytes {
		_ = archive.Close()
		return errors.New("workspace overlay archive has an invalid size or type")
	}
	extractPath, err := os.MkdirTemp(filepath.Dir(archivePath), ".overlay-extracted-")
	if err != nil {
		_ = archive.Close()
		return fmt.Errorf("create workspace overlay extraction directory: %w", err)
	}
	defer os.RemoveAll(extractPath)
	extractRoot, err := os.OpenRoot(extractPath)
	if err != nil {
		_ = archive.Close()
		return fmt.Errorf("open workspace overlay extraction directory: %w", err)
	}
	extracted, extractErr := workspaceoverlay.ExtractArchive(ctx, archive, extractRoot)
	closeArchiveErr := archive.Close()
	if extractErr != nil || closeArchiveErr != nil {
		_ = extractRoot.Close()
		return errors.Join(extractErr, closeArchiveErr)
	}
	defer extractRoot.Close()
	if err := matchCapturedManifest(extracted.Manifest, manifest); err != nil {
		return errors.New("workspace overlay does not match the source manifest")
	}
	if len(extracted.Manifest.Entries) == 0 {
		return errors.New("dirty workspace overlay contains no entries")
	}
	if err := r.preflightOverlay(ctx, repositoryPath, extractRoot, extracted); err != nil {
		return err
	}
	if err := r.materializeOverlayIndexObjects(ctx, repositoryPath, extractRoot, extracted); err != nil {
		return err
	}
	if err := r.resetOverlayBase(ctx, repositoryPath, manifest); err != nil {
		return err
	}
	if err := r.replaceOverlayIndex(ctx, repositoryPath, extracted.Manifest); err != nil {
		return err
	}
	if err := clearOverlayWorktree(ctx, repositoryPath, extracted.Manifest); err != nil {
		return err
	}
	if err := r.restoreIntentToAdd(ctx, repositoryPath, extracted.Manifest); err != nil {
		return err
	}
	if err := clearOverlayWorktree(ctx, repositoryPath, extracted.Manifest); err != nil {
		return err
	}
	if err := applyOverlayWorktree(ctx, repositoryPath, extractRoot, extracted); err != nil {
		return err
	}
	if extracted.Manifest.WorkingDirectory != "" {
		root, err := os.OpenRoot(repositoryPath)
		if err != nil {
			return err
		}
		mkdirErr := root.MkdirAll(filepath.FromSlash(extracted.Manifest.WorkingDirectory), 0o700)
		closeErr := root.Close()
		if mkdirErr != nil || closeErr != nil {
			return errors.Join(mkdirErr, closeErr)
		}
	}
	if _, err := r.outputWithLimit(
		ctx, repositoryPath, maximumGitPathOutput,
		"status", "--porcelain=v2", "-z", "--untracked-files=no", "--ignore-submodules=all",
	); err != nil {
		return preserveContextError(err, errors.New("refresh applied workspace index metadata"))
	}
	verified, err := r.captureOverlay(
		ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat, manifest.WorkingDirectory, "",
	)
	if err != nil {
		return fmt.Errorf("verify applied workspace overlay: %w", err)
	}
	if verified.manifest.SourceSnapshotHash != manifest.SourceSnapshotHash {
		return errors.New("applied workspace overlay does not reproduce the source snapshot")
	}
	return validateWorkingDirectory(repositoryPath, manifest.WorkingDirectory)
}

func (r Runner) preflightOverlay(
	ctx context.Context,
	repositoryPath string,
	payloadRoot *os.Root,
	extracted workspaceoverlay.Extracted,
) error {
	for _, entry := range extracted.Manifest.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if runtime.GOOS == "windows" {
			if err := validateWindowsOverlayEntry(entry); err != nil {
				return err
			}
		}
		if entry.Index != nil && !entry.Index.IntentToAdd {
			if entry.Index.PayloadSHA256 == "" {
				if err := r.run(
					ctx, repositoryPath, "--no-replace-objects", "cat-file", "-e", entry.Index.OID+"^{blob}",
				); err != nil {
					return fmt.Errorf("target overlay index object for %q is unavailable: %w", entry.Path, err)
				}
			} else {
				name, found := extracted.PayloadNames[entry.Index.PayloadSHA256]
				if !found {
					return errors.New("workspace overlay index payload was not extracted")
				}
				payload, err := payloadRoot.Open(name)
				if err != nil {
					return fmt.Errorf("open workspace overlay index payload: %w", err)
				}
				actualOID, hashErr := r.calculateObjectID(ctx, repositoryPath, payload)
				closeErr := payload.Close()
				if hashErr != nil || closeErr != nil {
					return errors.Join(hashErr, closeErr)
				}
				if actualOID != entry.Index.OID {
					return fmt.Errorf("workspace overlay index payload for %q has the wrong Git object ID", entry.Path)
				}
			}
			if entry.Index.Mode == "120000" {
				var target []byte
				var err error
				if entry.Index.PayloadSHA256 == "" {
					target, err = r.gitSymlinkTarget(ctx, repositoryPath, entry.Index.OID)
				} else {
					target, err = readOverlaySymlinkTarget(
						payloadRoot, extracted.PayloadNames, entry.Index.PayloadSHA256,
					)
				}
				if err != nil {
					return fmt.Errorf("read target overlay index symlink for %q: %w", entry.Path, err)
				}
				if err := validateOverlaySymlinkTarget(entry.Path, string(target)); err != nil {
					return err
				}
			}
		}
		if entry.Worktree.Kind == workspaceoverlay.NodeSymlink {
			target, err := readOverlaySymlinkTarget(
				payloadRoot, extracted.PayloadNames, entry.Worktree.PayloadSHA256,
			)
			if err != nil {
				return fmt.Errorf("read target overlay worktree symlink for %q: %w", entry.Path, err)
			}
			if err := validateOverlaySymlinkTarget(entry.Path, string(target)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r Runner) materializeOverlayIndexObjects(
	ctx context.Context,
	repositoryPath string,
	payloadRoot *os.Root,
	extracted workspaceoverlay.Extracted,
) error {
	written := make(map[string]struct{})
	for _, entry := range extracted.Manifest.Entries {
		if entry.Index == nil || entry.Index.IntentToAdd || entry.Index.PayloadSHA256 == "" {
			continue
		}
		if _, exists := written[entry.Index.PayloadSHA256]; exists {
			continue
		}
		name, found := extracted.PayloadNames[entry.Index.PayloadSHA256]
		if !found {
			return errors.New("workspace overlay index payload was not extracted")
		}
		payload, err := payloadRoot.Open(name)
		if err != nil {
			return fmt.Errorf("open workspace overlay index payload: %w", err)
		}
		actualOID, writeErr := r.writeObject(ctx, repositoryPath, payload)
		closeErr := payload.Close()
		if writeErr != nil || closeErr != nil {
			return errors.Join(writeErr, closeErr)
		}
		if actualOID != entry.Index.OID {
			return fmt.Errorf("workspace overlay index payload for %q changed after validation", entry.Path)
		}
		written[entry.Index.PayloadSHA256] = struct{}{}
	}
	return nil
}

func readOverlaySymlinkTarget(
	root *os.Root,
	payloadNames map[string]string,
	digest string,
) ([]byte, error) {
	name, found := payloadNames[digest]
	if !found {
		return nil, errors.New("workspace overlay symlink payload was not extracted")
	}
	payload, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	target, readErr := io.ReadAll(io.LimitReader(payload, workspaceoverlay.MaximumPathBytes+1))
	closeErr := payload.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if len(target) > workspaceoverlay.MaximumPathBytes {
		return nil, errors.New("workspace symlink target exceeds its byte limit")
	}
	return target, nil
}

func validateWindowsOverlayEntry(entry workspaceoverlay.Entry) error {
	if entry.Index != nil && entry.Index.IntentToAdd {
		switch entry.Index.Mode {
		case "100755":
			return fmt.Errorf("executable intent-to-add path %q cannot be represented by the Windows worker profile", entry.Path)
		case "120000":
			return fmt.Errorf("symlink intent-to-add path %q cannot be represented by the Windows worker profile", entry.Path)
		}
	}
	switch entry.Worktree.Kind {
	case workspaceoverlay.NodeAbsent:
		return nil
	case workspaceoverlay.NodeSymlink:
		return fmt.Errorf("dirty symlink %q cannot be represented by the Windows worker profile", entry.Path)
	case workspaceoverlay.NodeFile:
		logicalMode := "100644"
		if entry.Index != nil && (entry.Index.Mode == "100644" || entry.Index.Mode == "100755") {
			logicalMode = entry.Index.Mode
		}
		if entry.Worktree.Mode != logicalMode {
			return fmt.Errorf("dirty executable mode change for %q cannot be represented on Windows", entry.Path)
		}
		return nil
	default:
		return fmt.Errorf("unsupported target overlay worktree entry for %q", entry.Path)
	}
}

func (r Runner) VerifySnapshot(
	ctx context.Context,
	repositoryPath string,
	manifest protocol.WorkspaceManifest,
) error {
	r = r.forIsolatedTarget()
	if manifest.Clean {
		return r.VerifyDirect(ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat)
	}
	captured, err := r.captureOverlay(
		ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat, manifest.WorkingDirectory, "",
	)
	if err != nil {
		return err
	}
	if captured.manifest.SourceSnapshotHash != manifest.SourceSnapshotHash {
		return errors.New("prepared workspace no longer matches its source snapshot")
	}
	return nil
}

func (r Runner) resetOverlayBase(
	ctx context.Context,
	repositoryPath string,
	manifest protocol.WorkspaceManifest,
) error {
	if err := r.configureTargetCheckout(ctx, repositoryPath); err != nil {
		return err
	}
	if err := r.run(ctx, repositoryPath, "reset", "--hard", "--quiet", manifest.HeadOID); err != nil {
		return preserveContextError(err, errors.New("reset target workspace before overlay application"))
	}
	if err := r.run(ctx, repositoryPath, "clean", "-ffdx"); err != nil {
		return preserveContextError(err, errors.New("clean target workspace before overlay application"))
	}
	return r.VerifyDirect(ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat)
}

func (r Runner) replaceOverlayIndex(
	ctx context.Context,
	repositoryPath string,
	manifest workspaceoverlay.Manifest,
) error {
	zeroOID := strings.Repeat("0", len(manifest.HeadOID))
	var remove bytes.Buffer
	for _, entry := range manifest.Entries {
		fmt.Fprintf(&remove, "0 %s\t%s%c", zeroOID, entry.Path, byte(0))
	}
	if err := r.runUpdateIndex(ctx, repositoryPath, remove.Bytes()); err != nil {
		return fmt.Errorf("clear target overlay index paths: %w", err)
	}
	var replace bytes.Buffer
	for _, entry := range manifest.Entries {
		if entry.Index == nil || entry.Index.IntentToAdd {
			continue
		}
		fmt.Fprintf(&replace, "%s %s\t%s%c", entry.Index.Mode, entry.Index.OID, entry.Path, byte(0))
	}
	if replace.Len() != 0 {
		if err := r.runUpdateIndex(ctx, repositoryPath, replace.Bytes()); err != nil {
			return fmt.Errorf("write target overlay index state: %w", err)
		}
	}
	return nil
}

func (r Runner) runUpdateIndex(ctx context.Context, repositoryPath string, input []byte) error {
	return r.runInputCommand(ctx, repositoryPath, input, "update-index", "-z", "--index-info")
}

func (r Runner) runInputCommand(
	ctx context.Context,
	repositoryPath string,
	input []byte,
	args ...string,
) error {
	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	command := exec.CommandContext(
		commandContext,
		r.Binary,
		safeGitArgs(args)...,
	)
	command.WaitDelay = commandWaitDelay
	command.Dir = repositoryPath
	command.Env = r.commandEnvironment()
	command.Stdin = bytes.NewReader(input)
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
		detail := strings.TrimSpace(string(stderr.Bytes()))
		if detail != "" {
			return fmt.Errorf("Git command failed: %w: %s", err, detail)
		}
		return fmt.Errorf("Git command failed: %w", err)
	}
	if stderr.full {
		return errors.New("Git command error output exceeded the configured limit")
	}
	return nil
}

func (r Runner) restoreIntentToAdd(
	ctx context.Context,
	repositoryPath string,
	manifest workspaceoverlay.Manifest,
) (returnErr error) {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open target workspace for intent-to-add: %w", err)
	}
	defer root.Close()
	var paths bytes.Buffer
	temporary := make([]string, 0)
	defer func() {
		for _, name := range temporary {
			returnErr = errors.Join(returnErr, root.Remove(name))
		}
	}()
	for _, entry := range manifest.Entries {
		if entry.Index == nil || !entry.Index.IntentToAdd {
			continue
		}
		paths.WriteString(entry.Path)
		paths.WriteByte(0)
		name := filepath.FromSlash(entry.Path)
		if err := root.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			return err
		}
		switch entry.Index.Mode {
		case "100644", "100755":
			file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			temporary = append(temporary, name)
			closeErr := file.Close()
			chmodErr := root.Chmod(name, overlayFilePermission(entry.Index.Mode))
			if closeErr != nil || chmodErr != nil {
				return errors.Join(closeErr, chmodErr)
			}
		case "120000":
			if err := root.Symlink(temporaryIntentToAddSymlinkTarget, name); err != nil {
				return err
			}
			temporary = append(temporary, name)
		default:
			return fmt.Errorf("unsupported intent-to-add index mode %q", entry.Index.Mode)
		}
	}
	if paths.Len() == 0 {
		return nil
	}
	err = r.runInputCommand(
		ctx, repositoryPath, paths.Bytes(),
		"--literal-pathspecs", "add", "-N", "-f", "--pathspec-from-file=-", "--pathspec-file-nul",
	)
	if err != nil {
		return fmt.Errorf("restore target intent-to-add index state: %w", err)
	}
	return nil
}

func (r Runner) calculateObjectID(
	ctx context.Context,
	repositoryPath string,
	source io.Reader,
) (string, error) {
	return r.runHashObject(
		ctx, repositoryPath, source,
		[]string{"hash-object", "--no-filters", "--stdin"},
		"validate target Git index object",
	)
}

func (r Runner) writeObject(
	ctx context.Context,
	repositoryPath string,
	source io.Reader,
) (string, error) {
	return r.runHashObject(
		ctx, repositoryPath, source,
		[]string{"hash-object", "--no-filters", "-w", "--stdin"},
		"write target Git index object",
	)
}

func (r Runner) runHashObject(
	ctx context.Context,
	repositoryPath string,
	source io.Reader,
	args []string,
	failure string,
) (string, error) {
	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	command := exec.CommandContext(
		commandContext,
		r.Binary,
		safeGitArgs(args)...,
	)
	command.WaitDelay = commandWaitDelay
	command.Dir = repositoryPath
	command.Env = r.commandEnvironment()
	command.Stdin = source
	var stdout limitedBuffer
	var stderr limitedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if commandContext.Err() != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errCommandTimeout
	}
	if err != nil || stdout.full || stderr.full {
		return "", preserveContextError(err, errors.New(failure))
	}
	oid := strings.TrimSpace(string(stdout.Bytes()))
	if !gitObjectID(oid) {
		return "", errors.New("Git returned an invalid target index object ID")
	}
	return oid, nil
}

func applyOverlayWorktree(
	ctx context.Context,
	repositoryPath string,
	payloadRoot *os.Root,
	extracted workspaceoverlay.Extracted,
) error {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open target workspace root: %w", err)
	}
	defer root.Close()
	entries := sortedOverlayEntries(extracted.Manifest)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Worktree.Kind == workspaceoverlay.NodeAbsent {
			continue
		}
		name := filepath.FromSlash(entry.Path)
		parent := filepath.Dir(name)
		if err := root.MkdirAll(parent, 0o700); err != nil {
			return fmt.Errorf("create target overlay parent for %q: %w", entry.Path, err)
		}
		payloadName, found := extracted.PayloadNames[entry.Worktree.PayloadSHA256]
		if !found {
			return fmt.Errorf("target overlay payload for %q is missing", entry.Path)
		}
		payload, err := payloadRoot.Open(payloadName)
		if err != nil {
			return fmt.Errorf("open target overlay payload for %q: %w", entry.Path, err)
		}
		switch entry.Worktree.Kind {
		case workspaceoverlay.NodeFile:
			err = writeOverlayFile(root, name, payload, entry.Worktree.Mode)
		case workspaceoverlay.NodeSymlink:
			var target []byte
			target, err = io.ReadAll(io.LimitReader(payload, workspaceoverlay.MaximumPathBytes+1))
			if err == nil && len(target) > workspaceoverlay.MaximumPathBytes {
				err = errors.New("workspace symlink target exceeds its byte limit")
			}
			if err == nil {
				err = writeOverlaySymlink(root, name, entry.Path, string(target))
			}
		default:
			err = errors.New("unsupported target overlay worktree entry")
		}
		closeErr := payload.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func clearOverlayWorktree(
	ctx context.Context,
	repositoryPath string,
	manifest workspaceoverlay.Manifest,
) error {
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return fmt.Errorf("open target workspace root: %w", err)
	}
	defer root.Close()
	for _, entry := range sortedOverlayEntries(manifest) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := root.RemoveAll(filepath.FromSlash(entry.Path)); err != nil {
			return fmt.Errorf("remove target overlay path %q: %w", entry.Path, err)
		}
	}
	return nil
}

func sortedOverlayEntries(manifest workspaceoverlay.Manifest) []workspaceoverlay.Entry {
	entries := slices.Clone(manifest.Entries)
	slices.SortFunc(entries, func(left, right workspaceoverlay.Entry) int {
		leftDepth := strings.Count(left.Path, "/")
		rightDepth := strings.Count(right.Path, "/")
		if leftDepth != rightDepth {
			return leftDepth - rightDepth
		}
		return strings.Compare(left.Path, right.Path)
	})
	return entries
}

func writeOverlayFile(
	root *os.Root,
	name string,
	source io.Reader,
	mode string,
) (returnErr error) {
	file, temporary, err := createOverlayTemporaryFile(
		root, filepath.Dir(name), randomOverlayTemporaryName,
	)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if file != nil {
			returnErr = errors.Join(returnErr, file.Close())
		}
		if !keep {
			returnErr = errors.Join(returnErr, root.Remove(temporary))
		}
	}()
	written, copyErr := io.Copy(file, source)
	syncErr := file.Sync()
	closeErr := file.Close()
	file = nil
	if copyErr != nil || syncErr != nil || closeErr != nil || written > workspaceoverlay.MaximumPayloadBytes {
		if written > workspaceoverlay.MaximumPayloadBytes && copyErr == nil && syncErr == nil && closeErr == nil {
			return errors.New("workspace overlay file exceeds its byte limit")
		}
		return errors.Join(copyErr, syncErr, closeErr)
	}
	permission := overlayFilePermission(mode)
	if err := root.Chmod(temporary, permission); err != nil {
		return err
	}
	if err := root.Rename(temporary, name); err != nil {
		return err
	}
	keep = true
	return nil
}

func overlayFilePermission(mode string) os.FileMode {
	if mode == "100755" {
		return 0o755
	}
	return 0o644
}

func writeOverlaySymlink(
	root *os.Root,
	name, portableName, target string,
) (returnErr error) {
	if err := validateOverlaySymlinkTarget(portableName, target); err != nil {
		return err
	}
	temporary, err := createOverlayTemporarySymlink(
		root, filepath.Dir(name), target, randomOverlayTemporaryName,
	)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			returnErr = errors.Join(returnErr, root.Remove(temporary))
		}
	}()
	if err := root.Rename(temporary, name); err != nil {
		return err
	}
	keep = true
	return nil
}

func validateOverlaySymlinkTarget(name, target string) error {
	if target == "" || len(target) > workspaceoverlay.MaximumPathBytes || !utf8.ValidString(target) ||
		strings.ContainsRune(target, 0) || strings.ContainsAny(target, `\:`) ||
		path.IsAbs(target) || filepath.IsAbs(target) {
		return fmt.Errorf("dirty symlink %q has a non-portable or absolute target", name)
	}
	for _, character := range target {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("dirty symlink %q has a target containing control characters", name)
		}
	}
	resolved := path.Clean(path.Join(path.Dir(name), target))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("dirty symlink %q escapes the managed workspace", name)
	}
	if resolved != "." {
		if err := workspaceoverlay.ValidatePath(resolved); err != nil {
			return fmt.Errorf("dirty symlink %q has a non-portable target: %w", name, err)
		}
	}
	return nil
}

type overlayTemporaryNameGenerator func(string) (string, error)

func createOverlayTemporaryFile(
	root *os.Root,
	parent string,
	generateName overlayTemporaryNameGenerator,
) (*os.File, string, error) {
	for range 32 {
		name, err := generateName(parent)
		if err != nil {
			return nil, "", err
		}
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return file, name, err
	}
	return nil, "", errors.New("could not allocate a collision-free workspace overlay temporary file")
}

func createOverlayTemporarySymlink(
	root *os.Root,
	parent, target string,
	generateName overlayTemporaryNameGenerator,
) (string, error) {
	for range 32 {
		name, err := generateName(parent)
		if err != nil {
			return "", err
		}
		if err := root.Symlink(target, name); errors.Is(err, os.ErrExist) {
			continue
		} else if err != nil {
			return "", err
		}
		return name, nil
	}
	return "", errors.New("could not allocate a collision-free workspace overlay temporary symlink")
}

func randomOverlayTemporaryName(parent string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate workspace overlay temporary name: %w", err)
	}
	return filepath.Join(parent, ".delegation-"+hex.EncodeToString(value[:])+".tmp"), nil
}
