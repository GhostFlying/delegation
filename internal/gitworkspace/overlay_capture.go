package gitworkspace

import (
	"bytes"
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
	"github.com/GhostFlying/delegation/internal/workspaceoverlay"
)

const (
	maximumGitPathOutput          = 64 * 1024 * 1024
	maximumIndexPathArgumentBytes = 16 * 1024
)

type capturedOverlay struct {
	manifest     workspaceoverlay.Manifest
	payloadPaths map[string]string
}

type payloadCollector struct {
	directory     string
	payloads      map[string]workspaceoverlay.Payload
	paths         map[string]string
	uniqueBytes   int64
	expandedBytes int64
}

func (r Runner) CreateOverlay(
	ctx context.Context,
	repositoryPath, destination string,
	manifest protocol.WorkspaceManifest,
) (returnErr error) {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if manifest.Clean {
		return errors.New("workspace overlay requires a dirty source manifest")
	}
	if !filepath.IsAbs(repositoryPath) || !filepath.IsAbs(destination) {
		return errors.New("workspace overlay paths must be absolute")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("workspace overlay destination already exists")
		}
		return fmt.Errorf("inspect workspace overlay destination: %w", err)
	}
	payloadDirectory, err := os.MkdirTemp(filepath.Dir(destination), ".overlay-payloads-")
	if err != nil {
		return fmt.Errorf("create workspace overlay payload staging: %w", err)
	}
	defer os.RemoveAll(payloadDirectory)
	captured, err := r.captureOverlay(ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat, manifest.WorkingDirectory, payloadDirectory)
	if err != nil {
		return err
	}
	if err := matchCapturedManifest(captured.manifest, manifest); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create workspace overlay archive: %w", err)
	}
	keep := false
	defer func() {
		if file != nil {
			returnErr = errors.Join(returnErr, file.Close())
		}
		if !keep {
			returnErr = errors.Join(returnErr, os.Remove(destination))
		}
	}()
	bounded := &boundedFileWriter{file: file, remaining: protocol.MaximumWorkspaceArtifactBytes}
	if err := workspaceoverlay.WriteArchive(
		ctx,
		bounded,
		captured.manifest,
		func(digest string) (io.ReadCloser, error) {
			name, found := captured.payloadPaths[digest]
			if !found {
				return nil, errors.New("captured workspace payload is missing")
			}
			return os.Open(name)
		},
	); err != nil {
		if bounded.exceeded {
			return fmt.Errorf("%w of %d bytes", errWorkspaceArtifactTooLarge, protocol.MaximumWorkspaceArtifactBytes)
		}
		return fmt.Errorf("encode workspace overlay: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync workspace overlay archive: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close workspace overlay archive: %w", err)
	}
	file = nil
	if bounded.exceeded {
		return fmt.Errorf("%w of %d bytes", errWorkspaceArtifactTooLarge, protocol.MaximumWorkspaceArtifactBytes)
	}
	verified, err := r.captureOverlay(ctx, repositoryPath, manifest.HeadOID, manifest.ObjectFormat, manifest.WorkingDirectory, "")
	if err != nil {
		return err
	}
	if verified.manifest.SourceSnapshotHash != captured.manifest.SourceSnapshotHash {
		return errors.New("source workspace changed while its overlay was created")
	}
	keep = true
	return nil
}

func (r Runner) captureOverlay(
	ctx context.Context,
	repositoryPath, headOID, objectFormat, workingDirectory, payloadDirectory string,
) (capturedOverlay, error) {
	actualHead, err := r.output(ctx, repositoryPath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(string(actualHead)) != headOID {
		return capturedOverlay{}, preserveContextError(err, errors.New("source HEAD changed during workspace capture"))
	}
	actualFormat, err := r.output(ctx, repositoryPath, "rev-parse", "--show-object-format")
	if err != nil || strings.TrimSpace(string(actualFormat)) != objectFormat {
		return capturedOverlay{}, preserveContextError(err, errors.New("source Git object format changed during workspace capture"))
	}
	if err := r.rejectUnsupportedIndexState(ctx, repositoryPath); err != nil {
		return capturedOverlay{}, err
	}
	trustFileMode, err := r.repositoryTrustsFileMode(ctx, repositoryPath)
	if err != nil {
		return capturedOverlay{}, err
	}
	visible, err := r.rawDiffPaths(
		ctx, repositoryPath,
		"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev",
		"--no-renames", "--ita-visible-in-index", headOID, "--",
	)
	if err != nil {
		return capturedOverlay{}, preserveContextError(err, errors.New("inspect staged workspace changes"))
	}
	invisible, err := r.rawDiffPaths(
		ctx, repositoryPath,
		"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev",
		"--no-renames", "--ita-invisible-in-index", headOID, "--",
	)
	if err != nil {
		return capturedOverlay{}, preserveContextError(err, errors.New("inspect intent-to-add workspace changes"))
	}
	unstaged, err := r.rawDiffPaths(
		ctx, repositoryPath,
		"--no-replace-objects", "diff-files", "--raw", "-z", "--no-abbrev",
		"--no-renames", "--ignore-submodules=all", "--ita-invisible-in-index", "--",
	)
	if err != nil {
		return capturedOverlay{}, preserveContextError(err, errors.New("inspect unstaged workspace changes"))
	}
	untrackedOutput, err := r.outputWithLimit(
		ctx, repositoryPath, maximumGitPathOutput,
		"ls-files", "--others", "--exclude-standard", "-z", "--",
	)
	if err != nil {
		return capturedOverlay{}, preserveContextError(err, errors.New("inspect untracked workspace files"))
	}
	untracked, err := parseNULPaths(untrackedOutput)
	if err != nil {
		return capturedOverlay{}, fmt.Errorf("inspect untracked workspace files: %w", err)
	}
	intentToAdd := pathDifference(visible, invisible)
	staged := pathSet(visible)
	paths := sortedPathUnion(visible, unstaged, untracked)
	if len(paths) > workspaceoverlay.MaximumEntries {
		return capturedOverlay{}, fmt.Errorf("dirty workspace paths exceed limit of %d", workspaceoverlay.MaximumEntries)
	}
	tracked, err := r.validatePortableRepositoryPaths(ctx, repositoryPath, untracked)
	if err != nil {
		return capturedOverlay{}, err
	}
	index, err := r.indexStates(ctx, repositoryPath, paths, intentToAdd)
	if err != nil {
		return capturedOverlay{}, err
	}
	collector := payloadCollector{
		directory: payloadDirectory,
		payloads:  make(map[string]workspaceoverlay.Payload),
		paths:     make(map[string]string),
	}
	root, err := os.OpenRoot(repositoryPath)
	if err != nil {
		return capturedOverlay{}, fmt.Errorf("open source workspace root: %w", err)
	}
	defer root.Close()
	entries := make([]workspaceoverlay.Entry, 0, len(paths))
	for _, name := range paths {
		state := index[name]
		var indexSymlinkTarget []byte
		if state != nil && state.Mode == "120000" && !state.IntentToAdd {
			indexSymlinkTarget, err = r.gitSymlinkTarget(ctx, repositoryPath, state.OID)
			if err != nil {
				return capturedOverlay{}, fmt.Errorf("capture index symlink for %q: %w", name, err)
			}
			if err := validateOverlaySymlinkTarget(name, string(indexSymlinkTarget)); err != nil {
				return capturedOverlay{}, err
			}
		}
		if state != nil && staged[name] && !state.IntentToAdd {
			var payload workspaceoverlay.Payload
			if state.Mode == "120000" {
				payload, err = collector.addReader(bytes.NewReader(indexSymlinkTarget))
			} else {
				payload, err = collector.addGitBlob(ctx, r, repositoryPath, state.OID)
			}
			if err != nil {
				return capturedOverlay{}, fmt.Errorf("capture staged index blob for %q: %w", name, err)
			}
			if err := collector.reference(payload); err != nil {
				return capturedOverlay{}, err
			}
			state.PayloadSHA256 = payload.SHA256
		}
		worktree, err := collector.captureWorktree(root, name, state, trustFileMode)
		if err != nil {
			return capturedOverlay{}, err
		}
		entries = append(entries, workspaceoverlay.Entry{Path: name, Index: state, Worktree: worktree})
	}
	if err := validateFinalWorktreePortablePaths(tracked, entries); err != nil {
		return capturedOverlay{}, err
	}
	payloads := make([]workspaceoverlay.Payload, 0, len(collector.payloads))
	for _, payload := range collector.payloads {
		payloads = append(payloads, payload)
	}
	slices.SortFunc(payloads, func(left, right workspaceoverlay.Payload) int {
		return strings.Compare(left.SHA256, right.SHA256)
	})
	manifest, err := workspaceoverlay.NewManifest(headOID, objectFormat, workingDirectory, entries, payloads)
	if err != nil {
		return capturedOverlay{}, fmt.Errorf("build workspace overlay manifest: %w", err)
	}
	return capturedOverlay{manifest: manifest, payloadPaths: collector.paths}, nil
}

func (r Runner) rejectUnsupportedIndexState(ctx context.Context, repositoryPath string) error {
	unmerged, err := r.output(ctx, repositoryPath, "ls-files", "--unmerged", "-z")
	if err != nil {
		return preserveContextError(err, errors.New("inspect unmerged workspace index"))
	}
	if len(unmerged) != 0 {
		return errors.New("workspace overlay does not support an unmerged Git index")
	}
	resolveUndo, err := r.output(ctx, repositoryPath, "ls-files", "--resolve-undo", "-z")
	if err != nil {
		return preserveContextError(err, errors.New("inspect workspace resolve-undo index state"))
	}
	if len(resolveUndo) != 0 {
		return errors.New("workspace overlay does not support resolve-undo index state")
	}
	flags, err := r.outputWithLimit(ctx, repositoryPath, maximumGitPathOutput, "ls-files", "-v", "-z")
	if err != nil {
		return preserveContextError(err, errors.New("inspect workspace index flags"))
	}
	records, err := splitNULRecords(flags)
	if err != nil {
		return fmt.Errorf("inspect workspace index flags: %w", err)
	}
	for _, record := range records {
		if len(record) < 3 || record[1] != ' ' {
			return errors.New("Git returned invalid workspace index flags")
		}
		tag := record[0]
		if tag == 'S' || tag >= 'a' && tag <= 'z' {
			return errors.New("workspace overlay does not support skip-worktree or assume-unchanged index entries")
		}
	}
	return nil
}

func (r Runner) validatePortableRepositoryPaths(
	ctx context.Context,
	repositoryPath string,
	untracked []string,
) ([]string, error) {
	trackedOutput, err := r.outputWithLimit(ctx, repositoryPath, maximumGitPathOutput, "ls-files", "-z")
	if err != nil {
		return nil, preserveContextError(err, errors.New("list tracked workspace paths"))
	}
	tracked, err := parseNULPaths(trackedOutput)
	if err != nil {
		return nil, fmt.Errorf("list tracked workspace paths: %w", err)
	}
	for _, paths := range [][]string{tracked, untracked} {
		for _, name := range paths {
			if _, err := workspaceoverlay.PortablePathKey(name); err != nil {
				return nil, fmt.Errorf("workspace path %q is not portable: %w", name, err)
			}
		}
	}
	if err := validatePortableFileNamespace("index", tracked); err != nil {
		return nil, err
	}
	return tracked, nil
}

func validateFinalWorktreePortablePaths(
	tracked []string,
	entries []workspaceoverlay.Entry,
) error {
	active := make(map[string]struct{}, len(tracked)+len(entries))
	for _, name := range tracked {
		active[name] = struct{}{}
	}
	for _, entry := range entries {
		delete(active, entry.Path)
		if entry.Worktree.Kind != workspaceoverlay.NodeAbsent {
			active[entry.Path] = struct{}{}
		}
	}
	paths := make([]string, 0, len(active))
	for name := range active {
		paths = append(paths, name)
	}
	return validatePortableFileNamespace("worktree", paths)
}

func validatePortableFileNamespace(namespace string, paths []string) error {
	active := make(map[string]string, len(paths))
	for _, name := range paths {
		portable, err := workspaceoverlay.PortablePathKey(name)
		if err != nil {
			return fmt.Errorf("workspace path %q is not portable: %w", name, err)
		}
		if prior, exists := active[portable]; exists && prior != name {
			return fmt.Errorf("%s paths %q and %q collide on a supported target", namespace, prior, name)
		}
		active[portable] = name
	}
	for portable, original := range active {
		ancestor := portable
		for {
			separator := strings.LastIndexByte(ancestor, '/')
			if separator < 0 {
				break
			}
			ancestor = ancestor[:separator]
			if prior, exists := active[ancestor]; exists {
				return fmt.Errorf(
					"%s paths %q and %q have a portable file ancestor conflict",
					namespace, prior, original,
				)
			}
		}
	}
	return nil
}

func (r Runner) rawDiffPaths(ctx context.Context, repositoryPath string, args ...string) ([]string, error) {
	output, err := r.outputWithLimit(ctx, repositoryPath, maximumGitPathOutput, args...)
	if err != nil {
		return nil, err
	}
	records, err := splitNULRecords(output)
	if err != nil {
		return nil, err
	}
	if len(records)%2 != 0 {
		return nil, errors.New("Git returned an incomplete raw diff")
	}
	paths := make([]string, 0, len(records)/2)
	for index := 0; index < len(records); index += 2 {
		metadata := records[index]
		if len(metadata) < 2 || metadata[0] != ':' || bytes.ContainsAny(metadata, "\t\n\r") {
			return nil, errors.New("Git returned invalid raw diff metadata")
		}
		paths = append(paths, string(records[index+1]))
	}
	slices.Sort(paths)
	return slices.Compact(paths), nil
}

func (r Runner) indexStates(
	ctx context.Context,
	repositoryPath string,
	paths []string,
	intentToAdd map[string]bool,
) (map[string]*workspaceoverlay.IndexState, error) {
	states := make(map[string]*workspaceoverlay.IndexState, len(paths))
	for _, batch := range batchIndexPaths(paths) {
		args := []string{"--literal-pathspecs", "ls-files", "--stage", "-z", "--"}
		args = append(args, batch...)
		output, err := r.outputWithLimit(ctx, repositoryPath, maximumGitPathOutput, args...)
		if err != nil {
			return nil, preserveContextError(err, errors.New("inspect final workspace index state"))
		}
		records, err := splitNULRecords(output)
		if err != nil {
			return nil, fmt.Errorf("inspect final workspace index state: %w", err)
		}
		for _, record := range records {
			tab := bytes.IndexByte(record, '\t')
			if tab < 0 {
				return nil, errors.New("Git returned invalid final workspace index state")
			}
			fields := strings.Fields(string(record[:tab]))
			name := string(record[tab+1:])
			if len(fields) != 3 || fields[2] != "0" || states[name] != nil {
				return nil, errors.New("workspace overlay does not support a multi-stage Git index")
			}
			switch fields[0] {
			case "100644", "100755", "120000":
			case "160000":
				return nil, fmt.Errorf("workspace overlay does not support dirty submodule path %q", name)
			default:
				return nil, fmt.Errorf("workspace overlay does not support index mode %q", fields[0])
			}
			states[name] = &workspaceoverlay.IndexState{
				Mode: fields[0], OID: fields[1], IntentToAdd: intentToAdd[name],
			}
		}
	}
	return states, nil
}

func batchIndexPaths(paths []string) [][]string {
	if len(paths) == 0 {
		return nil
	}
	batches := make([][]string, 0, len(paths))
	for len(paths) != 0 {
		bytes := 512
		end := 0
		for end < len(paths) && (end == 0 || bytes+len(paths[end])+3 <= maximumIndexPathArgumentBytes) {
			bytes += len(paths[end]) + 3
			end++
		}
		batches = append(batches, paths[:end])
		paths = paths[end:]
	}
	return batches
}

func (c *payloadCollector) captureWorktree(
	root *os.Root,
	name string,
	index *workspaceoverlay.IndexState,
	trustFileMode bool,
) (workspaceoverlay.WorktreeState, error) {
	shadowed, err := inspectRealPathAncestors(root, name)
	if err != nil {
		return workspaceoverlay.WorktreeState{}, fmt.Errorf("inspect source path %q: %w", name, err)
	}
	if shadowed {
		return workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeAbsent}, nil
	}
	info, err := root.Lstat(filepath.FromSlash(name))
	if errors.Is(err, os.ErrNotExist) {
		return workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeAbsent}, nil
	}
	if err != nil {
		return workspaceoverlay.WorktreeState{}, fmt.Errorf("inspect source path %q: %w", name, err)
	}
	switch {
	case info.Mode().IsRegular():
		if info.Size() > workspaceoverlay.MaximumPayloadBytes {
			return workspaceoverlay.WorktreeState{}, fmt.Errorf("source file %q exceeds the overlay byte limit", name)
		}
		file, err := root.Open(filepath.FromSlash(name))
		if err != nil {
			return workspaceoverlay.WorktreeState{}, fmt.Errorf("open source file %q: %w", name, err)
		}
		payload, captureErr := c.addReader(file)
		closeErr := file.Close()
		if captureErr != nil || closeErr != nil {
			return workspaceoverlay.WorktreeState{}, errors.Join(captureErr, closeErr)
		}
		mode := "100644"
		if !trustFileMode && index != nil && (index.Mode == "100644" || index.Mode == "100755") {
			mode = index.Mode
		} else if info.Mode().Perm()&0o100 != 0 {
			mode = "100755"
		}
		if err := c.reference(payload); err != nil {
			return workspaceoverlay.WorktreeState{}, err
		}
		return workspaceoverlay.WorktreeState{
			Kind: workspaceoverlay.NodeFile, Mode: mode, PayloadSHA256: payload.SHA256,
		}, nil
	case info.Mode()&os.ModeSymlink != 0:
		target, err := root.Readlink(filepath.FromSlash(name))
		if err != nil {
			return workspaceoverlay.WorktreeState{}, fmt.Errorf("read source symlink %q: %w", name, err)
		}
		if err := validateOverlaySymlinkTarget(name, target); err != nil {
			return workspaceoverlay.WorktreeState{}, err
		}
		payload, err := c.addReader(strings.NewReader(target))
		if err != nil {
			return workspaceoverlay.WorktreeState{}, err
		}
		if err := c.reference(payload); err != nil {
			return workspaceoverlay.WorktreeState{}, err
		}
		return workspaceoverlay.WorktreeState{
			Kind: workspaceoverlay.NodeSymlink, Mode: "120000", PayloadSHA256: payload.SHA256,
		}, nil
	case info.IsDir():
		return workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeAbsent}, nil
	default:
		return workspaceoverlay.WorktreeState{}, fmt.Errorf("source path %q has an unsupported special file type", name)
	}
}

func (c *payloadCollector) addGitBlob(
	ctx context.Context,
	runner Runner,
	repositoryPath, oid string,
) (workspaceoverlay.Payload, error) {
	commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	command := exec.CommandContext(
		commandContext, runner.Binary,
		safeGitArgs([]string{"--no-replace-objects", "cat-file", "blob", oid})...,
	)
	command.WaitDelay = commandWaitDelay
	command.Dir = repositoryPath
	command.Env = runner.commandEnvironment()
	stdout, err := command.StdoutPipe()
	if err != nil {
		return workspaceoverlay.Payload{}, err
	}
	var stderr limitedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return workspaceoverlay.Payload{}, err
	}
	payload, captureErr := c.addReader(stdout)
	if captureErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if commandContext.Err() != nil {
		if ctx.Err() != nil {
			return workspaceoverlay.Payload{}, ctx.Err()
		}
		return workspaceoverlay.Payload{}, errCommandTimeout
	}
	if captureErr != nil || waitErr != nil || stderr.full {
		if stderr.full && captureErr == nil && waitErr == nil {
			return workspaceoverlay.Payload{}, errors.New("Git blob error output exceeded the configured limit")
		}
		return workspaceoverlay.Payload{}, errors.Join(captureErr, waitErr)
	}
	return payload, nil
}

func (c *payloadCollector) addReader(reader io.Reader) (workspaceoverlay.Payload, error) {
	digest := sha256.New()
	var file *os.File
	var temporary string
	if c.directory != "" {
		var err error
		file, err = os.CreateTemp(c.directory, ".payload-")
		if err != nil {
			return workspaceoverlay.Payload{}, err
		}
		temporary = file.Name()
	}
	keep := false
	defer func() {
		if file != nil {
			_ = file.Close()
		}
		if temporary != "" && !keep {
			_ = os.Remove(temporary)
		}
	}()
	destination := io.Writer(digest)
	if file != nil {
		destination = io.MultiWriter(file, digest)
	}
	written, err := io.Copy(destination, io.LimitReader(reader, workspaceoverlay.MaximumPayloadBytes+1))
	if err != nil {
		return workspaceoverlay.Payload{}, err
	}
	if written > workspaceoverlay.MaximumPayloadBytes {
		return workspaceoverlay.Payload{}, errors.New("workspace overlay payload exceeds its byte limit")
	}
	value := hex.EncodeToString(digest.Sum(nil))
	payload := workspaceoverlay.Payload{SHA256: value, Size: written}
	if existing, found := c.payloads[value]; found {
		if existing.Size != written {
			return workspaceoverlay.Payload{}, errors.New("workspace payload digest collision")
		}
		return existing, nil
	}
	if c.uniqueBytes > workspaceoverlay.MaximumPayloadTotal-written {
		return workspaceoverlay.Payload{}, errors.New("workspace overlay unique payloads exceed their total byte limit")
	}
	if file != nil {
		if err := file.Sync(); err != nil {
			return workspaceoverlay.Payload{}, err
		}
		if err := file.Close(); err != nil {
			return workspaceoverlay.Payload{}, err
		}
		file = nil
		final := filepath.Join(c.directory, value)
		if err := os.Rename(temporary, final); err != nil {
			return workspaceoverlay.Payload{}, err
		}
		temporary = ""
		c.paths[value] = final
	}
	c.payloads[value] = payload
	c.uniqueBytes += written
	keep = true
	return payload, nil
}

func (c *payloadCollector) reference(payload workspaceoverlay.Payload) error {
	if c.expandedBytes > workspaceoverlay.MaximumExpandedBytes-payload.Size {
		return errors.New("workspace overlay expanded payload references exceed their total byte limit")
	}
	c.expandedBytes += payload.Size
	return nil
}

func (r Runner) repositoryTrustsFileMode(ctx context.Context, repositoryPath string) (bool, error) {
	output, err := r.output(
		ctx, repositoryPath, "config", "--bool", "--default=true", "--get", "core.fileMode",
	)
	if err != nil {
		return false, preserveContextError(err, errors.New("inspect source core.fileMode"))
	}
	switch strings.TrimSpace(string(output)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errors.New("Git returned an invalid core.fileMode value")
	}
}

func (r Runner) gitSymlinkTarget(
	ctx context.Context,
	repositoryPath, oid string,
) ([]byte, error) {
	target, err := r.outputWithLimit(
		ctx, repositoryPath, workspaceoverlay.MaximumPathBytes+1,
		"--no-replace-objects", "cat-file", "blob", oid,
	)
	if err != nil {
		return nil, preserveContextError(err, errors.New("read Git symlink blob"))
	}
	return target, nil
}

func inspectRealPathAncestors(root *os.Root, name string) (bool, error) {
	components := strings.Split(name, "/")
	partial := ""
	for _, component := range components[:len(components)-1] {
		partial = filepath.Join(partial, component)
		info, err := root.Lstat(partial)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return false, errors.New("path contains a symbolic-link ancestor")
		}
		if !info.IsDir() {
			return true, nil
		}
	}
	return false, nil
}

func matchCapturedManifest(captured workspaceoverlay.Manifest, manifest protocol.WorkspaceManifest) error {
	if captured.HeadOID != manifest.HeadOID || captured.ObjectFormat != manifest.ObjectFormat ||
		captured.WorkingDirectory != manifest.WorkingDirectory ||
		captured.SourceSnapshotHash != manifest.SourceSnapshotHash ||
		(len(captured.Entries) == 0) != manifest.Clean {
		return errors.New("source workspace changed before overlay creation")
	}
	return nil
}

func parseNULPaths(output []byte) ([]string, error) {
	records, err := splitNULRecords(output)
	if err != nil {
		return nil, err
	}
	paths := make([]string, len(records))
	for index, record := range records {
		paths[index] = string(record)
	}
	slices.Sort(paths)
	return slices.Compact(paths), nil
}

func splitNULRecords(output []byte) ([][]byte, error) {
	if len(output) == 0 {
		return nil, nil
	}
	if output[len(output)-1] != 0 {
		return nil, errors.New("Git output is not NUL terminated")
	}
	records := bytes.Split(output[:len(output)-1], []byte{0})
	for _, record := range records {
		if len(record) == 0 {
			return nil, errors.New("Git output contains an empty path record")
		}
	}
	return records, nil
}

func pathSet(paths []string) map[string]bool {
	result := make(map[string]bool, len(paths))
	for _, name := range paths {
		result[name] = true
	}
	return result
}

func pathDifference(left, right []string) map[string]bool {
	result := pathSet(left)
	for _, name := range right {
		delete(result, name)
	}
	return result
}

func sortedPathUnion(groups ...[]string) []string {
	set := make(map[string]struct{})
	for _, group := range groups {
		for _, name := range group {
			set[name] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for name := range set {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}
