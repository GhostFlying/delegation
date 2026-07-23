package gitworkspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/workspaceoverlay"
)

func TestOverlayRoundTripsIndexAndWorktreeStates(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	for name, content := range map[string]string{
		"case.txt":     "case\n",
		"delete.txt":   "delete\n",
		"rename.txt":   "rename\n",
		"same.txt":     "same\n",
		"unstaged.txt": "unstaged\n",
	} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, runner.Binary, source, "add", ".")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "overlay base",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	gitRun(t, runner.Binary, filepath.Dir(source), "--git-dir="+filepath.Join(filepath.Dir(source), "remote.git"), "update-server-info")

	if err := os.WriteFile(filepath.Join(source, "same.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "same.txt")
	if err := os.WriteFile(filepath.Join(source, "same.txt"), []byte("worktree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "unstaged.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "binary.dat"), []byte{'a', 0, 'b'}, 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "binary.dat")
	if err := os.WriteFile(filepath.Join(source, "binary.dat"), []byte{'c', 0, 'd'}, 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "mv", "rename.txt", "moved.txt")
	gitRun(t, runner.Binary, source, "mv", "-f", "case.txt", "CASE.txt")
	if err := os.Remove(filepath.Join(source, "delete.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "untracked", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "untracked", "nested", "new.txt"), []byte("new\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "intent.txt"), []byte("intent\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "intent.txt")
	if runtime.GOOS != "windows" {
		if err := os.Symlink("same.txt", filepath.Join(source, "untracked-link")); err != nil {
			t.Fatal(err)
		}
	}

	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Manifest.Clean {
		t.Fatal("dirty fixture was reported clean")
	}
	destination := filepath.Join(t.TempDir(), "target")
	base, err := runner.PrepareBase(context.Background(), destination, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if base.BundleRequired || !base.OverlayRequired {
		t.Fatalf("target base = %#v", base)
	}
	first := filepath.Join(t.TempDir(), "first.tar.zst")
	second := filepath.Join(t.TempDir(), "second.tar.zst")
	if err := runner.CreateOverlay(context.Background(), source, first, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.CreateOverlay(context.Background(), source, second, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("identical source snapshots produced different overlay archives")
	}
	if err := runner.ApplyOverlay(context.Background(), destination, first, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.VerifySnapshot(context.Background(), destination, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev", "--no-renames", "--ita-visible-in-index", "HEAD", "--"},
		{"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev", "--no-renames", "--ita-invisible-in-index", "HEAD", "--"},
		{"--no-replace-objects", "diff-files", "--raw", "-z", "--no-abbrev", "--no-renames", "--ignore-submodules=all", "--ita-invisible-in-index", "--"},
		{"ls-files", "--others", "--exclude-standard", "-z", "--full-name"},
		{"status", "--porcelain=v2", "-z", "--untracked-files=normal", "--ignore-submodules=all"},
	} {
		sourceOutput := rawGitOutput(t, runner.Binary, source, args...)
		targetOutput := rawGitOutput(t, runner.Binary, destination, args...)
		if !bytes.Equal(sourceOutput, targetOutput) {
			t.Fatalf("git %v differs after overlay\nsource: %q\ntarget: %q", args, sourceOutput, targetOutput)
		}
	}
	for _, name := range []string{"same.txt", "binary.dat", "untracked/nested/new.txt", "intent.txt"} {
		sourceData, err := os.ReadFile(filepath.Join(source, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		targetData, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(name)))
		if err != nil || !bytes.Equal(sourceData, targetData) {
			t.Fatalf("target %s = %q, %v; want %q", name, targetData, err, sourceData)
		}
	}
	cleanTracked := filepath.Join("nested", "hello.txt")
	cleanTrackedData, err := os.ReadFile(filepath.Join(source, cleanTracked))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, cleanTracked), []byte("interfering tracked change\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(destination, "leftover-untracked.txt")
	if err := os.WriteFile(leftover, []byte("remove me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.ApplyOverlay(context.Background(), destination, first, repository.Manifest); err != nil {
		t.Fatalf("reapply overlay to dirty target: %v", err)
	}
	if err := runner.VerifySnapshot(context.Background(), destination, repository.Manifest); err != nil {
		t.Fatalf("verify reapplied overlay: %v", err)
	}
	if _, err := os.Stat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("overlay reapply retained unrelated untracked file: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(destination, cleanTracked)); err != nil ||
		!bytes.Equal(data, cleanTrackedData) {
		t.Fatalf("overlay reapply retained tracked interference = %q, %v", data, err)
	}
}

func TestCreateOverlayRejectsChangedSourceSnapshot(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "dirty.txt"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "dirty.txt"), []byte("second\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "overlay.tar.zst")
	if err := runner.CreateOverlay(context.Background(), source, destination, repository.Manifest); err == nil {
		t.Fatal("CreateOverlay accepted a changed source snapshot")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected overlay destination still exists: %v", err)
	}
}

func TestInspectRejectsEscapingDirtySymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires privileges on Windows")
	}
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.Symlink("../outside", filepath.Join(source, "escaping-link")); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err == nil {
		t.Fatal("Inspect accepted a dirty symlink escaping the managed workspace")
	}
}

func TestInspectRejectsUnsafeStagedSymlinkWithAbsentWorktree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires privileges on Windows")
	}
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	name := filepath.Join(source, "unsafe-staged-link")
	if err := os.Symlink("/outside", name); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "unsafe-staged-link")
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err == nil {
		t.Fatal("Inspect accepted an unsafe staged symlink with an absent worktree path")
	}
}

func TestOverlayUsesGitEffectiveFileMode(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	script := filepath.Join(source, "script.sh")
	if err := os.WriteFile(script, []byte("echo test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "script.sh")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add script",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	gitRun(t, runner.Binary, source, "config", "core.fileMode", "false")
	gitRun(t, runner.Binary, source, "update-index", "--chmod=+x", "script.sh")
	if err := os.Chmod(script, 0o644); err != nil {
		t.Fatal(err)
	}
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "overlay.tar.zst")
	if err := runner.CreateOverlay(context.Background(), source, archive, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	base, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if base.BundleRequired || !base.OverlayRequired {
		t.Fatalf("target base = %#v", base)
	}
	if err := runner.ApplyOverlay(context.Background(), target, archive, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.VerifySnapshot(context.Background(), target, repository.Manifest); err != nil {
		t.Fatal(err)
	}
}

func TestInspectUsesGitDefaultWhenCoreFileModeIsUnset(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	gitRun(t, runner.Binary, source, "config", "--unset", "core.fileMode")
	if err := os.WriteFile(
		filepath.Join(source, "nested", "hello.txt"), []byte("dirty\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err != nil {
		t.Fatal(err)
	}
}

func TestApplyOverlayRejectsUnsafeSymlinkBeforeChangingWorktree(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	targetData := []byte("../outside")
	digest := sha256.Sum256(targetData)
	payload := workspaceoverlay.Payload{
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(targetData)),
	}
	overlayManifest, err := workspaceoverlay.NewManifest(
		repository.Manifest.HeadOID, repository.Manifest.ObjectFormat,
		repository.Manifest.WorkingDirectory,
		[]workspaceoverlay.Entry{{
			Path: "unsafe-link",
			Worktree: workspaceoverlay.WorktreeState{
				Kind: workspaceoverlay.NodeSymlink, Mode: "120000", PayloadSHA256: payload.SHA256,
			},
		}},
		[]workspaceoverlay.Payload{payload},
	)
	if err != nil {
		t.Fatal(err)
	}
	dirtyManifest := repository.Manifest
	dirtyManifest.Clean = false
	dirtyManifest.SourceSnapshotHash = overlayManifest.SourceSnapshotHash
	archivePath := filepath.Join(t.TempDir(), "unsafe-overlay.tar.zst")
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := workspaceoverlay.WriteArchive(
		context.Background(), archive, overlayManifest,
		func(string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(targetData)), nil
		},
	)
	closeErr := archive.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(writeErr, closeErr))
	}
	target := filepath.Join(t.TempDir(), "target")
	base, err := runner.PrepareBase(context.Background(), target, dirtyManifest)
	if err != nil {
		t.Fatal(err)
	}
	if base.BundleRequired || !base.OverlayRequired {
		t.Fatalf("target base = %#v", base)
	}
	tracked := filepath.Join(target, "nested", "hello.txt")
	if err := os.WriteFile(tracked, []byte("preserve tracked change\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	untracked := filepath.Join(target, "preserve-untracked.txt")
	if err := os.WriteFile(untracked, []byte("preserve untracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.ApplyOverlay(context.Background(), target, archivePath, dirtyManifest); err == nil {
		t.Fatal("ApplyOverlay accepted an unsafe symlink")
	}
	for name, want := range map[string]string{
		tracked: "preserve tracked change\n", untracked: "preserve untracked\n",
	} {
		data, err := os.ReadFile(name)
		if err != nil || string(data) != want {
			t.Fatalf("rejected overlay changed %s = %q, %v", name, data, err)
		}
	}
}

func TestBatchIndexPathsBoundsCommandArguments(t *testing.T) {
	paths := make([]string, 20)
	for index := range paths {
		paths[index] = strings.Repeat(string(rune('a'+index)), workspaceoverlay.MaximumPathBytes)
	}
	var flattened []string
	for _, batch := range batchIndexPaths(paths) {
		argumentBytes := 512
		for _, name := range batch {
			argumentBytes += len(name) + 3
		}
		if argumentBytes > maximumIndexPathArgumentBytes {
			t.Fatalf("index path argument batch uses %d bytes", argumentBytes)
		}
		flattened = append(flattened, batch...)
	}
	if !slices.Equal(flattened, paths) {
		t.Fatal("index path argument batching changed path order")
	}
}

func TestOverlayTemporaryFileRetriesOccupiedCandidate(t *testing.T) {
	rootPath := t.TempDir()
	occupied := ".delegation-occupied.tmp"
	if err := os.WriteFile(filepath.Join(rootPath, occupied), []byte("preserve\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	candidates := []string{occupied, ".delegation-free.tmp"}
	index := 0
	file, name, err := createOverlayTemporaryFile(
		root, ".", func(string) (string, error) {
			candidate := candidates[index]
			index++
			return candidate, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := file.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	defer root.Remove(name)
	if index != 2 || name != candidates[1] {
		t.Fatalf("temporary file = %q after %d candidates", name, index)
	}
	data, err := os.ReadFile(filepath.Join(rootPath, occupied))
	if err != nil || string(data) != "preserve\n" {
		t.Fatalf("occupied candidate = %q, %v", data, err)
	}
}

func TestWindowsOverlayEntryModeBoundary(t *testing.T) {
	valid := workspaceoverlay.Entry{
		Path:     "script.sh",
		Index:    &workspaceoverlay.IndexState{Mode: "100755", OID: strings.Repeat("a", 40)},
		Worktree: workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeFile, Mode: "100755"},
	}
	if err := validateWindowsOverlayEntry(valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Index = nil
	if err := validateWindowsOverlayEntry(invalid); err == nil {
		t.Fatal("Windows overlay validation accepted an executable untracked file")
	}
	invalid = valid
	invalid.Worktree = workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeSymlink, Mode: "120000"}
	if err := validateWindowsOverlayEntry(invalid); err == nil {
		t.Fatal("Windows overlay validation accepted a worktree symlink")
	}
	invalid = workspaceoverlay.Entry{
		Path: "intent-link",
		Index: &workspaceoverlay.IndexState{
			Mode: "120000", OID: strings.Repeat("a", 40), IntentToAdd: true,
		},
		Worktree: workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeAbsent},
	}
	if err := validateWindowsOverlayEntry(invalid); err == nil {
		t.Fatal("Windows overlay validation accepted a symlink intent-to-add entry")
	}
	invalid = workspaceoverlay.Entry{
		Path: "intent-executable",
		Index: &workspaceoverlay.IndexState{
			Mode: "100755", OID: strings.Repeat("a", 40), IntentToAdd: true,
		},
		Worktree: workspaceoverlay.WorktreeState{Kind: workspaceoverlay.NodeAbsent},
	}
	if err := validateWindowsOverlayEntry(invalid); err == nil {
		t.Fatal("Windows overlay validation accepted an executable intent-to-add entry")
	}
}

func TestOverlaySymlinkTargetAllowsRepositoryRoot(t *testing.T) {
	for name, target := range map[string]string{"link": ".", "nested/link": ".."} {
		if err := validateOverlaySymlinkTarget(name, target); err != nil {
			t.Fatalf("validateOverlaySymlinkTarget(%q, %q) = %v", name, target, err)
		}
	}
}

func TestOverlaySymlinkTargetRejectsNonPortableText(t *testing.T) {
	tests := map[string]string{
		"backslash":    `nested\target`,
		"colon":        "drive:target",
		"nul":          "target\x00suffix",
		"control":      "target\nsuffix",
		"invalid utf8": string([]byte{0xff}),
	}
	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateOverlaySymlinkTarget("nested/link", target); err == nil {
				t.Fatalf("validateOverlaySymlinkTarget accepted %q", target)
			}
		})
	}
}

func rawGitOutput(t *testing.T, binary, directory string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return output
}
