//go:build windows

package gitworkspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/workspaceoverlay"
)

func TestApplyOverlayRejectsExecutableUntrackedFileBeforeChangingWorktree(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	payloadData := []byte("executable\n")
	digest := sha256.Sum256(payloadData)
	digestText := hex.EncodeToString(digest[:])
	overlayManifest, err := workspaceoverlay.NewManifest(
		repository.Manifest.HeadOID, repository.Manifest.ObjectFormat, repository.Manifest.WorkingDirectory,
		[]workspaceoverlay.Entry{{
			Path: "executable-tool",
			Worktree: workspaceoverlay.WorktreeState{
				Kind: workspaceoverlay.NodeFile, Mode: "100755", PayloadSHA256: digestText,
			},
		}},
		[]workspaceoverlay.Payload{{SHA256: digestText, Size: int64(len(payloadData))}},
	)
	if err != nil {
		t.Fatal(err)
	}
	dirtyManifest := repository.Manifest
	dirtyManifest.Clean = false
	dirtyManifest.SourceSnapshotHash = overlayManifest.SourceSnapshotHash
	archivePath := filepath.Join(t.TempDir(), "workspace.tar.zst")
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := workspaceoverlay.WriteArchive(
		context.Background(), archive, overlayManifest,
		func(string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(payloadData)), nil
		},
	)
	closeErr := archive.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(writeErr, closeErr))
	}
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, dirtyManifest)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.BundleRequired || !preparation.OverlayRequired {
		t.Fatalf("target preparation = %#v", preparation)
	}
	tracked := filepath.Join(target, "nested", "hello.txt")
	untracked := filepath.Join(target, "preserve-untracked.txt")
	if err := os.WriteFile(tracked, []byte("preserve tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(untracked, []byte("preserve untracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	applyErr := runner.ApplyOverlay(context.Background(), target, archivePath, dirtyManifest)
	if applyErr == nil || !strings.Contains(applyErr.Error(), "executable") {
		t.Fatalf("ApplyOverlay() = %v", applyErr)
	}
	for name, want := range map[string]string{
		tracked: "preserve tracked\n", untracked: "preserve untracked\n",
	} {
		data, err := os.ReadFile(name)
		if err != nil || string(data) != want {
			t.Fatalf("rejected overlay changed %s = %q, %v", name, data, err)
		}
	}
	if _, err := os.Stat(filepath.Join(target, "executable-tool")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected executable path exists: %v", err)
	}
}

func TestCloneDirectNeverReportsTrackedSymlinkAsRegularFile(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	linkPath := filepath.Join(source, "tracked-link")
	if err := os.WriteFile(linkPath, []byte("nested/hello.txt"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "config", "core.symlinks", "false")
	oid := gitOutput(t, runner.Binary, source, "hash-object", "-w", "tracked-link")
	gitRun(t, runner.Binary, source, "update-index", "--add", "--cacheinfo", "120000,"+oid+",tracked-link")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add tracked symlink",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if !repository.Manifest.Clean {
		t.Fatalf("tracked symlink repository is dirty: %#v", repository.Manifest)
	}
	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); err != nil {
		if _, statErr := os.Lstat(destination); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("failed symlink checkout left destination behind: %v", statErr)
		}
		return
	}
	info, err := os.Lstat(filepath.Join(destination, "tracked-link"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("tracked symlink was materialized as %s", info.Mode().Type())
	}
}
