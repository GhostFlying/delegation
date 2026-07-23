//go:build !windows

package gitworkspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestOverlayUsesOwnerExecuteBitAndPreservesUntrackedExecutable(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	tracked := filepath.Join(source, "nested", "hello.txt")
	if err := os.WriteFile(tracked, []byte("mode changed\n"), 0o654); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tracked, 0o654); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(tracked); err != nil || info.Mode().Perm() != 0o654 {
		t.Fatalf("source group-only execute fixture = %v, %v", info, err)
	}
	untracked := filepath.Join(source, "run.sh")
	if err := os.WriteFile(untracked, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	trackedInfo, err := os.Stat(filepath.Join(target, "nested", "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if trackedInfo.Mode().Perm()&0o100 != 0 {
		t.Fatalf("group-only source execute bit became owner execute: %o", trackedInfo.Mode().Perm())
	}
	untrackedInfo, err := os.Stat(filepath.Join(target, "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if untrackedInfo.Mode().Perm()&0o100 == 0 {
		t.Fatalf("untracked executable mode = %v", untrackedInfo.Mode())
	}
}

func TestOverlayPreservesUntrackedExecutableWhenSourceIgnoresTrackedModes(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	gitRun(t, runner.Binary, source, "config", "core.fileMode", "false")
	if err := os.WriteFile(filepath.Join(source, "untracked-tool"), []byte("tool\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	info, err := os.Stat(filepath.Join(target, "untracked-tool"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("target untracked executable mode = %v", info.Mode())
	}
}

func TestApplyOverlayOverridesTargetFileModeForDirtyExecutableChange(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	tracked := filepath.Join(source, "nested", "hello.txt")
	if err := os.WriteFile(tracked, []byte("executable worktree\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tracked, 0o755); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(tracked); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("source executable fixture = %v, %v", info, err)
	}
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(t.TempDir(), "workspace.tar.zst")
	if err := runner.CreateOverlay(context.Background(), repository.Root, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.BundleRequired || !preparation.OverlayRequired {
		t.Fatalf("target preparation = %#v", preparation)
	}
	gitRun(t, runner.Binary, target, "config", "core.fileMode", "false")
	if err := runner.ApplyOverlay(context.Background(), target, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, runner.Binary, target, "config", "--bool", "--get", "core.fileMode"); got != "true" {
		t.Fatalf("target core.fileMode = %q", got)
	}
	assertGitStateMatches(t, runner.Binary, source, target)
}

func TestOverlayRestoresIntentToAddIndexModes(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	present := filepath.Join(source, "present-ita.sh")
	if err := os.WriteFile(present, []byte("present\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "present-ita.sh")
	if err := os.Chmod(present, 0o755); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(source, "absent-ita.sh")
	if err := os.WriteFile(absent, []byte("absent\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "absent-ita.sh")
	if err := os.Remove(absent); err != nil {
		t.Fatal(err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	if _, err := os.Stat(filepath.Join(target, "absent-ita.sh")); !os.IsNotExist(err) {
		t.Fatalf("absent ITA target exists: %v", err)
	}
	info, err := os.Stat(filepath.Join(target, "present-ita.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("present ITA worktree mode = %v", info.Mode())
	}
}

func TestInspectRejectsSpecialFIFO(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	fifo := filepath.Join(source, "nested", "hello.txt")
	if err := os.Remove(fifo); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("creating FIFO is unavailable: %v", err)
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
		!strings.Contains(err.Error(), "unsupported special file type") {
		t.Fatalf("Inspect() = %v", err)
	}
}

func TestOverlayRoundTripsStagedSymlink(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.Symlink("nested/hello.txt", filepath.Join(source, "staged-link")); err != nil {
		t.Skipf("creating symlink is unavailable: %v", err)
	}
	gitRun(t, runner.Binary, source, "add", "staged-link")
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	link, err := os.Readlink(filepath.Join(target, "staged-link"))
	if err != nil || link != "nested/hello.txt" {
		t.Fatalf("target staged symlink = %q, %v", link, err)
	}
}

func TestOverlayRoundTripsIntentToAddSymlink(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.Symlink("nested/hello.txt", filepath.Join(source, "intent-link")); err != nil {
		t.Skipf("creating symlink is unavailable: %v", err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "intent-link")
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	link, err := os.Readlink(filepath.Join(target, "intent-link"))
	if err != nil || link != "nested/hello.txt" {
		t.Fatalf("target intent-to-add symlink = %q, %v", link, err)
	}
}

func TestOverlayRoundTripsIntentToAddSymlinkWithIndependentWorktreeState(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	absent := filepath.Join(source, "intent-link-absent")
	regular := filepath.Join(source, "intent-link-regular")
	for _, name := range []string{absent, regular} {
		if err := os.Symlink("nested/hello.txt", name); err != nil {
			t.Skipf("creating symlink is unavailable: %v", err)
		}
	}
	gitRun(t, runner.Binary, source, "add", "-N", "intent-link-absent", "intent-link-regular")
	if err := os.Remove(absent); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(regular); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(regular, []byte("regular replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	if _, err := os.Lstat(filepath.Join(target, "intent-link-absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target absent ITA symlink exists: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "intent-link-regular"))
	if err != nil || string(data) != "regular replacement\n" {
		t.Fatalf("target regular ITA replacement = %q, %v", data, err)
	}
}

func TestOverlayRoundTripsRegularIntentToAddReplacedBySymlink(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	name := filepath.Join(source, "intent-type-change")
	if err := os.WriteFile(name, []byte("intent\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "intent-type-change")
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/hello.txt", name); err != nil {
		t.Skipf("creating symlink is unavailable: %v", err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	link, err := os.Readlink(filepath.Join(target, "intent-type-change"))
	if err != nil || link != "nested/hello.txt" {
		t.Fatalf("target ITA type-change symlink = %q, %v", link, err)
	}
}

func TestOverlayTemporarySymlinkRetriesOccupiedCandidate(t *testing.T) {
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
	name, err := createOverlayTemporarySymlink(
		root, ".", "target", func(string) (string, error) {
			candidate := candidates[index]
			index++
			return candidate, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Remove(name)
	if index != 2 || name != candidates[1] {
		t.Fatalf("temporary symlink = %q after %d candidates", name, index)
	}
	data, err := os.ReadFile(filepath.Join(rootPath, occupied))
	if err != nil || string(data) != "preserve\n" {
		t.Fatalf("occupied candidate = %q, %v", data, err)
	}
}
