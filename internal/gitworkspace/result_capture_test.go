package gitworkspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestCaptureResultRepresentsUnchangedSnapshotsWithoutPayload(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		name := "clean"
		if dirty {
			name = "dirty"
		}
		t.Run(name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			if dirty {
				if err := os.WriteFile(
					filepath.Join(source, "nested", "hello.txt"), []byte("base dirty\n"), 0o600,
				); err != nil {
					t.Fatal(err)
				}
			}
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			artifacts := filepath.Join(t.TempDir(), "result")
			capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if !capture.Unchanged || capture.ResultClean != !dirty ||
				capture.ResultHeadOID != base.Manifest.HeadOID ||
				capture.ResultSnapshotHash != base.Manifest.SourceSnapshotHash ||
				capture.Bundle != nil || capture.Overlay != nil {
				t.Fatalf("capture = %#v", capture)
			}
			entries, err := os.ReadDir(artifacts)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("unchanged artifact directory contains %d entries", len(entries))
			}
		})
	}
}

func TestCaptureResultRepresentsDirtyBaseBecomingCleanWithoutPayload(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(
		filepath.Join(source, "nested", "hello.txt"), []byte("base dirty\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if base.Manifest.Clean {
		t.Fatal("base fixture is clean")
	}
	gitRun(t, runner.Binary, source, "reset", "--hard", "--quiet", base.Manifest.HeadOID)
	clean, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}

	artifacts := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Unchanged || !capture.ResultClean || capture.Bundle != nil || capture.Overlay != nil ||
		capture.ResultSnapshotHash != clean.Manifest.SourceSnapshotHash {
		t.Fatalf("capture = %#v", capture)
	}
	entries, err := os.ReadDir(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("clean result artifact directory contains %d entries", len(entries))
	}
}

func TestCaptureResultCreatesOverlayRelativeToUnchangedHead(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(source, "nested", "hello.txt"), []byte("worker dirty\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "new.txt"), []byte("untracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}

	artifacts := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Unchanged || capture.ResultClean || capture.Bundle != nil || capture.Overlay == nil ||
		capture.ResultHeadOID != base.Manifest.HeadOID ||
		capture.ResultSnapshotHash != result.Manifest.SourceSnapshotHash {
		t.Fatalf("capture = %#v", capture)
	}
	assertResultArtifact(t, capture.Overlay, artifacts, ChangesOverlayName, protocol.WorkspaceArtifactOverlay)

	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, result.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.BundleRequired || !preparation.OverlayRequired {
		t.Fatalf("target preparation = %#v", preparation)
	}
	if err := runner.ApplyOverlay(context.Background(), target, capture.Overlay.Path, result.Manifest); err != nil {
		t.Fatal(err)
	}
	assertGitStateMatches(t, runner.Binary, source, target)
}

func TestCaptureResultCreatesExactThinBundleWithOptionalOverlay(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		name := "clean"
		if dirty {
			name = "dirty"
		}
		t.Run(name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "committed.txt"), []byte("commit\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitRun(t, runner.Binary, source, "add", "committed.txt")
			commitForTest(t, runner.Binary, source, "worker commit")
			if dirty {
				if err := os.WriteFile(filepath.Join(source, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			result, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}

			artifacts := filepath.Join(t.TempDir(), "result")
			capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if capture.Unchanged || capture.ResultClean != !dirty || capture.Bundle == nil ||
				(capture.Overlay != nil) != dirty || capture.ResultHeadOID != result.Manifest.HeadOID ||
				capture.ResultSnapshotHash != result.Manifest.SourceSnapshotHash {
				t.Fatalf("capture = %#v", capture)
			}
			assertResultArtifact(t, capture.Bundle, artifacts, ChangesBundleName, protocol.WorkspaceArtifactBundle)
			if dirty {
				assertResultArtifact(
					t, capture.Overlay, artifacts, ChangesOverlayName, protocol.WorkspaceArtifactOverlay,
				)
			}
			bundleHeader, err := os.ReadFile(capture.Bundle.Path)
			if err != nil {
				t.Fatal(err)
			}
			prerequisite := "-" + base.Manifest.HeadOID + " "
			if !strings.Contains(string(bundleHeader), prerequisite) {
				t.Fatalf("bundle does not declare exact base prerequisite %q", prerequisite)
			}

			target := filepath.Join(t.TempDir(), "target")
			preparation, err := runner.PrepareBase(context.Background(), target, result.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if !preparation.BundleRequired || preparation.OverlayRequired != dirty {
				t.Fatalf("target preparation = %#v", preparation)
			}
			if err := runner.ApplyBundle(context.Background(), target, capture.Bundle.Path, result.Manifest); err != nil {
				t.Fatal(err)
			}
			if dirty {
				if err := runner.ApplyOverlay(
					context.Background(), target, capture.Overlay.Path, result.Manifest,
				); err != nil {
					t.Fatal(err)
				}
				assertGitStateMatches(t, runner.Binary, source, target)
			} else if err := runner.VerifyDirect(
				context.Background(), target, result.Manifest.HeadOID, result.Manifest.ObjectFormat,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCaptureResultReportsWorkerIntroducedLFSAndSubmoduleContent(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}

	submodule := t.TempDir()
	gitRun(t, runner.Binary, submodule, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(submodule, "payload.txt"), []byte("nested\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, submodule, "add", "payload.txt")
	commitForTest(t, runner.Binary, submodule, "nested commit")
	gitRun(
		t, runner.Binary, source, "-c", "protocol.file.allow=always",
		"submodule", "add", "--quiet", submodule, "vendor/nested",
	)
	if err := os.WriteFile(
		filepath.Join(source, ".gitattributes"), []byte("*.bin filter=lfs diff=lfs merge=lfs -text\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(source, "large.bin"),
		[]byte("version https://git-lfs.github.com/spec/v1\noid sha256:"+strings.Repeat("a", 64)+"\nsize 1\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", ".gitattributes", "large.bin", ".gitmodules", "vendor/nested")
	commitForTest(t, runner.Binary, source, "worker content requiring external payloads")

	artifacts := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), source, artifacts, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		protocol.WorkspaceWarningLFSPayloadNotTransferred,
		protocol.WorkspaceWarningSubmoduleRepositoryNotTransferred,
	}
	if !slices.Equal(capture.ResultWarnings, want) || capture.Bundle == nil {
		t.Fatalf("result warnings/bundle = %v / %#v, want %v", capture.ResultWarnings, capture.Bundle, want)
	}
}

func TestCaptureResultAfterSelfContainedBundleFallback(t *testing.T) {
	runner := testRunner(t)
	_, source, _ := createRemoteRepository(t, runner.Binary)
	const unavailableRemote = "https://127.0.0.1:1/unavailable.git"
	if err := os.WriteFile(filepath.Join(source, "base-dirty.txt"), []byte("base dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base, err := runner.Inspect(context.Background(), source, unavailableRemote)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !preparation.BundleRequired || !preparation.OverlayRequired || len(preparation.BasisOIDs) != 0 {
		t.Fatalf("base preparation = %#v", preparation)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	strategy, err := runner.CreateBundle(
		context.Background(), base.Root, bundle, base.Manifest, preparation.BasisOIDs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strategy != protocol.WorkspaceStrategyFull {
		t.Fatalf("bundle strategy = %q, want %q", strategy, protocol.WorkspaceStrategyFull)
	}
	if err := runner.ApplyBundle(context.Background(), target, bundle, base.Manifest); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(t.TempDir(), "workspace.tar.zst")
	if err := runner.CreateOverlay(context.Background(), base.Root, overlay, base.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.ApplyOverlay(context.Background(), target, overlay, base.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "committed.txt"), []byte("commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, target, "add", "committed.txt")
	commitForTest(t, runner.Binary, target, "worker commit")
	if err := os.WriteFile(filepath.Join(target, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	artifacts := filepath.Join(t.TempDir(), "result")
	capture, err := runner.CaptureResult(context.Background(), target, artifacts, base.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Unchanged || capture.ResultClean || capture.Bundle == nil || capture.Overlay == nil {
		t.Fatalf("capture = %#v", capture)
	}
}

func TestCaptureResultRejectsNonDescendantHistory(t *testing.T) {
	t.Run("unrelated_or_rebased", func(t *testing.T) {
		runner := testRunner(t)
		remote, source, _ := createRemoteRepository(t, runner.Binary)
		base, err := runner.Inspect(context.Background(), source, remote)
		if err != nil {
			t.Fatal(err)
		}
		unrelated := gitOutput(
			t, runner.Binary, source,
			"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
			"commit-tree", "HEAD^{tree}", "-m", "rebased result",
		)
		gitRun(t, runner.Binary, source, "reset", "--hard", "--quiet", unrelated)
		assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, "does not descend")
	})

	t.Run("ancestor_reversal", func(t *testing.T) {
		runner := testRunner(t)
		remote, source, ancestor := createRemoteRepository(t, runner.Binary)
		if err := os.WriteFile(filepath.Join(source, "later.txt"), []byte("later\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		gitRun(t, runner.Binary, source, "add", "later.txt")
		commitForTest(t, runner.Binary, source, "later")
		base, err := runner.Inspect(context.Background(), source, remote)
		if err != nil {
			t.Fatal(err)
		}
		gitRun(t, runner.Binary, source, "reset", "--hard", "--quiet", ancestor)
		assertCaptureResultRejectedAndCleaned(t, runner, source, base.Manifest, "does not descend")
	})
}

func TestCaptureResultEnforcesArtifactLimitAndCleansStaging(t *testing.T) {
	for _, kind := range []string{"bundle", "overlay"} {
		t.Run(kind, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			base, err := runner.Inspect(context.Background(), source, remote)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "bundle":
				if err := os.WriteFile(filepath.Join(source, "commit.txt"), []byte("commit\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				gitRun(t, runner.Binary, source, "add", "commit.txt")
				commitForTest(t, runner.Binary, source, "worker commit")
			case "overlay":
				if err := os.WriteFile(
					filepath.Join(source, "dirty.txt"), []byte(strings.Repeat("x", 4096)), 0o600,
				); err != nil {
					t.Fatal(err)
				}
			}
			artifacts := filepath.Join(t.TempDir(), "result")
			_, err = runner.captureResult(context.Background(), source, artifacts, base.Manifest, 64)
			if err == nil || !strings.Contains(err.Error(), "64") {
				t.Fatalf("captureResult() = %v, want 64-byte limit rejection", err)
			}
			if _, statErr := os.Lstat(artifacts); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed capture staging still exists: %v", statErr)
			}
		})
	}
}

func TestCaptureResultRejectsExistingOrNestedArtifactDirectory(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	base, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(t.TempDir(), "existing")
	if err := os.Mkdir(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(existing, "marker")
	if err := os.WriteFile(marker, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.CaptureResult(context.Background(), source, existing, base.Manifest); err == nil {
		t.Fatal("CaptureResult accepted an existing artifact directory")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "preserve" {
		t.Fatalf("existing artifact directory changed: %q, %v", data, err)
	}
	nested := filepath.Join(source, "artifacts")
	if _, err := runner.CaptureResult(context.Background(), source, nested, base.Manifest); err == nil ||
		!strings.Contains(err.Error(), "outside") {
		t.Fatalf("nested CaptureResult() = %v", err)
	}
	if _, err := os.Lstat(nested); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nested artifact directory was created: %v", err)
	}
}

func assertCaptureResultRejectedAndCleaned(
	t *testing.T,
	runner Runner,
	repositoryPath string,
	base protocol.WorkspaceManifest,
	wantError string,
) {
	t.Helper()
	artifacts := filepath.Join(t.TempDir(), "result")
	_, err := runner.CaptureResult(context.Background(), repositoryPath, artifacts, base)
	if err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf("CaptureResult() = %v, want %q", err, wantError)
	}
	if _, statErr := os.Lstat(artifacts); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("rejected capture staging still exists: %v", statErr)
	}
}

func assertResultArtifact(
	t *testing.T,
	artifact *ResultArtifact,
	directory, name string,
	kind protocol.WorkspaceArtifactKind,
) {
	t.Helper()
	if artifact == nil {
		t.Fatalf("%s artifact is nil", kind)
	}
	wantPath := filepath.Join(directory, name)
	if artifact.Kind != kind || artifact.Name != name || artifact.Path != wantPath || artifact.Size < 1 {
		t.Fatalf("artifact = %#v", artifact)
	}
	data, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if artifact.Size != int64(len(data)) || artifact.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("artifact descriptor = %#v", artifact)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(wantPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("artifact mode = %o, want 600", info.Mode().Perm())
		}
	}
}

func commitForTest(t *testing.T, gitBinary, repositoryPath, message string) {
	t.Helper()
	gitRun(
		t, gitBinary, repositoryPath,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", message,
	)
}
