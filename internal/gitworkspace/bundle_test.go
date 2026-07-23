package gitworkspace

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestThinBundleImportsUnpublishedHead(t *testing.T) {
	runner := testRunner(t)
	remote, source, remoteHead := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "unpublished.txt"), []byte("local only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "unpublished.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "unpublished",
	)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Manifest.HeadOID == remoteHead || !repository.Manifest.Clean {
		t.Fatalf("source manifest = %#v", repository.Manifest)
	}

	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !preparation.BundleRequired || preparation.OverlayRequired || len(preparation.BasisOIDs) == 0 {
		t.Fatalf("base preparation = %#v", preparation)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	strategy, err := runner.CreateBundle(
		context.Background(), repository.Root, bundle, repository.Manifest, preparation.BasisOIDs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strategy != protocol.WorkspaceStrategyThin {
		t.Fatalf("bundle strategy = %q, want %q", strategy, protocol.WorkspaceStrategyThin)
	}
	fresh := filepath.Join(t.TempDir(), "fresh")
	gitRun(t, runner.Binary, t.TempDir(), "init", fresh)
	command := exec.Command(runner.Binary, "bundle", "verify", bundle)
	command.Dir = fresh
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if err := command.Run(); err == nil {
		t.Fatal("thin bundle unexpectedly verified without its prerequisite")
	}
	if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	assertPreparedBundleWorkspace(t, runner, target, repository.Manifest)
}

func TestSelfContainedBundleImportsWithoutRemote(t *testing.T) {
	runner := testRunner(t)
	_, source, _ := createRemoteRepository(t, runner.Binary)
	const unavailableRemote = "https://127.0.0.1:1/unavailable.git"
	repository, err := runner.Inspect(context.Background(), source, unavailableRemote)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !preparation.BundleRequired || preparation.OverlayRequired || len(preparation.BasisOIDs) != 0 {
		t.Fatalf("base preparation = %#v", preparation)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed clone target still exists: %v", err)
	}

	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	strategy, err := runner.CreateBundle(
		context.Background(), repository.Root, bundle, repository.Manifest, preparation.BasisOIDs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strategy != protocol.WorkspaceStrategyFull {
		t.Fatalf("bundle strategy = %q, want %q", strategy, protocol.WorkspaceStrategyFull)
	}
	if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	assertPreparedBundleWorkspace(t, runner, target, repository.Manifest)
	if got := gitOutput(t, runner.Binary, target, "config", "--get", "remote.origin.url"); got != unavailableRemote {
		t.Fatalf("origin URL = %q, want %q", got, unavailableRemote)
	}
}

func TestSelfContainedBundleExcludesUnrelatedRefsAndObjects(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	unrelated := gitOutput(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit-tree", "HEAD^{tree}", "-m", "unrelated history",
	)
	gitRun(t, runner.Binary, source, "update-ref", "refs/heads/private-unrelated", unrelated)
	gitRun(t, runner.Binary, source, "update-ref", "refs/tags/private-unrelated", unrelated)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	strategy, err := runner.CreateBundle(
		context.Background(), repository.Root, bundle, repository.Manifest, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strategy != protocol.WorkspaceStrategyFull {
		t.Fatalf("bundle strategy = %q, want %q", strategy, protocol.WorkspaceStrategyFull)
	}
	heads, err := runner.output(context.Background(), source, "bundle", "list-heads", bundle)
	if err != nil {
		t.Fatal(err)
	}
	wantHeads := repository.Manifest.HeadOID + " HEAD\n"
	if string(heads) != wantHeads {
		t.Fatalf("bundle heads = %q, want %q", heads, wantHeads)
	}
	fresh := filepath.Join(t.TempDir(), "fresh")
	gitRun(t, runner.Binary, t.TempDir(), "init", fresh)
	if err := runner.run(context.Background(), fresh, "bundle", "unbundle", bundle); err != nil {
		t.Fatal(err)
	}
	if err := runner.run(context.Background(), fresh, "cat-file", "-e", unrelated+"^{commit}"); err == nil {
		t.Fatal("self-contained bundle exposed an object reachable only from an unrelated branch and tag")
	}
}

func TestCreateBundleFileEnforcesLimitWhileGitWrites(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	err = runner.createBundleFile(
		context.Background(), repository.Root, bundle, 64,
		[]string{"--no-replace-objects", "bundle", "create", "--quiet", "-", "HEAD"},
	)
	if !errors.Is(err, errWorkspaceArtifactTooLarge) {
		t.Fatalf("createBundleFile() = %v, want byte-limit rejection", err)
	}
	if _, err := os.Lstat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized bundle still exists: %v", err)
	}
}

func TestApplyBundleRejectsTamperedArtifactAndRemovesNewRepository(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	if _, err := runner.CreateBundle(context.Background(), repository.Root, bundle, repository.Manifest, nil); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(bundle, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("tampered"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err == nil {
		t.Fatal("ApplyBundle accepted a tampered bundle")
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected bundle target still exists: %v", err)
	}
}

func TestApplyBundleRejectsDifferentAdvertisedHead(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "newer.txt"), []byte("newer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "newer.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "newer",
	)
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	gitRun(t, runner.Binary, source, "bundle", "create", bundle, "HEAD")
	target := filepath.Join(t.TempDir(), "target")
	if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err == nil ||
		!strings.Contains(err.Error(), "pinned HEAD") {
		t.Fatalf("ApplyBundle() = %v, want advertised HEAD rejection", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected bundle target still exists: %v", err)
	}
}

func TestCreateBundleRejectsShallowSource(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "second.txt"), []byte("second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "second.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "second",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	shallow := filepath.Join(t.TempDir(), "shallow")
	filePath := filepath.ToSlash(remotePath)
	if runtime.GOOS == "windows" && !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}
	fileRemote := (&url.URL{Scheme: "file", Path: filePath}).String()
	gitRun(t, runner.Binary, t.TempDir(), "clone", "--depth=1", "--branch=main", fileRemote, shallow)
	repository, err := runner.Inspect(context.Background(), shallow, remote)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	if _, err := runner.CreateBundle(context.Background(), shallow, bundle, repository.Manifest, nil); err == nil ||
		!strings.Contains(err.Error(), "shallow") {
		t.Fatalf("CreateBundle() = %v, want shallow source rejection", err)
	}
}

func TestCreateBundleRejectsChangedSourceHead(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "changed.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "changed.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "changed head",
	)
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	if _, err := runner.CreateBundle(context.Background(), repository.Root, bundle, repository.Manifest, nil); err == nil ||
		!strings.Contains(err.Error(), "HEAD changed") {
		t.Fatalf("CreateBundle() = %v, want source HEAD change rejection", err)
	}
	if _, err := os.Lstat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected bundle still exists: %v", err)
	}
}

func TestCreateBundleSkipsUnavailableBasisCandidate(t *testing.T) {
	runner := testRunner(t)
	remote, source, remoteHead := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "unpublished.txt"), []byte("local only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "unpublished.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "unpublished",
	)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	candidates := []string{remoteHead, strings.Repeat("f", 40)}
	if candidates[1] < candidates[0] {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}
	strategy, err := runner.CreateBundle(
		context.Background(), repository.Root, bundle, repository.Manifest, candidates,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strategy != protocol.WorkspaceStrategyThin {
		t.Fatalf("bundle strategy = %q, want %q", strategy, protocol.WorkspaceStrategyThin)
	}
}

func assertPreparedBundleWorkspace(
	t *testing.T,
	runner Runner,
	target string,
	manifest protocol.WorkspaceManifest,
) {
	t.Helper()
	if got := gitOutput(t, runner.Binary, target, "rev-parse", "HEAD^{commit}"); got != manifest.HeadOID {
		t.Fatalf("target HEAD = %q, want %q", got, manifest.HeadOID)
	}
	if got := gitOutput(t, runner.Binary, target, "status", "--porcelain=v2"); got != "" {
		t.Fatalf("target status = %q", got)
	}
	if err := runner.VerifyDirect(context.Background(), target, manifest.HeadOID, manifest.ObjectFormat); err != nil {
		t.Fatalf("VerifyDirect() = %v", err)
	}
}
