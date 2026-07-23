package gitworkspace

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestRunDiscardingOutputDoesNotApplyDiagnosticBufferLimit(t *testing.T) {
	runner := testRunner(t)
	repository := t.TempDir()
	gitRun(t, runner.Binary, repository, "init")
	large := filepath.Join(repository, "large.bin")
	if err := os.WriteFile(large, bytes.Repeat([]byte("x"), maximumOutput+1), 0o600); err != nil {
		t.Fatal(err)
	}
	oid := gitOutput(t, runner.Binary, repository, "hash-object", "-w", large)
	if _, err := runner.output(context.Background(), repository, "cat-file", "blob", oid); err == nil ||
		!strings.Contains(err.Error(), "output exceeded") {
		t.Fatalf("buffered Git output = %v, want configured-limit rejection", err)
	}
	if err := runner.runDiscardingOutput(context.Background(), repository, "cat-file", "blob", oid); err != nil {
		t.Fatalf("discarded Git output = %v", err)
	}
}

func TestValidateRemoteURL(t *testing.T) {
	for _, remote := range []string{
		"git@github.com:GhostFlying/delegation.git",
		"ssh://git@github.com/GhostFlying/delegation.git",
		"https://github.com/GhostFlying/delegation.git",
	} {
		if err := ValidateRemoteURL(remote); err != nil {
			t.Errorf("ValidateRemoteURL(%q) = %v", remote, err)
		}
	}

	for _, remote := range []string{
		"", " ../repo ", "../repo", "/tmp/repo", "file:///tmp/repo",
		"ext::command", "git://example.com/repo", "https://user@example.com/repo",
		"https://example.com/repo?token=secret", "ssh://user:secret@example.com/repo",
		"ssh://example.com/", "https://example.com/repo#branch", "host:\\repo",
		"C:/repo", `C:\repo`, "C:relative", "-host:repo", "-user@host:repo",
		"ssh://-host/repo", "ssh://-user@host/repo", "https://-host/repo",
	} {
		t.Run(strings.ReplaceAll(remote, "/", "_"), func(t *testing.T) {
			if err := ValidateRemoteURL(remote); err == nil {
				t.Fatalf("ValidateRemoteURL(%q) succeeded", remote)
			}
		})
	}
}

func TestCloneDirectPreservesContextCancellation(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(ctx, destination, repository.Manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloneDirect() = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled clone destination still exists: %v", err)
	}
}

func TestInspectReportsSubmoduleAndLFSWarnings(t *testing.T) {
	runner := testRunner(t)
	remote, source, head := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(
		filepath.Join(source, ".gitattributes"), []byte("*.bin filter=lfs\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "asset.bin"), []byte("lfs pointer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", ".gitattributes", "asset.bin")
	gitRun(t, runner.Binary, source, "update-index", "--add", "--cacheinfo", "160000,"+head+",vendor/module")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add external content markers",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")

	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"lfs_payload_not_transferred", "submodule_repository_not_transferred"}
	if !reflect.DeepEqual(repository.Manifest.Warnings, want) {
		t.Fatalf("workspace warnings = %#v, want %#v", repository.Manifest.Warnings, want)
	}
}

func TestInspectRejectsRepositoryLocalExecutableFilter(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	gitRun(t, runner.Binary, source, "config", "filter.delegation-test.process", "untrusted-command")
	_, err := runner.Inspect(context.Background(), source, remote)
	if err == nil || !strings.Contains(err.Error(), "executable clean or process filters") {
		t.Fatalf("Inspect() = %v, want repository filter rejection", err)
	}
}

func TestCloneDirectRejectsSymlinkedWorkingDirectory(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.Symlink("nested", filepath.Join(source, "linked")); err != nil {
		t.Skipf("creating a directory symlink is unavailable: %v", err)
	}
	gitRun(t, runner.Binary, source, "add", "linked")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add linked working directory",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	repository.Manifest.WorkingDirectory = "linked"
	destination := filepath.Join(t.TempDir(), "prepared")
	err = runner.CloneDirect(context.Background(), destination, repository.Manifest)
	if err == nil || !strings.Contains(err.Error(), "real directories") {
		t.Fatalf("CloneDirect() = %v, want symlink rejection", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected clone destination still exists: %v", err)
	}
}

func TestInspectAndCloneDirectExactCleanHead(t *testing.T) {
	runner := testRunner(t)
	remote, source, head := createRemoteRepository(t, runner.Binary)
	nested := filepath.Join(source, "nested")

	repository, err := runner.Inspect(context.Background(), nested, remote)
	if err != nil {
		t.Fatal(err)
	}
	repositoryRootInfo, err := os.Stat(repository.Root)
	if err != nil {
		t.Fatal(err)
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(repositoryRootInfo, sourceInfo) {
		t.Fatalf("repository root = %q, want same directory as %q", repository.Root, source)
	}
	if repository.Manifest.GitURL != remote || repository.Manifest.HeadOID != head ||
		repository.Manifest.ObjectFormat != "sha1" || repository.Manifest.WorkingDirectory != "nested" ||
		!repository.Manifest.Clean || len(repository.Manifest.Warnings) != 0 {
		t.Fatalf("manifest = %#v", repository.Manifest)
	}
	if repository.Manifest.SourceSnapshotHash == strings.Repeat("0", 64) {
		t.Fatal("source snapshot hash was not derived from the repository")
	}

	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, runner.Binary, destination, "rev-parse", "HEAD^{commit}"); got != head {
		t.Fatalf("cloned HEAD = %q, want %q", got, head)
	}
	if got := gitOutput(t, runner.Binary, destination, "symbolic-ref", "-q", "HEAD"); got != "" {
		t.Fatalf("cloned HEAD is attached to %q", got)
	}
	if data, err := os.ReadFile(filepath.Join(destination, "nested", "hello.txt")); err != nil || string(data) != "hello\n" {
		t.Fatalf("cloned file = %q, %v", data, err)
	}
	if got := gitOutput(t, runner.Binary, destination, "status", "--porcelain=v2"); got != "" {
		t.Fatalf("cloned status = %q", got)
	}
	if err := runner.VerifyDirect(context.Background(), destination, head, "sha1"); err != nil {
		t.Fatalf("VerifyDirect() = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "nested", "hello.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.VerifyDirect(context.Background(), destination, head, "sha1"); err == nil {
		t.Fatal("VerifyDirect accepted a modified workspace")
	}
}

func TestDirtyWorkspaceRequiresBundleAndDoesNotPublishDestination(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Manifest.Clean {
		t.Fatal("dirty worktree reported clean")
	}
	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); !errors.Is(err, ErrBundleRequired) {
		t.Fatalf("CloneDirect() = %v, want ErrBundleRequired", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed clone destination still exists: %v", err)
	}
}

func TestDirectCloneRequiresSourceWorkingDirectoryInCheckout(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	empty := filepath.Join(source, "empty")
	if err := os.Mkdir(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	repository, err := runner.Inspect(context.Background(), empty, remote)
	if err != nil {
		t.Fatal(err)
	}
	if !repository.Manifest.Clean || repository.Manifest.WorkingDirectory != "empty" {
		t.Fatalf("empty cwd manifest = %#v", repository.Manifest)
	}
	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); err == nil ||
		!strings.Contains(err.Error(), "working directory is absent") {
		t.Fatalf("CloneDirect() = %v, want absent working directory error", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete destination still exists: %v", err)
	}
}

func TestPrepareBaseDiscardsRemoteWithDifferentObjectFormat(t *testing.T) {
	runner := testRunner(t)
	remote, _, _ := createRemoteRepository(t, runner.Binary)
	manifest := protocol.WorkspaceManifest{
		GitURL: remote, HeadOID: strings.Repeat("a", 64), ObjectFormat: "sha256",
		Clean: true, SourceSnapshotHash: strings.Repeat("b", 64), Warnings: []string{},
	}
	destination := filepath.Join(t.TempDir(), "prepared")
	prepared, err := runner.PrepareBase(context.Background(), destination, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.BundleRequired || prepared.OverlayRequired || len(prepared.BasisOIDs) != 0 {
		t.Fatalf("base preparation = %#v", prepared)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched clone destination still exists: %v", err)
	}
}

func TestHardenedEnvironmentReplacesGitOverrides(t *testing.T) {
	input := []string{
		"PATH=/bin", "GIT_DIR=/attacker", "git_work_tree=/attacker", "GCM_INTERACTIVE=Always",
		"CODEX_ACCESS_TOKEN=codex-secret", "TEST_PROVIDER_VALUE=provider-secret",
	}
	for _, variable := range input {
		name, value, _ := strings.Cut(variable, "=")
		t.Setenv(name, value)
	}
	environment := hardenedEnvironment([]string{"CODEX_ACCESS_TOKEN", "TEST_PROVIDER_VALUE"})
	want := map[string]string{
		"GIT_TERMINAL_PROMPT": "0",
		"GCM_INTERACTIVE":     "Never",
		"GIT_LFS_SKIP_SMUDGE": "1",
		"GIT_SSH_COMMAND":     "ssh -o BatchMode=yes",
	}
	got := make(map[string]string)
	for _, variable := range environment {
		name, value, _ := strings.Cut(variable, "=")
		if strings.EqualFold(name, "CODEX_ACCESS_TOKEN") || strings.EqualFold(name, "TEST_PROVIDER_VALUE") {
			t.Fatalf("hardened Git environment retained credential %s", name)
		}
		if strings.HasPrefix(strings.ToUpper(name), "GIT_") || strings.EqualFold(name, "GCM_INTERACTIVE") {
			got[strings.ToUpper(name)] = value
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hardened Git environment = %#v, want %#v", got, want)
	}
}

func testRunner(t *testing.T) Runner {
	t.Helper()
	binary, err := exec.LookPath("git")
	if err != nil {
		t.Skip("Git is unavailable")
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner(binary)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func createRemoteRepository(t *testing.T, gitBinary string) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	source := filepath.Join(root, "source")
	gitRun(t, gitBinary, root, "init", "--bare", remote)
	gitRun(t, gitBinary, root, "init", source)
	if err := os.Mkdir(filepath.Join(source, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, gitBinary, source, "add", "nested/hello.txt")
	gitRun(t, gitBinary, source, "-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid", "commit", "-m", "initial")
	gitRun(t, gitBinary, source, "remote", "add", "origin", remote)
	gitRun(t, gitBinary, source, "push", "origin", "HEAD:refs/heads/main")
	gitRun(t, gitBinary, root, "--git-dir="+remote, "update-server-info")
	server := httptest.NewTLSServer(http.FileServer(http.Dir(root)))
	t.Cleanup(server.Close)
	gitHome := filepath.Join(root, "git-home")
	if err := os.Mkdir(gitHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(gitHome, ".gitconfig"), []byte("[http]\n\tsslVerify = false\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", gitHome)
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	remoteURL := server.URL + "/" + filepath.Base(remote)
	return remoteURL, source, gitOutput(t, gitBinary, source, "rev-parse", "HEAD^{commit}")
}

func gitRun(t *testing.T, gitBinary, directory string, args ...string) {
	t.Helper()
	command := exec.Command(gitBinary, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}

func gitOutput(t *testing.T, gitBinary, directory string, args ...string) string {
	t.Helper()
	command := exec.Command(gitBinary, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := command.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok && args[0] == "symbolic-ref" {
			return ""
		}
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(output))
}
