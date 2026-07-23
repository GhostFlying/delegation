package gitworkspace

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestSHA256ThinBundleRoundTripsDirtyIndexAndIntentToAdd(t *testing.T) {
	runner := testRunner(t)
	remote, source, remoteHead := createRemoteRepositoryWithObjectFormat(t, runner.Binary, "sha256")
	if err := os.WriteFile(filepath.Join(source, "unpublished.txt"), []byte("local only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "unpublished.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "unpublished SHA-256 head",
	)
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("staged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "nested/hello.txt")
	if err := os.WriteFile(filepath.Join(source, "intent.txt"), []byte("intent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "intent.txt")

	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Manifest.ObjectFormat != "sha256" || len(repository.Manifest.HeadOID) != 64 ||
		repository.Manifest.HeadOID == remoteHead || repository.Manifest.Clean {
		t.Fatalf("SHA-256 dirty manifest = %#v", repository.Manifest)
	}
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !preparation.BundleRequired || !preparation.OverlayRequired || len(preparation.BasisOIDs) == 0 {
		t.Fatalf("SHA-256 base preparation = %#v", preparation)
	}
	bundle := filepath.Join(t.TempDir(), "workspace.bundle")
	strategy, err := runner.CreateBundle(
		context.Background(), repository.Root, bundle, repository.Manifest, preparation.BasisOIDs,
	)
	if err != nil || strategy != protocol.WorkspaceStrategyThin {
		t.Fatalf("SHA-256 bundle = %q, %v", strategy, err)
	}
	overlay := filepath.Join(t.TempDir(), "workspace.tar.zst")
	if err := runner.CreateOverlay(context.Background(), repository.Root, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.ApplyOverlay(context.Background(), target, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if err := runner.VerifySnapshot(context.Background(), target, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	assertGitStateMatches(t, runner.Binary, source, target)
}

func TestBundleOverlayCreatesWorkingDirectoryAbsentFromHead(t *testing.T) {
	for _, test := range []struct {
		name        string
		unavailable bool
		strategy    protocol.WorkspaceStrategy
	}{
		{name: "thin", strategy: protocol.WorkspaceStrategyThin},
		{name: "self-contained", unavailable: true, strategy: protocol.WorkspaceStrategyFull},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			if err := os.WriteFile(filepath.Join(source, "unpublished.txt"), []byte("local only\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitRun(t, runner.Binary, source, "add", "unpublished.txt")
			gitRun(
				t, runner.Binary, source,
				"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
				"commit", "-m", "unpublished head",
			)
			workingDirectory := filepath.Join(source, "new", "cwd")
			if err := os.MkdirAll(workingDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workingDirectory, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitURL := remote
			if test.unavailable {
				gitURL = "https://127.0.0.1:1/unavailable.git"
			}
			repository, err := runner.Inspect(context.Background(), workingDirectory, gitURL)
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "target")
			preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if !preparation.BundleRequired || !preparation.OverlayRequired ||
				(test.unavailable != (len(preparation.BasisOIDs) == 0)) {
				t.Fatalf("base preparation = %#v", preparation)
			}
			bundle := filepath.Join(t.TempDir(), "workspace.bundle")
			strategy, err := runner.CreateBundle(
				context.Background(), repository.Root, bundle, repository.Manifest, preparation.BasisOIDs,
			)
			if err != nil || strategy != test.strategy {
				t.Fatalf("bundle strategy = %q, %v", strategy, err)
			}
			overlay := filepath.Join(t.TempDir(), "workspace.tar.zst")
			if err := runner.CreateOverlay(context.Background(), repository.Root, overlay, repository.Manifest); err != nil {
				t.Fatal(err)
			}
			if err := runner.ApplyBundle(context.Background(), target, bundle, repository.Manifest); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(target, "new", "cwd")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("bundle unexpectedly created dirty cwd: %v", err)
			}
			if err := runner.ApplyOverlay(context.Background(), target, overlay, repository.Manifest); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(target, "new", "cwd", "dirty.txt"))
			if err != nil || string(data) != "dirty\n" {
				t.Fatalf("dirty cwd file = %q, %v", data, err)
			}
		})
	}
}

func TestOverlayIgnoresTargetGlobalExcludes(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	sourceIgnore := filepath.Join(t.TempDir(), "source-ignore")
	if err := os.WriteFile(sourceIgnore, []byte("ignored.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "config", "--global", "core.excludesFile", sourceIgnore)
	for name, content := range map[string]string{"trace.log": "visible\n", "ignored.tmp": "ignored\n"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(t.TempDir(), "workspace.tar.zst")
	if err := runner.CreateOverlay(context.Background(), repository.Root, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	targetHome := filepath.Join(t.TempDir(), "target-home")
	if err := os.Mkdir(targetHome, 0o700); err != nil {
		t.Fatal(err)
	}
	targetIgnore := filepath.Join(targetHome, "global-ignore")
	if err := os.WriteFile(targetIgnore, []byte("*.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := "[http]\n\tsslVerify = false\n[core]\n\texcludesFile = " + filepath.ToSlash(targetIgnore) + "\n"
	if err := os.WriteFile(filepath.Join(targetHome, ".gitconfig"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", targetHome)
	target := filepath.Join(t.TempDir(), "target")
	preparation, err := runner.PrepareBase(context.Background(), target, repository.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.BundleRequired || !preparation.OverlayRequired {
		t.Fatalf("target preparation = %#v", preparation)
	}
	if err := runner.ApplyOverlay(context.Background(), target, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, runner.Binary, target, "status", "--porcelain", "--untracked-files=normal"); got != "?? trace.log" {
		t.Fatalf("target status = %q", got)
	}
	if _, err := os.Stat(filepath.Join(target, "ignored.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source-ignored file reached target: %v", err)
	}
}

func TestTargetCheckoutIgnoresGlobalAttributes(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	targetHome := filepath.Join(t.TempDir(), "target-home")
	if err := os.Mkdir(targetHome, 0o700); err != nil {
		t.Fatal(err)
	}
	globalAttributes := filepath.Join(targetHome, "global-attributes")
	if err := os.WriteFile(globalAttributes, []byte("* text eol=crlf\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := "[http]\n\tsslVerify = false\n[core]\n\tattributesFile = " + filepath.ToSlash(globalAttributes) + "\n"
	if err := os.WriteFile(filepath.Join(targetHome, ".gitconfig"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", targetHome)
	target := filepath.Join(t.TempDir(), "target")
	if err := runner.CloneDirect(context.Background(), target, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, "nested", "hello.txt"))
	if err != nil || !bytes.Equal(data, []byte("hello\n")) {
		t.Fatalf("target bytes = %q, %v", data, err)
	}
	if got := gitOutput(t, runner.Binary, target, "config", "--local", "--get", "core.attributesFile"); got != "" {
		t.Fatalf("target core.attributesFile = %q", got)
	}
}

func TestTargetCheckoutDoesNotExecuteAmbientFilter(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(
		filepath.Join(source, ".gitattributes"), []byte("*.txt filter=delegationtest\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", ".gitattributes")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "select an optional filter",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	targetHome := filepath.Join(t.TempDir(), "target-home")
	if err := os.Mkdir(targetHome, 0o700); err != nil {
		t.Fatal(err)
	}
	config := "[http]\n\tsslVerify = false\n" +
		"[filter \"delegationtest\"]\n" +
		"\tsmudge = delegation-filter-command-that-does-not-exist\n" +
		"\trequired = true\n"
	if err := os.WriteFile(filepath.Join(targetHome, ".gitconfig"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", targetHome)
	target := filepath.Join(t.TempDir(), "target")
	if err := runner.CloneDirect(context.Background(), target, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, "nested", "hello.txt"))
	if err != nil || !bytes.Equal(data, []byte("hello\n")) {
		t.Fatalf("target bytes = %q, %v", data, err)
	}
}

func TestTargetCheckoutIgnoresAmbientGitTemplate(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	targetHome := filepath.Join(t.TempDir(), "target-home")
	template := filepath.Join(targetHome, "template")
	if err := os.MkdirAll(filepath.Join(template, "info"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(template, "info", "attributes"), []byte("* text eol=crlf\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	config := "[http]\n\tsslVerify = false\n[init]\n\ttemplateDir = " + filepath.ToSlash(template) + "\n"
	if err := os.WriteFile(filepath.Join(targetHome, ".gitconfig"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", targetHome)
	target := filepath.Join(t.TempDir(), "target")
	if err := runner.CloneDirect(context.Background(), target, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(target, ".git", "info", "attributes")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ambient template attributes reached target: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "nested", "hello.txt"))
	if err != nil || !bytes.Equal(data, []byte("hello\n")) {
		t.Fatalf("target bytes = %q, %v", data, err)
	}
}

func TestInspectRejectsGitAdministrativeFilesystemAliases(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("constructing aliases of .git is safe only on the Linux test filesystem")
	}
	for _, alias := range []string{"git~1/config", ".g\u200cit/config"} {
		t.Run(alias, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			name := filepath.Join(source, filepath.FromSlash(alias))
			if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
				t.Skipf("filesystem cannot construct alias fixture: %v", err)
			}
			if err := os.WriteFile(name, []byte("not Git metadata\n"), 0o600); err != nil {
				t.Skipf("filesystem cannot construct alias fixture: %v", err)
			}
			if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
				!strings.Contains(err.Error(), "Git administrative data") {
				t.Fatalf("Inspect() = %v", err)
			}
		})
	}
}

func TestInspectRejectsPortableAncestorAliases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows cannot construct case-distinct ancestor aliases")
	}
	for _, test := range []struct {
		name  string
		dirty bool
	}{
		{name: "final index"},
		{name: "final worktree", dirty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			base := filepath.Join(source, "portable-root")
			if err := os.WriteFile(base, []byte("base\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			gitRun(t, runner.Binary, source, "add", "portable-root")
			gitRun(
				t, runner.Binary, source,
				"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
				"commit", "-m", "add portable alias base",
			)
			aliasDirectory := filepath.Join(source, "PORTABLE-ROOT")
			if err := os.Mkdir(aliasDirectory, 0o700); err != nil {
				t.Skipf("filesystem is case-insensitive: %v", err)
			}
			if err := os.WriteFile(filepath.Join(aliasDirectory, "child.txt"), []byte("child\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if !test.dirty {
				gitRun(t, runner.Binary, source, "add", "PORTABLE-ROOT/child.txt")
				gitRun(
					t, runner.Binary, source,
					"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
					"commit", "-m", "add portable ancestor alias",
				)
			}
			if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
				!strings.Contains(err.Error(), "portable file ancestor conflict") {
				t.Fatalf("Inspect() = %v", err)
			}
		})
	}
}

func TestOverlayRoundTripsFileDirectoryTransitions(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	if err := os.WriteFile(filepath.Join(source, "file-to-dir"), []byte("file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "dir-to-file"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "dir-to-file", "child.txt"), []byte("child\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "file-to-dir", "dir-to-file/child.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add type-transition fixtures",
	)
	gitRun(t, runner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, runner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, runner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	if err := os.Remove(filepath.Join(source, "file-to-dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "file-to-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "file-to-dir", "child.txt"), []byte("replacement child\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(source, "dir-to-file")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "dir-to-file"), []byte("replacement file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target, manifest := roundTripDirtyOverlay(t, runner, remote, source)
	if err := runner.VerifySnapshot(context.Background(), target, manifest); err != nil {
		t.Fatal(err)
	}
	assertGitStateMatches(t, runner.Binary, source, target)
	for name, want := range map[string]string{
		"file-to-dir/child.txt": "replacement child\n",
		"dir-to-file":           "replacement file\n",
	} {
		data, err := os.ReadFile(filepath.Join(target, filepath.FromSlash(name)))
		if err != nil || string(data) != want {
			t.Fatalf("target %s = %q, %v", name, data, err)
		}
	}
}

func TestOverlayRoundTripsIntentToAddTopologyTransitions(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	ancestorITA := filepath.Join(source, "ita-to-directory")
	if err := os.WriteFile(ancestorITA, []byte("intent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "ita-to-directory")
	if err := os.Remove(ancestorITA); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ancestorITA, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ancestorITA, "child.txt"), []byte("child\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	descendantParent := filepath.Join(source, "ita-under-file")
	if err := os.Mkdir(descendantParent, 0o700); err != nil {
		t.Fatal(err)
	}
	descendantITA := filepath.Join(descendantParent, "child.txt")
	if err := os.WriteFile(descendantITA, []byte("intent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "-N", "ita-under-file/child.txt")
	if err := os.RemoveAll(descendantParent); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(descendantParent, []byte("shadowing file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target, _ := roundTripDirtyOverlay(t, runner, remote, source)
	assertGitStateMatches(t, runner.Binary, source, target)
	for name, want := range map[string]string{
		"ita-to-directory/child.txt": "child\n",
		"ita-under-file":             "shadowing file\n",
	} {
		data, err := os.ReadFile(filepath.Join(target, filepath.FromSlash(name)))
		if err != nil || string(data) != want {
			t.Fatalf("target %s = %q, %v", name, data, err)
		}
	}
}

func TestInspectRejectsUnsupportedIndexStates(t *testing.T) {
	for _, test := range []struct {
		name string
		arg  string
	}{
		{name: "skip-worktree", arg: "--skip-worktree"},
		{name: "assume-unchanged", arg: "--assume-unchanged"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			gitRun(t, runner.Binary, source, "update-index", test.arg, "nested/hello.txt")
			if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
				!strings.Contains(err.Error(), "skip-worktree or assume-unchanged") {
				t.Fatalf("Inspect() = %v", err)
			}
		})
	}
}

func TestInspectRejectsUnmergedAndResolveUndoIndex(t *testing.T) {
	runner := testRunner(t)
	remote, source, base := createRemoteRepository(t, runner.Binary)
	gitRun(t, runner.Binary, source, "checkout", "-b", "conflicting-side")
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("side\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "nested/hello.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "side",
	)
	gitRun(t, runner.Binary, source, "checkout", "--detach", base)
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "nested/hello.txt")
	gitRun(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "main",
	)
	gitRunExpectFailure(
		t, runner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"merge", "--no-edit", "conflicting-side",
	)
	if got := rawGitOutput(t, runner.Binary, source, "ls-files", "--unmerged", "-z"); len(got) == 0 {
		t.Fatal("conflict fixture did not create unmerged index entries")
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
		!strings.Contains(err.Error(), "unmerged Git index") {
		t.Fatalf("unmerged Inspect() = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "hello.txt"), []byte("resolved\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, runner.Binary, source, "add", "nested/hello.txt")
	if got := rawGitOutput(t, runner.Binary, source, "ls-files", "--resolve-undo", "-z"); len(got) == 0 {
		t.Fatal("resolved fixture did not retain resolve-undo state")
	}
	if _, err := runner.Inspect(context.Background(), source, remote); err == nil ||
		!strings.Contains(err.Error(), "resolve-undo") {
		t.Fatalf("resolve-undo Inspect() = %v", err)
	}
}

func roundTripDirtyOverlay(
	t *testing.T,
	runner Runner,
	remote, source string,
) (string, protocol.WorkspaceManifest) {
	t.Helper()
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Manifest.Clean {
		t.Fatal("dirty fixture was reported clean")
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
	if err := runner.ApplyOverlay(context.Background(), target, overlay, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	return target, repository.Manifest
}

func assertGitStateMatches(t *testing.T, gitBinary, source, target string) {
	t.Helper()
	for _, args := range [][]string{
		{"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev", "--no-renames", "--ita-visible-in-index", "HEAD", "--"},
		{"--no-replace-objects", "diff-index", "--cached", "--raw", "-z", "--no-abbrev", "--no-renames", "--ita-invisible-in-index", "HEAD", "--"},
		{"--no-replace-objects", "diff-files", "--raw", "-z", "--no-abbrev", "--no-renames", "--ignore-submodules=all", "--ita-invisible-in-index", "--"},
		{"ls-files", "--others", "--exclude-standard", "-z", "--full-name"},
		{"status", "--porcelain=v2", "-z", "--untracked-files=normal", "--ignore-submodules=all"},
	} {
		sourceOutput := rawGitOutput(t, gitBinary, source, args...)
		targetOutput := rawGitOutput(t, gitBinary, target, args...)
		if !bytes.Equal(sourceOutput, targetOutput) {
			t.Fatalf("git %v differs after overlay\nsource: %q\ntarget: %q", args, sourceOutput, targetOutput)
		}
	}
}

func gitRunExpectFailure(t *testing.T, gitBinary, directory string, args ...string) {
	t.Helper()
	command := exec.Command(gitBinary, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("git %v unexpectedly succeeded\n%s", args, output)
	}
}
