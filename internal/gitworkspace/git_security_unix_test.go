//go:build !windows

package gitworkspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCloneDirectBlocksInsteadOfCustomTransport(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	repository, err := runner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}
	helperDirectory := t.TempDir()
	marker := filepath.Join(helperDirectory, "invoked")
	helper := filepath.Join(helperDirectory, "git-remote-delegation-test")
	if err := os.WriteFile(
		helper,
		[]byte("#!/bin/sh\nprintf invoked > \"$DELEGATION_TEST_MARKER\"\nexit 1\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_MARKER", marker)
	t.Setenv("PATH", helperDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".gitconfig")
	file, err := os.OpenFile(config, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("\n[url \"delegation-test::\"]\n\tinsteadOf = " + remote + "\n"); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); !errors.Is(err, ErrBundleRequired) {
		t.Fatalf("CloneDirect() = %v, want ErrBundleRequired", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("custom Git remote helper was invoked: %v", err)
	}
}

func TestCloneDirectOverridesSymlinkFallbackBeforeCheckout(t *testing.T) {
	baseRunner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, baseRunner.Binary)
	if err := os.Symlink("nested/hello.txt", filepath.Join(source, "tracked-link")); err != nil {
		t.Skipf("creating a symlink is unavailable: %v", err)
	}
	gitRun(t, baseRunner.Binary, source, "add", "tracked-link")
	gitRun(
		t, baseRunner.Binary, source,
		"-c", "user.name=Delegation Test", "-c", "user.email=test@example.invalid",
		"commit", "-m", "add tracked symlink",
	)
	gitRun(t, baseRunner.Binary, source, "push", "origin", "HEAD:refs/heads/main")
	remotePath := gitOutput(t, baseRunner.Binary, source, "remote", "get-url", "origin")
	gitRun(t, baseRunner.Binary, source, "--git-dir="+remotePath, "update-server-info")
	repository, err := baseRunner.Inspect(context.Background(), source, remote)
	if err != nil {
		t.Fatal(err)
	}

	wrapper := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(wrapper, []byte(`#!/bin/sh
is_clone=
destination=
for argument in "$@"; do
	if [ "$argument" = clone ]; then
		is_clone=1
	fi
	destination=$argument
done
"$DELEGATION_TEST_REAL_GIT" "$@"
status=$?
if [ "$status" -eq 0 ] && [ "$is_clone" = 1 ]; then
	"$DELEGATION_TEST_REAL_GIT" -C "$destination" config core.symlinks false || exit $?
fi
exit "$status"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_REAL_GIT", baseRunner.Binary)
	runner, err := NewRunner(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "prepared")
	if err := runner.CloneDirect(context.Background(), destination, repository.Manifest); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(destination, "tracked-link"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("tracked symlink was materialized as %s", info.Mode().Type())
	}
	if got := gitOutput(t, baseRunner.Binary, destination, "config", "--bool", "core.symlinks"); got != "true" {
		t.Fatalf("target core.symlinks = %q", got)
	}
}

func TestInspectDisablesRepositoryFSMonitor(t *testing.T) {
	runner := testRunner(t)
	remote, source, _ := createRemoteRepository(t, runner.Binary)
	helperDirectory := t.TempDir()
	marker := filepath.Join(helperDirectory, "fsmonitor-invoked")
	helper := filepath.Join(helperDirectory, "fsmonitor")
	if err := os.WriteFile(
		helper,
		[]byte("#!/bin/sh\nprintf invoked > \"$DELEGATION_TEST_MARKER\"\nexit 1\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DELEGATION_TEST_MARKER", marker)
	gitRun(t, runner.Binary, source, "config", "core.fsmonitor", helper)
	if _, err := runner.Inspect(context.Background(), source, remote); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("repository fsmonitor was invoked: %v", err)
	}
}

func TestInspectRejectsRepositoryExecutableFilters(t *testing.T) {
	for _, scope := range []string{"--local", "--worktree"} {
		t.Run(strings.TrimPrefix(scope, "--"), func(t *testing.T) {
			runner := testRunner(t)
			remote, source, _ := createRemoteRepository(t, runner.Binary)
			helperDirectory := t.TempDir()
			marker := filepath.Join(helperDirectory, "filter-invoked")
			helper := filepath.Join(helperDirectory, "filter")
			if err := os.WriteFile(
				helper,
				[]byte("#!/bin/sh\nprintf invoked > \"$DELEGATION_TEST_MARKER\"\ncat\n"),
				0o700,
			); err != nil {
				t.Fatal(err)
			}
			t.Setenv("DELEGATION_TEST_MARKER", marker)
			if scope == "--worktree" {
				gitRun(t, runner.Binary, source, "config", "extensions.worktreeConfig", "true")
			}
			gitRun(t, runner.Binary, source, "config", scope, "filter.delegation-test.clean", helper)
			_, err := runner.Inspect(context.Background(), source, remote)
			if err == nil || !strings.Contains(err.Error(), "executable clean or process filters") {
				t.Fatalf("Inspect() = %v, want repository filter rejection", err)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("repository clean filter was invoked: %v", err)
			}
		})
	}
}
