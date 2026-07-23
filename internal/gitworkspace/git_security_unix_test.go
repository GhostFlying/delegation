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
