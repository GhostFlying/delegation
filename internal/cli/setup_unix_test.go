//go:build !windows

package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveGitExecutableAcceptsShebangWrapper(t *testing.T) {
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "enterprise-git")
	if err := os.WriteFile(
		wrapper,
		[]byte("#!/bin/sh\nexec \"$DELEGATION_TEST_REAL_GIT\" \"$@\"\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveGitExecutable(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(resolved, "--version")
	command.Env = append(os.Environ(), "DELEGATION_TEST_REAL_GIT="+realGit)
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(output), "git version ") {
		t.Fatalf("wrapper output = %q", output)
	}
}
