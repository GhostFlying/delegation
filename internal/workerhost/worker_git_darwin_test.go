//go:build darwin

package workerhost

import (
	"context"
	"testing"
)

func TestResolveWorkerGitBinaryUsesDeveloperGit(t *testing.T) {
	const configured = "/opt/homebrew/bin/git"
	resolved, err := resolveWorkerGitBinary(context.Background(), configured)
	if err != nil {
		t.Fatal(err)
	}
	if resolved == configured {
		t.Fatalf("managed worker Git binary retains configured Homebrew Git %q", resolved)
	}
}
