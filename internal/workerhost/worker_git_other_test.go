//go:build !darwin

package workerhost

import (
	"context"
	"testing"
)

func TestResolveWorkerGitBinaryKeepsConfiguredGit(t *testing.T) {
	const configured = "/opt/delegation-test/bin/git"
	resolved, err := resolveWorkerGitBinary(context.Background(), configured)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != configured {
		t.Fatalf("managed worker Git binary = %q, want %q", resolved, configured)
	}
}
