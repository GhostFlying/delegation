package cli

import (
	"bytes"
	"testing"
)

func TestVersionJSON(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"version", "--json"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("Run() code = %d, want 0; stderr = %q", code, stderr.String())
	}
	if got, want := stdout.String(), "{\"version\":\"0.1.0-alpha.0\"}\n"; got != want {
		t.Fatalf("Run() stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("Run() stderr = %q, want empty", stderr.String())
	}
}

func TestMCPRootIsExplicitlyUnavailable(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"mcp", "root"}, &stdout, &stderr)

	if code != exitUnavailable {
		t.Fatalf("Run() code = %d, want %d", code, exitUnavailable)
	}
	if got, want := stderr.String(), "delegation: root MCP is not available in the M0 runtime scaffold\n"; got != want {
		t.Fatalf("Run() stderr = %q, want %q", got, want)
	}
}
