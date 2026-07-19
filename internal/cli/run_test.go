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
	if got, want := stdout.String(), "{\"version\":\"0.1.0-alpha.0.m1.1\"}\n"; got != want {
		t.Fatalf("Run() stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("Run() stderr = %q, want empty", stderr.String())
	}
}

func TestMCPRootReportsMissingConfiguration(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	t.Setenv("DELEGATION_CONFIG", t.TempDir()+"/missing.json")

	code := Run([]string{"mcp", "root"}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("Run() code = %d, want 1", code)
	}
	if got := stderr.String(); !bytes.Contains([]byte(got), []byte("read config")) {
		t.Fatalf("Run() stderr = %q, want missing config error", got)
	}
}
