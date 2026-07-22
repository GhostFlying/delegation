package serviceenv

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const serviceConfig = `{"model":"test","model_provider":"gateway","model_providers.gateway":{"name":"Gateway","base_url":"https://gateway.example.test/v1","wire_api":"responses","requires_openai_auth":false,"env_key":"GATEWAY_KEY"}}`

func TestLoadProtectedFileResolvesOnlyReferencedProviderEnvironment(t *testing.T) {
	path := protectedEnvironmentFile(t, strings.Join([]string{
		"# literal service environment",
		codexconfig.EnvironmentVariable + "=" + serviceConfig,
		"GATEWAY_KEY=value=with=equals",
		"",
	}, "\r\n"))
	resolved, err := LoadProtectedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Config["model"] != "test" || resolved.Environment["GATEWAY_KEY"] != "value=with=equals" ||
		len(resolved.Environment) != 1 {
		t.Fatalf("resolved environment = %#v", resolved)
	}
}

func TestLoadProtectedFileRejectsUnsafeOrAmbiguousContent(t *testing.T) {
	for name, contents := range map[string]string{
		"unreferenced": codexconfig.EnvironmentVariable + "=" + serviceConfig + "\nGATEWAY_KEY=key\nOTHER=value\n",
		"duplicate":    "KEY=one\nKEY=two\n",
		"shell export": "export KEY=value\n",
		"empty key":    "=value\n",
		"carriage":     "KEY=one\rtwo\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadProtectedFile(protectedEnvironmentFile(t, contents)); err == nil {
				t.Fatal("LoadProtectedFile accepted unsafe content")
			}
		})
	}
}

func TestEnvironmentNameCaseSemanticsMatchPlatform(t *testing.T) {
	environment, err := parse([]byte("KEY=one\nkey=two\n"))
	if runtime.GOOS == "windows" {
		if err == nil {
			t.Fatal("parse accepted case-fold duplicate Windows variables")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if environment["KEY"] != "one" || environment["key"] != "two" {
		t.Fatalf("parsed environment = %#v", environment)
	}
}

func TestEnvironmentParserEnforcesInputBounds(t *testing.T) {
	entries := make([]string, maximumEntries+1)
	for index := range entries {
		entries[index] = fmt.Sprintf("KEY_%d=value", index)
	}
	for name, data := range map[string][]byte{
		"invalid UTF-8": {0xff},
		"NUL":           []byte("KEY=one\x00two\n"),
		"long line":     []byte("KEY=" + strings.Repeat("x", maximumLineBytes)),
		"too many":      []byte(strings.Join(entries, "\n")),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parse(data); err == nil {
				t.Fatal("parse accepted out-of-bounds input")
			}
		})
	}
}

func TestLoadProtectedFileRejectsOversizedInput(t *testing.T) {
	path := protectedEnvironmentFile(t, strings.Repeat("x", maximumFileBytes+1))
	if _, err := LoadProtectedFile(path); err == nil {
		t.Fatal("LoadProtectedFile accepted an oversized file")
	}
}

func TestLoadProtectedFileDoesNotFallBackToAmbientCredentials(t *testing.T) {
	t.Setenv("GATEWAY_KEY", "ambient-secret")
	path := protectedEnvironmentFile(t, codexconfig.EnvironmentVariable+"="+serviceConfig+"\n")
	errText := ""
	if _, err := LoadProtectedFile(path); err != nil {
		errText = err.Error()
	}
	if !strings.Contains(errText, "GATEWAY_KEY") || strings.Contains(errText, "ambient-secret") {
		t.Fatalf("LoadProtectedFile error = %q", errText)
	}
}

func TestLoadProtectedFileRejectsUnprotectedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peer.env")
	if err := os.WriteFile(
		path,
		[]byte(codexconfig.EnvironmentVariable+"="+serviceConfig+"\nGATEWAY_KEY=key\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := LoadProtectedFile(path); err == nil {
		t.Fatal("LoadProtectedFile accepted an unprotected file")
	}
}

func protectedEnvironmentFile(t *testing.T, contents string) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "peer.env")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
