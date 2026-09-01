package cli

import (
	"os"
	"path/filepath"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const testTraeAuth = `{"auth_mode":"trae","trae":{"access_token":"test-access-token","credential_kind":"cloud_cli_jwt","login_method":"test","region":"CN","version":2}}`

func privateTestDirectory(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "private")
}

func privateTestPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(privateTestDirectory(t), name)
}

func testTraeAuthFile(t *testing.T) string {
	t.Helper()
	directory := privateTestDirectory(t)
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "trae-auth.json")
	if err := os.WriteFile(path, []byte(testTraeAuth), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
