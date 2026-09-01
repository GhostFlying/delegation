package traexauth

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

func TestReadSourceRequiresProtectedSupportedTraeAccount(t *testing.T) {
	root := privateAuthDirectory(t)
	valid := testAuth("first-token")
	path := filepath.Join(root, "auth.json")
	if err := os.WriteFile(path, valid, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSource(path)
	if err != nil || string(got) != string(valid) {
		t.Fatalf("ReadSource() = %q, %v", got, err)
	}

	for _, test := range []struct {
		name string
		data string
		want string
	}{
		{name: "malformed JSON", data: `{`, want: "valid JSON"},
		{name: "wrong mode", data: `{"auth_mode":"api"}`, want: "auth_mode trae"},
		{name: "missing token", data: `{"auth_mode":"trae","trae":{"credential_kind":"cloud_cli_jwt","login_method":"test","region":"CN","version":2}}`, want: "missing a Trae access token"},
		{name: "wrong credential kind", data: `{"auth_mode":"trae","trae":{"access_token":"secret","credential_kind":"api_key","login_method":"test","region":"CN","version":2}}`, want: "unsupported credential kind"},
		{name: "incomplete metadata", data: `{"auth_mode":"trae","trae":{"access_token":"secret","credential_kind":"cloud_cli_jwt","login_method":"","region":"CN","version":2}}`, want: "incomplete account metadata"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := ReadSource(path)
			if !errors.Is(err, ErrInvalidSource) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ReadSource() error = %v, want invalid source containing %q", err, test.want)
			}
		})
	}

	if err := os.WriteFile(path, valid, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSource(path); !errors.Is(err, ErrInvalidSource) ||
		!strings.Contains(err.Error(), "mode 0600") {
		t.Fatalf("broad source mode error = %v", err)
	}
}

func TestReadSourceRejectsHardLinkedCredential(t *testing.T) {
	root := privateAuthDirectory(t)
	path := filepath.Join(root, "auth.json")
	if err := os.WriteFile(path, testAuth("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, filepath.Join(root, "second-link.json")); err != nil {
		t.Skipf("hard links are unavailable: %v", err)
	}
	if _, err := ReadSource(path); !errors.Is(err, ErrInvalidSource) ||
		!strings.Contains(err.Error(), "hard-link count 2") {
		t.Fatalf("hard-linked source error = %v", err)
	}
}

func TestSyncPublishesExactPrivateManagedCopy(t *testing.T) {
	managedHome := privateAuthDirectory(t)
	first := testAuth("first-token")
	path, err := Sync(first, managedHome)
	if err != nil {
		t.Fatal(err)
	}
	if path != ManagedPath(managedHome) {
		t.Fatalf("managed path = %q", path)
	}
	assertManagedAuth(t, path, first)

	second := testAuth("second-token")
	if _, err := Sync(second, managedHome); err != nil {
		t.Fatal(err)
	}
	assertManagedAuth(t, path, second)
	if err := ValidateManagedCopy(managedHome, true); err != nil {
		t.Fatal(err)
	}
}

func TestValidateManagedCopyRejectsMissingMalformedAndBroadCopy(t *testing.T) {
	managedHome := privateAuthDirectory(t)
	if err := ValidateManagedCopy(managedHome, false); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManagedCopy(managedHome, true); !errors.Is(err, ErrInvalidManagedCopy) {
		t.Fatalf("missing managed copy error = %v", err)
	}
	path := ManagedPath(managedHome)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManagedCopy(managedHome, true); !errors.Is(err, ErrInvalidManagedCopy) ||
		!strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("malformed managed copy error = %v", err)
	}
	if err := os.WriteFile(path, testAuth("token"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManagedCopy(managedHome, true); !errors.Is(err, ErrInvalidManagedCopy) ||
		!strings.Contains(err.Error(), "mode 0600") {
		t.Fatalf("broad managed copy error = %v", err)
	}
}

func privateAuthDirectory(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func testAuth(token string) []byte {
	return []byte(`{"auth_mode":"trae","trae":{"access_token":"` + token + `","credential_kind":"cloud_cli_jwt","login_method":"test","region":"CN","version":2}}`)
}

func assertManagedAuth(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(want) {
		t.Fatalf("managed copy = %q, %v; want exact source bytes", got, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("managed copy mode = %#v, %v", info, err)
	}
}
