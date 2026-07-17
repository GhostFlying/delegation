package tokenfile

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestEnsureCreatesAndReusesProtectedToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secrets", "broker.token")
	created, err := Ensure(path)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("Ensure() did not report creating the token")
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err != nil {
		t.Fatal(err)
	}

	created, err = Ensure(path)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("Ensure() replaced an existing token")
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, first) {
		t.Fatal("Ensure() changed existing token material")
	}
}

func TestValidateRejectsMalformedToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	overwriteProtectedToken(t, path, []byte("not-a-256-bit-token\n"))
	if err := Validate(path); err == nil {
		t.Fatal("Validate() accepted malformed token material")
	}
}

func TestValidateAcceptsTokenWithoutTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	token := base64.RawURLEncoding.EncodeToString(make([]byte, tokenBytes))
	overwriteProtectedToken(t, path, []byte(token))
	if err := Validate(path); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsOversizedTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	overwriteProtectedToken(t, path, bytes.Repeat([]byte("a"), maxTokenFileSize+1))
	if err := Validate(path); err == nil {
		t.Fatal("Validate() accepted oversized token material")
	}
}

func TestEnsureRequiresAbsolutePath(t *testing.T) {
	if _, err := Ensure("relative.token"); err == nil {
		t.Fatal("Ensure() accepted a relative token path")
	}
}

func TestEnsureSyncsEveryNewDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "one", "two", "token")
	originalSync := syncTokenDirectory
	t.Cleanup(func() { syncTokenDirectory = originalSync })
	var synced []string
	syncTokenDirectory = func(syncPath string) error {
		synced = append(synced, syncPath)
		return nil
	}

	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Dir(root), root, filepath.Join(root, "one"), filepath.Join(root, "one", "two")}
	if !slices.Equal(synced, want) {
		t.Fatalf("synced paths = %q, want %q", synced, want)
	}
}

func TestEnsureRetrySyncsExistingDirectoryAnchor(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "one", "two")
	path := filepath.Join(dir, "token")
	originalSync := syncTokenDirectory
	t.Cleanup(func() { syncTokenDirectory = originalSync })
	failingParent := filepath.Join(root, "one")
	syncTokenDirectory = func(syncPath string) error {
		if syncPath == failingParent {
			return errors.New("injected sync failure")
		}
		return nil
	}

	if _, err := Ensure(path); err == nil {
		t.Fatal("Ensure() succeeded despite injected directory sync failure")
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Fatalf("created directory missing after injected failure: %v", statErr)
	}
	var synced []string
	syncTokenDirectory = func(syncPath string) error {
		synced = append(synced, syncPath)
		return nil
	}
	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(synced, failingParent) {
		t.Fatalf("retry synced paths = %q, want existing directory parent %q", synced, failingParent)
	}
}

func overwriteProtectedToken(t *testing.T, path string, data []byte) {
	t.Helper()
	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
