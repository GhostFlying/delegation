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

func TestWriteNewAndReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secrets", "device.token")
	token := Token{1, 2, 3}
	created, err := WriteNew(path, token)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("WriteNew did not report creating the token")
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != token {
		t.Fatalf("Read() = %#v, want %#v", got, token)
	}
	if _, err := WriteNew(path, Token{9}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second WriteNew error = %v, want os.ErrExist", err)
	}
	if got, err := Read(path); err != nil || got != token {
		t.Fatalf("token after rejected replacement = %#v, %v", got, err)
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

func TestReadRequiresAbsolutePath(t *testing.T) {
	if _, err := Read("relative.token"); err == nil {
		t.Fatal("Read accepted a relative token path")
	}
}

func TestWriteNewPublishesOneCompleteTokenUnderContention(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "secrets")
	path := filepath.Join(directory, "device.token")
	tokens := []Token{{1}, {2}}
	start := make(chan struct{})
	results := make(chan error, len(tokens))
	for _, token := range tokens {
		go func() {
			<-start
			_, err := WriteNew(path, token)
			results <- err
		}()
	}
	close(start)

	successes := 0
	for range tokens {
		err := <-results
		if err == nil {
			successes++
		} else if !errors.Is(err, os.ErrExist) {
			t.Fatalf("WriteNew contention error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful WriteNew calls = %d, want 1", successes)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != tokens[0] && got != tokens[1] {
		t.Fatalf("published token = %#v, want one complete input token", got)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "device.token" {
		t.Fatalf("token directory entries = %v, want only device.token", entries)
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
