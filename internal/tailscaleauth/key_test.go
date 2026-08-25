package tailscaleauth

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
)

const secretSentinel = "M6_AUTH_KEY_SECRET_7fK3wP9xQ2vN8cR5sT1yH4mJ6dL0bG"

const validAuthKey = authKeyPrefix + secretSentinel

const otherSecretSentinel = "DIFFERENT_SECRET_u8V2nM5kQ9rT4wX7"

var secretFragments = []string{
	validAuthKey,
	secretSentinel,
	"7fK3wP9xQ2vN8cR5",
	"sT1yH4mJ6dL0bG",
	authKeyPrefix + otherSecretSentinel,
	otherSecretSentinel,
	"u8V2nM5kQ9rT4",
	"DIFFERENT_SECRET",
}

func TestReadAcceptsProtectedAuthKey(t *testing.T) {
	for name, suffix := range map[string]string{
		"no newline": "",
		"LF":         "\n",
		"CRLF":       "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			key, err := Read(protectedKeyFile(t, validAuthKey+suffix))
			if err != nil {
				assertNoSecretLeak(t, "Read error", err.Error())
				t.Fatal(err)
			}
			if got := key.AuthKey(); got != validAuthKey {
				t.Fatalf("AuthKey() mismatch: got length %d, want %d", len(got), len(validAuthKey))
			}
		})
	}
}

func TestKeyFormattingRedactsAllVerbsAndOptions(t *testing.T) {
	key := mustReadKey(t, validAuthKey)
	for _, verb := range []rune{
		'v', 's', 'q', 'd', 'p', 'c', 'o', 'f',
		'b', 'e', 'E', 'F', 'g', 'G', 'O', 't', 'U', 'x', 'X',
		'z',
	} {
		format := "%" + string(verb)
		output := fmt.Sprintf(format, key)
		assertNoSecretLeak(t, format, output)
		if verb == 'p' {
			if strings.Contains(output, "%!p") {
				t.Fatalf("%s formatting used bad-verb fallback: %q", format, output)
			}
			continue
		}
		if output != redactedKey {
			t.Fatalf("%s formatting = %q, want %q", format, output, redactedKey)
		}
	}

	typeOutput := fmt.Sprintf("%T", key)
	assertNoSecretLeak(t, "%T", typeOutput)
	if strings.Contains(typeOutput, "%!T") {
		t.Fatalf("%%T formatting used bad-verb fallback: %q", typeOutput)
	}

	for _, format := range []string{
		"%#v", "%+v", "%-v", "% v", "%0v",
		"%#08x", "%+20d", "%-20s", "% 020f",
		"%.0v", "%.3s", "%.100q", "%20.3v", "%-20.7f",
	} {
		output := fmt.Sprintf(format, key)
		assertNoSecretLeak(t, format, output)
		if output != redactedKey {
			t.Fatalf("%s formatting = %q, want %q", format, output, redactedKey)
		}
	}
	if output := fmt.Sprintf("%*.*v", 20, 3, key); output != redactedKey {
		t.Fatalf("dynamic width/precision formatting = %q, want %q", output, redactedKey)
	}

	pointerFormats := []string{"%p", "%#p", "%+p", "%-20p", "%020p", "%.3p"}
	other := mustReadKey(t, authKeyPrefix+otherSecretSentinel)
	for _, format := range pointerFormats {
		first := fmt.Sprintf(format, key)
		second := fmt.Sprintf(format, other)
		assertNoSecretLeak(t, format, first)
		if strings.Contains(first, "%!p") {
			t.Fatalf("%s formatting used bad-verb fallback: %q", format, first)
		}
		if first != second {
			t.Fatalf("%s formatting varies with key material: %q != %q", format, first, second)
		}
	}
}

func TestZeroKeyFormattingRedacts(t *testing.T) {
	var key Key
	for _, format := range []string{"%v", "%s", "%q", "%d", "%c", "%o", "%f", "%#v", "%20.3v"} {
		if output := fmt.Sprintf(format, key); output != redactedKey {
			t.Fatalf("%s zero-key formatting = %q, want %q", format, output, redactedKey)
		}
	}
	if output := fmt.Sprintf("%p", key); output != "0x0" {
		t.Fatalf("%%p zero-key formatting = %q, want stable nil redaction", output)
	}
}

func TestReadRejectsMalformedAuthKeyWithoutLeaking(t *testing.T) {
	for name, contents := range map[string]string{
		"empty":              "",
		"prefix only":        authKeyPrefix,
		"wrong prefix":       "tskey-client-" + secretSentinel,
		"embedded LF":        validAuthKey + "\nsecond",
		"multiple LF":        validAuthKey + "\n\n",
		"multiple CRLF":      validAuthKey + "\r\n\r\n",
		"bare final CR":      validAuthKey + "\r",
		"NUL":                authKeyPrefix + secretSentinel + "\x00",
		"space":              authKeyPrefix + secretSentinel + " ",
		"tab":                authKeyPrefix + secretSentinel + "\t",
		"non-ASCII":          authKeyPrefix + secretSentinel + "é",
		"leading whitespace": " " + validAuthKey,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Read(protectedKeyFile(t, contents))
			if err == nil {
				t.Fatal("Read accepted malformed auth key")
			}
			assertErrorRedacts(t, err)
		})
	}
}

func TestReadRejectsOversizedAuthKeyWithoutLeaking(t *testing.T) {
	contents := authKeyPrefix + secretSentinel +
		strings.Repeat("x", maximumKeyFileBytes-len(authKeyPrefix)-len(secretSentinel)+1)
	_, err := Read(protectedKeyFile(t, contents))
	if err == nil {
		t.Fatal("Read accepted oversized auth key")
	}
	assertErrorRedacts(t, err)
}

func TestReadRejectsUnprotectedAuthKeyWithoutLeaking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.key")
	if err := os.WriteFile(path, []byte(validAuthKey), 0o644); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_, err := Read(path)
	if err == nil {
		t.Fatal("Read accepted an unprotected auth key")
	}
	assertErrorRedacts(t, err)
	if runtime.GOOS != "windows" && !strings.Contains(err.Error(), "exact mode 0600") {
		t.Fatalf("Read error = %q, want exact mode 0600 requirement", err)
	}
}

func TestReadRequiresExactMode0600OnUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file-mode contract")
	}
	path := protectedKeyFile(t, validAuthKey)
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
	_, err := Read(path)
	if err == nil {
		t.Fatal("Read accepted a mode 0400 auth key")
	}
	assertErrorRedacts(t, err)
	if !strings.Contains(err.Error(), "exact mode 0600") {
		t.Fatalf("Read error = %q, want exact mode 0600 requirement", err)
	}
}

func TestReadRejectsAuthKeySymlinkWithoutLeaking(t *testing.T) {
	directory := protectedKeyDirectory(t)
	target := filepath.Join(directory, "target.key")
	if err := os.WriteFile(target, []byte(validAuthKey), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "auth.key")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("creating an auth-key symlink is unavailable: %v", err)
	}
	_, err := Read(alias)
	if err == nil {
		t.Fatal("Read accepted an auth-key symlink")
	}
	assertErrorRedacts(t, err)
	if !strings.Contains(err.Error(), "must not be a symbolic link") {
		t.Fatalf("Read error = %q, want leaf symbolic-link rejection", err)
	}
}

func TestReadDoesNotUseAmbientAuthKeyOrModifyFile(t *testing.T) {
	t.Setenv("TS_AUTHKEY", validAuthKey)
	path := protectedKeyFile(t, "")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Read(path)
	if err == nil {
		t.Fatal("Read used ambient TS_AUTHKEY for an empty file")
	}
	assertErrorRedacts(t, err)
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("Read modified the auth-key file")
	}
}

func protectedKeyFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(protectedKeyDirectory(t), "auth.key")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func protectedKeyDirectory(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	return directory
}

func mustReadKey(t *testing.T, contents string) Key {
	t.Helper()
	key, err := Read(protectedKeyFile(t, contents))
	if err != nil {
		assertErrorRedacts(t, err)
		t.Fatal(err)
	}
	return key
}

func assertErrorRedacts(t *testing.T, err error) {
	t.Helper()
	formatted := map[string]string{
		"Error": err.Error(),
		"%v":    fmt.Sprintf("%v", err),
		"%s":    fmt.Sprintf("%s", err),
		"%q":    fmt.Sprintf("%q", err),
		"%#v":   fmt.Sprintf("%#v", err),
		"%d":    fmt.Sprintf("%d", err),
		"%p":    fmt.Sprintf("%p", err),
		"%c":    fmt.Sprintf("%c", err),
		"%o":    fmt.Sprintf("%o", err),
		"%f":    fmt.Sprintf("%f", err),
	}
	for format, output := range formatted {
		assertNoSecretLeak(t, format+" error formatting", output)
	}
}

func assertNoSecretLeak(t *testing.T, context, output string) {
	t.Helper()
	for _, secret := range secretFragments {
		if strings.Contains(output, secret) {
			t.Fatalf("%s leaked auth-key material", context)
		}
	}
}
