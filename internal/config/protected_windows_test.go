//go:build windows

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestProtectedConfigRejectsSharedWindowsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := WriteNew(path, protectedTestConfig(t)); err == nil {
		t.Fatal("WriteNew accepted an inherited shared Windows directory")
	}
}

func TestProtectedConfigAcceptsSafeWindowsFileInheritance(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := createConfigDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "config.json")
	data, err := json.Marshal(protectedTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err != nil {
		t.Fatalf("Read rejected safe inherited DACL: %v", err)
	}
}

func TestPrivateDirectoryPreparationRepairsAndValidationDetectsDACLDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	broadenPrivateDirectoryDACL(t, path)
	if err := PreparePrivateDirectory(path); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePrivateDirectory(path); err != nil {
		t.Fatal(err)
	}
	broadenPrivateDirectoryDACL(t, path)
	if err := ValidatePrivateDirectory(path); err == nil {
		t.Fatal("ValidatePrivateDirectory accepted a broadened DACL")
	}
}

func broadenPrivateDirectoryDACL(t *testing.T, path string) {
	t.Helper()
	descriptor, err := windows.SecurityDescriptorFromString("D:(A;OICI;GA;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.UNPROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		t.Fatal(err)
	}
}
