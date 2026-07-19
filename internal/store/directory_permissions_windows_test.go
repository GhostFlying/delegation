//go:build windows

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestOpenCreatesCurrentUserOnlyWindowsState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "broker.sqlite3")
	registry, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	directory := filepath.Dir(path)
	info, err := os.Lstat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePrivateDirectory(directory, info); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Lstat(artifact)
		if err != nil {
			t.Fatalf("inspect %s: %v", artifact, err)
		}
		if err := validatePrivateStateFile(artifact, info); err != nil {
			t.Fatalf("validate %s: %v", artifact, err)
		}
		if err := validateCurrentWindowsOwner(artifact); err != nil {
			t.Fatalf("validate owner %s: %v", artifact, err)
		}
	}
}

func TestOpenRejectsPermissiveWindowsStateDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	user, err := currentWindowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	everyone, err := windows.StringToSid("S-1-1-0")
	if err != nil {
		t.Fatal(err)
	}
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{
		{
			AccessPermissions: windows.GENERIC_ALL,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE,
			Trustee: windows.TRUSTEE{
				TrusteeForm: windows.TRUSTEE_IS_SID, TrusteeValue: windows.TrusteeValueFromSID(user),
			},
		},
		{
			AccessPermissions: windows.GENERIC_READ,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE,
			Trustee: windows.TRUSTEE{
				TrusteeForm: windows.TRUSTEE_IS_SID, TrusteeValue: windows.TrusteeValueFromSID(everyone),
			},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		directory,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		acl,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "broker.sqlite3")
	if _, err := Open(context.Background(), path); err == nil {
		t.Fatal("Open accepted a state directory accessible to Everyone")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected state directory was mutated: %v", err)
	}
}
