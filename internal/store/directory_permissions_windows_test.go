//go:build windows

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"golang.org/x/sys/windows"
)

func TestValidatePathAcceptsPreparedPrivateWindowsDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	if err := delegationconfig.ValidatePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "future-tailscale.tailscale.lock")
	if err := ValidatePath(path); err != nil {
		t.Fatalf("ValidatePath rejected prepared private directory: %v", err)
	}
	stateDirectory := filepath.Join(directory, "future-tailscale")
	if err := ValidateTailscaleStateDir(stateDirectory); err != nil {
		t.Fatalf("ValidateTailscaleStateDir rejected prepared private directory: %v", err)
	}
	for _, path := range []string{stateDirectory, stateDirectory + ".tailscale.lock"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("validation mutated %s: %v", path, err)
		}
	}
}

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

func TestValidateWindowsStateDirectoryDACLAcceptsOnlyCanonicalForms(t *testing.T) {
	current, err := currentWindowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		sddl string
		want bool
	}{
		{
			name: "combined",
			sddl: "D:P(A;OICI;GA;;;" + current.String() + ")",
			want: true,
		},
		{
			name: "windows_split",
			sddl: "D:P(A;;FA;;;" + current.String() + ")(A;OICIIO;GA;;;" + current.String() + ")",
			want: true,
		},
		{
			name: "windows_split_reversed",
			sddl: "D:P(A;OICIIO;GA;;;" + current.String() + ")(A;;FA;;;" + current.String() + ")",
			want: true,
		},
		{
			name: "duplicate_combined",
			sddl: "D:P(A;OICI;GA;;;" + current.String() + ")(A;OICI;GA;;;" + current.String() + ")",
		},
		{
			name: "split_wrong_effective_mask",
			sddl: "D:P(A;;GR;;;" + current.String() + ")(A;OICIIO;GA;;;" + current.String() + ")",
		},
		{
			name: "split_missing_inherit_only",
			sddl: "D:P(A;;FA;;;" + current.String() + ")(A;OICI;GA;;;" + current.String() + ")",
		},
		{
			name: "split_inherited",
			sddl: "D:P(A;;FA;;;" + current.String() + ")(A;OICIIOID;GA;;;" + current.String() + ")",
		},
		{
			name: "split_no_propagate",
			sddl: "D:P(A;;FA;;;" + current.String() + ")(A;OICINPIO;GA;;;" + current.String() + ")",
		},
		{
			name: "deny",
			sddl: "D:P(D;OICI;GA;;;" + current.String() + ")",
		},
		{
			name: "third_entry",
			sddl: "D:P(A;;FA;;;" + current.String() + ")(A;OICIIO;GA;;;" + current.String() + ")(A;;FA;;;" + current.String() + ")",
		},
		{
			name: "other_principal",
			sddl: "D:P(A;OICI;GA;;;WD)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor, err := windows.SecurityDescriptorFromString(test.sddl)
			if err != nil {
				t.Fatal(err)
			}
			dacl, _, err := descriptor.DACL()
			if err != nil {
				t.Fatal(err)
			}
			err = validateWindowsStateDirectoryDACL(dacl, current)
			if (err == nil) != test.want {
				t.Fatalf("validateWindowsStateDirectoryDACL() error = %v, want success %t", err, test.want)
			}
		})
	}
}

func TestValidateWindowsStateFileDACLRejectsDirectorySplitForm(t *testing.T) {
	current, err := currentWindowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:P(A;;FA;;;" + current.String() + ")(A;OICIIO;GA;;;" + current.String() + ")",
	)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsStateFileDACL(dacl); err == nil {
		t.Fatal("validateWindowsStateFileDACL accepted a directory split DACL")
	}
}

func TestWindowsStateACESIDRejectsTruncatedSID(t *testing.T) {
	ace := &windows.ACCESS_ALLOWED_ACE{
		Header: windows.ACE_HEADER{
			AceType: windows.ACCESS_ALLOWED_ACE_TYPE,
			AceSize: uint16(unsafe.Offsetof(windows.ACCESS_ALLOWED_ACE{}.SidStart) + 7),
		},
		Mask: windows.GENERIC_ALL,
	}
	if sid, valid := windowsStateACESID(ace); valid || sid != nil {
		t.Fatalf("windowsStateACESID() = (%v, %t), want (nil, false)", sid, valid)
	}
}
