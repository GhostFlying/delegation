//go:build windows

package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

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

func TestPrivateDirectoryPreparationNormalizesAdministratorsOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
		administrators,
		nil,
		nil,
		nil,
	); err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
			errors.Is(err, windows.ERROR_INVALID_OWNER) ||
			errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) {
			t.Skipf("current Windows token cannot assign the Administrators owner: %v", err)
		}
		t.Fatal(err)
	}

	if err := PreparePrivateDirectory(path); err != nil {
		t.Fatal(err)
	}
	assertNormalizedPrivateDirectory(t, path)
}

func TestDescribeConfigDACLClassifiesPrincipalsWithoutExposingSIDs(t *testing.T) {
	current, err := currentConfigUserSID()
	if err != nil {
		t.Fatal(err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:(A;;GA;;;" + current.String() + ")(A;;GR;;;" + system.String() + ")",
	)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	description := describeConfigDACL(dacl, current)
	if !strings.Contains(description, "principal=current") ||
		!strings.Contains(description, "principal=system") {
		t.Fatalf("DACL description = %q, want current and system classifications", description)
	}
	if strings.Contains(description, current.String()) || strings.Contains(description, system.String()) {
		t.Fatalf("DACL description exposed a raw SID: %q", description)
	}
}

func TestValidateConfigDirectoryDACLAcceptsOnlyCanonicalForms(t *testing.T) {
	current, err := currentConfigUserSID()
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
			err = validateConfigDirectoryDACL(dacl, current)
			if (err == nil) != test.want {
				t.Fatalf("validateConfigDirectoryDACL() error = %v, want success %t", err, test.want)
			}
		})
	}
}

func TestConfigACESIDRejectsTruncatedSID(t *testing.T) {
	ace := &windows.ACCESS_ALLOWED_ACE{
		Header: windows.ACE_HEADER{
			AceType: windows.ACCESS_ALLOWED_ACE_TYPE,
			AceSize: uint16(unsafe.Offsetof(windows.ACCESS_ALLOWED_ACE{}.SidStart) + 7),
		},
		Mask: windows.GENERIC_ALL,
	}
	if sid, valid := configACESID(ace); valid || sid != nil {
		t.Fatalf("configACESID() = (%v, %t), want (nil, false)", sid, valid)
	}
}

func assertNormalizedPrivateDirectory(t *testing.T, path string) {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		t.Fatal(err)
	}
	user, err := currentConfigUserSID()
	if err != nil {
		t.Fatal(err)
	}
	if owner == nil || !owner.Equals(user) {
		t.Fatalf("private directory owner = %v, want current user %v", owner, user)
	}
	control, _, err := descriptor.Control()
	if err != nil {
		t.Fatal(err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatal("private directory DACL is not protected")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateConfigDirectoryDACL(dacl, user); err != nil {
		t.Fatal(err)
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
