//go:build windows

package tokenfile

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"golang.org/x/sys/windows"
)

func TestEnsureCreatesTokenWithCurrentUserOnlyDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "token")
	if _, err := Ensure(path); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err != nil {
		t.Fatal(err)
	}
	if err := validateTokenDirectory(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureAcceptsPreparedPrivateWindowsDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(directory); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "token")
	if _, err := Ensure(path); err != nil {
		t.Fatalf("Ensure rejected prepared private directory: %v", err)
	}
	if err := Validate(path); err != nil {
		t.Fatalf("Validate rejected token in prepared private directory: %v", err)
	}
}

func TestValidateTokenDirectoryDACLAcceptsOnlyCanonicalForms(t *testing.T) {
	current, err := currentUserSID()
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
			name: "combined_wrong_mask",
			sddl: "D:P(A;OICI;GR;;;" + current.String() + ")",
		},
		{
			name: "combined_inherited",
			sddl: "D:P(A;OICIID;GA;;;" + current.String() + ")",
		},
		{
			name: "combined_no_propagate",
			sddl: "D:P(A;OICINP;GA;;;" + current.String() + ")",
		},
		{
			name: "combined_no_inherit",
			sddl: "D:P(A;;GA;;;" + current.String() + ")",
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
			err = validateTokenDirectoryDACL(dacl, current)
			if (err == nil) != test.want {
				t.Fatalf("validateTokenDirectoryDACL() error = %v, want success %t", err, test.want)
			}
		})
	}
}

func TestTokenDirectoryACESIDRejectsTruncatedSID(t *testing.T) {
	ace := &windows.ACCESS_ALLOWED_ACE{
		Header: windows.ACE_HEADER{
			AceType: windows.ACCESS_ALLOWED_ACE_TYPE,
			AceSize: uint16(unsafe.Offsetof(windows.ACCESS_ALLOWED_ACE{}.SidStart) + 7),
		},
		Mask: windows.GENERIC_ALL,
	}
	if sid, valid := tokenDirectoryACESID(ace); valid || sid != nil {
		t.Fatalf("tokenDirectoryACESID() = (%v, %t), want (nil, false)", sid, valid)
	}
}

func TestWriteNewRejectsSharedTokenDirectory(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "token")
	if _, err := WriteNew(path, Token{1}); err == nil {
		t.Fatal("WriteNew accepted an inherited shared token directory")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("rejected token path exists: %v", err)
	}
}

func TestValidateRejectsInheritedWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Validate(path); err == nil {
		t.Fatal("Validate() accepted an inherited Windows DACL")
	}
}

func TestOpenSecureReadRejectsReparsePoint(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "private", "target")
	link := filepath.Join(dir, "token-link")
	if _, err := Ensure(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("creating symlink requires unavailable Windows privileges: %v", err)
	}
	if _, err := Read(link); err == nil {
		t.Fatal("Read() accepted a reparse point")
	}
}
