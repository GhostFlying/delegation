//go:build windows

package traexauth

import (
	"testing"

	"golang.org/x/sys/windows"
)

func makeAuthFileUnsafe(t *testing.T, path string) {
	t.Helper()
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:P(A;;GA;;;" + user.User.Sid.String() + ")(A;;GR;;;WD)",
	)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	); err != nil {
		t.Fatal(err)
	}
}

func unsafeAuthFileError() string {
	return "only to the current user"
}
