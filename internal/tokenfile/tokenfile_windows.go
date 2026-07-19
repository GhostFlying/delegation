//go:build windows

package tokenfile

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"github.com/GhostFlying/delegation/internal/securefs"
	"golang.org/x/sys/windows"
)

func openSecureNew(directory *securefs.Root, name string) (*os.File, error) {
	return directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, 0o600)
}

func openSecureRead(directory *securefs.Root, name string) (*os.File, error) {
	return directory.OpenFile(name, os.O_RDONLY, 0)
}

func validateFilePermissions(file *os.File, _ os.FileInfo) error {
	var fileInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &fileInfo); err != nil {
		return fmt.Errorf("inspect token file handle: %w", err)
	}
	if fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("token file must not be a reparse point")
	}
	descriptor, err := windows.GetSecurityInfo(
		windows.Handle(file.Fd()),
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read token file security descriptor: %w", err)
	}
	if descriptor == nil {
		return errors.New("token file has no security descriptor")
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return fmt.Errorf("read token file DACL control: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read token file DACL: %w", err)
	}
	if dacl == nil || dacl.AceCount != 1 {
		return errors.New("token file DACL must grant access only to the current user")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read token file DACL entry: %w", err)
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return errors.New("token file DACL must contain one allow entry")
	}
	if control&windows.SE_DACL_PROTECTED == 0 && ace.Header.AceFlags&windows.INHERITED_ACE == 0 {
		return errors.New("token file DACL must be protected or inherited from its validated directory")
	}
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !aceSID.Equals(sid) {
		return errors.New("token file DACL must grant access only to the current user")
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read token file owner: %w", err)
	}
	if owner == nil || (!owner.Equals(sid) && !owner.IsWellKnown(windows.WinBuiltinAdministratorsSid)) {
		return errors.New("token file must be owned by the current user or local Administrators")
	}
	return nil
}

func currentUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid, nil
}

func syncParentDirectory(string) error {
	return nil
}
