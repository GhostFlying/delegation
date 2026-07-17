//go:build windows

package tokenfile

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openSecureNew(path string) (*os.File, error) {
	sid, err := currentUserSID()
	if err != nil {
		return nil, err
	}
	sidString := sid.String()
	descriptor, err := windows.SecurityDescriptorFromString("O:" + sidString + "D:P(A;;FA;;;" + sidString + ")")
	if err != nil {
		return nil, fmt.Errorf("build token security descriptor: %w", err)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	attributes := windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_WRITE,
		0,
		&attributes,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_WRITE_THROUGH,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_EXISTS) || errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			return nil, os.ErrExist
		}
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func openSecureRead(path string) (*os.File, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
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
	if control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("token file DACL must be protected from inheritance")
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
	if owner == nil || !owner.Equals(sid) {
		return errors.New("token file must be owned by the current user")
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
