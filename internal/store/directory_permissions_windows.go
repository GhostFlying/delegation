//go:build windows

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

func createPrivateDirectory(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	descriptor, err := currentUserStateDescriptor(true)
	if err != nil {
		return err
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes := windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	if err := windows.CreateDirectory(pathPtr, &attributes); err != nil &&
		!errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		return err
	}
	return nil
}

func validatePrivateDirectory(path string, _ os.FileInfo) error {
	if err := validateWindowsStateObject(path, true); err != nil {
		return fmt.Errorf("broker state directory is not private: %w", err)
	}
	return nil
}

func validatePrivateStateFile(path string, _ os.FileInfo) error {
	if err := validateWindowsStateObject(path, false); err != nil {
		return fmt.Errorf("broker state artifact is not private: %w", err)
	}
	return nil
}

func protectDatabaseFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect opened broker state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("opened broker state is not a regular file")
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(pathPtr)
	if err != nil {
		return err
	}
	if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("opened broker state must not be a reparse point")
	}
	descriptor, err := currentUserStateDescriptor(false)
	if err != nil {
		return err
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		owner,
		nil,
		dacl,
		nil,
	); err != nil {
		return fmt.Errorf("protect broker state: %w", err)
	}
	if err := validateCurrentWindowsOwner(path); err != nil {
		return fmt.Errorf("protect broker state owner: %w", err)
	}
	return nil
}

func currentUserStateDescriptor(directory bool) (*windows.SECURITY_DESCRIPTOR, error) {
	sid, err := currentWindowsUserSID()
	if err != nil {
		return nil, err
	}
	flags := ""
	if directory {
		flags = "OICI"
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		fmt.Sprintf("O:%sD:P(A;%s;GA;;;%s)", sid.String(), flags, sid.String()),
	)
	if err != nil {
		return nil, fmt.Errorf("build broker state security descriptor: %w", err)
	}
	return descriptor, nil
}

func validateWindowsStateObject(path string, directory bool) error {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(pathPtr)
	if err != nil {
		return err
	}
	if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("path must not be a reparse point")
	}
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read security descriptor: %w", err)
	}
	if descriptor == nil {
		return errors.New("path has no security descriptor")
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return err
	}
	if directory && control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("directory DACL must be protected from inheritance")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	if dacl == nil || dacl.AceCount != 1 {
		return errors.New("DACL must grant access only to the current user")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return err
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return errors.New("DACL must contain one allow entry")
	}
	if directory {
		inheritance := uint8(windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE)
		if ace.Header.AceFlags&inheritance != inheritance || ace.Header.AceFlags&windows.INHERITED_ACE != 0 {
			return errors.New("directory DACL must propagate one non-inherited entry")
		}
	}
	sid, err := currentWindowsUserSID()
	if err != nil {
		return err
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !aceSID.Equals(sid) {
		return errors.New("DACL must grant access only to the current user")
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return err
	}
	// Elevated tokens can assign SQLite sidecars to BUILTIN\Administrators even
	// under the protected current-user-only DACL. Open normalizes that owner.
	if owner == nil || (!owner.Equals(sid) && (directory || !owner.IsWellKnown(windows.WinBuiltinAdministratorsSid))) {
		return errors.New("path must be owned by the current user or local Administrators")
	}
	return nil
}

func validateCurrentWindowsOwner(path string) error {
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read owner: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return err
	}
	sid, err := currentWindowsUserSID()
	if err != nil {
		return err
	}
	if owner == nil || !owner.Equals(sid) {
		return errors.New("path must be owned by the current user")
	}
	return nil
}

func currentWindowsUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid, nil
}
