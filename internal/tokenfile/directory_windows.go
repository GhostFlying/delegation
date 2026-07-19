//go:build windows

package tokenfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/GhostFlying/delegation/internal/securefs"
	"github.com/GhostFlying/delegation/internal/winlocalpath"
	"golang.org/x/sys/windows"
)

func createTokenDirectory(path string) error {
	if err := validateLocalTokenVolume(path); err != nil {
		return err
	}
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"O:" + sid.String() + "D:P(A;OICI;GA;;;" + sid.String() + ")",
	)
	if err != nil {
		return fmt.Errorf("build token directory security descriptor: %w", err)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes := windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	if err := windows.CreateDirectory(pathPtr, &attributes); err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			return os.ErrExist
		}
		return err
	}
	return nil
}

func validateTokenDirectoryLocation(path string) error {
	return winlocalpath.ValidateDirectoryPath(path)
}

func validateTokenDirectory(path string) error {
	directory, err := holdTokenDirectory(path)
	if err != nil {
		return err
	}
	return directory.Close()
}

func holdTokenDirectory(path string) (*securefs.Root, error) {
	if err := validateLocalTokenVolume(path); err != nil {
		return nil, err
	}
	directory, err := securefs.OpenRoot(path, func(file *os.File) error {
		if err := winlocalpath.ValidateDirectory(file); err != nil {
			return err
		}
		return validateTokenDirectoryHandle(windows.Handle(file.Fd()))
	})
	if err != nil {
		return nil, fmt.Errorf("open token directory: %w", err)
	}
	return directory, nil
}

func validateTokenDirectoryHandle(handle windows.Handle) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return fmt.Errorf("inspect token directory handle: %w", err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("token directory must be a local directory, not a reparse point")
	}
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read token directory security descriptor: %w", err)
	}
	if descriptor == nil {
		return errors.New("token directory has no security descriptor")
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return fmt.Errorf("read token directory DACL control: %w", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("token directory DACL must be protected from inheritance")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read token directory DACL: %w", err)
	}
	if dacl == nil || dacl.AceCount != 1 {
		return errors.New("token directory DACL must grant access only to the current user")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read token directory DACL entry: %w", err)
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
		ace.Header.AceFlags&windows.INHERITED_ACE != 0 ||
		ace.Header.AceFlags&(windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE) !=
			windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE {
		return errors.New("token directory DACL must contain one inheritable allow entry")
	}
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !aceSID.Equals(sid) {
		return errors.New("token directory DACL must grant access only to the current user")
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read token directory owner: %w", err)
	}
	if owner == nil || !owner.Equals(sid) {
		return errors.New("token directory must be owned by the current user")
	}
	return nil
}

func validateLocalTokenVolume(path string) error {
	volume := filepath.VolumeName(path)
	if strings.HasPrefix(volume, `\\`) {
		return errors.New("token directory must not use a Windows network path")
	}
	if volume == "" {
		return errors.New("token directory must use a local Windows volume")
	}
	root, err := windows.UTF16PtrFromString(volume + `\`)
	if err != nil {
		return err
	}
	switch windows.GetDriveType(root) {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		return nil
	case windows.DRIVE_REMOTE:
		return errors.New("token directory must not use a mapped Windows network drive")
	default:
		return errors.New("token directory must use a writable local Windows volume")
	}
}
