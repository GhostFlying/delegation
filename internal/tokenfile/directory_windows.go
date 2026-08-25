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
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	if err := validateTokenDirectoryDACL(dacl, sid); err != nil {
		return err
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

type tokenDirectoryACEExpectation struct {
	flags uint8
}

const tokenFileAllAccess windows.ACCESS_MASK = windows.STANDARD_RIGHTS_REQUIRED | windows.SYNCHRONIZE | 0x1ff

func validateTokenDirectoryDACL(dacl *windows.ACL, current *windows.SID) error {
	combined := []tokenDirectoryACEExpectation{{
		flags: windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE,
	}}
	split := []tokenDirectoryACEExpectation{
		{},
		{
			flags: windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE | windows.INHERIT_ONLY_ACE,
		},
	}
	reversedSplit := []tokenDirectoryACEExpectation{split[1], split[0]}
	for _, expected := range [][]tokenDirectoryACEExpectation{combined, split, reversedSplit} {
		matches, err := tokenDirectoryDACLMatches(dacl, current, expected)
		if err != nil {
			return fmt.Errorf("read token directory DACL entry: %w", err)
		}
		if matches {
			return nil
		}
	}
	return errors.New("token directory DACL must be a canonical current-user-only ACL")
}

func tokenDirectoryDACLMatches(
	dacl *windows.ACL,
	current *windows.SID,
	expected []tokenDirectoryACEExpectation,
) (bool, error) {
	if dacl == nil || int(dacl.AceCount) != len(expected) {
		return false, nil
	}
	for index, want := range expected {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			return false, err
		}
		if ace == nil ||
			ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags != want.flags ||
			!isTokenDirectoryFullAccess(ace.Mask) {
			return false, nil
		}
		aceSID, valid := tokenDirectoryACESID(ace)
		if !valid || !aceSID.Equals(current) {
			return false, nil
		}
	}
	return true, nil
}

func isTokenDirectoryFullAccess(mask windows.ACCESS_MASK) bool {
	return mask == windows.GENERIC_ALL || mask == tokenFileAllAccess
}

type tokenSIDHeader struct {
	revision          uint8
	subAuthorityCount uint8
	authority         [6]byte
}

func tokenDirectoryACESID(ace *windows.ACCESS_ALLOWED_ACE) (*windows.SID, bool) {
	if ace == nil {
		return nil, false
	}
	sidOffset := unsafe.Offsetof(windows.ACCESS_ALLOWED_ACE{}.SidStart)
	headerSize := unsafe.Sizeof(tokenSIDHeader{})
	if uintptr(ace.Header.AceSize) < sidOffset+headerSize {
		return nil, false
	}
	header := (*tokenSIDHeader)(unsafe.Pointer(&ace.SidStart))
	sidSize := headerSize + uintptr(header.subAuthorityCount)*4
	if sidOffset+sidSize > uintptr(ace.Header.AceSize) {
		return nil, false
	}
	sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !sid.IsValid() || uintptr(sid.Len()) != sidSize {
		return nil, false
	}
	return sid, true
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
