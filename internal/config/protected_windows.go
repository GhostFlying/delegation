//go:build windows

package config

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

func createConfigDirectory(path string) error {
	if err := validateLocalConfigVolume(path); err != nil {
		return err
	}
	sid, err := currentConfigUserSID()
	if err != nil {
		return err
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"O:" + sid.String() + "D:P(A;OICI;GA;;;" + sid.String() + ")",
	)
	if err != nil {
		return fmt.Errorf("build config directory security descriptor: %w", err)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes := windows.SecurityAttributes{
		Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: descriptor,
	}
	if err := windows.CreateDirectory(pathPtr, &attributes); err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			return os.ErrExist
		}
		return err
	}
	return nil
}

func validateConfigDirectoryLocation(path string) error {
	return winlocalpath.ValidateDirectoryPath(path)
}

func holdConfigDirectory(path string) (*securefs.Root, error) {
	if err := validateLocalConfigVolume(path); err != nil {
		return nil, err
	}
	directory, err := securefs.OpenRoot(path, func(file *os.File) error {
		if err := winlocalpath.ValidateDirectory(file); err != nil {
			return err
		}
		return validateConfigHandle(windows.Handle(file.Fd()), true)
	})
	if err != nil {
		return nil, fmt.Errorf("open config directory: %w", err)
	}
	return directory, nil
}

func protectPrivateDirectory(path string) error {
	if err := validateLocalConfigVolume(path); err != nil {
		return err
	}
	descriptor, err := privateDirectorySecurityDescriptor()
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
	directory, err := securefs.OpenRoot(path, func(file *os.File) error {
		if err := winlocalpath.ValidateDirectory(file); err != nil {
			return err
		}
		return windows.SetSecurityInfo(
			windows.Handle(file.Fd()),
			windows.SE_FILE_OBJECT,
			windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION|
				windows.PROTECTED_DACL_SECURITY_INFORMATION,
			owner,
			nil,
			dacl,
			nil,
		)
	})
	if err != nil {
		return fmt.Errorf("set private directory security descriptor: %w", err)
	}
	return directory.Close()
}

func holdPrivateDirectory(path string) (*securefs.Root, error) {
	return holdConfigDirectory(path)
}

func privateDirectorySecurityDescriptor() (*windows.SECURITY_DESCRIPTOR, error) {
	sid, err := currentConfigUserSID()
	if err != nil {
		return nil, err
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"O:" + sid.String() + "D:P(A;OICI;GA;;;" + sid.String() + ")",
	)
	if err != nil {
		return nil, fmt.Errorf("build private directory security descriptor: %w", err)
	}
	return descriptor, nil
}

func createConfigTemp(directory *securefs.Root) (string, *os.File, error) {
	for range 100 {
		suffix, err := randomConfigSuffix()
		if err != nil {
			return "", nil, err
		}
		name := ".config-" + suffix + ".tmp"
		file, err := directory.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return name, file, nil
	}
	return "", nil, errors.New("create temporary config: exhausted name attempts")
}

func openProtectedConfig(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("config file path must be absolute")
	}
	directory, err := holdConfigDirectory(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	file, err := directory.OpenFile(filepath.Base(path), os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	if err := validateConfigHandle(windows.Handle(file.Fd()), false); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := directory.VerifyPath(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func validateConfigHandle(handle windows.Handle, directory bool) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return fmt.Errorf("inspect config handle: %w", err)
	}
	isDirectory := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0
	if isDirectory != directory || info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("config path type or reparse-point state is invalid")
	}
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read config security descriptor: %w", err)
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return errors.New("read config DACL control")
	}
	if directory && control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("config directory DACL must be protected from inheritance")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil || dacl.AceCount != 1 {
		return errors.New("config DACL must grant access only to the current user")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read config DACL entry: %w", err)
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return errors.New("config DACL must contain one allow entry")
	}
	if directory {
		if ace.Header.AceFlags&windows.INHERITED_ACE != 0 ||
			ace.Header.AceFlags&(windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE) !=
				windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE {
			return errors.New("config directory DACL must contain one non-inherited, inheritable allow entry")
		}
	} else if control&windows.SE_DACL_PROTECTED == 0 && ace.Header.AceFlags&windows.INHERITED_ACE == 0 {
		return errors.New("config file DACL must be protected or inherited from its validated directory")
	}
	sid, err := currentConfigUserSID()
	if err != nil {
		return err
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	owner, _, ownerErr := descriptor.Owner()
	validOwner := owner != nil && (owner.Equals(sid) || (!directory && owner.IsWellKnown(windows.WinBuiltinAdministratorsSid)))
	if !aceSID.Equals(sid) || ownerErr != nil || !validOwner {
		return errors.New("config must be current-user-only and owned by that user or local Administrators")
	}
	return nil
}

func validateLocalConfigVolume(path string) error {
	volume := filepath.VolumeName(path)
	if strings.HasPrefix(volume, `\\`) || volume == "" {
		return errors.New("config must use a local Windows volume")
	}
	root, err := windows.UTF16PtrFromString(volume + `\`)
	if err != nil {
		return err
	}
	switch windows.GetDriveType(root) {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		return nil
	default:
		return errors.New("config must use a writable local Windows volume")
	}
}

func currentConfigUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid, nil
}
