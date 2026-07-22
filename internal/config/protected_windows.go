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
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.READ_CONTROL|windows.WRITE_DAC|windows.WRITE_OWNER,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return fmt.Errorf("open private directory for DACL protection: %w", err)
	}
	directory := os.NewFile(uintptr(handle), path)
	if directory == nil {
		_ = windows.CloseHandle(handle)
		return errors.New("open private directory for DACL protection")
	}
	defer directory.Close()
	if err := validatePrivateDirectoryForProtection(directory); err != nil {
		return err
	}
	if err := windows.SetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
		owner,
		nil,
		nil,
		nil,
	); err != nil {
		return fmt.Errorf("set private directory owner: %w", err)
	}
	if err := windows.SetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		return fmt.Errorf("protect private directory DACL: %w", err)
	}
	return nil
}

func validatePrivateDirectoryForProtection(directory *os.File) error {
	if err := winlocalpath.ValidateDirectory(directory); err != nil {
		return err
	}
	handle := windows.Handle(directory.Fd())
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return fmt.Errorf("inspect private directory handle: %w", err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("private directory path type or reparse-point state is invalid")
	}
	descriptor, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read private directory owner: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read private directory owner: %w", err)
	}
	sid, err := currentConfigUserSID()
	if err != nil {
		return err
	}
	if owner == nil || (!owner.Equals(sid) && !owner.IsWellKnown(windows.WinBuiltinAdministratorsSid)) {
		return errors.New("private directory must be owned by the current user or local Administrators")
	}
	return nil
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
		return fmt.Errorf(
			"config directory DACL must be protected from inheritance (control=%#x)",
			control,
		)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read config DACL: %w", err)
	}
	sid, err := currentConfigUserSID()
	if err != nil {
		return err
	}
	if directory {
		if err := validateConfigDirectoryDACL(dacl, sid); err != nil {
			return err
		}
	} else if err := validateConfigFileDACL(dacl, control, sid); err != nil {
		return err
	}
	owner, _, ownerErr := descriptor.Owner()
	validOwner := owner != nil && (owner.Equals(sid) || (!directory && owner.IsWellKnown(windows.WinBuiltinAdministratorsSid)))
	if ownerErr != nil || !validOwner {
		return errors.New("config must be current-user-only and owned by that user or local Administrators")
	}
	return nil
}

type configACEExpectation struct {
	flags uint8
}

const configFileAllAccess windows.ACCESS_MASK = windows.STANDARD_RIGHTS_REQUIRED | windows.SYNCHRONIZE | 0x1ff

func validateConfigDirectoryDACL(dacl *windows.ACL, current *windows.SID) error {
	// Windows may split an inheritable generic ACE into mapped effective and
	// inherit-only generic ACEs when applying it to a directory.
	combined := []configACEExpectation{{
		flags: windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE,
	}}
	split := []configACEExpectation{
		{},
		{
			flags: windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE | windows.INHERIT_ONLY_ACE,
		},
	}
	reversedSplit := []configACEExpectation{split[1], split[0]}
	for _, expected := range [][]configACEExpectation{combined, split, reversedSplit} {
		matches, err := configDACLMatches(dacl, current, expected)
		if err != nil {
			return fmt.Errorf("read config DACL entry: %w", err)
		}
		if matches {
			return nil
		}
	}
	aceCount := uint16(0)
	if dacl != nil {
		aceCount = dacl.AceCount
	}
	return fmt.Errorf(
		"config directory DACL must be a canonical current-user-only ACL (present=%t, aceCount=%d, entries=%s)",
		dacl != nil,
		aceCount,
		describeConfigDACL(dacl, current),
	)
}

func configDACLMatches(
	dacl *windows.ACL,
	current *windows.SID,
	expected []configACEExpectation,
) (bool, error) {
	if dacl == nil || int(dacl.AceCount) != len(expected) {
		return false, nil
	}
	for index, want := range expected {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			return false, err
		}
		if ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceFlags != want.flags ||
			!isConfigFullAccess(ace.Mask) {
			return false, nil
		}
		aceSID, valid := configACESID(ace)
		if !valid || !aceSID.Equals(current) {
			return false, nil
		}
	}
	return true, nil
}

func isConfigFullAccess(mask windows.ACCESS_MASK) bool {
	return mask == windows.GENERIC_ALL || mask == configFileAllAccess
}

type configSIDHeader struct {
	revision          uint8
	subAuthorityCount uint8
	authority         [6]byte
}

func configACESID(ace *windows.ACCESS_ALLOWED_ACE) (*windows.SID, bool) {
	if ace == nil {
		return nil, false
	}
	sidOffset := unsafe.Offsetof(windows.ACCESS_ALLOWED_ACE{}.SidStart)
	headerSize := unsafe.Sizeof(configSIDHeader{})
	if uintptr(ace.Header.AceSize) < sidOffset+headerSize {
		return nil, false
	}
	header := (*configSIDHeader)(unsafe.Pointer(&ace.SidStart))
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

func validateConfigFileDACL(
	dacl *windows.ACL,
	control windows.SECURITY_DESCRIPTOR_CONTROL,
	current *windows.SID,
) error {
	if dacl == nil || dacl.AceCount != 1 {
		return errors.New("config file DACL must grant access only to the current user")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read config DACL entry: %w", err)
	}
	if ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return errors.New("config file DACL must contain one allow entry")
	}
	if control&windows.SE_DACL_PROTECTED == 0 && ace.Header.AceFlags&windows.INHERITED_ACE == 0 {
		return errors.New("config file DACL must be protected or inherited from its validated directory")
	}
	aceSID, valid := configACESID(ace)
	if !valid || !aceSID.Equals(current) {
		return errors.New("config file DACL must grant access only to the current user")
	}
	return nil
}

func describeConfigDACL(dacl *windows.ACL, current *windows.SID) string {
	if dacl == nil {
		return "[]"
	}
	const maximumEntries = 8
	entryCount := min(int(dacl.AceCount), maximumEntries)
	entries := make([]string, 0, entryCount+1)
	for index := range entryCount {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			entries = append(entries, fmt.Sprintf("{index=%d,error=unreadable}", index))
			continue
		}
		if ace == nil {
			entries = append(entries, fmt.Sprintf("{index=%d,error=unreadable}", index))
			continue
		}
		principal := "unparsed"
		if ace.Header.AceType == windows.ACCESS_ALLOWED_ACE_TYPE ||
			ace.Header.AceType == windows.ACCESS_DENIED_ACE_TYPE {
			if sid, valid := configACESID(ace); valid {
				principal = classifyConfigSID(sid, current)
			} else {
				principal = "invalid"
			}
		}
		entries = append(entries, fmt.Sprintf(
			"{index=%d,type=%d,flags=%#x,mask=%#x,principal=%s}",
			index,
			ace.Header.AceType,
			ace.Header.AceFlags,
			ace.Mask,
			principal,
		))
	}
	if int(dacl.AceCount) > maximumEntries {
		entries = append(entries, "{truncated=true}")
	}
	return "[" + strings.Join(entries, ",") + "]"
}

func classifyConfigSID(sid, current *windows.SID) string {
	switch {
	case sid.Equals(current):
		return "current"
	case sid.IsWellKnown(windows.WinLocalSystemSid):
		return "system"
	case sid.IsWellKnown(windows.WinBuiltinAdministratorsSid):
		return "administrators"
	case sid.IsWellKnown(windows.WinCreatorOwnerSid):
		return "creator-owner"
	case sid.IsWellKnown(windows.WinCreatorOwnerRightsSid):
		return "owner-rights"
	case sid.IsWellKnown(windows.WinWorldSid):
		return "everyone"
	default:
		return "other"
	}
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
