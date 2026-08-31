//go:build windows

package securefs

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func moveNoReplace(source *os.Root, oldName string, destination *os.Root, newName string) error {
	sourceDirectory, err := source.Open(".")
	if err != nil {
		return err
	}
	defer sourceDirectory.Close()
	destinationDirectory, err := destination.Open(".")
	if err != nil {
		return err
	}
	defer destinationDirectory.Close()

	objectName, err := windows.NewNTUnicodeString(oldName)
	if err != nil {
		return err
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(sourceDirectory.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	var handle windows.Handle
	err = windows.NtCreateFile(
		&handle, windows.DELETE|windows.SYNCHRONIZE, attributes,
		&windows.IO_STATUS_BLOCK{}, nil, windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0, 0,
	)
	if err != nil {
		return windowsError(err)
	}
	defer windows.CloseHandle(handle)

	encoded, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	nameLength := (len(encoded) - 1) * 2
	var layout fileRenameInformation
	offset := int(unsafe.Offsetof(layout.FileName))
	buffer := make([]byte, offset+nameLength)
	information := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	information.RootDirectory = windows.Handle(destinationDirectory.Fd())
	information.FileNameLength = uint32(nameLength)
	name := unsafe.Slice((*uint16)(unsafe.Pointer(&buffer[offset])), len(encoded)-1)
	copy(name, encoded[:len(encoded)-1])
	if err := windows.NtSetInformationFile(
		handle, &windows.IO_STATUS_BLOCK{}, &buffer[0], uint32(len(buffer)),
		windows.FileRenameInformation,
	); err != nil {
		return windowsError(err)
	}
	return nil
}
