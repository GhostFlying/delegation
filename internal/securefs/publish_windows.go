//go:build windows

package securefs

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type fileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func publishNoReplace(root *os.Root, temporary, destination string) (bool, error) {
	directory, err := root.Open(".")
	if err != nil {
		return false, err
	}
	defer directory.Close()

	objectName, err := windows.NewNTUnicodeString(temporary)
	if err != nil {
		return false, err
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(directory.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	var handle windows.Handle
	err = windows.NtCreateFile(
		&handle,
		windows.FILE_GENERIC_WRITE|windows.DELETE|windows.SYNCHRONIZE,
		attributes,
		&windows.IO_STATUS_BLOCK{},
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|
			windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)
	if err != nil {
		return false, windowsError(err)
	}
	defer windows.CloseHandle(handle)

	encoded, err := windows.UTF16FromString(destination)
	if err != nil {
		return false, err
	}
	nameLength := (len(encoded) - 1) * 2
	var layout fileRenameInformation
	offset := int(unsafe.Offsetof(layout.FileName))
	buffer := make([]byte, offset+nameLength)
	information := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	information.RootDirectory = windows.Handle(directory.Fd())
	information.FileNameLength = uint32(nameLength)
	name := unsafe.Slice((*uint16)(unsafe.Pointer(&buffer[offset])), len(encoded)-1)
	copy(name, encoded[:len(encoded)-1])

	err = windows.NtSetInformationFile(
		handle,
		&windows.IO_STATUS_BLOCK{},
		&buffer[0],
		uint32(len(buffer)),
		windows.FileRenameInformation,
	)
	if err != nil {
		return false, windowsError(err)
	}
	if err := windows.FlushFileBuffers(handle); err != nil {
		return true, err
	}
	return true, nil
}

func windowsError(err error) error {
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}
