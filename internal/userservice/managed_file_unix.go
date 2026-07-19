//go:build linux || darwin

package userservice

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxServiceDescriptorSize = 1 << 20

var syncManagedDirectory = syncDirectory

func installManagedFile(path string, descriptor Descriptor) (State, error) {
	if len(descriptor.Content) > maxServiceDescriptorSize {
		return StateAbsent, errors.New("service definition is too large")
	}
	if !ownsDescriptor(descriptor.Kind, descriptor.Content) {
		return StateAbsent, errors.New("service definition does not contain valid Delegation ownership metadata")
	}
	state, existing, err := inspectManagedFile(path, descriptor.Kind)
	if err != nil {
		return state, err
	}
	switch state {
	case StatePrepared:
		if bytes.Equal(existing, descriptor.Content) {
			return StatePrepared, nil
		}
		return StatePrepared, errors.New("managed service definition differs; remove it explicitly before reinstalling")
	case StateForeignConflict:
		return state, errors.New("service definition path is occupied by an unmanaged file")
	case StateAbsent:
	}

	dir := filepath.Dir(path)
	if err := createManagedDirectoriesDurably(dir); err != nil {
		return StateAbsent, fmt.Errorf("create service directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".delegation-service-*.tmp")
	if err != nil {
		return StateAbsent, fmt.Errorf("create temporary service definition: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return StateAbsent, fmt.Errorf("protect temporary service definition: %w", err)
	}
	if _, err := temp.Write(descriptor.Content); err != nil {
		temp.Close()
		return StateAbsent, fmt.Errorf("write service definition: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return StateAbsent, fmt.Errorf("sync service definition: %w", err)
	}
	if err := temp.Close(); err != nil {
		return StateAbsent, fmt.Errorf("close service definition: %w", err)
	}
	if err := os.Link(tempPath, path); err != nil {
		state, existing, inspectErr := inspectManagedFile(path, descriptor.Kind)
		if inspectErr == nil && state == StatePrepared && bytes.Equal(existing, descriptor.Content) {
			return StatePrepared, nil
		}
		if inspectErr != nil {
			err = errors.Join(err, inspectErr)
		}
		return state, fmt.Errorf("install service definition without replacement: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return StatePrepared, &CommittedError{Err: fmt.Errorf("remove linked temporary definition: %w", err)}
	}
	if err := syncManagedDirectory(dir); err != nil {
		return StatePrepared, &CommittedError{Err: fmt.Errorf("sync installed service definition: %w", err)}
	}
	return StatePrepared, nil
}

func inspectManagedFile(path string, kind Kind) (State, []byte, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return StateAbsent, nil, nil
	}
	if err != nil {
		return StateAbsent, nil, fmt.Errorf("inspect service definition: %w", err)
	}
	if !info.Mode().IsRegular() {
		return StateForeignConflict, nil, nil
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return StateAbsent, nil, fmt.Errorf("open service definition: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return StateAbsent, nil, fmt.Errorf("inspect opened service definition: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return StateForeignConflict, nil, nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maxServiceDescriptorSize+1))
	if err != nil {
		return StateAbsent, nil, fmt.Errorf("read service definition: %w", err)
	}
	if len(data) > maxServiceDescriptorSize {
		return StateForeignConflict, nil, nil
	}
	if !ownsDescriptor(kind, data) {
		return StateForeignConflict, data, nil
	}
	return StatePrepared, data, nil
}

func createManagedDirectoriesDurably(path string) error {
	var missing []string
	current := filepath.Clean(path)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("path component is not a directory: %s", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("no existing parent directory for %s", path)
		}
		current = parent
	}
	if err := syncManagedDirectory(filepath.Dir(current)); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		directory := missing[i]
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncManagedDirectory(filepath.Dir(directory)); err != nil {
			return err
		}
	}
	return nil
}

func ownsDescriptor(kind Kind, data []byte) bool {
	switch kind {
	case KindSystemd:
		return bytes.HasPrefix(data, []byte("# "+MarkerBroker+"\n")) ||
			bytes.HasPrefix(data, []byte("# "+MarkerPeer+"\n"))
	case KindLaunchAgent:
		return ownsLaunchAgent(data)
	case KindScheduledTask:
		return false
	}
	return false
}

func ownsLaunchAgent(data []byte) bool {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	depth := 0
	rootSeen := false
	rootClosed := false
	dictSeen := false
	dictClosed := false
	pendingKey := ""
	values := make(map[string]string, 2)
	seen := make(map[string]bool, 2)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
		switch element := token.(type) {
		case xml.StartElement:
			if rootClosed {
				return false
			}
			if depth == 0 {
				if rootSeen || element.Name.Space != "" || element.Name.Local != "plist" {
					return false
				}
				rootSeen = true
				depth = 1
				continue
			}
			if depth == 1 {
				if dictSeen || element.Name.Space != "" || element.Name.Local != "dict" {
					return false
				}
				dictSeen = true
				depth = 2
				continue
			}
			if depth != 2 || element.Name.Space != "" {
				return false
			}
			if element.Name.Local == "key" {
				if pendingKey != "" {
					return false
				}
				key, ok := readPlistScalar(decoder, element)
				if !ok {
					return false
				}
				if key == "Label" || key == "Description" {
					if seen[key] {
						return false
					}
					seen[key] = true
				}
				pendingKey = key
				continue
			}
			if pendingKey == "" {
				return false
			}
			key := pendingKey
			pendingKey = ""
			if key == "Label" || key == "Description" {
				if element.Name.Local != "string" {
					return false
				}
				value, ok := readPlistScalar(decoder, element)
				if !ok {
					return false
				}
				values[key] = value
				continue
			}
			if err := decoder.Skip(); err != nil {
				return false
			}
		case xml.EndElement:
			switch depth {
			case 2:
				if element.Name.Space != "" || element.Name.Local != "dict" || pendingKey != "" {
					return false
				}
				dictClosed = true
				depth = 1
			case 1:
				if element.Name.Space != "" || element.Name.Local != "plist" || !dictClosed {
					return false
				}
				rootClosed = true
				depth = 0
			default:
				return false
			}
		case xml.CharData:
			if strings.TrimSpace(string(element)) != "" {
				return false
			}
		case xml.Directive, xml.ProcInst:
			if rootSeen {
				return false
			}
		}
	}
	if !rootSeen || !rootClosed || !dictSeen || !dictClosed || depth != 0 || pendingKey != "" ||
		!seen["Label"] || !seen["Description"] {
		return false
	}
	for _, role := range []ServiceRole{ServiceRoleBroker, ServiceRolePeer} {
		spec, _ := specFor(role)
		if values["Label"] == spec.launchAgent && values["Description"] == spec.marker {
			return true
		}
	}
	return false
}

func readPlistScalar(decoder *xml.Decoder, start xml.StartElement) (string, bool) {
	var value strings.Builder
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", false
		}
		switch element := token.(type) {
		case xml.CharData:
			value.Write(element)
		case xml.EndElement:
			if element.Name != start.Name {
				return "", false
			}
			return value.String(), true
		default:
			return "", false
		}
	}
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
