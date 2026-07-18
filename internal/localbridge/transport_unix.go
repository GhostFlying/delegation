//go:build linux || darwin

package localbridge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const maximumUnixSocketPath = 100

type unixListener struct {
	net.Listener
	path      string
	fileInfo  os.FileInfo
	closeOnce sync.Once
	closeErr  error
}

func normalizeConfigPath(path string) string {
	return path
}

func platformEndpoint(_ string, name string) (string, error) {
	endpoint := filepath.Join("/tmp", fmt.Sprintf("delegation-%d", os.Geteuid()), name+".sock")
	if len([]byte(endpoint)) > maximumUnixSocketPath {
		return "", fmt.Errorf("local bridge socket path exceeds %d bytes", maximumUnixSocketPath)
	}
	return endpoint, nil
}

func validateEndpoint(endpoint string) error {
	if !filepath.IsAbs(endpoint) || len([]byte(endpoint)) > maximumUnixSocketPath {
		return errors.New("local bridge endpoint must be an absolute, bounded Unix socket path")
	}
	return nil
}

func listen(endpoint string) (net.Listener, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	directory := filepath.Dir(endpoint)
	if err := prepareSocketDirectory(directory); err != nil {
		return nil, err
	}
	if err := removeStaleSocket(endpoint); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		_ = listener.Close()
		_ = os.Remove(endpoint)
	}
	if err := os.Chmod(endpoint, 0o600); err != nil {
		cleanup()
		return nil, fmt.Errorf("restrict local bridge socket: %w", err)
	}
	info, err := secureSocketInfo(endpoint)
	if err != nil {
		cleanup()
		return nil, err
	}
	return &unixListener{Listener: listener, path: endpoint, fileInfo: info}, nil
}

func dial(ctx context.Context, endpoint string) (net.Conn, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	if err := validateSocketDirectory(filepath.Dir(endpoint)); err != nil {
		return nil, err
	}
	if _, err := secureSocketInfo(endpoint); err != nil {
		return nil, err
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", endpoint)
}

func prepareSocketDirectory(path string) error {
	err := os.Mkdir(path, 0o700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create local bridge directory: %w", err)
	}
	return validateSocketDirectory(path)
}

func validateSocketDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect local bridge directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("local bridge directory must be a non-symlink directory with mode 0700")
	}
	if !ownedByCurrentUser(info) {
		return errors.New("local bridge directory must be owned by the current user")
	}
	return nil
}

func secureSocketInfo(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect local bridge socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("local bridge endpoint must be a same-user Unix socket")
	}
	if !ownedByCurrentUser(info) {
		return nil, errors.New("local bridge socket must be owned by the current user")
	}
	return info, nil
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing local bridge endpoint: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(info) {
		return errors.New("local bridge endpoint is occupied by an unsafe file")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	connection, dialErr := dialer.DialContext(ctx, "unix", path)
	if dialErr == nil {
		connection.Close()
		return errors.New("local delegation service is already running")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
		return fmt.Errorf("existing local bridge endpoint state is uncertain: %w", dialErr)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale local bridge socket: %w", err)
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func (l *unixListener) Close() error {
	l.closeOnce.Do(func() {
		closeErr := l.Listener.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		info, err := os.Lstat(l.path)
		if err == nil && os.SameFile(info, l.fileInfo) {
			err = os.Remove(l.path)
		}
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		l.closeErr = errors.Join(closeErr, err)
	})
	return l.closeErr
}
