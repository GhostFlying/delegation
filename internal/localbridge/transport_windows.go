//go:build windows

package localbridge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

var pipePattern = regexp.MustCompile(`^\\\\\.\\pipe\\delegation-[0-9a-f]{32}$`)

const existingPipeProbeTimeout = 100 * time.Millisecond

func platformEndpoint(name string) (string, error) {
	return `\\.\pipe\delegation-` + name, nil
}

func validateEndpoint(endpoint string) error {
	if !pipePattern.MatchString(endpoint) {
		return errors.New("local bridge endpoint must be a delegation named pipe")
	}
	return nil
}

func listen(endpoint string) (net.Listener, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	sid, err := currentUserSID()
	if err != nil {
		return nil, err
	}
	listener, err := winio.ListenPipe(endpoint, &winio.PipeConfig{
		SecurityDescriptor: fmt.Sprintf("O:%sD:P(A;;GA;;;%s)", sid, sid),
		InputBufferSize:    64 * 1024,
		OutputBufferSize:   64 * 1024,
	})
	if err == nil {
		return listener, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), existingPipeProbeTimeout)
	defer cancel()
	connection, probeErr := winio.DialPipeContext(ctx, endpoint)
	if probeErr == nil {
		_ = connection.Close()
		return nil, errors.New("local delegation service is already running")
	}
	return nil, err
}

func dial(ctx context.Context, endpoint string) (net.Conn, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	connection, err := winio.DialPipeContext(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if err := validatePipeServer(connection); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func validatePipeServer(connection net.Conn) error {
	file, ok := connection.(interface{ Fd() uintptr })
	if !ok {
		return errors.New("local bridge named pipe does not expose a server-authentication handle")
	}
	var processID uint32
	if err := windows.GetNamedPipeServerProcessId(windows.Handle(file.Fd()), &processID); err != nil {
		return fmt.Errorf("resolve local bridge server process: %w", err)
	}
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, processID)
	if err != nil {
		return fmt.Errorf("open local bridge server process: %w", err)
	}
	defer windows.CloseHandle(process)
	var token windows.Token
	if err := windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token); err != nil {
		return fmt.Errorf("open local bridge server token: %w", err)
	}
	defer token.Close()
	serverUser, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("resolve local bridge server user: %w", err)
	}
	currentUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("resolve current Windows user: %w", err)
	}
	if !serverUser.User.Sid.Equals(currentUser.User.Sid) {
		return errors.New("local bridge named-pipe server is owned by another Windows user")
	}
	return nil
}

func currentUserSID() (string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid.String(), nil
}
