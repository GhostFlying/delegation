//go:build windows

package localbridge

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

var pipePattern = regexp.MustCompile(`^\\\\\.\\pipe\\delegation-[0-9a-f]{32}$`)

func normalizeConfigPath(path string) string {
	return strings.ToLower(path)
}

func platformEndpoint(_ string, name string) (string, error) {
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
	return winio.ListenPipe(endpoint, &winio.PipeConfig{
		SecurityDescriptor: fmt.Sprintf("D:P(A;;GA;;;%s)", sid),
		InputBufferSize:    64 * 1024,
		OutputBufferSize:   64 * 1024,
	})
}

func dial(ctx context.Context, endpoint string) (net.Conn, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	return winio.DialPipeContext(ctx, endpoint)
}

func currentUserSID() (string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("resolve current Windows user: %w", err)
	}
	return user.User.Sid.String(), nil
}
