//go:build windows

package userservice

import "golang.org/x/sys/windows"

func renderPlatformDescriptor(role ServiceRole, invocation Invocation) (Descriptor, error) {
	sid, err := windowsUserSID()
	if err != nil {
		return Descriptor{}, err
	}
	return RenderScheduledTask(role, invocation, sid, windows.EscapeArg)
}
