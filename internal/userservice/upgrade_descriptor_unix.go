//go:build linux || darwin

package userservice

import "runtime"

func renderPlatformDescriptor(role ServiceRole, invocation Invocation) (Descriptor, error) {
	if runtime.GOOS == "darwin" {
		return RenderLaunchAgent(role, invocation)
	}
	return RenderSystemd(role, invocation)
}
