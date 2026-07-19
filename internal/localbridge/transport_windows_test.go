//go:build windows

package localbridge

import (
	"context"
	"net"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsBridgePipeUsesCurrentUserOnlyDACL(t *testing.T) {
	endpoint, err := Endpoint(bridgeTestControllerID, bridgeTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := listen(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			acceptErrors <- err
			return
		}
		accepted <- connection
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	client, err := dial(ctx, endpoint)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server net.Conn
	select {
	case server = <-accepted:
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("named pipe accept timed out")
	}
	defer server.Close()
	handle, ok := server.(interface{ Fd() uintptr })
	if !ok {
		t.Fatalf("accepted named pipe %T does not expose its handle", server)
	}
	descriptor, err := windows.GetSecurityInfo(
		windows.Handle(handle.Fd()),
		windows.SE_KERNEL_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := descriptor.Control()
	if err != nil {
		t.Fatal(err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatal("named pipe DACL inherits access entries")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if dacl == nil {
		t.Fatal("named pipe has no DACL")
	}
	if dacl.AceCount != 1 {
		t.Fatalf("named pipe DACL has %d entries, want 1", dacl.AceCount)
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		t.Fatal(err)
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		t.Fatalf("named pipe ACE type = %d, want allow", ace.Header.AceType)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !aceSID.Equals(user.User.Sid) {
		t.Fatal("named pipe grants access to a principal other than the current user")
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		t.Fatal(err)
	}
	if owner == nil || !owner.Equals(user.User.Sid) {
		t.Fatal("named pipe is not owned by the current user")
	}
}
