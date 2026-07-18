//go:build linux || darwin

package localbridge

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
)

func TestUnixBridgeUsesPrivateSocketAndRejectsSecondServer(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	endpoint, err := Endpoint(configPath, bridgeTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeBackend{}
	server, err := Listen(endpoint, control.DeviceRoleController, backend)
	if err != nil {
		t.Fatal(err)
	}
	directoryInfo, err := os.Stat(filepath.Dir(endpoint))
	if err != nil || directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("socket directory = %#v, error %v", directoryInfo, err)
	}
	socketInfo, err := os.Lstat(endpoint)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket = %#v, error %v", socketInfo, err)
	}
	if second, err := Listen(endpoint, control.DeviceRoleController, backend); err == nil {
		second.Close()
		t.Fatal("second local bridge server bound the same endpoint")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(endpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed bridge retained socket: %v", err)
	}
}

func TestUnixBridgeReplacesOnlyPrivateStaleSockets(t *testing.T) {
	for _, test := range []struct {
		name  string
		stale func(*testing.T, string)
		want  bool
	}{
		{name: "stale socket", want: true, stale: func(t *testing.T, endpoint string) {
			listener, err := net.Listen("unix", endpoint)
			if err != nil {
				t.Fatal(err)
			}
			unix := listener.(*net.UnixListener)
			unix.SetUnlinkOnClose(false)
			if err := os.Chmod(endpoint, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "regular file", stale: func(t *testing.T, endpoint string) {
			if err := os.WriteFile(endpoint, []byte("occupied"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", stale: func(t *testing.T, endpoint string) {
			target := filepath.Join(filepath.Dir(endpoint), "target")
			if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, endpoint); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.json")
			endpoint, err := Endpoint(configPath, bridgeTestDeviceID)
			if err != nil {
				t.Fatal(err)
			}
			if err := prepareSocketDirectory(filepath.Dir(endpoint)); err != nil {
				t.Fatal(err)
			}
			test.stale(t, endpoint)
			server, err := Listen(endpoint, control.DeviceRoleController, &fakeBackend{})
			if test.want {
				if err != nil {
					t.Fatal(err)
				}
				server.Close()
				return
			}
			if err == nil {
				server.Close()
				t.Fatal("bridge replaced an unsafe endpoint")
			}
		})
	}
}

func TestUnixClientRejectsInsecureSocketPermissions(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	endpoint, err := Endpoint(configPath, bridgeTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareSocketDirectory(filepath.Dir(endpoint)); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(endpoint, 0o666); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "future.call", "", nil, struct{}{}, nil); err == nil {
		t.Fatal("client connected to an insecure local socket")
	}
}
