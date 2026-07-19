//go:build linux || darwin

package localbridge

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("/tmp", "dlh-")
	if err != nil {
		panic(err)
	}
	if err := os.Chmod(home, 0o700); err != nil {
		panic(err)
	}
	previous, hadPrevious := os.LookupEnv("HOME")
	if err := os.Setenv("HOME", home); err != nil {
		panic(err)
	}
	code := m.Run()
	if hadPrevious {
		_ = os.Setenv("HOME", previous)
	} else {
		_ = os.Unsetenv("HOME")
	}
	_ = os.RemoveAll(home)
	os.Exit(code)
}

func TestUnixBridgeUsesPrivateSocketAndRejectsSecondServer(t *testing.T) {
	endpoint := testEndpoint(t)
	backend := &fakeBackend{}
	server, err := Listen(endpoint, testServiceIdentity(), backend)
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
	if second, err := Listen(endpoint, testServiceIdentity(), backend); err == nil {
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

func TestUnixBridgeRejectsPreclaimableRuntimeNamespace(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "dh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	if err := os.Chmod(home, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	endpoint, err := Endpoint(bridgeTestControllerID, bridgeTestDeviceID)
	if err != nil {
		t.Fatal(err)
	}
	if server, err := Listen(
		endpoint, testServiceIdentity(), &fakeBackend{},
	); err == nil {
		server.Close()
		t.Fatal("Listen accepted a user runtime namespace writable by another account")
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
			endpoint := testEndpoint(t)
			if err := prepareSocketDirectory(filepath.Dir(endpoint)); err != nil {
				t.Fatal(err)
			}
			test.stale(t, endpoint)
			server, err := Listen(
				endpoint, testServiceIdentity(), &fakeBackend{},
			)
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
	endpoint := testEndpoint(t)
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

func TestUnixListenerDoesNotUnlinkReplacementSocket(t *testing.T) {
	endpoint := testEndpoint(t)
	var err error
	first, err := listen(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(endpoint); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := os.Chmod(endpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(endpoint); err != nil {
		t.Fatalf("closing old listener removed replacement socket: %v", err)
	}
}

func TestServerCancellationClosesIncompleteLocalCall(t *testing.T) {
	endpoint := testEndpoint(t)
	var err error
	server, err := Listen(endpoint, testServiceIdentity(), &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	connection, err := dial(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	cancel()
	started := time.Now()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() waited %v for an incomplete local call", elapsed)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after cancellation")
	}
}
