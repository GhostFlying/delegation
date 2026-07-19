//go:build !windows

package connector

import (
	"os"
	"syscall"
	"testing"
)

func TestMain(m *testing.M) {
	syscall.Umask(0o077)
	os.Exit(m.Run())
}
