//go:build windows

package store

import (
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestDataSourceNameUsesLocalWindowsFileURI(t *testing.T) {
	dsn := dataSourceName(`C:\Users\delegation\state\broker.sqlite3`)
	if !strings.HasPrefix(dsn, "file:///C:/Users/delegation/state/broker.sqlite3?") {
		t.Fatalf("Windows DSN = %q", dsn)
	}
	if strings.HasPrefix(dsn, "file://C:") {
		t.Fatalf("Windows DSN contains a drive authority: %q", dsn)
	}
}

func TestPreparePathRejectsWindowsNetworkPath(t *testing.T) {
	if _, err := preparePath(`\\server\share\delegation\broker.sqlite3`); err == nil {
		t.Fatal("preparePath accepted a Windows network path")
	}
}

func TestValidatePathRejectsWindowsNetworkPath(t *testing.T) {
	if err := ValidatePath(`\\server\share\delegation\broker.sqlite3`); err == nil {
		t.Fatal("ValidatePath accepted a Windows network path")
	}
}

func TestWindowsDriveTypeRejectsMappedNetworkDrive(t *testing.T) {
	if err := validateWindowsDriveType(windows.DRIVE_REMOTE); err == nil {
		t.Fatal("validateWindowsDriveType accepted a mapped network drive")
	}
	for _, driveType := range []uint32{windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK} {
		if err := validateWindowsDriveType(driveType); err != nil {
			t.Fatalf("validateWindowsDriveType(%d): %v", driveType, err)
		}
	}
}
