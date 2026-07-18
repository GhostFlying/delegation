//go:build windows

package store

import (
	"strings"
	"testing"
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
