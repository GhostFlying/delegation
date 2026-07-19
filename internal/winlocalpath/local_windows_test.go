//go:build windows

package winlocalpath

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestValidateFinalDirectoryPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		path      string
		driveType uint32
		wantError string
	}{
		{name: "fixed", path: `\\?\C:\private`, driveType: windows.DRIVE_FIXED},
		{name: "removable", path: `\\?\E:\private`, driveType: windows.DRIVE_REMOVABLE},
		{name: "ramdisk", path: `\\?\R:\private`, driveType: windows.DRIVE_RAMDISK},
		{name: "UNC", path: `\\?\UNC\server\share\private`, driveType: windows.DRIVE_FIXED, wantError: "network path"},
		{name: "mapped", path: `\\?\Z:\private`, driveType: windows.DRIVE_REMOTE, wantError: "mapped"},
		{name: "volume GUID", path: `\\?\Volume{00000000-0000-0000-0000-000000000000}\private`, driveType: windows.DRIVE_FIXED, wantError: "unsupported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateFinalDirectoryPath(test.path, func(root string) uint32 {
				if len(root) != 3 || root[1:] != `:\` {
					t.Fatalf("drive root = %q", root)
				}
				return test.driveType
			})
			if test.wantError == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Fatalf("validation error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestFinalDirectoryPathResolvesJunctionAncestor(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	alias := filepath.Join(parent, "alias")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", alias, target).CombinedOutput(); err != nil {
		t.Skipf("creating a directory junction is unavailable: %v: %s", err, output)
	}
	aliasFile, err := os.Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer aliasFile.Close()
	targetFile, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer targetFile.Close()
	aliasPath, err := finalDirectoryPath(windows.Handle(aliasFile.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	targetPath, err := finalDirectoryPath(windows.Handle(targetFile.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(aliasPath, targetPath) {
		t.Fatalf("junction final path = %q, target = %q", aliasPath, targetPath)
	}
}
