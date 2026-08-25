//go:build windows

package pathguard

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsCanonicalFinalPath(t *testing.T) {
	t.Run("local drive uses volume GUID", func(t *testing.T) {
		calls := 0
		path, err := windowsCanonicalFinalPath(func(volumeName uint32) (string, error) {
			calls++
			switch volumeName {
			case windowsVolumeNameDOS:
				return `\\?\C:\authority`, nil
			case windowsVolumeNameGUID:
				return `\\?\Volume{11111111-2222-3333-4444-555555555555}\authority`, nil
			default:
				t.Fatalf("unexpected volume name mode %d", volumeName)
				return "", nil
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		want := `\\?\Volume{11111111-2222-3333-4444-555555555555}\authority`
		if path != want || calls != 2 {
			t.Fatalf("windowsCanonicalFinalPath() = %q after %d calls, want %q after 2", path, calls, want)
		}
	})

	t.Run("volume GUID input stays volume GUID", func(t *testing.T) {
		path, err := windowsCanonicalFinalPath(func(volumeName uint32) (string, error) {
			switch volumeName {
			case windowsVolumeNameDOS, windowsVolumeNameGUID:
				return `\\?\Volume{11111111-2222-3333-4444-555555555555}\authority`, nil
			default:
				t.Fatalf("unexpected volume name mode %d", volumeName)
				return "", nil
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if path != `\\?\Volume{11111111-2222-3333-4444-555555555555}\authority` {
			t.Fatalf("windowsCanonicalFinalPath() = %q", path)
		}
	})

	t.Run("UNC stays normalized UNC", func(t *testing.T) {
		path, err := windowsCanonicalFinalPath(func(volumeName uint32) (string, error) {
			if volumeName != windowsVolumeNameDOS {
				t.Fatal("UNC path requested a volume GUID")
			}
			return `\\?\UNC\server\share\authority`, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if path != `\\?\UNC\server\share\authority` {
			t.Fatalf("windowsCanonicalFinalPath() = %q", path)
		}
	})

	for _, test := range []struct {
		name string
		path func(volumeName uint32) (string, error)
		want string
	}{
		{
			name: "DOS metadata failure",
			path: func(uint32) (string, error) {
				return "", windows.ERROR_ACCESS_DENIED
			},
			want: "resolve DOS final path",
		},
		{
			name: "volume GUID unavailable",
			path: func(volumeName uint32) (string, error) {
				if volumeName == windowsVolumeNameDOS {
					return `\\?\C:\authority`, nil
				}
				return "", windows.ERROR_PATH_NOT_FOUND
			},
			want: "resolve volume GUID final path",
		},
		{
			name: "unsupported DOS spelling",
			path: func(uint32) (string, error) {
				return `\Device\HarddiskVolume1\authority`, nil
			},
			want: "unsupported volume spelling",
		},
		{
			name: "malformed volume GUID spelling",
			path: func(volumeName uint32) (string, error) {
				if volumeName == windowsVolumeNameDOS {
					return `\\?\C:\authority`, nil
				}
				return `\\?\Volume{missing-separator}`, nil
			},
			want: "unsupported spelling",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := windowsCanonicalFinalPath(test.path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("windowsCanonicalFinalPath() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateBrokerAuthorityRejectsRootRelativeParentSymlink(t *testing.T) {
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	rootRelative := strings.TrimPrefix(target, filepath.VolumeName(target))
	if err := os.Symlink(rootRelative, alias); err != nil {
		t.Skipf("creating a Windows directory symlink is unavailable: %v", err)
	}
	authority := filepath.Join(target, "authority")
	err := ValidateBrokerAuthority(
		filepath.Join(alias, "authority"),
		filepath.Join(t.TempDir(), "state", "broker.sqlite3"),
		authority,
	)
	if err == nil || !strings.Contains(err.Error(), "master token") {
		t.Fatalf("ValidateBrokerAuthority() error = %v", err)
	}
}

func TestWindowsNameSurrogateClassification(t *testing.T) {
	for _, test := range []struct {
		name       string
		mode       os.FileMode
		attributes uint32
		reparseTag uint32
		want       bool
	}{
		{
			name:       "junction",
			mode:       os.ModeIrregular,
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: windows.IO_REPARSE_TAG_MOUNT_POINT,
			want:       true,
		},
		{
			name:       "irregular name surrogate",
			mode:       os.ModeIrregular,
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: 0xA000001D,
			want:       true,
		},
		{
			name:       "irregular non-name-surrogate reparse point",
			mode:       os.ModeIrregular,
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: 0x8000001B,
		},
		{
			name:       "irregular non-reparse file",
			mode:       os.ModeIrregular,
			reparseTag: windows.IO_REPARSE_TAG_MOUNT_POINT,
		},
		{
			name:       "regular file",
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: windows.IO_REPARSE_TAG_MOUNT_POINT,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := windowsNameSurrogate(test.mode, test.attributes, test.reparseTag); got != test.want {
				t.Fatalf("windowsNameSurrogate() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestWindowsPathAliasTargetFromMetadataDoesNotReadNonLinks(t *testing.T) {
	for _, test := range []struct {
		name       string
		mode       os.FileMode
		attributes uint32
		reparseTag uint32
	}{
		{
			name:       "irregular non-name-surrogate reparse point",
			mode:       os.ModeIrregular,
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: 0x8000001B,
		},
		{
			name:       "irregular non-reparse file",
			mode:       os.ModeIrregular,
			reparseTag: windows.IO_REPARSE_TAG_MOUNT_POINT,
		},
		{
			name:       "regular reparse point",
			attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT,
			reparseTag: windows.IO_REPARSE_TAG_MOUNT_POINT,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, isAlias, err := windowsPathAliasTargetFromMetadata(
				`C:\guarded`,
				test.mode,
				test.attributes,
				test.reparseTag,
				func(string) (string, error) {
					t.Fatal("non-link metadata reached Readlink")
					return "", nil
				},
			)
			if err != nil || isAlias || target != "" {
				t.Fatalf(
					"windowsPathAliasTargetFromMetadata() = %q, %t, %v",
					target,
					isAlias,
					err,
				)
			}
		})
	}
}

func TestWindowsDriveAndVolumeGUIDPathsShareIdentity(t *testing.T) {
	drivePath, guidPath := windowsExistingPathSpellings(t, t.TempDir())
	for _, paths := range [][2]string{
		{drivePath, guidPath},
		{guidPath, drivePath},
		{strings.ToUpper(drivePath), strings.ToLower(guidPath)},
	} {
		contained, err := pathWithin(paths[0], paths[1])
		if err != nil {
			t.Fatal(err)
		}
		if !contained {
			t.Fatalf("pathWithin(%q, %q) = false", paths[0], paths[1])
		}
		conflicts, err := equivalent(paths[0], paths[1])
		if err != nil {
			t.Fatal(err)
		}
		if !conflicts {
			t.Fatalf("equivalent(%q, %q) = false", paths[0], paths[1])
		}
	}
}

func TestWindowsDriveAndVolumeGUIDFuturePathsShareIdentity(t *testing.T) {
	drivePath, guidPath := windowsExistingPathSpellings(t, t.TempDir())
	driveFuture := filepath.Join(drivePath, "future", "authority")
	guidFuture := filepath.Join(guidPath, "future", "authority")
	driveCanonical, err := canonicalFuturePath(driveFuture)
	if err != nil {
		t.Fatal(err)
	}
	guidCanonical, err := canonicalFuturePath(guidFuture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(driveCanonical, guidCanonical) {
		t.Fatalf("future canonical paths differ: %q != %q", driveCanonical, guidCanonical)
	}
	contained, err := pathWithin(guidFuture, drivePath)
	if err != nil {
		t.Fatal(err)
	}
	if !contained {
		t.Fatalf("pathWithin(%q, %q) = false", guidFuture, drivePath)
	}
}

func TestWindowsCanonicalFuturePathFailsClosedOnVolumeIdentityError(t *testing.T) {
	original := platformCanonicalExistingPath
	platformCanonicalExistingPath = func(string) (string, error) {
		return "", windows.ERROR_ACCESS_DENIED
	}
	t.Cleanup(func() {
		platformCanonicalExistingPath = original
	})
	path := filepath.Join(t.TempDir(), "future", "authority")
	if _, err := canonicalFuturePath(path); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("canonicalFuturePath() error = %v", err)
	}
	if _, err := pathWithin(path, filepath.Dir(path)); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("pathWithin() error = %v", err)
	}
}

func TestWindowsDriveRootAndVolumeGUIDFuturePathsShareIdentity(t *testing.T) {
	driveRoot := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	drivePath, guidPath := windowsExistingPathSpellings(t, driveRoot)
	driveFuture := filepath.Join(drivePath, "future", "authority")
	guidFuture := filepath.Join(guidPath, "future", "authority")
	driveCanonical, err := canonicalFuturePath(driveFuture)
	if err != nil {
		t.Fatal(err)
	}
	guidCanonical, err := canonicalFuturePath(guidFuture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(driveCanonical, guidCanonical) {
		t.Fatalf("drive-root future canonical paths differ: %q != %q", driveCanonical, guidCanonical)
	}
}

func TestValidatePeerTailscaleAuthorityRejectsVolumeGUIDJunctionStateInManagedHome(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "cli")
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	_, codexHomeGUID := windowsExistingPathSpellings(t, codexHome)
	tailscaleStateDir := filepath.Join(root, "tailscale-state")
	createDirectoryJunction(t, tailscaleStateDir, codexHomeGUID)

	err := ValidatePeerTailscaleAuthority(
		filepath.Join(root, "peer.json"),
		filepath.Join(root, "state", "peer.sqlite3"),
		filepath.Join(root, "secrets", "peer.token"),
		codexHome,
		filepath.Join(root, "workspaces"),
		tailscaleStateDir,
		filepath.Join(root, "secrets", "tailscale-auth.key"),
	)
	if err == nil || !strings.Contains(err.Error(), "Tailscale state directory must not be inside worker CODEX_HOME") {
		t.Fatalf("ValidatePeerTailscaleAuthority() error = %v", err)
	}
}

func TestValidateBrokerTailscaleAuthorityRejectsVolumeGUIDJunctionStateContainingAuthority(t *testing.T) {
	root := t.TempDir()
	authorityRoot := filepath.Join(root, "authority")
	if err := os.Mkdir(authorityRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	_, authorityRootGUID := windowsExistingPathSpellings(t, authorityRoot)
	tailscaleStateDir := filepath.Join(root, "tailscale-state")
	createDirectoryJunction(t, tailscaleStateDir, authorityRootGUID)

	err := ValidateBrokerTailscaleAuthority(
		filepath.Join(authorityRoot, "broker.json"),
		filepath.Join(root, "state", "broker.sqlite3"),
		filepath.Join(root, "secrets", "broker.token"),
		tailscaleStateDir,
		filepath.Join(root, "secrets", "tailscale-auth.key"),
	)
	if err == nil || !strings.Contains(err.Error(), "broker configuration must not be inside Tailscale state directory") {
		t.Fatalf("ValidateBrokerTailscaleAuthority() error = %v", err)
	}
}

func TestWindowsDistinctExistingVolumesRemainDisjoint(t *testing.T) {
	currentDrivePath, currentGUIDPath := windowsExistingPathSpellings(t, t.TempDir())
	otherRoot, found := windowsOtherLocalVolumeTempDir(t, currentDrivePath, currentGUIDPath)
	if !found {
		t.Log("no second writable local Windows volume is present")
		return
	}
	defer os.RemoveAll(otherRoot)
	_, otherGUIDPath := windowsExistingPathSpellings(t, otherRoot)
	if strings.EqualFold(filepath.VolumeName(currentGUIDPath), filepath.VolumeName(otherGUIDPath)) {
		t.Fatalf("different drive roots resolved to the same volume: %q and %q", currentGUIDPath, otherGUIDPath)
	}
	contained, err := pathWithin(currentGUIDPath, otherGUIDPath)
	if err != nil {
		t.Fatal(err)
	}
	if contained {
		t.Fatalf("pathWithin treated distinct volumes as containment: %q, %q", currentGUIDPath, otherGUIDPath)
	}
	conflicts, err := equivalent(currentGUIDPath, otherGUIDPath)
	if err != nil {
		t.Fatal(err)
	}
	if conflicts {
		t.Fatalf("equivalent treated distinct volumes as identical: %q, %q", currentGUIDPath, otherGUIDPath)
	}
}

func TestResolveFuturePathRejectsJunctionAtDepthLimit(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	createDirectoryJunction(t, alias, target)

	if _, err := resolveFuturePath(alias, 255); err == nil ||
		!strings.Contains(err.Error(), "too many symbolic links") {
		t.Fatalf("resolveFuturePath() error = %v", err)
	}
}

func TestCanonicalFuturePathRejectsJunctionLoop(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	createRawDirectoryJunction(t, filepath.Join(first, "next"), second)
	createRawDirectoryJunction(t, filepath.Join(second, "next"), first)
	loopPath := first
	for range 256 {
		loopPath = filepath.Join(loopPath, "next")
	}

	if _, err := canonicalFuturePath(loopPath); err == nil ||
		!strings.Contains(err.Error(), "too many symbolic links") {
		t.Fatalf("canonicalFuturePath() error = %v", err)
	}
}

func windowsExistingPathSpellings(t *testing.T, path string) (string, string) {
	t.Helper()
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(
		pathPointer,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		t.Fatalf("open existing path for Windows spellings: %v", err)
	}
	defer windows.CloseHandle(handle)
	drivePath, err := windowsFinalPathByHandle(handle, windowsVolumeNameDOS)
	if err != nil {
		t.Fatalf("resolve drive-letter path: %v", err)
	}
	if !windowsDriveFinalPath(drivePath) {
		t.Fatalf("test path has no drive-letter final spelling: %q", drivePath)
	}
	guidPath, err := windowsFinalPathByHandle(handle, windowsVolumeNameGUID)
	if err != nil {
		t.Fatalf("resolve volume GUID path: %v", err)
	}
	if !windowsGUIDFinalPath(guidPath) {
		t.Fatalf("test path has no volume GUID final spelling: %q", guidPath)
	}
	return filepath.Clean(drivePath), filepath.Clean(guidPath)
}

func windowsOtherLocalVolumeTempDir(
	t *testing.T, currentDrivePath, currentGUIDPath string,
) (string, bool) {
	t.Helper()
	currentDrive := filepath.VolumeName(currentDrivePath)
	currentVolume := filepath.VolumeName(currentGUIDPath)
	drives, err := windows.GetLogicalDrives()
	if err != nil {
		t.Fatalf("enumerate Windows drives: %v", err)
	}
	for index := 0; index < 26; index++ {
		if drives&(1<<index) == 0 {
			continue
		}
		root := fmt.Sprintf("%c:\\", 'A'+index)
		if strings.EqualFold(filepath.VolumeName(root), currentDrive) {
			continue
		}
		rootPointer, err := windows.UTF16PtrFromString(root)
		if err != nil {
			t.Fatal(err)
		}
		switch windows.GetDriveType(rootPointer) {
		case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		default:
			continue
		}
		directory, err := os.MkdirTemp(root, "delegation-pathguard-")
		if err != nil {
			t.Fatalf("create test directory on discovered local volume %s: %v", root, err)
		}
		_, candidateGUIDPath := windowsExistingPathSpellings(t, directory)
		if strings.EqualFold(filepath.VolumeName(candidateGUIDPath), currentVolume) {
			if err := os.RemoveAll(directory); err != nil {
				t.Fatalf("remove same-volume test directory %s: %v", directory, err)
			}
			continue
		}
		return directory, true
	}
	return "", false
}

func createDirectoryJunction(t *testing.T, alias, target string) {
	t.Helper()
	createRawDirectoryJunction(t, alias, target)
	info, err := os.Lstat(alias)
	if err != nil {
		t.Fatal(err)
	}
	targetPath, isAlias, err := pathAliasTarget(alias, info)
	if err != nil {
		t.Fatalf("read ordinary directory junction: %v", err)
	}
	if !isAlias {
		t.Fatalf("ordinary directory junction mode %v was not recognized as an alias", info.Mode())
	}
	canonicalTarget, err := canonicalFuturePath(target)
	if err != nil {
		t.Fatal(err)
	}
	canonicalAliasTarget, err := canonicalFuturePath(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(canonicalAliasTarget, canonicalTarget) {
		t.Fatalf("directory junction target = %q, want %q", canonicalAliasTarget, canonicalTarget)
	}
}

func createRawDirectoryJunction(t *testing.T, alias, target string) {
	t.Helper()
	output, err := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", alias, target).CombinedOutput()
	if err != nil {
		t.Fatalf("create ordinary directory junction: %v: %s", err, output)
	}
}

func TestPathWithinTreatsDifferentVolumesAsDisjoint(t *testing.T) {
	contained, err := pathWithinCanonical(
		`\\?\Volume{11111111-2222-3333-4444-555555555555}\delegation\peer.sqlite3`,
		`\\?\Volume{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}\delegation\workspaces`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if contained {
		t.Fatal("pathWithin treated different Windows volumes as containment")
	}
}
