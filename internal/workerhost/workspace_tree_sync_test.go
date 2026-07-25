package workerhost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncWorkspaceTreePersistsManagedCheckoutShapeWithoutFollowingSymlinks(t *testing.T) {
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, ".git", "objects", "ab"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"tracked.txt":                                    "tracked\n",
		filepath.Join(".git", "HEAD"):                    "ref: refs/heads/main\n",
		filepath.Join(".git", "objects", "ab", "object"): "object\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(directory, "external-link")); err != nil {
		t.Logf("symlink coverage unavailable: %v", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := syncWorkspaceTree(root); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "outside\n" {
		t.Fatalf("sync changed external symlink target = %q, %v", data, err)
	}
}

func TestPublishPreparedWorkspaceDirectoryRejectsPendingSymlink(t *testing.T) {
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "target"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(directory, "pending")); err != nil {
		t.Skipf("symlink coverage unavailable: %v", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	published, err := publishPreparedWorkspaceDirectory(root, "pending", "final")
	if err == nil || published {
		t.Fatalf("publishPreparedWorkspaceDirectory() = %v, %v; want rejected pending symlink", published, err)
	}
	if _, err := root.Lstat("final"); !os.IsNotExist(err) {
		t.Fatalf("rejected pending symlink created final path: %v", err)
	}
}
