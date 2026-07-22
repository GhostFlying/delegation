//go:build linux || darwin

package userservice

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestManagedFileLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "delegation.service")
	descriptor := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\nservice\n")}

	state, err := installManagedFile(path, descriptor)
	if err != nil || state != StatePrepared {
		t.Fatalf("installManagedFile() = %q, %v", state, err)
	}
	state, data, err := inspectManagedFile(path, descriptor.Kind)
	if err != nil || state != StatePrepared || string(data) != string(descriptor.Content) {
		t.Fatalf("inspectManagedFile() = %q, %q, %v", state, data, err)
	}
	if state, err := installManagedFile(path, descriptor); err != nil || state != StatePrepared {
		t.Fatalf("idempotent install = %q, %v", state, err)
	}
}

func TestManagedFileRefusesReplacementAndForeignCollision(t *testing.T) {
	dir := t.TempDir()
	managedPath := filepath.Join(dir, "managed")
	first := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\nfirst\n")}
	if _, err := installManagedFile(managedPath, first); err != nil {
		t.Fatal(err)
	}
	second := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\nsecond\n")}
	if _, err := installManagedFile(managedPath, second); err == nil {
		t.Fatal("installManagedFile() replaced a changed managed definition")
	}
	data, err := os.ReadFile(managedPath)
	if err != nil || string(data) != string(first.Content) {
		t.Fatalf("managed definition changed: %q, %v", data, err)
	}

	foreignPath := filepath.Join(dir, "foreign")
	if err := os.WriteFile(foreignPath, []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	if state, err := installManagedFile(foreignPath, first); err == nil || state != StateForeignConflict {
		t.Fatalf("foreign install = %q, %v", state, err)
	}
}

func TestManagedFileRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	path := filepath.Join(dir, "service")
	if err := os.WriteFile(target, []byte("# "+MarkerPeer+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	descriptor := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\n")}
	if state, err := installManagedFile(path, descriptor); err == nil || state != StateForeignConflict {
		t.Fatalf("symlink install = %q, %v", state, err)
	}
}

func TestConcurrentManagedFileInstallIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service")
	descriptor := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\n")}
	results := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for range 2 {
		go func() {
			start.Wait()
			_, err := installManagedFile(path, descriptor)
			results <- err
		}()
	}
	start.Done()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedFileReportsCommittedSyncFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service")
	originalSync := syncManagedDirectory
	t.Cleanup(func() { syncManagedDirectory = originalSync })
	syncManagedDirectory = func(syncPath string) error {
		if syncPath == dir {
			return errors.New("injected sync failure")
		}
		return nil
	}

	descriptor := Descriptor{Kind: KindSystemd, Content: []byte("# " + MarkerPeer + "\n")}
	state, err := installManagedFile(path, descriptor)
	if state != StatePrepared || !IsCommitted(err) {
		t.Fatalf("installManagedFile() = %q, %v; want committed error", state, err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("committed service definition missing: %v", statErr)
	}
}

func TestManagedFileRequiresStructuralOwnership(t *testing.T) {
	dir := t.TempDir()
	for name, foreignSystemd := range map[string][]byte{
		"embedded marker": []byte("# foreign unit mentioning " + MarkerPeer + "\n"),
		"marker suffix":   []byte("# " + MarkerPeer + "-foreign\n"),
	} {
		t.Run(name, func(t *testing.T) {
			systemdPath := filepath.Join(dir, strings.ReplaceAll(name, " ", "-"))
			if err := os.WriteFile(systemdPath, foreignSystemd, 0o600); err != nil {
				t.Fatal(err)
			}
			if state, _, err := inspectManagedFile(systemdPath, KindSystemd); err != nil || state != StateForeignConflict {
				t.Fatalf("foreign systemd ownership = %q, %v", state, err)
			}
		})
	}

	launchPath := filepath.Join(dir, "launch.plist")
	foreignLaunch := []byte(`<?xml version="1.0"?><plist><dict><key>Label</key><string>foreign</string><key>Description</key><string>` + MarkerPeer + `</string></dict></plist>`)
	if err := os.WriteFile(launchPath, foreignLaunch, 0o600); err != nil {
		t.Fatal(err)
	}
	if state, _, err := inspectManagedFile(launchPath, KindLaunchAgent); err != nil || state != StateForeignConflict {
		t.Fatalf("foreign LaunchAgent ownership = %q, %v", state, err)
	}

	owned, err := RenderLaunchAgent(ServiceRolePeer, testInvocation(ServiceRolePeer, "/opt/delegation", "/home/test/config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(launchPath, owned.Content, 0o600); err != nil {
		t.Fatal(err)
	}
	if state, _, err := inspectManagedFile(launchPath, KindLaunchAgent); err != nil || state != StatePrepared {
		t.Fatalf("managed LaunchAgent ownership = %q, %v", state, err)
	}
}

func TestManagedFileRejectsInvalidDesiredDescriptorWithoutSideEffects(t *testing.T) {
	dir := t.TempDir()
	tests := []Descriptor{
		{Kind: KindSystemd, Content: []byte("markerless")},
		{Kind: KindScheduledTask, Content: []byte("# " + MarkerPeer + "\n")},
		{Kind: KindSystemd, Content: make([]byte, maxServiceDescriptorSize+1)},
	}
	for index, descriptor := range tests {
		path := filepath.Join(dir, fmt.Sprintf("service-%d", index))
		if state, err := installManagedFile(path, descriptor); err == nil || state != StateAbsent {
			t.Fatalf("invalid descriptor %d install = %q, %v", index, state, err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid descriptor %d created destination: %v", index, err)
		}
	}
}

func TestLaunchAgentOwnershipRejectsAmbiguousStructures(t *testing.T) {
	tests := map[string]string{
		"sibling dictionaries": "<plist><dict><key>Label</key><string>" + LaunchAgentPeerName + "</string></dict><dict><key>Description</key><string>" + MarkerPeer + "</string></dict></plist>",
		"wrapped dictionary":   "<wrapper><plist><dict><key>Label</key><string>" + LaunchAgentPeerName + "</string><key>Description</key><string>" + MarkerPeer + "</string></dict></plist></wrapper>",
		"namespaced root":      "<plist xmlns=\"urn:foreign\"><dict><key>Label</key><string>" + LaunchAgentPeerName + "</string><key>Description</key><string>" + MarkerPeer + "</string></dict></plist>",
		"mixed duplicate":      "<plist><dict><key>Label</key><string>" + LaunchAgentPeerName + "</string><key>Label</key><array/><key>Description</key><string>" + MarkerPeer + "</string></dict></plist>",
		"nested key":           "<plist><dict><key>La<x/>bel</key><string>" + LaunchAgentPeerName + "</string><key>Description</key><string>" + MarkerPeer + "</string></dict></plist>",
		"nested string":        "<plist><dict><key>Label</key><string>" + LaunchAgentPeerName + "<x/></string><key>Description</key><string>" + MarkerPeer + "</string></dict></plist>",
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if ownsLaunchAgent([]byte(document)) {
				t.Fatalf("ownsLaunchAgent() accepted %s", document)
			}
		})
	}
}
