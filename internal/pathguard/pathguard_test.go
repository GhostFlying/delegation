package pathguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateBrokerAuthorityAcceptsDistinctPaths(t *testing.T) {
	root := t.TempDir()
	err := ValidateBrokerAuthority(
		filepath.Join(root, "config.json"),
		filepath.Join(root, "state", "broker.sqlite3"),
		filepath.Join(root, "secrets", "broker.token"),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestValidatePeerAuthorityRejectsAliases(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	tokenPath := filepath.Join(root, "device.token")
	statePath := filepath.Join(root, "peer.sqlite3")
	if err := ValidatePeerAuthority(configPath, statePath, tokenPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(configPath, tokenPath); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	if err := ValidatePeerAuthority(configPath, statePath, tokenPath); err == nil ||
		!strings.Contains(err.Error(), "peer token") {
		t.Fatalf("ValidatePeerAuthority() error = %v", err)
	}
}

func TestValidatePeerAuthorityRejectsStateSidecarAliases(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "peer.sqlite3")
	if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(statePath, statePath+"-wal"); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	err := ValidatePeerAuthority(filepath.Join(root, "peer.json"), statePath, "")
	if err == nil || !strings.Contains(err.Error(), "peer WAL path conflicts with peer state") {
		t.Fatalf("ValidatePeerAuthority() error = %v", err)
	}
}

func TestValidateBrokerAuthorityRejectsAliases(t *testing.T) {
	t.Run("case folded master token", func(t *testing.T) {
		root := t.TempDir()
		err := ValidateBrokerAuthority(
			filepath.Join(root, "Config"),
			filepath.Join(root, "broker.sqlite3"),
			filepath.Join(root, "config"),
		)
		if err == nil || !strings.Contains(err.Error(), "master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked state sidecar", func(t *testing.T) {
		root := t.TempDir()
		masterPath := filepath.Join(root, "master.token")
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(masterPath, []byte("authority"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(masterPath, statePath+"-wal"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(filepath.Join(root, "config.json"), statePath, masterPath)
		if err == nil || !strings.Contains(err.Error(), "broker master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked main database and WAL", func(t *testing.T) {
		root := t.TempDir()
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(statePath, []byte("state"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(statePath, statePath+"-wal"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(root, "config.json"), statePath, filepath.Join(root, "master.token"),
		)
		if err == nil || !strings.Contains(err.Error(), "broker WAL path conflicts with broker state") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("hard linked master token and instance lease", func(t *testing.T) {
		root := t.TempDir()
		masterPath := filepath.Join(root, "master.token")
		statePath := filepath.Join(root, "broker.sqlite3")
		if err := os.WriteFile(masterPath, []byte("authority"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(masterPath, statePath+".broker.lock"); err != nil {
			t.Skipf("creating a hard link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(filepath.Join(root, "config.json"), statePath, masterPath)
		if err == nil || !strings.Contains(err.Error(), "broker instance lease path conflicts with broker master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("symlinked state sidecars", func(t *testing.T) {
		root := t.TempDir()
		statePath := filepath.Join(root, "broker.sqlite3")
		journalPath := statePath + "-journal"
		if err := os.WriteFile(journalPath, []byte("journal"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Base(journalPath), statePath+"-shm"); err != nil {
			t.Skipf("creating a symbolic link is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(root, "config.json"), statePath, filepath.Join(root, "master.token"),
		)
		if err == nil || !strings.Contains(err.Error(), "broker shared memory path conflicts with broker rollback journal") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})

	t.Run("dangling parent symlink", func(t *testing.T) {
		target := t.TempDir()
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(target, alias); err != nil {
			t.Skipf("creating a directory symlink is unavailable: %v", err)
		}
		err := ValidateBrokerAuthority(
			filepath.Join(alias, "authority"),
			filepath.Join(t.TempDir(), "broker.sqlite3"),
			filepath.Join(target, "authority"),
		)
		if err == nil || !strings.Contains(err.Error(), "master token") {
			t.Fatalf("ValidateBrokerAuthority() error = %v", err)
		}
	})
}

func TestValidateCredentialOutputRejectsAuthorityFiles(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	statePath := filepath.Join(root, "broker.sqlite3")
	masterPath := filepath.Join(root, "master.token")
	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "configuration", path: configPath, want: "broker configuration"},
		{name: "master token", path: masterPath, want: "broker master token"},
		{name: "state", path: statePath, want: "broker state"},
		{name: "rollback journal", path: statePath + "-journal", want: "broker rollback journal"},
		{name: "WAL", path: statePath + "-wal", want: "broker WAL"},
		{name: "shared memory", path: statePath + "-shm", want: "broker shared memory"},
		{name: "instance lease", path: statePath + ".broker.lock", want: "broker instance lease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCredentialOutput(test.path, configPath, statePath, masterPath)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateCredentialOutput() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateCredentialOutputRejectsHardLinkedBrokerLease(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.json")
	statePath := filepath.Join(root, "broker.sqlite3")
	masterPath := filepath.Join(root, "master.token")
	leasePath := statePath + ".broker.lock"
	outputPath := filepath.Join(root, "device.token")
	if err := os.WriteFile(leasePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(leasePath, outputPath); err != nil {
		t.Skipf("creating a hard link is unavailable: %v", err)
	}
	err := ValidateCredentialOutput(outputPath, configPath, statePath, masterPath)
	if err == nil || !strings.Contains(err.Error(), "broker instance lease") {
		t.Fatalf("ValidateCredentialOutput() error = %v", err)
	}
}
