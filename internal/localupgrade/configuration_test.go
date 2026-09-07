package localupgrade

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareSwitchResumeAndRollbackConfiguration(t *testing.T) {
	directory := privateUpgradeTestDirectory(t)
	canonical := filepath.Join(directory, "peer.json")
	sourceMaterial := filepath.Join(directory, "transaction-source.json")
	targetMaterial := filepath.Join(directory, "transaction-target.json")
	source := []byte("source configuration\n")
	target := []byte("target configuration\n")
	for path, data := range map[string][]byte{
		canonical: source, sourceMaterial: source, targetMaterial: target,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	configuration := Configuration{
		CanonicalPath: canonical, SourcePath: sourceMaterial, TargetPath: targetMaterial,
		ShadowPath:   filepath.Join(directory, ".peer.json.shadow"),
		RollbackPath: filepath.Join(directory, ".peer.json.rollback"),
		SourceDigest: digestBytes(source), TargetDigest: digestBytes(target),
	}
	prepared, err := PrepareConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string][]byte{
		prepared.ShadowPath: target, prepared.RollbackPath: source,
	} {
		if got, err := os.ReadFile(path); err != nil || string(got) != string(want) {
			t.Fatalf("material %s = %q, %v", path, got, err)
		}
	}
	if switched, err := ReconcileConfigurationSwitch(prepared); err != nil || !switched {
		t.Fatalf("switch = %v, %v", switched, err)
	}
	if switched, err := ReconcileConfigurationSwitch(prepared); err != nil || switched {
		t.Fatalf("idempotent switch = %v, %v", switched, err)
	}
	if _, err := PrepareConfiguration(prepared); err != nil {
		t.Fatalf("resume after switch = %v", err)
	}
	if restored, err := ReconcileConfigurationRollback(prepared); err != nil || !restored {
		t.Fatalf("rollback = %v, %v", restored, err)
	}
	if restored, err := ReconcileConfigurationRollback(prepared); err != nil || restored {
		t.Fatalf("idempotent rollback = %v, %v", restored, err)
	}
	if got, err := os.ReadFile(canonical); err != nil || string(got) != string(source) {
		t.Fatalf("restored configuration = %q, %v", got, err)
	}
}

func TestConfigurationPreparationRejectsChangedMaterial(t *testing.T) {
	directory := privateUpgradeTestDirectory(t)
	source := []byte("source configuration\n")
	target := []byte("target configuration\n")
	configuration := Configuration{
		CanonicalPath: filepath.Join(directory, "peer.json"),
		SourcePath:    filepath.Join(directory, "source.json"),
		TargetPath:    filepath.Join(directory, "target.json"),
		ShadowPath:    filepath.Join(directory, ".peer.json.shadow"),
		RollbackPath:  filepath.Join(directory, ".peer.json.rollback"),
		SourceDigest:  digestBytes(source), TargetDigest: digestBytes(target),
	}
	for path, data := range map[string][]byte{
		configuration.CanonicalPath: source, configuration.SourcePath: source,
		configuration.TargetPath: []byte("changed target\n"),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := PrepareConfiguration(configuration); err == nil {
		t.Fatal("PrepareConfiguration accepted changed target material")
	}
	if got, err := os.ReadFile(configuration.CanonicalPath); err != nil || string(got) != string(source) {
		t.Fatalf("failed preparation changed canonical config: %q, %v", got, err)
	}
}

func TestPrepareConfigurationSupportsCurrentSchemaNoOp(t *testing.T) {
	directory := privateUpgradeTestDirectory(t)
	data := []byte("current configuration\n")
	configuration := Configuration{
		CanonicalPath: filepath.Join(directory, "peer.json"),
		SourcePath:    filepath.Join(directory, "source.json"),
		TargetPath:    filepath.Join(directory, "target.json"),
		ShadowPath:    filepath.Join(directory, ".peer.json.shadow"),
		RollbackPath:  filepath.Join(directory, ".peer.json.rollback"),
		SourceDigest:  digestBytes(data), TargetDigest: digestBytes(data),
	}
	for _, path := range []string{
		configuration.CanonicalPath, configuration.SourcePath, configuration.TargetPath,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := PrepareConfiguration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if switched, err := ReconcileConfigurationSwitch(prepared); err != nil || switched {
		t.Fatalf("no-op switch = %v, %v", switched, err)
	}
	for _, path := range []string{configuration.ShadowPath, configuration.RollbackPath} {
		if got, err := os.ReadFile(path); err != nil || string(got) != string(data) {
			t.Fatalf("no-op material %s = %q, %v", path, got, err)
		}
	}
}
