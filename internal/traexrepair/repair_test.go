package traexrepair

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
)

func TestRunQuarantinesEntriesAndCommitsValidatedReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	root, configPath, managedHome := repairFixture(t)
	original := []byte("original config\n")
	replacement := []byte("replacement config\n")
	writeFile(t, configPath, original, 0o600)
	writeFile(t, filepath.Join(managedHome, "AGENTS.md"), []byte("instructions"), 0o600)
	writeFile(t, filepath.Join(managedHome, "skills", "custom", "SKILL.md"), []byte("skill"), 0o600)
	writeFile(t, filepath.Join(managedHome, "skills", ".system", "kept"), []byte("system"), 0o600)
	writeFile(t, filepath.Join(managedHome, "cli", "rules", "rule.md"), []byte("rule"), 0o600)
	var doctorCalls, smokeCalls int
	result, err := Run(context.Background(), Options{
		ConfigPath: configPath, ManagedHome: managedHome,
		OriginalConfig: original, ReplacementConfig: replacement,
		Doctor: func(context.Context) error {
			doctorCalls++
			if err := codexconfig.ValidateManagedRuntimeHome(hostkind.TraeX, managedHome); err != nil {
				return err
			}
			return nil
		},
		Smoke: func(_ context.Context, got Result) error {
			smokeCalls++
			if got.QuarantinePath == "" {
				t.Fatal("smoke did not receive the quarantine path")
			}
			return nil
		},
		Now: func() time.Time { return time.Unix(1700000000, 123) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if doctorCalls != 1 || smokeCalls != 1 {
		t.Fatalf("hooks = doctor %d, smoke %d", doctorCalls, smokeCalls)
	}
	if got, err := os.ReadFile(configPath); err != nil || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("repaired config = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(managedHome, "skills", ".system", "kept")); err != nil {
		t.Fatalf("system skill was not preserved: %v", err)
	}
	wantPaths := []string{"AGENTS.md", "cli/rules", "skills/custom"}
	gotPaths := make([]string, len(result.Entries))
	for index, entry := range result.Entries {
		gotPaths[index] = entry.Path
		if entry.Type == "" || entry.Mode == 0 || len(entry.SHA256) != 64 {
			t.Fatalf("incomplete manifest entry: %#v", entry)
		}
		if _, err := os.Lstat(filepath.Join(managedHome, filepath.FromSlash(entry.Path))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source entry %s remains: %v", entry.Path, err)
		}
	}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("quarantined paths = %#v, want %#v", gotPaths, wantPaths)
	}
	manifestData, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 || manifest.ManagedHome != managedHome ||
		manifest.Config.Path != configPath || len(manifest.Config.SHA256) != 64 ||
		manifest.ConfigBackup != configBackupName || !reflect.DeepEqual(manifest.Entries, result.Entries) {
		t.Fatalf("manifest = %#v", manifest)
	}
	if got, err := os.ReadFile(filepath.Join(result.QuarantinePath, configBackupName)); err != nil ||
		!reflect.DeepEqual(got, original) {
		t.Fatalf("config backup = %q, %v", got, err)
	}
	_ = root
}

func TestRunRollsBackConfigAndEntriesWhenSmokeFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	_, configPath, managedHome := repairFixture(t)
	original := []byte("original config\n")
	replacement := []byte("replacement config\n")
	writeFile(t, configPath, original, 0o600)
	artifact := filepath.Join(managedHome, "AGENTS.md")
	writeFile(t, artifact, []byte("instructions"), 0o600)
	smokeErr := errors.New("smoke failed")
	_, err := Run(context.Background(), Options{
		ConfigPath: configPath, ManagedHome: managedHome,
		OriginalConfig: original, ReplacementConfig: replacement,
		Doctor: func(context.Context) error { return nil },
		Smoke:  func(context.Context, Result) error { return smokeErr },
	})
	if !errors.Is(err, smokeErr) || errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("Run() error = %v", err)
	}
	if got, readErr := os.ReadFile(configPath); readErr != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("rolled back config = %q, %v", got, readErr)
	}
	if got, readErr := os.ReadFile(artifact); readErr != nil || string(got) != "instructions" {
		t.Fatalf("rolled back artifact = %q, %v", got, readErr)
	}
}

func TestRunPreservesQuarantineAndReportsRollbackConflict(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	_, configPath, managedHome := repairFixture(t)
	original := []byte("original config\n")
	replacement := []byte("replacement config\n")
	writeFile(t, configPath, original, 0o600)
	artifact := filepath.Join(managedHome, "AGENTS.md")
	writeFile(t, artifact, []byte("instructions"), 0o600)
	result, err := Run(context.Background(), Options{
		ConfigPath: configPath, ManagedHome: managedHome,
		OriginalConfig: original, ReplacementConfig: replacement,
		Doctor: func(context.Context) error { return nil },
		Smoke: func(context.Context, Result) error {
			if err := os.WriteFile(artifact, []byte("conflict"), 0o600); err != nil {
				return err
			}
			return errors.New("smoke failed")
		},
	})
	if !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("Run() error = %v, want rollback_failed", err)
	}
	if _, statErr := os.Stat(result.ManifestPath); statErr != nil {
		t.Fatalf("rollback conflict lost recovery manifest: %v", statErr)
	}
	if got, readErr := os.ReadFile(configPath); readErr != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("config rollback = %q, %v", got, readErr)
	}
	if got, readErr := os.ReadFile(filepath.Join(result.QuarantinePath, payloadName, result.Entries[0].Payload)); readErr != nil || string(got) != "instructions" {
		t.Fatalf("quarantined recovery artifact = %q, %v", got, readErr)
	}
	var persisted journal
	journalData, readErr := os.ReadFile(result.JournalPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err := json.Unmarshal(journalData, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Phase != phaseRollbackFailed || persisted.Failure == "" {
		t.Fatalf("persisted rollback state = %#v", persisted)
	}
}

func TestRunRecoversEveryDurableForwardCrashPoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	for _, point := range []FaultPoint{
		FaultAfterPrepared, FaultAfterEntryRename, FaultAfterEntryMove, FaultAfterConfigReplace,
		FaultAfterDoctor, FaultAfterSmoke,
	} {
		t.Run(string(point), func(t *testing.T) {
			_, configPath, managedHome := repairFixture(t)
			original := []byte("original config\n")
			replacement := []byte("replacement config\n")
			writeFile(t, configPath, original, 0o600)
			artifact := filepath.Join(managedHome, "AGENTS.md")
			writeFile(t, artifact, []byte("instructions"), 0o600)
			result, err := Run(context.Background(), Options{
				ConfigPath: configPath, ManagedHome: managedHome,
				OriginalConfig: original, ReplacementConfig: replacement,
				Doctor: func(context.Context) error { return nil },
				Smoke:  func(context.Context, Result) error { return nil },
				Fault: func(got FaultPoint) error {
					if got == point {
						return errInjectedCrash
					}
					return nil
				},
			})
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("Run() error = %v, want injected crash", err)
			}
			if _, statErr := os.Stat(result.JournalPath); statErr != nil {
				t.Fatalf("durable journal missing: %v", statErr)
			}
			currentConfig, readErr := os.ReadFile(configPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			second, err := Run(context.Background(), Options{
				ConfigPath: configPath, ManagedHome: managedHome,
				OriginalConfig: currentConfig, ReplacementConfig: replacement,
				Doctor: func(context.Context) error { return nil },
				Smoke:  func(context.Context, Result) error { return nil },
			})
			if err != nil {
				t.Fatalf("recover and rerun: %v", err)
			}
			if second.QuarantinePath == result.QuarantinePath {
				t.Fatal("recovery reused the interrupted transaction")
			}
			if got, readErr := os.ReadFile(configPath); readErr != nil || !reflect.DeepEqual(got, replacement) {
				t.Fatalf("repaired config = %q, %v", got, readErr)
			}
			if _, statErr := os.Lstat(artifact); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("artifact remains after recovered repair: %v", statErr)
			}
			assertJournalPhase(t, result.JournalPath, phaseRolledBack)
			assertJournalPhase(t, second.JournalPath, phaseCommitted)
		})
	}
}

func TestRunRecoversInterruptedRollback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	for _, point := range []FaultPoint{FaultAfterRollbackConfig, FaultAfterRollbackEntry} {
		t.Run(string(point), func(t *testing.T) {
			_, configPath, managedHome := repairFixture(t)
			original := []byte("original config\n")
			replacement := []byte("replacement config\n")
			writeFile(t, configPath, original, 0o600)
			artifact := filepath.Join(managedHome, "AGENTS.md")
			writeFile(t, artifact, []byte("instructions"), 0o600)
			result, err := Run(context.Background(), Options{
				ConfigPath: configPath, ManagedHome: managedHome,
				OriginalConfig: original, ReplacementConfig: replacement,
				Doctor: func(context.Context) error { return errors.New("doctor failed") },
				Smoke:  func(context.Context, Result) error { return nil },
				Fault: func(got FaultPoint) error {
					if got == point {
						return errInjectedCrash
					}
					return nil
				},
			})
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("Run() error = %v, want injected rollback crash", err)
			}
			currentConfig, readErr := os.ReadFile(configPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			_, err = Run(context.Background(), Options{
				ConfigPath: configPath, ManagedHome: managedHome,
				OriginalConfig: currentConfig, ReplacementConfig: replacement,
				Doctor: func(context.Context) error { return nil },
				Smoke:  func(context.Context, Result) error { return nil },
			})
			if err != nil {
				t.Fatalf("recover interrupted rollback: %v", err)
			}
			assertJournalPhase(t, result.JournalPath, phaseRolledBack)
		})
	}
}

func TestRunRefusesPersistedRollbackFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TraeX repair is unsupported on Windows")
	}
	_, configPath, managedHome := repairFixture(t)
	original := []byte("original config\n")
	replacement := []byte("replacement config\n")
	writeFile(t, configPath, original, 0o600)
	artifact := filepath.Join(managedHome, "AGENTS.md")
	writeFile(t, artifact, []byte("instructions"), 0o600)
	result, err := Run(context.Background(), Options{
		ConfigPath: configPath, ManagedHome: managedHome,
		OriginalConfig: original, ReplacementConfig: replacement,
		Doctor: func(context.Context) error { return nil },
		Smoke: func(context.Context, Result) error {
			if err := os.WriteFile(artifact, []byte("conflict"), 0o600); err != nil {
				return err
			}
			return errors.New("smoke failed")
		},
	})
	if !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("first repair error = %v", err)
	}
	_, err = Run(context.Background(), Options{
		ConfigPath: configPath, ManagedHome: managedHome,
		OriginalConfig: original, ReplacementConfig: replacement,
		Doctor: func(context.Context) error { return nil },
		Smoke:  func(context.Context, Result) error { return nil },
	})
	if !errors.Is(err, ErrRollbackFailed) || !strings.Contains(err.Error(), result.JournalPath) {
		t.Fatalf("second repair error = %v", err)
	}
}

func assertJournalPhase(t *testing.T, path string, want phase) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got journal
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Phase != want {
		t.Fatalf("journal phase = %q, want %q", got.Phase, want)
	}
}

func repairFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "private")
	if err := delegationconfig.PreparePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	managedHome := filepath.Join(root, "managed-trae")
	if err := delegationconfig.PreparePrivateDirectory(managedHome); err != nil {
		t.Fatal(err)
	}
	return root, filepath.Join(root, "peer.json"), managedHome
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}
