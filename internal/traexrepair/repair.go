package traexrepair

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/GhostFlying/delegation/internal/codexconfig"
	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
)

const (
	manifestVersion  = 1
	journalVersion   = 1
	manifestName     = "manifest.json"
	journalName      = "journal.json"
	configBackupName = "original-config.json"
	payloadName      = "payload"
	quarantineName   = ".delegation-quarantine"
)

var (
	ErrRollbackFailed = errors.New("rollback_failed")
	errInjectedCrash  = errors.New("injected repair crash")
)

type phase string

const (
	phasePrepared        phase = "prepared"
	phaseQuarantining    phase = "quarantining"
	phaseQuarantined     phase = "quarantined"
	phaseConfigReplacing phase = "config_replacing"
	phaseConfigReplaced  phase = "config_replaced"
	phaseValidating      phase = "validating"
	phaseCommitted       phase = "committed"
	phaseRollingBack     phase = "rolling_back"
	phaseRolledBack      phase = "rolled_back"
	phaseRollbackFailed  phase = "rollback_failed"
)

type FaultPoint string

const (
	FaultAfterPrepared       FaultPoint = "after_prepared"
	FaultAfterEntryMove      FaultPoint = "after_entry_move"
	FaultAfterEntryRename    FaultPoint = "after_entry_rename"
	FaultAfterConfigReplace  FaultPoint = "after_config_replace"
	FaultAfterDoctor         FaultPoint = "after_doctor"
	FaultAfterSmoke          FaultPoint = "after_smoke"
	FaultAfterRollbackConfig FaultPoint = "after_rollback_config"
	FaultAfterRollbackEntry  FaultPoint = "after_rollback_entry"
)

type Entry struct {
	Path    string `json:"path"`
	Payload string `json:"payload"`
	Type    string `json:"type"`
	Mode    uint32 `json:"mode"`
	SHA256  string `json:"sha256"`
}

type Manifest struct {
	Version      int     `json:"version"`
	CreatedAt    string  `json:"createdAt"`
	Config       Entry   `json:"config"`
	ConfigBackup string  `json:"configBackup"`
	ManagedHome  string  `json:"managedHome"`
	Entries      []Entry `json:"entries"`
}

type journal struct {
	Version           int      `json:"version"`
	Phase             phase    `json:"phase"`
	UpdatedAt         string   `json:"updatedAt"`
	ConfigPath        string   `json:"configPath"`
	ManagedHome       string   `json:"managedHome"`
	OriginalSHA256    string   `json:"originalSha256"`
	ReplacementSHA256 string   `json:"replacementSha256"`
	Moving            string   `json:"moving,omitempty"`
	Moved             []string `json:"moved,omitempty"`
	Restored          []string `json:"restored,omitempty"`
	Failure           string   `json:"failure,omitempty"`
}

type Options struct {
	ConfigPath        string
	ManagedHome       string
	OriginalConfig    []byte
	ReplacementConfig []byte
	Doctor            func(context.Context) error
	Smoke             func(context.Context, Result) error
	Now               func() time.Time
	Fault             func(FaultPoint) error
}

type Result struct {
	QuarantinePath string  `json:"quarantinePath"`
	ManifestPath   string  `json:"manifestPath"`
	JournalPath    string  `json:"journalPath"`
	Entries        []Entry `json:"entries"`
}

type transaction struct {
	options        Options
	result         Result
	manifest       Manifest
	journal        journal
	managedRoot    *securefs.Root
	quarantineRoot *securefs.Root
	directory      *securefs.Root
	payload        *securefs.Root
}

func Run(ctx context.Context, options Options) (Result, error) {
	if err := checkSupportedPlatform(); err != nil {
		return Result{}, err
	}
	if options.ConfigPath == "" || options.ManagedHome == "" {
		return Result{}, errors.New("repair config path and managed home are required")
	}
	if !filepath.IsAbs(options.ConfigPath) || !filepath.IsAbs(options.ManagedHome) {
		return Result{}, errors.New("repair paths must be absolute")
	}
	if len(options.OriginalConfig) == 0 || len(options.ReplacementConfig) == 0 {
		return Result{}, errors.New("repair config snapshots are required")
	}
	if options.Doctor == nil || options.Smoke == nil {
		return Result{}, errors.New("repair doctor and smoke hooks are required")
	}
	if err := delegationconfig.ValidatePrivateDirectory(options.ManagedHome); err != nil {
		return Result{}, fmt.Errorf("validate managed TRAE_HOME: %w", err)
	}
	managedRoot, err := securefs.OpenRoot(options.ManagedHome, nil)
	if err != nil {
		return Result{}, fmt.Errorf("hold managed TRAE_HOME: %w", err)
	}
	defer managedRoot.Close()
	quarantineRoot, err := prepareQuarantineRoot(options.ManagedHome, managedRoot)
	if err != nil {
		return Result{}, err
	}
	defer quarantineRoot.Close()
	recoveredOriginal, err := recoverTransactions(options, managedRoot, quarantineRoot)
	if err != nil {
		return Result{}, err
	}
	if recoveredOriginal != nil {
		options.OriginalConfig = recoveredOriginal
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	tx, err := prepareTransaction(ctx, options, now(), managedRoot, quarantineRoot)
	if err != nil {
		return Result{}, err
	}
	defer tx.close()
	if err := tx.fault(FaultAfterPrepared); err != nil {
		return tx.handleFailure(err)
	}
	if err := tx.quarantine(ctx); err != nil {
		return tx.handleFailure(fmt.Errorf("quarantine managed TraeX home: %w", err))
	}
	if err := tx.setPhase(phaseConfigReplacing); err != nil {
		return tx.handleFailure(err)
	}
	if err := delegationconfig.ReplaceProtectedFile(
		options.ConfigPath, options.OriginalConfig, options.ReplacementConfig,
	); err != nil {
		return tx.handleFailure(fmt.Errorf("replace repaired config: %w", err))
	}
	if err := tx.setPhase(phaseConfigReplaced); err != nil {
		return tx.handleFailure(err)
	}
	if err := tx.fault(FaultAfterConfigReplace); err != nil {
		return tx.handleFailure(err)
	}
	if err := tx.setPhase(phaseValidating); err != nil {
		return tx.handleFailure(err)
	}
	if err := options.Doctor(ctx); err != nil {
		return tx.handleFailure(fmt.Errorf("doctor repaired TraeX service: %w", err))
	}
	if err := tx.fault(FaultAfterDoctor); err != nil {
		return tx.handleFailure(err)
	}
	if err := options.Smoke(ctx, tx.result); err != nil {
		return tx.handleFailure(fmt.Errorf("fresh-thread smoke for repaired TraeX service: %w", err))
	}
	if err := tx.fault(FaultAfterSmoke); err != nil {
		return tx.handleFailure(err)
	}
	if err := tx.setPhase(phaseCommitted); err != nil {
		return tx.handleFailure(fmt.Errorf("commit repair journal: %w", err))
	}
	return tx.result, nil
}

func prepareQuarantineRoot(managedHome string, managedRoot *securefs.Root) (*securefs.Root, error) {
	if err := managedRoot.Mkdir(quarantineName, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create TraeX quarantine root: %w", err)
	}
	if err := managedRoot.Sync(); err != nil {
		return nil, fmt.Errorf("sync managed TRAE_HOME: %w", err)
	}
	path := filepath.Join(managedHome, quarantineName)
	if err := delegationconfig.ValidatePrivateDirectory(path); err != nil {
		return nil, fmt.Errorf("validate TraeX quarantine root: %w", err)
	}
	root, err := managedRoot.OpenRoot(quarantineName, nil)
	if err != nil {
		return nil, fmt.Errorf("hold TraeX quarantine root: %w", err)
	}
	return root, nil
}

func prepareTransaction(
	ctx context.Context, options Options, now time.Time,
	managedRoot, quarantineRoot *securefs.Root,
) (*transaction, error) {
	relative, err := codexconfig.TraeXRepairEntries(managedRoot)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(relative))
	for index, path := range relative {
		entry, err := inspectRelative(ctx, managedRoot, path)
		if err != nil {
			return nil, err
		}
		entry.Payload = fmt.Sprintf("entry-%04d", index)
		entries = append(entries, entry)
	}
	configInfo, err := os.Lstat(options.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("inspect repair config: %w", err)
	}
	if !configInfo.Mode().IsRegular() || configInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("repair config must be a regular file, not a symbolic link")
	}
	originalDigest := sha256.Sum256(options.OriginalConfig)
	replacementDigest := sha256.Sum256(options.ReplacementConfig)
	name, err := createTransactionDirectory(quarantineRoot, now)
	if err != nil {
		return nil, err
	}
	directory, err := quarantineRoot.OpenRoot(name, nil)
	if err != nil {
		return nil, fmt.Errorf("hold TraeX repair transaction: %w", err)
	}
	fail := func(err error) (*transaction, error) {
		_ = directory.Close()
		_ = quarantineRoot.RemoveAll(name)
		_ = quarantineRoot.Sync()
		return nil, err
	}
	if err := directory.Mkdir(payloadName, 0o700); err != nil {
		return fail(fmt.Errorf("create TraeX quarantine payload: %w", err))
	}
	if err := directory.Sync(); err != nil {
		return fail(fmt.Errorf("sync TraeX repair transaction: %w", err))
	}
	payload, err := directory.OpenRoot(payloadName, nil)
	if err != nil {
		return fail(fmt.Errorf("hold TraeX quarantine payload: %w", err))
	}
	manifest := Manifest{
		Version: manifestVersion, CreatedAt: now.UTC().Format(time.RFC3339Nano),
		Config: Entry{Path: options.ConfigPath, Type: entryType(configInfo),
			Mode: uint32(configInfo.Mode()), SHA256: hex.EncodeToString(originalDigest[:])},
		ConfigBackup: configBackupName, ManagedHome: options.ManagedHome, Entries: entries,
	}
	result := Result{
		QuarantinePath: filepath.Join(options.ManagedHome, quarantineName, name),
		ManifestPath:   filepath.Join(options.ManagedHome, quarantineName, name, manifestName),
		JournalPath:    filepath.Join(options.ManagedHome, quarantineName, name, journalName),
		Entries:        entries,
	}
	tx := &transaction{
		options: options, result: result, manifest: manifest, managedRoot: managedRoot,
		quarantineRoot: quarantineRoot, directory: directory, payload: payload,
		journal: journal{
			Version: journalVersion, Phase: phasePrepared, UpdatedAt: now.UTC().Format(time.RFC3339Nano),
			ConfigPath: options.ConfigPath, ManagedHome: options.ManagedHome,
			OriginalSHA256:    hex.EncodeToString(originalDigest[:]),
			ReplacementSHA256: hex.EncodeToString(replacementDigest[:]),
		},
	}
	if err := writeNewFile(directory, configBackupName, options.OriginalConfig); err != nil {
		tx.close()
		return fail(fmt.Errorf("write TraeX repair config backup: %w", err))
	}
	if err := writeNewJSON(directory, manifestName, manifest); err != nil {
		tx.close()
		return fail(fmt.Errorf("write TraeX repair manifest: %w", err))
	}
	if err := writeNewJSON(directory, journalName, tx.journal); err != nil {
		tx.close()
		return fail(fmt.Errorf("write TraeX repair journal: %w", err))
	}
	return tx, nil
}

func (t *transaction) close() {
	if t.payload != nil {
		_ = t.payload.Close()
	}
	if t.directory != nil {
		_ = t.directory.Close()
	}
}

func (t *transaction) quarantine(ctx context.Context) error {
	if err := t.setPhase(phaseQuarantining); err != nil {
		return err
	}
	for _, entry := range t.manifest.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		t.journal.Moving = entry.Payload
		if err := t.writeJournal(); err != nil {
			return err
		}
		if err := t.moveEntryToQuarantine(ctx, entry); err != nil {
			return err
		}
		t.journal.Moved = append(t.journal.Moved, entry.Payload)
		t.journal.Moving = ""
		if err := t.writeJournal(); err != nil {
			return err
		}
		if err := t.fault(FaultAfterEntryMove); err != nil {
			return err
		}
	}
	return t.setPhase(phaseQuarantined)
}

func (t *transaction) moveEntryToQuarantine(ctx context.Context, entry Entry) error {
	parent, leaf, closeRoots, err := openRelativeParent(t.managedRoot, entry.Path)
	if err != nil {
		return err
	}
	defer closeRoots()
	source, sourceErr := inspectLeaf(ctx, parent, leaf, entry.Path)
	quarantined, quarantineErr := inspectLeaf(ctx, t.payload, entry.Payload, entry.Path)
	sourceExists := sourceErr == nil
	quarantineExists := quarantineErr == nil
	if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
		return sourceErr
	}
	if quarantineErr != nil && !errors.Is(quarantineErr, os.ErrNotExist) {
		return quarantineErr
	}
	switch {
	case sourceExists && !sameEntry(source, entry):
		return fmt.Errorf("TraeX repair entry %s changed before quarantine", entry.Path)
	case quarantineExists && !sameEntry(quarantined, entry):
		return fmt.Errorf("quarantined entry %s changed during move", entry.Path)
	case sourceExists && quarantineExists:
		return fmt.Errorf("repair entry %s exists in both target and quarantine", entry.Path)
	case quarantineExists:
		return nil
	case !sourceExists:
		return fmt.Errorf("repair entry %s is missing from both target and quarantine", entry.Path)
	}
	if err := parent.MoveNoReplace(leaf, t.payload, entry.Payload); err != nil {
		return err
	}
	if err := errors.Join(parent.Sync(), t.payload.Sync()); err != nil {
		return err
	}
	if err := t.fault(FaultAfterEntryRename); err != nil {
		return err
	}
	moved, err := inspectLeaf(ctx, t.payload, entry.Payload, entry.Path)
	if err != nil || !sameEntry(moved, entry) {
		return fmt.Errorf("quarantined entry %s changed during move: %w", entry.Path, err)
	}
	return nil
}

func (t *transaction) handleFailure(cause error) (Result, error) {
	if errors.Is(cause, errInjectedCrash) {
		return t.result, cause
	}
	rollbackErr := t.rollback(context.Background())
	if errors.Is(rollbackErr, errInjectedCrash) {
		return t.result, rollbackErr
	}
	if rollbackErr != nil {
		t.journal.Phase = phaseRollbackFailed
		t.journal.Failure = boundedFailure(errors.Join(cause, rollbackErr))
		journalErr := t.writeJournal()
		return t.result, fmt.Errorf(
			"%w: repair failed: %v; rollback failed: %v",
			ErrRollbackFailed, cause, errors.Join(rollbackErr, journalErr),
		)
	}
	return Result{}, cause
}

func (t *transaction) rollback(ctx context.Context) error {
	t.journal.Phase = phaseRollingBack
	if err := t.writeJournal(); err != nil {
		return err
	}
	if err := t.reconcileMoving(ctx); err != nil {
		return err
	}
	if err := restoreConfig(t.options.ConfigPath, t.journal, t.options.OriginalConfig); err != nil {
		return err
	}
	if err := t.fault(FaultAfterRollbackConfig); err != nil {
		return err
	}
	for index := len(t.journal.Moved) - 1; index >= 0; index-- {
		entry, ok := entryByPayload(t.manifest.Entries, t.journal.Moved[index])
		if !ok {
			return fmt.Errorf("repair journal references unknown moved payload %q", t.journal.Moved[index])
		}
		if err := restoreEntry(ctx, t.managedRoot, t.payload, entry); err != nil {
			return err
		}
		if !slices.Contains(t.journal.Restored, entry.Payload) {
			t.journal.Restored = append(t.journal.Restored, entry.Payload)
		}
		if err := t.writeJournal(); err != nil {
			return err
		}
		if err := t.fault(FaultAfterRollbackEntry); err != nil {
			return err
		}
	}
	return t.setPhase(phaseRolledBack)
}

func recoverTransactions(
	options Options, managedRoot, quarantineRoot *securefs.Root,
) ([]byte, error) {
	entries, err := quarantineRoot.Entries()
	if err != nil {
		return nil, fmt.Errorf("list TraeX repair transactions: %w", err)
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	var recoveredOriginal []byte
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("invalid TraeX repair transaction entry %s", entry.Name())
		}
		directory, err := quarantineRoot.OpenRoot(entry.Name(), nil)
		if err != nil {
			return nil, err
		}
		tx, loadErr := loadTransaction(options, managedRoot, quarantineRoot, directory, entry.Name())
		if loadErr != nil {
			directory.Close()
			return nil, loadErr
		}
		switch tx.journal.Phase {
		case phaseCommitted, phaseRolledBack:
			tx.close()
			continue
		case phaseRollbackFailed:
			path := tx.result.JournalPath
			tx.close()
			return nil, fmt.Errorf("%w: unresolved repair journal %s", ErrRollbackFailed, path)
		case phasePrepared, phaseQuarantining, phaseQuarantined, phaseConfigReplacing,
			phaseConfigReplaced, phaseValidating, phaseRollingBack:
			if err := tx.reconcileMoving(context.Background()); err != nil {
				tx.journal.Phase = phaseRollbackFailed
				tx.journal.Failure = boundedFailure(err)
				journalErr := tx.writeJournal()
				path := tx.result.JournalPath
				tx.close()
				return nil, fmt.Errorf("%w: reconcile repair journal %s: %v", ErrRollbackFailed, path, errors.Join(err, journalErr))
			}
			if err := tx.rollback(context.Background()); err != nil {
				tx.journal.Phase = phaseRollbackFailed
				tx.journal.Failure = boundedFailure(err)
				journalErr := tx.writeJournal()
				path := tx.result.JournalPath
				tx.close()
				return nil, fmt.Errorf("%w: recover repair journal %s: %v", ErrRollbackFailed, path, errors.Join(err, journalErr))
			}
			recoveredOriginal = slices.Clone(tx.options.OriginalConfig)
			tx.close()
		default:
			tx.close()
			return nil, fmt.Errorf("invalid TraeX repair journal phase %q", tx.journal.Phase)
		}
	}
	return recoveredOriginal, nil
}

func (t *transaction) reconcileMoving(ctx context.Context) error {
	if t.journal.Moving == "" || slices.Contains(t.journal.Moved, t.journal.Moving) {
		return nil
	}
	entry, ok := entryByPayload(t.manifest.Entries, t.journal.Moving)
	if !ok {
		return fmt.Errorf("repair journal references unknown moving payload %q", t.journal.Moving)
	}
	parent, leaf, closeRoots, err := openRelativeParent(t.managedRoot, entry.Path)
	if err != nil {
		return err
	}
	defer closeRoots()
	source, sourceErr := inspectLeaf(ctx, parent, leaf, entry.Path)
	quarantined, quarantineErr := inspectLeaf(ctx, t.payload, entry.Payload, entry.Path)
	sourceExists := sourceErr == nil
	quarantineExists := quarantineErr == nil
	if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
		return sourceErr
	}
	if quarantineErr != nil && !errors.Is(quarantineErr, os.ErrNotExist) {
		return quarantineErr
	}
	switch {
	case sourceExists && quarantineExists:
		return fmt.Errorf("repair entry %s exists in both target and quarantine", entry.Path)
	case !sourceExists && !quarantineExists:
		return fmt.Errorf("repair entry %s is missing from both target and quarantine", entry.Path)
	case sourceExists && !sameEntry(source, entry):
		// The move did not happen. The source is outside this transaction, so
		// leave it untouched while clearing the in-flight journal marker.
	case quarantineExists && !sameEntry(quarantined, entry):
		return fmt.Errorf("quarantined entry %s changed while its move was pending", entry.Path)
	case quarantineExists:
		t.journal.Moved = append(t.journal.Moved, entry.Payload)
	}
	t.journal.Moving = ""
	return t.writeJournal()
}

func entryByPayload(entries []Entry, payload string) (Entry, bool) {
	for _, entry := range entries {
		if entry.Payload == payload {
			return entry, true
		}
	}
	return Entry{}, false
}

func loadTransaction(
	options Options, managedRoot, quarantineRoot, directory *securefs.Root, name string,
) (*transaction, error) {
	var manifest Manifest
	if err := readJSON(directory, manifestName, &manifest); err != nil {
		return nil, fmt.Errorf("read TraeX repair manifest %s: %w", name, err)
	}
	var state journal
	if err := readJSON(directory, journalName, &state); err != nil {
		return nil, fmt.Errorf("read TraeX repair journal %s: %w", name, err)
	}
	if manifest.Version != manifestVersion || state.Version != journalVersion ||
		manifest.ManagedHome != options.ManagedHome || state.ManagedHome != options.ManagedHome ||
		manifest.Config.Path != options.ConfigPath || state.ConfigPath != options.ConfigPath {
		return nil, fmt.Errorf("TraeX repair transaction %s does not match this instance", name)
	}
	payload, err := directory.OpenRoot(payloadName, nil)
	if err != nil {
		return nil, err
	}
	original, err := readFile(directory, configBackupName, 1<<20)
	if err != nil {
		payload.Close()
		return nil, err
	}
	if digestBytes(original) != state.OriginalSHA256 || state.OriginalSHA256 != manifest.Config.SHA256 {
		payload.Close()
		return nil, errors.New("TraeX repair config backup digest does not match its journal")
	}
	options.OriginalConfig = original
	result := Result{
		QuarantinePath: filepath.Join(options.ManagedHome, quarantineName, name),
		ManifestPath:   filepath.Join(options.ManagedHome, quarantineName, name, manifestName),
		JournalPath:    filepath.Join(options.ManagedHome, quarantineName, name, journalName),
		Entries:        manifest.Entries,
	}
	return &transaction{
		options: options, result: result, manifest: manifest, journal: state,
		managedRoot: managedRoot, quarantineRoot: quarantineRoot, directory: directory, payload: payload,
	}, nil
}

func restoreConfig(path string, state journal, original []byte) error {
	current, err := delegationconfig.ReadProtectedFile(path, 1<<20)
	if err != nil {
		return fmt.Errorf("read config during rollback: %w", err)
	}
	digest := digestBytes(current)
	switch digest {
	case state.OriginalSHA256:
		return nil
	case state.ReplacementSHA256:
		if state.Phase == phasePrepared || state.Phase == phaseQuarantining ||
			state.Phase == phaseQuarantined {
			return errors.New("config changed before the recorded replacement phase")
		}
		if err := delegationconfig.ReplaceProtectedFile(path, current, original); err != nil {
			return fmt.Errorf("restore original config: %w", err)
		}
		return nil
	default:
		return errors.New("config changed before repair rollback")
	}
}

func restoreEntry(
	ctx context.Context, managedRoot, payload *securefs.Root, entry Entry,
) error {
	parent, leaf, closeRoots, err := openRelativeParent(managedRoot, entry.Path)
	if err != nil {
		return err
	}
	defer closeRoots()
	source, sourceErr := inspectLeaf(ctx, parent, leaf, entry.Path)
	quarantined, quarantineErr := inspectLeaf(ctx, payload, entry.Payload, entry.Path)
	sourceExists := sourceErr == nil
	quarantineExists := quarantineErr == nil
	if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
		return sourceErr
	}
	if quarantineErr != nil && !errors.Is(quarantineErr, os.ErrNotExist) {
		return quarantineErr
	}
	switch {
	case sourceExists && !sameEntry(source, entry):
		return fmt.Errorf("repair target %s is occupied", entry.Path)
	case quarantineExists && !sameEntry(quarantined, entry):
		return fmt.Errorf("quarantined entry %s changed before rollback", entry.Path)
	case sourceExists && quarantineExists:
		return fmt.Errorf("repair target %s and its quarantine payload both exist", entry.Path)
	case sourceExists:
		return nil
	case !quarantineExists:
		return fmt.Errorf("repair entry %s is missing from both target and quarantine", entry.Path)
	}
	if err := payload.MoveNoReplace(entry.Payload, parent, leaf); err != nil {
		return fmt.Errorf("restore repair entry %s: %w", entry.Path, err)
	}
	return errors.Join(payload.Sync(), parent.Sync())
}

func (t *transaction) setPhase(next phase) error {
	t.journal.Phase = next
	t.journal.Moving = ""
	return t.writeJournal()
}

func (t *transaction) writeJournal() error {
	now := time.Now
	if t.options.Now != nil {
		now = t.options.Now
	}
	t.journal.UpdatedAt = now().UTC().Format(time.RFC3339Nano)
	return replaceJSON(t.directory, journalName, t.journal)
}

func (t *transaction) fault(point FaultPoint) error {
	if t.options.Fault == nil {
		return nil
	}
	return t.options.Fault(point)
}

func createTransactionDirectory(root *securefs.Root, now time.Time) (string, error) {
	for range 100 {
		random := make([]byte, 8)
		if _, err := rand.Read(random); err != nil {
			return "", fmt.Errorf("create quarantine identifier: %w", err)
		}
		name := fmt.Sprintf("repair-%s-%s", now.UTC().Format("20060102T150405.000000000Z"), hex.EncodeToString(random))
		if err := root.Mkdir(name, 0o700); errors.Is(err, os.ErrExist) {
			continue
		} else if err != nil {
			return "", fmt.Errorf("create TraeX quarantine transaction: %w", err)
		}
		if err := root.Sync(); err != nil {
			return "", fmt.Errorf("sync TraeX quarantine root: %w", err)
		}
		return name, nil
	}
	return "", errors.New("create TraeX quarantine transaction: exhausted names")
}

func openRelativeParent(
	root *securefs.Root, relative string,
) (*securefs.Root, string, func(), error) {
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return nil, "", func() {}, errors.New("unsafe TraeX repair entry path")
	}
	parts := strings.Split(filepath.ToSlash(clean), "/")
	current := root
	opened := make([]*securefs.Root, 0, len(parts)-1)
	closeRoots := func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}
	for _, part := range parts[:len(parts)-1] {
		next, err := current.OpenRoot(part, nil)
		if err != nil {
			closeRoots()
			return nil, "", func() {}, fmt.Errorf("open TraeX repair path %s: %w", relative, err)
		}
		opened = append(opened, next)
		current = next
	}
	return current, parts[len(parts)-1], closeRoots, nil
}

func inspectRelative(ctx context.Context, root *securefs.Root, relative string) (Entry, error) {
	parent, leaf, closeRoots, err := openRelativeParent(root, relative)
	if err != nil {
		return Entry{}, err
	}
	defer closeRoots()
	return inspectLeaf(ctx, parent, leaf, filepath.ToSlash(relative))
}

func inspectLeaf(
	ctx context.Context, parent *securefs.Root, leaf, displayPath string,
) (Entry, error) {
	info, err := parent.Lstat(leaf)
	if err != nil {
		return Entry{}, fmt.Errorf("inspect TraeX repair entry %s: %w", displayPath, err)
	}
	digest := sha256.New()
	if err := hashLeaf(ctx, parent, leaf, ".", digest); err != nil {
		return Entry{}, fmt.Errorf("digest TraeX repair entry %s: %w", displayPath, err)
	}
	return Entry{
		Path: filepath.ToSlash(filepath.Clean(filepath.FromSlash(displayPath))),
		Type: entryType(info), Mode: uint32(info.Mode()), SHA256: hex.EncodeToString(digest.Sum(nil)),
	}, nil
}

func hashLeaf(ctx context.Context, parent *securefs.Root, leaf, relative string, digest hash.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := parent.Lstat(leaf)
	if err != nil {
		return err
	}
	typeName := entryType(info)
	_, _ = fmt.Fprintf(digest, "%d:%s|%s|%d|", len(relative), filepath.ToSlash(relative), typeName, uint32(info.Mode()))
	switch typeName {
	case "file":
		file, err := parent.OpenFile(leaf, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		opened, statErr := file.Stat()
		if statErr == nil && !os.SameFile(info, opened) {
			statErr = errors.New("file changed while it was being opened")
		}
		_, copyErr := io.Copy(digest, file)
		return errors.Join(statErr, copyErr, file.Close())
	case "symlink":
		target, err := parent.Readlink(leaf)
		if err != nil {
			return err
		}
		_, _ = io.WriteString(digest, target)
	case "directory":
		directory, err := parent.OpenRoot(leaf, nil)
		if err != nil {
			return err
		}
		defer directory.Close()
		entries, err := directory.Entries()
		if err != nil {
			return err
		}
		slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
		for _, entry := range entries {
			if err := hashLeaf(ctx, directory, entry.Name(), filepath.Join(relative, entry.Name()), digest); err != nil {
				return err
			}
		}
	}
	_, _ = digest.Write([]byte{0})
	return nil
}

func sameEntry(actual, expected Entry) bool {
	return actual.Path == expected.Path && actual.Type == expected.Type &&
		actual.Mode == expected.Mode && actual.SHA256 == expected.SHA256
}

func entryType(info os.FileInfo) string {
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		return "symlink"
	case info.Mode().IsRegular():
		return "file"
	case info.IsDir():
		return "directory"
	default:
		return "other"
	}
}

func writeNewJSON(root *securefs.Root, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeNewFile(root, name, append(data, '\n'))
}

func writeNewFile(root *securefs.Root, name string, data []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return root.Sync()
}

func replaceJSON(root *securefs.Root, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary := "." + name + ".tmp"
	if err := root.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeNewFile(root, temporary, data); err != nil {
		return err
	}
	if err := root.Rename(temporary, name); err != nil {
		return err
	}
	return root.Sync()
}

func readJSON(root *securefs.Root, name string, value any) error {
	data, err := readFile(root, name, 1<<20)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("unexpected data after JSON object")
	}
	return nil
}

func readFile(root *securefs.Root, name string, limit int64) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("repair metadata must be a regular file")
	}
	file, err := root.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr == nil && !os.SameFile(info, opened) {
		statErr = errors.New("repair metadata changed while it was being opened")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if err := errors.Join(statErr, readErr, closeErr); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("repair metadata exceeds its size limit")
	}
	return data, nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func boundedFailure(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\r", " "), "\n", " ")
	if len(text) > 1024 {
		return text[:1024]
	}
	return text
}
