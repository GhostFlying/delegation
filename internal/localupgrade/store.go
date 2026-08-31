package localupgrade

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/securefs"
	"golang.org/x/mod/semver"
)

const (
	journalName = "journal.json"
	lockName    = "journal.lock"
)

var ErrTargetConflict = errors.New("another local upgrade target is already active")

type Store struct {
	path string
	now  func() time.Time
}

func OpenStore(path string) (*Store, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("upgrade root must be an absolute clean path")
	}
	if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
		return nil, fmt.Errorf("prepare protected upgrade root: %w", err)
	}
	return &Store{path: path, now: time.Now}, nil
}

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) Load() (Journal, error) {
	if s == nil {
		return Journal{}, errors.New("upgrade store is required")
	}
	data, err := delegationconfig.ReadProtectedFile(filepath.Join(s.path, journalName), maximumJournalBytes)
	if err != nil {
		return Journal{}, err
	}
	return decodeJournal(data)
}

func (s *Store) CreateOrResume(candidate Journal) (Journal, bool, error) {
	if s == nil {
		return Journal{}, false, errors.New("upgrade store is required")
	}
	lock, err := acquireJournalLock(filepath.Join(s.path, lockName))
	if err != nil {
		return Journal{}, false, err
	}
	defer lock.Close()
	existing, err := s.Load()
	replaceExisting := err == nil
	if err == nil {
		if existing.TargetVersion == candidate.TargetVersion {
			if !existing.Terminal() && !sameImmutableTransaction(existing, candidate) {
				return Journal{}, false, errors.New("same-target upgrade does not match the protected transaction identity")
			}
			return existing, true, nil
		}
		if !existing.Terminal() {
			if existing.TargetVersion != candidate.TargetVersion {
				return Journal{}, false, ErrTargetConflict
			}
		}
		if existing.State == StateRollbackFailed {
			return Journal{}, false, errors.New("failed rollback must be repaired before another upgrade")
		}
		expectedSource := existing.SourceVersion
		if existing.State == StateCommitted {
			expectedSource = existing.TargetVersion
		}
		if candidate.SourceVersion != expectedSource ||
			semver.Compare("v"+candidate.TargetVersion, "v"+existing.TargetVersion) <= 0 {
			return Journal{}, false, errors.New("new upgrade must continue from the installed version to a higher target")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Journal{}, false, err
	}
	if candidate.State != StatePrepared || candidate.CommitAuthorized {
		return Journal{}, false, errors.New("new upgrade journal must start prepared and unauthorized")
	}
	now := s.now()
	if candidate.CreatedAt == 0 {
		candidate.CreatedAt = now.UnixMilli()
	}
	if candidate.UpdatedAt == 0 {
		candidate.UpdatedAt = candidate.CreatedAt
	}
	if err := candidate.Validate(); err != nil {
		return Journal{}, false, err
	}
	if err := s.write(candidate, replaceExisting); err != nil {
		return Journal{}, false, err
	}
	return candidate, false, nil
}

func (s *Store) Update(transactionID string, mutate func(*Journal) error) (Journal, error) {
	if s == nil || mutate == nil {
		return Journal{}, errors.New("upgrade store and mutation are required")
	}
	lock, err := acquireJournalLock(filepath.Join(s.path, lockName))
	if err != nil {
		return Journal{}, err
	}
	defer lock.Close()
	before, err := s.Load()
	if err != nil {
		return Journal{}, err
	}
	if before.TransactionID != transactionID {
		return Journal{}, errors.New("upgrade transaction ID does not match the active journal")
	}
	after := before
	if err := mutate(&after); err != nil {
		return Journal{}, err
	}
	if err := validateMutation(before, after); err != nil {
		return Journal{}, err
	}
	after.UpdatedAt = nextTimestamp(before.UpdatedAt, s.now())
	if err := after.Validate(); err != nil {
		return Journal{}, err
	}
	if err := s.write(after, true); err != nil {
		return Journal{}, err
	}
	return after, nil
}

func (s *Store) write(journal Journal, replace bool) error {
	data, err := json.Marshal(journal)
	if err != nil {
		return fmt.Errorf("encode upgrade journal: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maximumJournalBytes {
		return errors.New("upgrade journal exceeds its size limit")
	}
	root, err := securefs.OpenRoot(s.path, nil)
	if err != nil {
		return err
	}
	defer root.Close()
	temporary := fmt.Sprintf(".journal-%d.tmp", time.Now().UnixNano())
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = root.Remove(temporary)
		}
	}()
	writeErr := writeAll(file, data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("write upgrade journal: %w", err)
	}
	if err := root.VerifyPath(); err != nil {
		return err
	}
	if replace {
		committed, err = root.Replace(temporary, journalName)
	} else {
		committed, err = root.PublishNoReplace(temporary, journalName)
	}
	if err == nil {
		return nil
	}
	if committed {
		current, loadErr := s.Load()
		if loadErr == nil && reflect.DeepEqual(current, journal) {
			return nil
		}
		return errors.Join(err, loadErr)
	}
	return err
}

func decodeJournal(data []byte) (Journal, error) {
	var journal Journal
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&journal); err != nil {
		return Journal{}, fmt.Errorf("decode upgrade journal: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Journal{}, errors.New("upgrade journal must contain one JSON value")
	}
	if err := journal.Validate(); err != nil {
		return Journal{}, fmt.Errorf("validate upgrade journal: %w", err)
	}
	return journal, nil
}

func validateMutation(before, after Journal) error {
	immutableBefore := before
	immutableAfter := after
	immutableBefore.State, immutableAfter.State = "", ""
	immutableBefore.CommitAuthorized, immutableAfter.CommitAuthorized = false, false
	immutableBefore.Progress, immutableAfter.Progress = Progress{}, Progress{}
	immutableBefore.FailureCode, immutableAfter.FailureCode = "", ""
	immutableBefore.UpdatedAt, immutableAfter.UpdatedAt = 0, 0
	immutableBefore.Database.SourceDigest, immutableAfter.Database.SourceDigest = "", ""
	immutableBefore.Database.TargetDigest, immutableAfter.Database.TargetDigest = "", ""
	if !reflect.DeepEqual(immutableBefore, immutableAfter) {
		return errors.New("upgrade journal immutable identity changed")
	}
	if before.CommitAuthorized && !after.CommitAuthorized {
		return errors.New("upgrade commit authorization is monotonic")
	}
	if !before.CommitAuthorized && after.CommitAuthorized &&
		(before.State != StateArmed || after.State != StateActivating) {
		return errors.New("upgrade commit authorization requires the armed to activating transition")
	}
	if !progressMonotonic(before.Progress, after.Progress) {
		return errors.New("upgrade activation progress is monotonic")
	}
	if !transitionAllowed(before.State, after.State, after.CommitAuthorized) {
		return fmt.Errorf("invalid upgrade transition %s -> %s", before.State, after.State)
	}
	return nil
}

func progressMonotonic(before, after Progress) bool {
	return (!before.ServiceStopped || after.ServiceStopped) &&
		(!before.DatabasePrepared || after.DatabasePrepared) &&
		(!before.DefinitionSwitched || after.DefinitionSwitched) &&
		(!before.DatabaseSwitched || after.DatabaseSwitched) &&
		(!before.ServiceStarted || after.ServiceStarted) &&
		(!before.Qualified || after.Qualified)
}

func transitionAllowed(before, after State, authorized bool) bool {
	if before == after {
		return true
	}
	if authorized {
		switch before {
		case StateArmed:
			return after == StateActivating
		case StateActivating:
			return after == StateStarted || after == StateForwardRecoveryRequired
		case StateStarted:
			return after == StateQualified || after == StateForwardRecoveryRequired
		case StateQualified:
			return after == StateCommitted || after == StateForwardRecoveryRequired
		case StateForwardRecoveryRequired:
			return after == StateCommitted
		}
		return false
	}
	switch before {
	case StatePrepared:
		return after == StateArmed || after == StateRolledBack
	case StateArmed:
		return after == StateRollbackRequired
	case StateActivating, StateStarted, StateQualified:
		if before == StateActivating {
			return after == StateStarted || after == StateRollbackRequired
		}
		if before == StateStarted {
			return after == StateQualified || after == StateRollbackRequired
		}
		return after == StateRollbackRequired
	case StateRollbackRequired:
		return after == StateRolledBack || after == StateRollbackFailed
	}
	return false
}

func sameImmutableTransaction(left, right Journal) bool {
	left.State, right.State = StatePrepared, StatePrepared
	left.CommitAuthorized, right.CommitAuthorized = false, false
	left.Progress, right.Progress = Progress{}, Progress{}
	left.FailureCode, right.FailureCode = "", ""
	left.CreatedAt, right.CreatedAt = 0, 0
	left.UpdatedAt, right.UpdatedAt = 0, 0
	left.Database.SourceDigest, right.Database.SourceDigest = "", ""
	left.Database.TargetDigest, right.Database.TargetDigest = "", ""
	return reflect.DeepEqual(left, right)
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
