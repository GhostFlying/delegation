package coordinatedupgrade

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
)

const (
	journalName = "journal.json"
	lockName    = "journal.lock"
)

var ErrTargetConflict = errors.New("another coordinated upgrade target is already active")

type Store struct {
	path string
	now  func() time.Time
}

func OpenStore(path string) (*Store, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("coordinated upgrade root must be absolute and clean")
	}
	if err := delegationconfig.PreparePrivateDirectory(path); err != nil {
		return nil, fmt.Errorf("prepare coordinated upgrade root: %w", err)
	}
	return &Store{path: path, now: time.Now}, nil
}

func (s *Store) Load() (Journal, error) {
	data, err := delegationconfig.ReadProtectedFile(filepath.Join(s.path, journalName), maximumJournalBytes)
	if err != nil {
		return Journal{}, err
	}
	var journal Journal
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&journal); err != nil {
		return Journal{}, fmt.Errorf("decode coordinated upgrade journal: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Journal{}, errors.New("coordinated upgrade journal must contain one JSON value")
	}
	if err := journal.Validate(); err != nil {
		return Journal{}, fmt.Errorf("validate coordinated upgrade journal: %w", err)
	}
	return journal, nil
}

func (s *Store) CreateOrResume(candidate Journal) (Journal, bool, error) {
	lock, err := acquireJournalLock(filepath.Join(s.path, lockName))
	if err != nil {
		return Journal{}, false, err
	}
	defer lock.Close()
	existing, err := s.Load()
	replace := err == nil
	if err == nil {
		if !existing.Terminal() {
			if existing.TargetVersion != candidate.TargetVersion {
				return Journal{}, false, ErrTargetConflict
			}
			return existing, true, nil
		}
		if existing.TargetVersion == candidate.TargetVersion {
			return existing, true, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Journal{}, false, err
	}
	if candidate.State != StatePreparing || candidate.GlobalCommit {
		return Journal{}, false, errors.New("new coordinated upgrade must start preparing before COMMIT")
	}
	now := s.now().UnixMilli()
	if candidate.CreatedAt == 0 {
		candidate.CreatedAt = now
	}
	if candidate.UpdatedAt == 0 {
		candidate.UpdatedAt = candidate.CreatedAt
	}
	if err := candidate.Validate(); err != nil {
		return Journal{}, false, err
	}
	if err := s.write(candidate, replace); err != nil {
		return Journal{}, false, err
	}
	return candidate, false, nil
}

func (s *Store) Update(transactionID string, mutate func(*Journal) error) (Journal, error) {
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
		return Journal{}, errors.New("coordinated upgrade transaction ID does not match")
	}
	after := before
	after.Participants = append([]Participant(nil), before.Participants...)
	if err := mutate(&after); err != nil {
		return Journal{}, err
	}
	if err := validateMutation(before, after); err != nil {
		return Journal{}, err
	}
	after.UpdatedAt = max(before.UpdatedAt+1, s.now().UnixMilli())
	if err := after.Validate(); err != nil {
		return Journal{}, err
	}
	if err := s.write(after, true); err != nil {
		return Journal{}, err
	}
	return after, nil
}

func validateMutation(before, after Journal) error {
	left, right := before, after
	left.State, right.State = "", ""
	left.GlobalCommit, right.GlobalCommit = false, false
	left.CompletionDeadline, right.CompletionDeadline = 0, 0
	left.Participants, right.Participants = nil, nil
	left.Broker, right.Broker = LocalParticipant{}, LocalParticipant{}
	left.FailureCode, right.FailureCode = "", ""
	left.UpdatedAt, right.UpdatedAt = 0, 0
	if !reflect.DeepEqual(left, right) {
		return errors.New("coordinated upgrade immutable identity changed")
	}
	if before.GlobalCommit && !after.GlobalCommit {
		return errors.New("coordinated upgrade COMMIT is irreversible")
	}
	if !transitionAllowed(before.State, after.State) {
		return fmt.Errorf("invalid coordinated upgrade transition %s -> %s", before.State, after.State)
	}
	if len(before.Participants) != len(after.Participants) {
		return errors.New("coordinated upgrade participant set is immutable")
	}
	for index := range before.Participants {
		if before.Participants[index].DeviceID != after.Participants[index].DeviceID ||
			before.Participants[index].ConnectionID != after.Participants[index].ConnectionID ||
			before.Participants[index].SourceVersion != after.Participants[index].SourceVersion {
			return errors.New("coordinated upgrade participant identity changed")
		}
		if err := validateParticipantMutation(before.Participants[index], after.Participants[index]); err != nil {
			return fmt.Errorf("participant %s: %w", before.Participants[index].DeviceID, err)
		}
	}
	if err := validateBrokerMutation(before.Broker, after.Broker); err != nil {
		return err
	}
	return nil
}

func validateParticipantMutation(before, after Participant) error {
	if before.LocalTransactionID != "" && (before.LocalTransactionID != after.LocalTransactionID ||
		before.TargetRuntimeDigest != after.TargetRuntimeDigest ||
		before.ConfigDigest != after.ConfigDigest ||
		before.SourceReadinessEpoch != after.SourceReadinessEpoch) {
		return errors.New("prepared transaction identity changed")
	}
	if participantRank(after.State) < participantRank(before.State) {
		return errors.New("participant state moved backward")
	}
	return nil
}

func validateBrokerMutation(before, after LocalParticipant) error {
	if before.TransactionID != "" && (before.TransactionID != after.TransactionID ||
		before.TargetRuntimeDigest != after.TargetRuntimeDigest ||
		before.ConfigDigest != after.ConfigDigest) {
		return errors.New("broker prepared transaction identity changed")
	}
	if before.CommitAuthorized && !after.CommitAuthorized {
		return errors.New("broker commit authorization moved backward")
	}
	return nil
}

func participantRank(state ParticipantState) int {
	switch state {
	case ParticipantPending:
		return 0
	case ParticipantPrepared:
		return 1
	case ParticipantArmed:
		return 2
	case ParticipantActivationRequested:
		return 3
	case ParticipantQualified, ParticipantIntervention:
		return 4
	case ParticipantCanceled:
		return 5
	default:
		return -1
	}
}

func transitionAllowed(before, after State) bool {
	if before == after {
		return true
	}
	switch before {
	case StatePreparing:
		return after == StateArming || after == StateCanceling
	case StateArming:
		return after == StateArmed || after == StateCanceling
	case StateArmed:
		return after == StateCommitted || after == StateCanceling
	case StateCommitted:
		return after == StateActivatingPeers
	case StateActivatingPeers:
		return after == StateActivatingBroker
	case StateActivatingBroker:
		return after == StateQualifying
	case StateQualifying:
		return after == StateCompleted || after == StateCompletedErrors
	case StateCanceling:
		return after == StateCanceled
	}
	return false
}

func (s *Store) write(journal Journal, replace bool) error {
	data, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > maximumJournalBytes {
		return errors.New("coordinated upgrade journal exceeds size limit")
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
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if replace {
		committed, err = root.Replace(temporary, journalName)
	} else {
		committed, err = root.PublishNoReplace(temporary, journalName)
	}
	return err
}
