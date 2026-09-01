// Package coordinatedupgrade implements the broker-owned, crash-consistent
// controller transaction layered over role-local upgrade journals.
package coordinatedupgrade

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/GhostFlying/delegation/internal/identity"
	"golang.org/x/mod/semver"
)

const (
	JournalSchemaVersion = 1
	maximumJournalBytes  = 256 << 10
)

type State string

const (
	StatePreparing        State = "preparing"
	StateArming           State = "arming"
	StateArmed            State = "armed"
	StateCommitted        State = "committed"
	StateActivatingPeers  State = "activating_peers"
	StateActivatingBroker State = "activating_broker"
	StateQualifying       State = "qualifying"
	StateCanceling        State = "canceling"
	StateCanceled         State = "canceled"
	StateCompleted        State = "completed"
	StateCompletedErrors  State = "completed_with_errors"
)

type ParticipantState string

const (
	ParticipantPending             ParticipantState = "pending"
	ParticipantPrepared            ParticipantState = "prepared"
	ParticipantArmed               ParticipantState = "armed"
	ParticipantActivationRequested ParticipantState = "activation_requested"
	ParticipantQualified           ParticipantState = "qualified"
	ParticipantCanceled            ParticipantState = "canceled"
	ParticipantIntervention        ParticipantState = "intervention_required"
)

type Participant struct {
	DeviceID             string           `json:"deviceId"`
	ConnectionID         string           `json:"connectionId"`
	SourceVersion        string           `json:"sourceVersion"`
	LocalTransactionID   string           `json:"localTransactionId,omitempty"`
	State                ParticipantState `json:"state"`
	TargetRuntimeDigest  string           `json:"targetRuntimeDigest,omitempty"`
	ConfigDigest         string           `json:"configDigest,omitempty"`
	SourceReadinessEpoch uint64           `json:"sourceReadinessEpoch,omitempty"`
	FailureCode          string           `json:"failureCode,omitempty"`
	UpdatedAt            int64            `json:"updatedAt"`
}

type LocalParticipant struct {
	TransactionID       string `json:"transactionId,omitempty"`
	State               string `json:"state,omitempty"`
	TargetRuntimeDigest string `json:"targetRuntimeDigest,omitempty"`
	ConfigDigest        string `json:"configDigest,omitempty"`
	CommitAuthorized    bool   `json:"commitAuthorized"`
	FailureCode         string `json:"failureCode,omitempty"`
	UpdatedAt           int64  `json:"updatedAt,omitempty"`
}

type Journal struct {
	SchemaVersion      int              `json:"schemaVersion"`
	TransactionID      string           `json:"transactionId"`
	State              State            `json:"state"`
	SourceVersion      string           `json:"sourceVersion"`
	TargetVersion      string           `json:"targetVersion"`
	GlobalCommit       bool             `json:"globalCommit"`
	Deadline           int64            `json:"deadline"`
	CompletionDeadline int64            `json:"completionDeadline,omitempty"`
	Participants       []Participant    `json:"participants"`
	Broker             LocalParticipant `json:"broker"`
	FailureCode        string           `json:"failureCode,omitempty"`
	CreatedAt          int64            `json:"createdAt"`
	UpdatedAt          int64            `json:"updatedAt"`
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (j Journal) Terminal() bool {
	return j.State == StateCanceled || j.State == StateCompleted || j.State == StateCompletedErrors
}

func (j Journal) Validate() error {
	if j.SchemaVersion != JournalSchemaVersion {
		return fmt.Errorf("unsupported coordinated upgrade schema %d", j.SchemaVersion)
	}
	if err := identity.ValidateID(j.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	if !semver.IsValid("v"+j.SourceVersion) || !semver.IsValid("v"+j.TargetVersion) ||
		semver.Compare("v"+j.TargetVersion, "v"+j.SourceVersion) <= 0 {
		return errors.New("coordinated upgrade versions are not a strict forward transition")
	}
	if !validState(j.State) || j.Deadline <= 0 || j.CreatedAt <= 0 || j.UpdatedAt < j.CreatedAt {
		return errors.New("coordinated upgrade state or timestamps are invalid")
	}
	if j.GlobalCommit && !postCommitState(j.State) || !j.GlobalCommit && postCommitState(j.State) {
		return errors.New("coordinated upgrade commit decision is inconsistent")
	}
	if j.CompletionDeadline != 0 && (!j.GlobalCommit || j.CompletionDeadline < j.CreatedAt) {
		return errors.New("coordinated upgrade completion deadline is invalid")
	}
	if j.FailureCode != "" && !validCode(j.FailureCode) {
		return errors.New("coordinated upgrade failure code is invalid")
	}
	previous := ""
	for index := range j.Participants {
		participant := j.Participants[index]
		if err := participant.validate(j, index); err != nil {
			return err
		}
		if participant.DeviceID <= previous {
			return errors.New("coordinated upgrade participants must be sorted and unique")
		}
		previous = participant.DeviceID
	}
	if err := j.Broker.validate(j); err != nil {
		return err
	}
	return nil
}

func (p Participant) validate(j Journal, index int) error {
	if identity.ValidateID(p.DeviceID) != nil || identity.ValidateID(p.ConnectionID) != nil ||
		!semver.IsValid("v"+p.SourceVersion) || p.UpdatedAt <= 0 || !validParticipantState(p.State) {
		return fmt.Errorf("participant %d identity or state is invalid", index)
	}
	if p.LocalTransactionID == "" {
		if p.State != ParticipantPending || p.TargetRuntimeDigest != "" || p.ConfigDigest != "" {
			return fmt.Errorf("participant %d lacks prepared transaction identity", index)
		}
		return nil
	}
	if identity.ValidateID(p.LocalTransactionID) != nil || !digestPattern.MatchString(p.TargetRuntimeDigest) ||
		!digestPattern.MatchString(p.ConfigDigest) {
		return fmt.Errorf("participant %d prepared identity is invalid", index)
	}
	if p.FailureCode != "" && !validCode(p.FailureCode) {
		return fmt.Errorf("participant %d failure code is invalid", index)
	}
	if p.State == ParticipantIntervention && !j.GlobalCommit {
		return fmt.Errorf("participant %d requires intervention before COMMIT", index)
	}
	return nil
}

func (p LocalParticipant) validate(j Journal) error {
	if p.TransactionID == "" {
		if p.State != "" || p.TargetRuntimeDigest != "" || p.ConfigDigest != "" ||
			p.CommitAuthorized || p.UpdatedAt != 0 {
			return errors.New("broker participant lacks prepared transaction identity")
		}
		return nil
	}
	if identity.ValidateID(p.TransactionID) != nil || !digestPattern.MatchString(p.TargetRuntimeDigest) ||
		!digestPattern.MatchString(p.ConfigDigest) || p.State == "" || p.UpdatedAt <= 0 {
		return errors.New("broker participant identity is invalid")
	}
	if p.CommitAuthorized && !j.GlobalCommit {
		return errors.New("broker local transaction was authorized before global COMMIT")
	}
	if p.FailureCode != "" && !validCode(p.FailureCode) {
		return errors.New("broker participant failure code is invalid")
	}
	return nil
}

func validState(state State) bool {
	return slices.Contains([]State{
		StatePreparing, StateArming, StateArmed, StateCommitted, StateActivatingPeers,
		StateActivatingBroker, StateQualifying, StateCanceling, StateCanceled,
		StateCompleted, StateCompletedErrors,
	}, state)
}

func postCommitState(state State) bool {
	return slices.Contains([]State{
		StateCommitted, StateActivatingPeers, StateActivatingBroker, StateQualifying,
		StateCompleted, StateCompletedErrors,
	}, state)
}

func validParticipantState(state ParticipantState) bool {
	return slices.Contains([]ParticipantState{
		ParticipantPending, ParticipantPrepared, ParticipantArmed,
		ParticipantActivationRequested, ParticipantQualified, ParticipantCanceled,
		ParticipantIntervention,
	}, state)
}

func validCode(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || index > 0 && character >= '0' && character <= '9' ||
			index > 0 && character == '_' {
			continue
		}
		return false
	}
	return true
}

func RootForBroker(home, instanceID string) (string, error) {
	if home == "" || !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return "", errors.New("delegation home must be an absolute clean path")
	}
	if instanceID == "" || filepath.Base(instanceID) != instanceID {
		return "", errors.New("broker instance ID is invalid")
	}
	return filepath.Join(home, "upgrades", instanceID, "broker", "controller"), nil
}
