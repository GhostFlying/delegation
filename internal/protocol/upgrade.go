package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/identity"
	"golang.org/x/mod/semver"
)

var upgradeDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// UpgradeSnapshot is the bounded, non-secret projection of a peer-local
// upgrade transaction used by the broker coordinator. It deliberately omits
// all local paths and release acquisition details.
type UpgradeSnapshot struct {
	ControllerTransactionID string `json:"controllerTransactionId"`
	TransactionID           string `json:"transactionId"`
	State                   string `json:"state"`
	SourceVersion           string `json:"sourceVersion"`
	TargetVersion           string `json:"targetVersion"`
	TargetRuntimeDigest     string `json:"targetRuntimeDigest"`
	ConfigDigest            string `json:"configDigest"`
	SourceReadinessEpoch    uint64 `json:"sourceReadinessEpoch"`
	CommitAuthorized        bool   `json:"commitAuthorized"`
	FailureCode             string `json:"failureCode,omitempty"`
	UpdatedAt               int64  `json:"updatedAt"`
}

func (s UpgradeSnapshot) Validate() error {
	if err := identity.ValidateID(s.ControllerTransactionID); err != nil {
		return fmt.Errorf("controllerTransactionId %w", err)
	}
	if err := identity.ValidateID(s.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	if !validUpgradeText(s.State, 64) || !validUpgradeText(s.SourceVersion, 128) ||
		!validUpgradeText(s.TargetVersion, 128) ||
		(s.FailureCode != "" && !validUpgradeText(s.FailureCode, 64)) {
		return errors.New("upgrade snapshot contains invalid bounded text")
	}
	if !semver.IsValid("v"+s.SourceVersion) || !semver.IsValid("v"+s.TargetVersion) ||
		semver.Compare("v"+s.TargetVersion, "v"+s.SourceVersion) <= 0 {
		return errors.New("upgrade snapshot contains an invalid version transition")
	}
	if !upgradeDigestPattern.MatchString(s.TargetRuntimeDigest) ||
		!upgradeDigestPattern.MatchString(s.ConfigDigest) || s.UpdatedAt <= 0 {
		return errors.New("upgrade snapshot contains invalid identity material")
	}
	return nil
}

type PrepareUpgradeParams struct {
	ControllerTransactionID string `json:"controllerTransactionId"`
	TargetVersion           string `json:"targetVersion"`
}

func (p PrepareUpgradeParams) Validate() error {
	if err := identity.ValidateID(p.ControllerTransactionID); err != nil {
		return fmt.Errorf("controllerTransactionId %w", err)
	}
	if !semver.IsValid("v" + p.TargetVersion) {
		return errors.New("targetVersion is invalid")
	}
	return nil
}

type UpgradeTransactionParams struct {
	ControllerTransactionID string `json:"controllerTransactionId"`
	TransactionID           string `json:"transactionId"`
}

func (p UpgradeTransactionParams) Validate() error {
	if err := identity.ValidateID(p.ControllerTransactionID); err != nil {
		return fmt.Errorf("controllerTransactionId %w", err)
	}
	if err := identity.ValidateID(p.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	return nil
}

func validUpgradeText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
	}
	return true
}
