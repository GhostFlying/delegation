package localbridge

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
	"golang.org/x/mod/semver"
)

const (
	methodControllerUpgradeStart          = "controller_upgrade.start"
	methodControllerUpgradeCancel         = "controller_upgrade.cancel"
	methodControllerUpgradeStatus         = "controller_upgrade.status"
	maximumControllerUpgradeTimeoutMillis = int64(30 * 60 * 1000)
)

type ControllerUpgradeCounts struct {
	Total                int `json:"total"`
	Pending              int `json:"pending"`
	Prepared             int `json:"prepared"`
	Armed                int `json:"armed"`
	ActivationRequested  int `json:"activationRequested"`
	Qualified            int `json:"qualified"`
	Canceled             int `json:"canceled"`
	InterventionRequired int `json:"interventionRequired"`
}

type ControllerUpgradeSnapshot struct {
	TransactionID      string                  `json:"transactionId"`
	State              string                  `json:"state"`
	SourceVersion      string                  `json:"sourceVersion"`
	TargetVersion      string                  `json:"targetVersion"`
	GlobalCommit       bool                    `json:"globalCommit"`
	Deadline           int64                   `json:"deadline"`
	CompletionDeadline int64                   `json:"completionDeadline,omitempty"`
	Participants       ControllerUpgradeCounts `json:"participants"`
	BrokerState        string                  `json:"brokerState,omitempty"`
	BrokerFailureCode  string                  `json:"brokerFailureCode,omitempty"`
	FailureCode        string                  `json:"failureCode,omitempty"`
	UpdatedAt          int64                   `json:"updatedAt"`
}

func (s ControllerUpgradeSnapshot) Validate() error {
	if err := identity.ValidateID(s.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	if !boundedUpgradeText(s.State, 64) || !semver.IsValid("v"+s.SourceVersion) ||
		!semver.IsValid("v"+s.TargetVersion) ||
		semver.Compare("v"+s.TargetVersion, "v"+s.SourceVersion) <= 0 ||
		s.Deadline <= 0 || s.UpdatedAt <= 0 || s.CompletionDeadline < 0 {
		return errors.New("controller upgrade snapshot is invalid")
	}
	if s.BrokerState != "" && !boundedUpgradeText(s.BrokerState, 64) ||
		s.BrokerFailureCode != "" && !boundedUpgradeText(s.BrokerFailureCode, 64) ||
		s.FailureCode != "" && !boundedUpgradeText(s.FailureCode, 64) {
		return errors.New("controller upgrade snapshot contains invalid bounded text")
	}
	counts := []int{
		s.Participants.Total, s.Participants.Pending, s.Participants.Prepared,
		s.Participants.Armed, s.Participants.ActivationRequested, s.Participants.Qualified,
		s.Participants.Canceled, s.Participants.InterventionRequired,
	}
	for _, count := range counts {
		if count < 0 || count > s.Participants.Total {
			return errors.New("controller upgrade participant counts are invalid")
		}
	}
	classified := s.Participants.Pending + s.Participants.Prepared + s.Participants.Armed +
		s.Participants.ActivationRequested + s.Participants.Qualified +
		s.Participants.Canceled + s.Participants.InterventionRequired
	if classified != s.Participants.Total {
		return errors.New("controller upgrade participant counts are inconsistent")
	}
	return nil
}

type ControllerUpgradeManager interface {
	StartControllerUpgrade(context.Context, string, int64) (ControllerUpgradeSnapshot, error)
	CancelControllerUpgrade(context.Context, string) (ControllerUpgradeSnapshot, error)
	ControllerUpgradeStatus(context.Context) (*ControllerUpgradeSnapshot, error)
}

type controllerUpgradeStartParams struct {
	TargetVersion string `json:"targetVersion"`
	TimeoutMillis int64  `json:"timeoutMillis"`
}

type controllerUpgradeTransactionParams struct {
	TransactionID string `json:"transactionId"`
}

func StartControllerUpgrade(
	ctx context.Context, endpoint, targetVersion string, timeoutMillis int64,
) (ControllerUpgradeSnapshot, error) {
	if !semver.IsValid("v"+targetVersion) || timeoutMillis <= 0 ||
		timeoutMillis > maximumControllerUpgradeTimeoutMillis {
		return ControllerUpgradeSnapshot{}, errors.New("controller upgrade target or timeout is invalid")
	}
	return callControllerUpgrade(
		ctx, endpoint, methodControllerUpgradeStart,
		controllerUpgradeStartParams{TargetVersion: targetVersion, TimeoutMillis: timeoutMillis},
	)
}

func CancelControllerUpgrade(
	ctx context.Context, endpoint, transactionID string,
) (ControllerUpgradeSnapshot, error) {
	if err := identity.ValidateID(transactionID); err != nil {
		return ControllerUpgradeSnapshot{}, fmt.Errorf("controller upgrade transaction ID: %w", err)
	}
	return callControllerUpgrade(
		ctx, endpoint, methodControllerUpgradeCancel,
		controllerUpgradeTransactionParams{TransactionID: transactionID},
	)
}

func ReadControllerUpgrade(
	ctx context.Context, endpoint string,
) (*ControllerUpgradeSnapshot, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	var result struct {
		Upgrade *ControllerUpgradeSnapshot `json:"upgrade"`
	}
	if err := client.Call(ctx, methodControllerUpgradeStatus, "", nil, struct{}{}, &result); err != nil {
		return nil, fmt.Errorf("read controller upgrade status: %w", err)
	}
	if result.Upgrade != nil {
		if err := result.Upgrade.Validate(); err != nil {
			return nil, fmt.Errorf("invalid controller upgrade status: %w", err)
		}
	}
	return result.Upgrade, nil
}

func callControllerUpgrade(
	ctx context.Context, endpoint, method string, params any,
) (ControllerUpgradeSnapshot, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return ControllerUpgradeSnapshot{}, err
	}
	var result ControllerUpgradeSnapshot
	if err := client.Call(ctx, method, "", nil, params, &result); err != nil {
		return ControllerUpgradeSnapshot{}, fmt.Errorf("local %s: %w", method, err)
	}
	if err := result.Validate(); err != nil {
		return ControllerUpgradeSnapshot{}, fmt.Errorf("invalid local %s result: %w", method, err)
	}
	return result, nil
}
