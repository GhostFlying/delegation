package localbridge

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/identity"
	"golang.org/x/mod/semver"
)

const (
	methodUpgradePrepare  = "upgrade.prepare"
	methodUpgradeArm      = "upgrade.arm"
	methodUpgradeActivate = "upgrade.activate"
	methodUpgradeCancel   = "upgrade.cancel"
	methodUpgradeStatus   = "upgrade.status"
	methodUpgradeRuntime  = "upgrade.runtime"
)

var upgradeDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type UpgradeSnapshot struct {
	TransactionID    string `json:"transactionId"`
	State            string `json:"state"`
	SourceVersion    string `json:"sourceVersion"`
	TargetVersion    string `json:"targetVersion"`
	CommitAuthorized bool   `json:"commitAuthorized"`
	FailureCode      string `json:"failureCode,omitempty"`
	UpdatedAt        int64  `json:"updatedAt"`
}

func (s UpgradeSnapshot) Validate() error {
	if err := identity.ValidateID(s.TransactionID); err != nil {
		return fmt.Errorf("transactionId %w", err)
	}
	if !boundedUpgradeText(s.State, 64) || !boundedUpgradeText(s.SourceVersion, 128) ||
		!boundedUpgradeText(s.TargetVersion, 128) ||
		(s.FailureCode != "" && !boundedUpgradeText(s.FailureCode, 64)) {
		return errors.New("upgrade snapshot contains invalid bounded text")
	}
	if !semver.IsValid("v"+s.SourceVersion) || !semver.IsValid("v"+s.TargetVersion) ||
		semver.Compare("v"+s.TargetVersion, "v"+s.SourceVersion) <= 0 || s.UpdatedAt <= 0 {
		return errors.New("upgrade snapshot contains invalid version or timestamp")
	}
	return nil
}

type UpgradeManager interface {
	PrepareLocalUpgrade(context.Context, string, string, string) (UpgradeSnapshot, error)
	ArmLocalUpgrade(context.Context, string) (UpgradeSnapshot, error)
	ActivateLocalUpgrade(context.Context, string) (UpgradeSnapshot, error)
	CancelLocalUpgrade(context.Context, string) (UpgradeSnapshot, error)
	LocalUpgrade(context.Context) (*UpgradeSnapshot, error)
	LocalUpgradeRuntime(context.Context) (UpgradeRuntimeIdentity, error)
}

type UpgradeRuntimeIdentity struct {
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

func (i UpgradeRuntimeIdentity) Validate() error {
	if !semver.IsValid("v"+i.Version) || !upgradeDigestPattern.MatchString(i.Digest) {
		return errors.New("upgrade runtime identity is invalid")
	}
	return nil
}

type upgradePrepareParams struct {
	TargetVersion   string `json:"targetVersion"`
	ConfigPath      string `json:"configPath"`
	EnvironmentFile string `json:"environmentFile,omitempty"`
}

type upgradeTransactionParams struct {
	TransactionID string `json:"transactionId"`
}

func ReadUpgrade(ctx context.Context, endpoint string) (*UpgradeSnapshot, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	var result struct {
		Upgrade *UpgradeSnapshot `json:"upgrade"`
	}
	if err := client.Call(ctx, methodUpgradeStatus, "", nil, struct{}{}, &result); err != nil {
		return nil, fmt.Errorf("read local upgrade status: %w", err)
	}
	if result.Upgrade != nil {
		if err := result.Upgrade.Validate(); err != nil {
			return nil, fmt.Errorf("invalid local upgrade status: %w", err)
		}
	}
	return result.Upgrade, nil
}

func ReadUpgradeRuntime(ctx context.Context, endpoint string) (UpgradeRuntimeIdentity, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return UpgradeRuntimeIdentity{}, err
	}
	var result UpgradeRuntimeIdentity
	if err := client.Call(ctx, methodUpgradeRuntime, "", nil, struct{}{}, &result); err != nil {
		return UpgradeRuntimeIdentity{}, fmt.Errorf("read local upgrade runtime identity: %w", err)
	}
	if err := result.Validate(); err != nil {
		return UpgradeRuntimeIdentity{}, err
	}
	return result, nil
}

func PrepareUpgrade(
	ctx context.Context, endpoint, targetVersion, configPath, environmentFile string,
) (UpgradeSnapshot, error) {
	return callUpgrade(ctx, endpoint, methodUpgradePrepare, upgradePrepareParams{
		TargetVersion: targetVersion, ConfigPath: configPath, EnvironmentFile: environmentFile,
	})
}

func ArmUpgrade(ctx context.Context, endpoint, transactionID string) (UpgradeSnapshot, error) {
	return callUpgrade(ctx, endpoint, methodUpgradeArm, upgradeTransactionParams{TransactionID: transactionID})
}

func ActivateUpgrade(ctx context.Context, endpoint, transactionID string) (UpgradeSnapshot, error) {
	return callUpgrade(ctx, endpoint, methodUpgradeActivate, upgradeTransactionParams{TransactionID: transactionID})
}

func CancelUpgrade(ctx context.Context, endpoint, transactionID string) (UpgradeSnapshot, error) {
	return callUpgrade(ctx, endpoint, methodUpgradeCancel, upgradeTransactionParams{TransactionID: transactionID})
}

func callUpgrade(ctx context.Context, endpoint, method string, params any) (UpgradeSnapshot, error) {
	client, err := NewClient(endpoint)
	if err != nil {
		return UpgradeSnapshot{}, err
	}
	var result UpgradeSnapshot
	if err := client.Call(ctx, method, "", nil, params, &result); err != nil {
		return UpgradeSnapshot{}, fmt.Errorf("local %s: %w", method, err)
	}
	if err := result.Validate(); err != nil {
		return UpgradeSnapshot{}, fmt.Errorf("invalid local %s result: %w", method, err)
	}
	return result, nil
}

func boundedUpgradeText(value string, maximum int) bool {
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
