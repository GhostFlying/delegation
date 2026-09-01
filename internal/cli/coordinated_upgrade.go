package cli

import (
	"context"
	"time"

	"github.com/GhostFlying/delegation/internal/coordinatedupgrade"
	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/statuspage"
)

type controllerUpgradeManagement struct {
	manager *coordinatedupgrade.Manager
}

func statusPageControllerUpgrade(
	manager *coordinatedupgrade.Manager,
) (*statuspage.ControllerUpgrade, error) {
	journal, err := manager.Status()
	if err != nil || journal == nil {
		return nil, err
	}
	return toStatusPageControllerUpgrade(controllerUpgradeSnapshot(*journal)), nil
}

func toStatusPageControllerUpgrade(
	snapshot localbridge.ControllerUpgradeSnapshot,
) *statuspage.ControllerUpgrade {
	return &statuspage.ControllerUpgrade{
		TransactionID: snapshot.TransactionID, State: snapshot.State,
		SourceVersion: snapshot.SourceVersion, TargetVersion: snapshot.TargetVersion,
		GlobalCommit: snapshot.GlobalCommit, Deadline: snapshot.Deadline,
		CompletionDeadline: snapshot.CompletionDeadline,
		Participants: statuspage.ControllerUpgradeCounts{
			Total: snapshot.Participants.Total, Pending: snapshot.Participants.Pending,
			Prepared: snapshot.Participants.Prepared, Armed: snapshot.Participants.Armed,
			ActivationRequested: snapshot.Participants.ActivationRequested,
			Qualified:           snapshot.Participants.Qualified, Canceled: snapshot.Participants.Canceled,
			InterventionRequired: snapshot.Participants.InterventionRequired,
		},
		BrokerState: snapshot.BrokerState, BrokerFailureCode: snapshot.BrokerFailureCode,
		FailureCode: snapshot.FailureCode, UpdatedAt: snapshot.UpdatedAt,
	}
}

func (m controllerUpgradeManagement) StartControllerUpgrade(
	ctx context.Context, targetVersion string, timeoutMillis int64,
) (localbridge.ControllerUpgradeSnapshot, error) {
	journal, err := m.manager.Start(ctx, targetVersion, time.Duration(timeoutMillis)*time.Millisecond)
	return controllerUpgradeSnapshot(journal), err
}

func (m controllerUpgradeManagement) CancelControllerUpgrade(
	ctx context.Context, transactionID string,
) (localbridge.ControllerUpgradeSnapshot, error) {
	journal, err := m.manager.Cancel(ctx, transactionID)
	return controllerUpgradeSnapshot(journal), err
}

func (m controllerUpgradeManagement) ControllerUpgradeStatus(
	context.Context,
) (*localbridge.ControllerUpgradeSnapshot, error) {
	journal, err := m.manager.Status()
	if err != nil || journal == nil {
		return nil, err
	}
	snapshot := controllerUpgradeSnapshot(*journal)
	return &snapshot, nil
}

func controllerUpgradeSnapshot(journal coordinatedupgrade.Journal) localbridge.ControllerUpgradeSnapshot {
	counts := localbridge.ControllerUpgradeCounts{Total: len(journal.Participants)}
	for _, participant := range journal.Participants {
		switch participant.State {
		case coordinatedupgrade.ParticipantPending:
			counts.Pending++
		case coordinatedupgrade.ParticipantPrepared:
			counts.Prepared++
		case coordinatedupgrade.ParticipantArmed:
			counts.Armed++
		case coordinatedupgrade.ParticipantActivationRequested:
			counts.ActivationRequested++
		case coordinatedupgrade.ParticipantQualified:
			counts.Qualified++
		case coordinatedupgrade.ParticipantCanceled:
			counts.Canceled++
		case coordinatedupgrade.ParticipantIntervention:
			counts.InterventionRequired++
		}
	}
	return localbridge.ControllerUpgradeSnapshot{
		TransactionID: journal.TransactionID, State: string(journal.State),
		SourceVersion: journal.SourceVersion, TargetVersion: journal.TargetVersion,
		GlobalCommit: journal.GlobalCommit, Deadline: journal.Deadline,
		CompletionDeadline: journal.CompletionDeadline, Participants: counts,
		BrokerState: journal.Broker.State, BrokerFailureCode: journal.Broker.FailureCode,
		FailureCode: journal.FailureCode, UpdatedAt: journal.UpdatedAt,
	}
}
