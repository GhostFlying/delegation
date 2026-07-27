package rootapply

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"slices"
	"time"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/localbridge"
)

const (
	defaultMaximumActiveJournals   = 64
	defaultMaximumActiveBytes      = 4 * 1024 * 1024 * 1024
	defaultMaximumActiveAge        = 24 * time.Hour
	defaultMaximumTerminalJournals = 1024
	defaultMaximumTerminalBytes    = 64 * 1024 * 1024
	defaultMaximumTerminalAge      = 30 * 24 * time.Hour
)

type journalRetention struct {
	maximumActiveJournals   int
	maximumActiveBytes      int64
	maximumActiveAge        time.Duration
	maximumTerminalJournals int
	maximumTerminalBytes    int64
	maximumTerminalAge      time.Duration
}

var defaultJournalRetention = journalRetention{
	maximumActiveJournals:   defaultMaximumActiveJournals,
	maximumActiveBytes:      defaultMaximumActiveBytes,
	maximumActiveAge:        defaultMaximumActiveAge,
	maximumTerminalJournals: defaultMaximumTerminalJournals,
	maximumTerminalBytes:    defaultMaximumTerminalBytes,
	maximumTerminalAge:      defaultMaximumTerminalAge,
}

type journalUsage struct {
	activeJournals int
	activeBytes    int64
}

func (u journalUsage) canAdmit(retention journalRetention) bool {
	return u.activeJournals < retention.maximumActiveJournals &&
		u.activeBytes <= retention.maximumActiveBytes-maximumJournalBytes
}

type retainedTerminal struct {
	applyID   string
	updatedAt int64
	size      int64
}

func (m *Manager) maintainJournals(reserveTerminal bool) (journalUsage, error) {
	entries, err := fs.ReadDir(m.journal.FS(), ".")
	if err != nil {
		return journalUsage{}, err
	}
	now := m.now()
	usage := journalUsage{}
	terminals := make([]retainedTerminal, 0, len(entries))
	removedIncomplete := false
	for _, entry := range entries {
		if !entry.IsDir() || identity.ValidateID(entry.Name()) != nil {
			return journalUsage{}, localbridge.ErrApplyRecoveryRequired
		}
		lease, err := m.openJournal(entry.Name())
		if err != nil {
			return journalUsage{}, err
		}
		record, readErr := lease.read()
		if readErr != nil {
			_, journalErr := lease.root.Lstat(journalFileName)
			if errors.Is(journalErr, fs.ErrNotExist) {
				closeErr := lease.close()
				removeErr := m.journal.RemoveAll(entry.Name())
				if errors.Is(removeErr, fs.ErrNotExist) {
					removeErr = nil
				}
				if closeErr != nil || removeErr != nil {
					return journalUsage{}, errors.Join(closeErr, removeErr)
				}
				removedIncomplete = true
				continue
			}
			if journalErr != nil {
				readErr = errors.Join(readErr, journalErr)
			}
		}
		if readErr == nil && isExpired(record.UpdatedAt, now, m.retention.maximumActiveAge) &&
			record.State != journalCompleted && record.State != journalRecoveryRequired {
			if record.State == journalMutating || record.State == journalVerifying {
				record.State = journalRecoveryRequired
				record.UpdatedAt = now.Unix()
				readErr = lease.write(record)
			} else {
				result := localbridge.ApplyAgentChangesResult{
					ApplyID: record.Request.ApplyID, PackageID: record.Request.PackageID,
					Outcome:     localbridge.ApplyAgentChangesNeedsResolution,
					FailureCode: "root_workspace_recovery_required",
				}
				record.State = journalCompleted
				record.Result = &result
				record.UpdatedAt = now.Unix()
				readErr = lease.compactTerminal(record)
			}
		}
		if readErr == nil && record.State == journalCompleted {
			readErr = lease.compactArtifacts()
		}
		size, sizeErr := journalDirectoryBytes(lease.root)
		closeErr := lease.close()
		if readErr != nil || sizeErr != nil || closeErr != nil {
			return journalUsage{}, errors.Join(readErr, sizeErr, closeErr)
		}
		if record.State == journalCompleted {
			terminals = append(terminals, retainedTerminal{
				applyID: entry.Name(), updatedAt: record.UpdatedAt, size: size,
			})
			continue
		}
		if usage.activeBytes > math.MaxInt64-size {
			return journalUsage{}, localbridge.ErrApplyRecoveryRequired
		}
		usage.activeJournals++
		usage.activeBytes += size
	}
	if removedIncomplete {
		if err := syncDirectory(m.journal); err != nil {
			return journalUsage{}, err
		}
	}
	if err := m.pruneTerminalJournals(terminals, now, reserveTerminal); err != nil {
		return journalUsage{}, err
	}
	return usage, nil
}

func (m *Manager) pruneTerminalJournals(
	terminals []retainedTerminal,
	now time.Time,
	reserve bool,
) error {
	slices.SortFunc(terminals, func(left, right retainedTerminal) int {
		if left.updatedAt != right.updatedAt {
			if left.updatedAt < right.updatedAt {
				return -1
			}
			return 1
		}
		if left.applyID < right.applyID {
			return -1
		}
		if left.applyID > right.applyID {
			return 1
		}
		return 0
	})
	maximumJournals := m.retention.maximumTerminalJournals
	maximumBytes := m.retention.maximumTerminalBytes
	if reserve {
		maximumJournals--
		maximumBytes -= maximumJournalBytes
	}
	if maximumJournals < 0 {
		maximumJournals = 0
	}
	if maximumBytes < 0 {
		maximumBytes = 0
	}
	var totalBytes int64
	for _, terminal := range terminals {
		if totalBytes > math.MaxInt64-terminal.size {
			return localbridge.ErrApplyRecoveryRequired
		}
		totalBytes += terminal.size
	}
	removed := false
	for len(terminals) != 0 && (isExpired(
		terminals[0].updatedAt, now, m.retention.maximumTerminalAge,
	) || len(terminals) > maximumJournals || totalBytes > maximumBytes) {
		terminal := terminals[0]
		terminals = terminals[1:]
		if err := m.journal.RemoveAll(terminal.applyID); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("prune terminal root apply journal: %w", err)
		}
		totalBytes -= terminal.size
		removed = true
	}
	if removed {
		return syncDirectory(m.journal)
	}
	return nil
}

func journalDirectoryBytes(root *os.Root) (int64, error) {
	var total int64
	err := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil || info.Size() < 0 || total > math.MaxInt64-info.Size() {
			return localbridge.ErrApplyRecoveryRequired
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func isExpired(updatedAt int64, now time.Time, maximumAge time.Duration) bool {
	updated := time.Unix(updatedAt, 0)
	return !updated.After(now) && now.Sub(updated) >= maximumAge
}
