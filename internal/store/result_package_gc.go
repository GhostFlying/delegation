package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	// MaximumBrokerResultPackageDetailsPerRoot matches the peer inbox bound. A
	// cold root can therefore discover every package whose payload can still be
	// retained by its peer.
	MaximumBrokerResultPackageDetailsPerRoot = MaximumPeerResultPackages
	// MaximumResultPackageDetailCompactionBatch bounds one SQLite transaction.
	MaximumResultPackageDetailCompactionBatch = 128
)

// ResultPackageDetailCompaction reports one bounded GC transaction.
type ResultPackageDetailCompaction struct {
	Compacted int
	More      bool
}

type resultPackageDetailKey struct {
	treeID    string
	packageID string
}

// CompactReleasedResultPackageDetails removes old broker metadata only after
// the source peer has acknowledged and released its payload. Delivered and
// compacted lifetime counters preserve aggregate status after detail removal.
func (s *Store) CompactReleasedResultPackageDetails(
	ctx context.Context,
	controllerID string,
	limit int,
) (ResultPackageDetailCompaction, error) {
	if err := identity.ValidateID(controllerID); err != nil {
		return ResultPackageDetailCompaction{}, fmt.Errorf("controllerId %w", err)
	}
	if limit < 1 || limit > MaximumResultPackageDetailCompactionBatch {
		return ResultPackageDetailCompaction{}, fmt.Errorf(
			"result package detail compaction limit must be from 1 through %d",
			MaximumResultPackageDetailCompactionBatch,
		)
	}

	var result ResultPackageDetailCompaction
	err := s.withImmediateTransaction(ctx, func(connection *sql.Conn) error {
		rows, err := connection.QueryContext(ctx, `
WITH ranked AS (
	SELECT
		tree_id,
		package_id,
			root_retention_ordinal,
		source_released_at,
		row_number() OVER (
			PARTITION BY root_device_id
				ORDER BY root_retention_ordinal DESC
		) AS retention_rank
	FROM result_packages
	WHERE controller_id = ? AND state = 'delivered'
)
SELECT tree_id, package_id
FROM ranked
WHERE retention_rank > ? AND source_released_at > 0
ORDER BY root_retention_ordinal, tree_id, package_id
LIMIT ?
`, controllerID, MaximumBrokerResultPackageDetailsPerRoot, limit+1)
		if err != nil {
			return fmt.Errorf("list compactable result package details: %w", err)
		}
		keys := make([]resultPackageDetailKey, 0, limit+1)
		for rows.Next() {
			var key resultPackageDetailKey
			if err := rows.Scan(&key.treeID, &key.packageID); err != nil {
				rows.Close()
				return fmt.Errorf("scan compactable result package detail: %w", err)
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("list compactable result package details: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close compactable result package details: %w", err)
		}

		result.More = len(keys) > limit
		keys = keys[:min(len(keys), limit)]
		for _, key := range keys {
			deleted, err := connection.ExecContext(ctx, `
DELETE FROM result_packages
WHERE controller_id = ? AND tree_id = ? AND package_id = ?
	AND state = 'delivered' AND source_released_at > 0
`, controllerID, key.treeID, key.packageID)
			if err != nil {
				return fmt.Errorf("compact result package detail: %w", err)
			}
			affected, err := deleted.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect result package detail compaction: %w", err)
			}
			if affected != 1 {
				return errors.New("result package detail changed during compaction")
			}
		}
		if len(keys) == 0 {
			return nil
		}
		if err := incrementStatusLifetimeCounters(
			ctx,
			connection,
			controllerID,
			statusLifetimeCounterIncrement{ResultPackageDetailsCompacted: len(keys)},
		); err != nil {
			return err
		}
		result.Compacted = len(keys)
		return nil
	})
	if err != nil {
		return ResultPackageDetailCompaction{}, err
	}
	return result, nil
}
