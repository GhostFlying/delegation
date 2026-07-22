package connector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func (s *session) syncWorkerLifecycles(
	ctx context.Context,
	baseRevision, throughRevision uint64,
) (uint64, error) {
	if throughRevision < baseRevision {
		return 0, errors.New("worker lifecycle high-water regressed behind the broker cursor")
	}
	for baseRevision < throughRevision {
		params, err := s.nextWorkerLifecyclePage(ctx, baseRevision, throughRevision)
		if err != nil {
			return 0, err
		}
		var result protocol.SyncWorkerLifecycleResult
		payload, err := s.call(
			ctx, protocol.MethodSyncWorkerLifecycle, "", nil, params,
		)
		if err != nil {
			return 0, fmt.Errorf("sync worker lifecycle: %w", err)
		}
		if err := decodeResult(payload, &result); err != nil {
			return 0, fmt.Errorf("decode worker lifecycle result: %w", err)
		}
		expected, err := params.AppliedRevision()
		if err != nil {
			return 0, err
		}
		if result.Validate() != nil || result.AppliedRevision != expected {
			return 0, errors.New("broker returned a mismatched worker lifecycle cursor")
		}
		baseRevision = result.AppliedRevision
	}
	return baseRevision, nil
}

func (s *session) nextWorkerLifecyclePage(
	ctx context.Context,
	baseRevision, throughRevision uint64,
) (protocol.SyncWorkerLifecycleParams, error) {
	snapshots, err := s.client.workerLifecycle.ListWorkerLifecycles(ctx)
	if err != nil {
		return protocol.SyncWorkerLifecycleParams{}, fmt.Errorf("list worker lifecycles: %w", err)
	}
	candidates := make([]protocol.WorkerLifecycleSnapshot, 0, len(snapshots))
	seen := make(map[string]struct{}, len(snapshots))
	for index, snapshot := range snapshots {
		if err := snapshot.Validate(); err != nil {
			return protocol.SyncWorkerLifecycleParams{}, fmt.Errorf(
				"worker lifecycle snapshot %d: %w", index, err,
			)
		}
		key := snapshot.TreeID + "\x00" + snapshot.AgentID
		if _, duplicate := seen[key]; duplicate {
			return protocol.SyncWorkerLifecycleParams{}, errors.New(
				"worker lifecycle source returned a duplicate agent",
			)
		}
		seen[key] = struct{}{}
		if snapshot.Revision > baseRevision && snapshot.Revision <= throughRevision {
			candidates = append(candidates, snapshot)
		}
	}
	slices.SortFunc(candidates, func(left, right protocol.WorkerLifecycleSnapshot) int {
		return cmp.Compare(left.Revision, right.Revision)
	})
	complete := len(candidates) <= protocol.MaximumWorkerLifecyclePage
	if !complete {
		candidates = candidates[:protocol.MaximumWorkerLifecyclePage]
	}
	params := protocol.SyncWorkerLifecycleParams{
		BaseRevision: baseRevision, ThroughRevision: throughRevision,
		Complete: complete, Workers: candidates,
	}
	if err := params.Validate(); err != nil {
		return protocol.SyncWorkerLifecycleParams{}, fmt.Errorf(
			"build worker lifecycle page: %w", err,
		)
	}
	return params, nil
}

func (s *session) workerLifecycleLoop(appliedRevision uint64) {
	changes := s.client.workerLifecycle.WorkerLifecycleChanges()
	for {
		throughRevision := s.client.workerLifecycle.WorkerRevision()
		if throughRevision < appliedRevision {
			s.close(errors.New("worker lifecycle high-water regressed"))
			return
		}
		if throughRevision > appliedRevision {
			ctx, cancel := context.WithTimeout(s.context, connectTimeout)
			updated, err := s.syncWorkerLifecycles(ctx, appliedRevision, throughRevision)
			cancel()
			if err != nil {
				s.close(err)
				return
			}
			appliedRevision = updated
			s.client.updateWorkerRevision(s, appliedRevision)
			continue
		}
		select {
		case <-s.done:
			return
		case <-changes:
		}
	}
}
