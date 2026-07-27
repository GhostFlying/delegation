package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
)

func TestResultInboxBeginBoundsLeaseAndRequiresExplicitExpiredReclaim(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	key := resultInboxSourceKey(changesTestID(6001))
	metadata := resultMetadata(t, key, changesTestID(6002), changesTestID(6003), 4)
	now := time.Unix(1_700_010_000, 0)
	params := protocol.BeginResultPackageParams{
		AttemptID: changesTestID(6004), PackageID: key.PackageID,
		LeaseExpiresAt: now.Add(time.Minute).Unix(), Metadata: metadata,
	}
	tooEarly := params
	tooEarly.LeaseExpiresAt = now.Unix()
	if _, err := state.BeginResultInbox(ctx, authority, tooEarly, now); err == nil {
		t.Fatal("begin accepted a non-future lease")
	}
	tooLate := params
	tooLate.LeaseExpiresAt = now.Add(MaximumResultInboxLease + time.Second).Unix()
	if _, err := state.BeginResultInbox(ctx, authority, tooLate, now); err == nil {
		t.Fatal("begin accepted an overlong lease")
	}
	overflowingNow := time.Unix(math.MaxInt64-1, 0)
	overflowing := params
	overflowing.LeaseExpiresAt = math.MaxInt64
	if _, err := state.BeginResultInbox(ctx, authority, overflowing, overflowingNow); err == nil {
		t.Fatal("begin accepted an observed time whose maximum lease overflows")
	}

	started, err := state.BeginResultInbox(ctx, authority, params, now)
	if err != nil {
		t.Fatal(err)
	}
	if started.Outcome != protocol.ResultPackageReceiving || started.RetentionOrdinal != 1 ||
		len(started.Offsets) != 1 {
		t.Fatalf("begin result = %#v", started)
	}
	replayed, err := state.BeginResultInbox(ctx, authority, params, now.Add(time.Second))
	if err != nil || !reflect.DeepEqual(replayed, started) {
		t.Fatalf("begin replay = %#v, %v; want %#v", replayed, err, started)
	}
	higherFloor := params
	higherFloor.RetentionFloor = 100
	if replayed, err := state.BeginResultInbox(
		ctx, authority, higherFloor, now.Add(time.Second),
	); err != nil || !reflect.DeepEqual(replayed, started) {
		t.Fatalf("begin replay above a new floor = %#v, %v; want %#v", replayed, err, started)
	}
	replayed, err = state.BeginResultInbox(ctx, authority, params, now.Add(2*time.Minute))
	if err != nil || !reflect.DeepEqual(replayed, started) {
		t.Fatalf("expired exact begin replay = %#v, %v; want %#v", replayed, err, started)
	}
	differentAttempt := params
	differentAttempt.AttemptID = changesTestID(6005)
	differentAttempt.LeaseExpiresAt = now.Add(11 * time.Minute).Unix()
	if _, err := state.BeginResultInbox(
		ctx, authority, differentAttempt, now.Add(61*time.Second),
	); !errors.Is(err, ErrResultPackageConflict) {
		t.Fatalf("expired receive replacement error = %v, want explicit conflict before reclaim", err)
	}
	if _, err := state.PrepareExpiredResultInboxReclaim(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(30*time.Second),
	); !errors.Is(err, ErrResultPackageTransition) {
		t.Fatalf("early reclaim error = %v, want ErrResultPackageTransition", err)
	}
	removal, err := state.PrepareExpiredResultInboxReclaim(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(MaximumResultInboxLease),
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := state.ListPreparedResultInboxRemovals(
		ctx, authority.ControllerID, authority.RootDeviceID, 10,
	)
	if err != nil || len(prepared) != 1 || prepared[0].Phase != ResultInboxRemovalPrepared {
		t.Fatalf("prepared removals after first crash point = %#v, %v", prepared, err)
	}
	if receiving, err := state.GetResultInbox(ctx, authority, params.PackageID); err != nil ||
		receiving.State != ResultInboxReceiving {
		t.Fatalf("prepared removal lost receiving row = %#v, %v", receiving, err)
	}
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID,
		resultChunk(protocol.ResultPackagePartRollout, 0, []byte("ab")), now.Add(time.Minute),
	); !errors.Is(err, ErrResultPackageTransition) {
		t.Fatalf("prepared removal accepted a chunk: %v", err)
	}
	completed, err := state.CommitResultInboxRemoval(
		ctx, removal, now.Add(MaximumResultInboxLease+time.Second),
	)
	if err != nil || completed.Phase != ResultInboxRemovalCompleted {
		t.Fatalf("commit removal after filesystem deletion = %#v, %v", completed, err)
	}
	if _, err := state.GetResultInbox(ctx, authority, params.PackageID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed removal retained inbox: %v", err)
	}
	if replayed, err := state.CommitResultInboxRemoval(
		ctx, removal, now.Add(MaximumResultInboxLease+2*time.Second),
	); err != nil || replayed.Phase != ResultInboxRemovalCompleted {
		t.Fatalf("removal replay after second crash point = %#v, %v", replayed, err)
	}
	if _, err := state.BeginResultInbox(
		ctx, authority, differentAttempt, now.Add(MaximumResultInboxLease+time.Second),
	); err != nil {
		t.Fatalf("begin after explicit reclaim: %v", err)
	}
}

func TestResultInboxFreshStateAdvancesAboveBrokerRetentionFloor(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	now := time.Unix(1_700_020_000, 0)

	begin := func(index int, floor uint64) protocol.BeginResultPackageResult {
		key := resultInboxSourceKey(changesTestID(6500 + index))
		result, err := state.BeginResultInbox(ctx, authority, protocol.BeginResultPackageParams{
			AttemptID:      changesTestID(6600 + index),
			PackageID:      key.PackageID,
			RetentionFloor: floor,
			LeaseExpiresAt: now.Add(time.Minute).Unix(),
			Metadata:       resultMetadata(t, key, changesTestID(6700+index), changesTestID(6800+index), 4),
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	if got := begin(1, 100).RetentionOrdinal; got != 101 {
		t.Fatalf("first retention ordinal = %d, want 101", got)
	}
	if got := begin(2, 0).RetentionOrdinal; got != 102 {
		t.Fatalf("second retention ordinal = %d, want 102", got)
	}
}

func TestResultInboxChunkReceiptsFinishAndAvailableReplay(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	key := resultInboxSourceKey(changesTestID(6010))
	threadID, turnID := changesTestID(6011), changesTestID(6012)
	metadata := resultMetadata(t, key, threadID, turnID, int64(protocol.ResultPackageChunkBytes)+2)
	now := time.Unix(1_700_011_000, 0)
	params := protocol.BeginResultPackageParams{
		AttemptID: changesTestID(6013), PackageID: key.PackageID,
		LeaseExpiresAt: now.Add(time.Minute).Unix(), Metadata: metadata,
	}
	if _, err := state.BeginResultInbox(ctx, authority, params, now); err != nil {
		t.Fatal(err)
	}
	first := resultChunk(
		protocol.ResultPackagePartRollout, 0, bytes.Repeat([]byte{'a'}, protocol.ResultPackageChunkBytes),
	)
	committed, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, first, now.Add(time.Second),
	)
	if err != nil || committed != (ResultInboxChunkCommitResult{NextOffset: int64(protocol.ResultPackageChunkBytes)}) {
		t.Fatalf("first chunk = %#v, %v", committed, err)
	}
	replayed, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, first, now.Add(2*time.Second),
	)
	if err != nil || replayed != (ResultInboxChunkCommitResult{
		NextOffset: int64(protocol.ResultPackageChunkBytes), Replay: true,
	}) {
		t.Fatalf("chunk replay = %#v, %v", replayed, err)
	}
	conflict := first
	conflict.SHA256 = fmt.Sprintf("%064x", 1)
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, conflict, now,
	); !errors.Is(err, ErrResultPackageConflict) {
		t.Fatalf("different replay error = %v, want ErrResultPackageConflict", err)
	}
	gap := resultChunk(
		protocol.ResultPackagePartRollout, int64(protocol.ResultPackageChunkBytes)+1, []byte("d"),
	)
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, gap, now,
	); !errors.Is(err, ErrResultPackageConflict) {
		t.Fatalf("gap error = %v, want ErrResultPackageConflict", err)
	}
	if _, err := state.CommitResultInboxAvailable(
		ctx, authority, params.AttemptID, params.PackageID, now,
	); !errors.Is(err, ErrResultPackageTransition) {
		t.Fatalf("incomplete finish error = %v, want ErrResultPackageTransition", err)
	}
	second := resultChunk(
		protocol.ResultPackagePartRollout, int64(protocol.ResultPackageChunkBytes), []byte("cd"),
	)
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, second, now.Add(3*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	available, err := state.CommitResultInboxAvailable(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(4*time.Second),
	)
	if err != nil || available.State != ResultInboxAvailable {
		t.Fatalf("finish = %#v, %v", available, err)
	}
	finishReplay, err := state.CommitResultInboxAvailable(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(5*time.Second),
	)
	if err != nil || !reflect.DeepEqual(finishReplay, available) {
		t.Fatalf("finish replay = %#v, %v; want %#v", finishReplay, err, available)
	}

	newAttempt := params
	newAttempt.AttemptID = changesTestID(6014)
	newAttempt.LeaseExpiresAt = now.Add(2 * time.Minute).Unix()
	beginReplay, err := state.BeginResultInbox(ctx, authority, newAttempt, now.Add(time.Minute))
	if err != nil || beginReplay.Outcome != protocol.ResultPackageAlreadyAvailable || len(beginReplay.Offsets) != 0 {
		t.Fatalf("available begin replay = %#v, %v", beginReplay, err)
	}
	stored, err := state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AttemptID != params.AttemptID {
		t.Fatalf("available replay rewrote attempt to %s", stored.AttemptID)
	}
	conflictingMetadata := resultMetadata(t, key, threadID, turnID, 3)
	newAttempt.Metadata = conflictingMetadata
	if _, err := state.BeginResultInbox(ctx, authority, newAttempt, now.Add(time.Minute)); !errors.Is(
		err, ErrResultPackageConflict,
	) {
		t.Fatalf("available metadata conflict error = %v", err)
	}
	availability, err := state.LookupResultInboxAvailability(ctx, authority, params.PackageID)
	if err != nil || availability != ResultInboxAvailabilityAvailable {
		t.Fatalf("availability = %q, %v", availability, err)
	}
}

func TestResultInboxChunkActivityRenewsLocalLeaseAndAcceptsStaleBeginReplay(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	key := resultInboxSourceKey(changesTestID(6090))
	now := time.Unix(1_700_011_500, 0)
	params := protocol.BeginResultPackageParams{
		AttemptID: changesTestID(6091), PackageID: key.PackageID,
		LeaseExpiresAt: now.Add(time.Minute).Unix(),
		Metadata: resultMetadata(
			t, key, changesTestID(6092), changesTestID(6093),
			int64(protocol.ResultPackageChunkBytes)+2,
		),
	}
	if _, err := state.BeginResultInbox(ctx, authority, params, now); err != nil {
		t.Fatal(err)
	}
	stored, err := state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil || stored.LeaseExpiresAt != now.Add(MaximumResultInboxLease).Unix() {
		t.Fatalf("initial local lease = %#v, %v", stored, err)
	}
	first := resultChunk(
		protocol.ResultPackagePartRollout, 0,
		bytes.Repeat([]byte{'a'}, protocol.ResultPackageChunkBytes),
	)
	activityAt := now.Add(9 * time.Minute)
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, first, activityAt,
	); err != nil {
		t.Fatal(err)
	}
	stored, err = state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil || stored.LeaseExpiresAt != activityAt.Add(MaximumResultInboxLease).Unix() {
		t.Fatalf("renewed local lease = %#v, %v", stored, err)
	}
	if _, err := state.BeginResultInbox(ctx, authority, params, now.Add(11*time.Minute)); err != nil {
		t.Fatalf("stale wire begin replay after lease renewal: %v", err)
	}
	if replay, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, first, now.Add(5*time.Minute),
	); err != nil || !replay.Replay {
		t.Fatalf("out-of-order chunk activity replay = %#v, %v", replay, err)
	}
	stored, err = state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil || stored.LeaseExpiresAt != activityAt.Add(MaximumResultInboxLease).Unix() {
		t.Fatalf("lease shortened by out-of-order replay = %#v, %v", stored, err)
	}
	second := resultChunk(
		protocol.ResultPackagePartRollout, int64(protocol.ResultPackageChunkBytes), []byte{'b', 'b'},
	)
	if _, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, second, now.Add(6*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	stored, err = state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil || stored.LeaseExpiresAt != activityAt.Add(MaximumResultInboxLease).Unix() {
		t.Fatalf("lease shortened by out-of-order new chunk = %#v, %v", stored, err)
	}
	replayAt := now.Add(18 * time.Minute)
	if replay, err := state.CommitResultInboxChunk(
		ctx, authority, params.AttemptID, params.PackageID, first, replayAt,
	); err != nil || !replay.Replay {
		t.Fatalf("chunk activity replay = %#v, %v", replay, err)
	}
	stored, err = state.GetResultInbox(ctx, authority, params.PackageID)
	if err != nil || stored.LeaseExpiresAt != replayAt.Add(MaximumResultInboxLease).Unix() {
		t.Fatalf("replay-renewed local lease = %#v, %v", stored, err)
	}
	if _, err := state.PrepareExpiredResultInboxReclaim(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(20*time.Minute),
	); !errors.Is(err, ErrResultPackageTransition) {
		t.Fatalf("active renewed transfer reclaim error = %v", err)
	}
}

func TestResultInboxCancelReplayCannotRemoveNewAttempt(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	key := resultInboxSourceKey(changesTestID(6020))
	metadata := resultMetadata(t, key, changesTestID(6021), changesTestID(6022), 4)
	now := time.Unix(1_700_012_000, 0)
	first := protocol.BeginResultPackageParams{
		AttemptID: changesTestID(6023), PackageID: key.PackageID,
		LeaseExpiresAt: now.Add(time.Minute).Unix(), Metadata: metadata,
	}
	if _, err := state.BeginResultInbox(ctx, authority, first, now); err != nil {
		t.Fatal(err)
	}
	firstRemoval, err := state.PrepareResultInboxCancel(
		ctx, authority, first.AttemptID, first.PackageID, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.CommitResultInboxRemoval(ctx, firstRemoval, now); err != nil {
		t.Fatal(err)
	}
	if replay, err := state.PrepareResultInboxCancel(
		ctx, authority, first.AttemptID, first.PackageID, now,
	); err != nil || replay.Phase != ResultInboxRemovalCompleted {
		t.Fatalf("cancel replay = %#v, %v", replay, err)
	}
	second := first
	second.AttemptID = changesTestID(6024)
	second.LeaseExpiresAt = now.Add(2 * time.Minute).Unix()
	if _, err := state.BeginResultInbox(ctx, authority, second, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if replay, err := state.PrepareResultInboxCancel(
		ctx, authority, first.AttemptID, first.PackageID, now,
	); err != nil || replay.Phase != ResultInboxRemovalCompleted {
		t.Fatalf("late old-attempt cancel replay = %#v, %v", replay, err)
	}
	stored, err := state.GetResultInbox(ctx, authority, second.PackageID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != ResultInboxReceiving || stored.AttemptID != second.AttemptID {
		t.Fatalf("old cancellation changed new receive = %#v", stored)
	}
}

func TestResultInboxRemovalReceiptsAreBoundedAndPruneOldestCompleted(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	now := time.Unix(1_700_012_500, 0)
	err := withImmediateTransaction(ctx, state.db, "peer", func(connection *sql.Conn) error {
		for index := 0; index < MaximumResultInboxRemovalReceipts; index++ {
			if _, err := connection.ExecContext(ctx, `
INSERT INTO peer_result_inbox_attempt_receipts(
	controller_id, tree_id, root_agent_id, root_device_id,
	package_id, attempt_id, outcome, phase, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, 'cancelled', 'completed', ?, ?)
`, authority.ControllerID, authority.TreeID, authority.RootAgentID, authority.RootDeviceID,
				changesTestID(7100+index), changesTestID(7400+index),
				now.Add(time.Duration(index)*time.Second).Unix(),
				now.Add(time.Duration(index)*time.Second).Unix()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	key := resultInboxSourceKey(changesTestID(7990))
	params := protocol.BeginResultPackageParams{
		AttemptID: changesTestID(7991), PackageID: key.PackageID,
		LeaseExpiresAt: now.Add(9 * time.Minute).Unix(),
		Metadata:       resultMetadata(t, key, changesTestID(7992), changesTestID(7993), 0),
	}
	if _, err := state.BeginResultInbox(ctx, authority, params, now); err != nil {
		t.Fatal(err)
	}
	prepared, err := state.PrepareResultInboxCancel(
		ctx, authority, params.AttemptID, params.PackageID, now.Add(5*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := state.CommitResultInboxRemoval(ctx, prepared, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := state.ListCompletedResultInboxRemovals(
		ctx, authority.ControllerID, authority.RootDeviceID, MaximumResultInboxRemovalReceipts,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != MaximumResultInboxRemovalReceipts {
		t.Fatalf("completed receipt count = %d, want %d", len(receipts), MaximumResultInboxRemovalReceipts)
	}
	if receipts[0].PackageID != changesTestID(7101) {
		t.Fatalf("oldest retained receipt = %s, want second inserted package", receipts[0].PackageID)
	}
	oldest := ResultInboxRemoval{
		Authority: authority, PackageID: changesTestID(7100), AttemptID: changesTestID(7400),
		Outcome: ResultInboxRemovalCancelled, Phase: ResultInboxRemovalCompleted,
		CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if _, err := state.PrepareResultInboxCancel(
		ctx, authority, oldest.AttemptID, oldest.PackageID, now,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pruned receipt replay error = %v, want bounded-window ErrNotFound", err)
	}
	if replay, err := state.PrepareResultInboxCancel(
		ctx, authority, completed.AttemptID, completed.PackageID, now,
	); err != nil || replay.Phase != ResultInboxRemovalCompleted {
		t.Fatalf("newest receipt replay = %#v, %v", replay, err)
	}
	if err := state.DeleteCompletedResultInboxRemoval(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if err := state.DeleteCompletedResultInboxRemoval(ctx, completed); err != nil {
		t.Fatalf("completed receipt delete replay: %v", err)
	}
}

func TestResultInboxGCIsOldestFirstAndTombstoneIsRecoverable(t *testing.T) {
	ctx := context.Background()
	state := openResultPeer(t)
	defer state.Close()
	authority := resultInboxAuthority()
	now := time.Unix(1_700_013_000, 0)
	var packageIDs []string
	for index := 0; index < 2; index++ {
		key := resultInboxSourceKey(changesTestID(6030 + index))
		metadata := resultMetadata(
			t, key, changesTestID(6040+index), changesTestID(6050+index), 0,
		)
		params := protocol.BeginResultPackageParams{
			AttemptID: changesTestID(6060 + index), PackageID: key.PackageID,
			LeaseExpiresAt: now.Add(5 * time.Minute).Unix(), Metadata: metadata,
		}
		if _, err := state.BeginResultInbox(ctx, authority, params, now.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := state.CommitResultInboxAvailable(
			ctx, authority, params.AttemptID, params.PackageID,
			now.Add(time.Duration(index+1)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		packageIDs = append(packageIDs, params.PackageID)
	}
	available, err := state.ListAvailableResultInboxes(ctx, authority.ControllerID, authority.RootDeviceID, 10)
	if err != nil || len(available) != 2 || available[0].PackageID != packageIDs[0] ||
		available[1].PackageID != packageIDs[1] {
		t.Fatalf("available GC order = %#v, %v", available, err)
	}
	tombstone, err := state.PrepareResultInboxEviction(ctx, authority, packageIDs[0], now.Add(time.Minute))
	if err != nil || tombstone.State != ResultInboxEvictionTombstone {
		t.Fatalf("prepare eviction = %#v, %v", tombstone, err)
	}
	if _, err := state.PrepareResultInboxEviction(ctx, authority, packageIDs[0], now.Add(time.Minute)); err != nil {
		t.Fatalf("prepare eviction replay: %v", err)
	}
	availability, err := state.LookupResultInboxAvailability(ctx, authority, packageIDs[0])
	if err != nil || availability != ResultInboxAvailabilityEvicted {
		t.Fatalf("tombstone availability = %q, %v", availability, err)
	}
	tombstones, err := state.ListResultInboxEvictionTombstones(
		ctx, authority.ControllerID, authority.RootDeviceID, 10,
	)
	if err != nil || len(tombstones) != 1 || tombstones[0].PackageID != packageIDs[0] {
		t.Fatalf("eviction recovery list = %#v, %v", tombstones, err)
	}
	available, err = state.ListAvailableResultInboxes(ctx, authority.ControllerID, authority.RootDeviceID, 10)
	if err != nil || len(available) != 1 || available[0].PackageID != packageIDs[1] {
		t.Fatalf("post-tombstone available list = %#v, %v", available, err)
	}
	if err := state.CompactResultInboxEviction(ctx, authority, packageIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := state.CompactResultInboxEviction(ctx, authority, packageIDs[0]); err != nil {
		t.Fatalf("compact eviction replay: %v", err)
	}
	availability, err = state.LookupResultInboxAvailability(ctx, authority, packageIDs[0])
	if err != nil || availability != ResultInboxAvailabilityEvicted {
		t.Fatalf("compacted availability = %q, %v", availability, err)
	}
}

func TestResultInboxAdmissionEnforcesCountAndByteBudgets(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		ctx := context.Background()
		state := openResultPeer(t)
		defer state.Close()
		authority := resultInboxAuthority()
		now := time.Unix(1_700_014_000, 0)
		for index := 0; index < MaximumPeerResultPackages; index++ {
			key := resultInboxSourceKey(changesTestID(6100 + index))
			params := protocol.BeginResultPackageParams{
				AttemptID: changesTestID(6200 + index), PackageID: key.PackageID,
				LeaseExpiresAt: now.Add(time.Minute).Unix(),
				Metadata: resultMetadata(
					t, key, changesTestID(6300+index), changesTestID(6400+index), 0,
				),
			}
			if _, err := state.BeginResultInbox(ctx, authority, params, now); err != nil {
				t.Fatalf("begin package %d: %v", index, err)
			}
		}
		key := resultInboxSourceKey(changesTestID(6500))
		overflow := protocol.BeginResultPackageParams{
			AttemptID: changesTestID(6501), PackageID: key.PackageID,
			LeaseExpiresAt: now.Add(time.Minute).Unix(),
			Metadata:       resultMetadata(t, key, changesTestID(6502), changesTestID(6503), 0),
		}
		if _, err := state.BeginResultInbox(ctx, authority, overflow, now); !errors.Is(
			err, ErrResultPackageQuota,
		) {
			t.Fatalf("count overflow error = %v, want ErrResultPackageQuota", err)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		ctx := context.Background()
		state := openResultPeer(t)
		defer state.Close()
		authority := resultInboxAuthority()
		now := time.Unix(1_700_015_000, 0)
		var quotaErr error
		for index := 0; index < MaximumPeerResultPackages; index++ {
			key := resultInboxSourceKey(changesTestID(6600 + index))
			params := protocol.BeginResultPackageParams{
				AttemptID: changesTestID(6700 + index), PackageID: key.PackageID,
				LeaseExpiresAt: now.Add(time.Minute).Unix(),
				Metadata: resultMetadata(
					t, key, changesTestID(6800+index), changesTestID(6900+index),
					protocol.MaximumResultRolloutBytes,
				),
			}
			_, quotaErr = state.BeginResultInbox(ctx, authority, params, now)
			if quotaErr != nil {
				break
			}
		}
		if !errors.Is(quotaErr, ErrResultPackageQuota) {
			t.Fatalf("byte overflow error = %v, want ErrResultPackageQuota", quotaErr)
		}
		retention, err := state.GetResultInboxRetention(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if retention.Count >= MaximumPeerResultPackages || retention.Bytes > MaximumPeerResultStoreBytes {
			t.Fatalf("byte budget retention = %#v", retention)
		}
	})
}

func openResultPeer(t *testing.T) *PeerStore {
	t.Helper()
	state, err := OpenPeer(
		context.Background(), filepath.Join(t.TempDir(), "state", "peer.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func resultInboxAuthority() ResultInboxAuthority {
	return ResultInboxAuthority{
		ControllerID: workerControllerID,
		TreeID:       workerTreeID,
		RootAgentID:  changesTestID(6991),
		RootDeviceID: changesTestID(6992),
	}
}

func resultInboxSourceKey(packageID string) ResultOutboxKey {
	return ResultOutboxKey{
		WorkerKey: WorkerKey{
			ControllerID: workerControllerID,
			TreeID:       workerTreeID,
			AgentID:      changesTestID(6993),
		},
		SourceDeviceID: workerDeviceID,
		PackageID:      packageID,
	}
}

func resultChunk(
	kind protocol.ResultPackagePartKind,
	offset int64,
	data []byte,
) ResultInboxChunkCommit {
	digest := sha256.Sum256(data)
	return ResultInboxChunkCommit{
		Kind: kind, Offset: offset, Size: int64(len(data)), SHA256: fmt.Sprintf("%x", digest),
	}
}
