package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliverPendingResultPackageUsesRootRelayLock(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	_, root := resultPackageRelayPrincipals()
	record.RootPrincipal = root
	server := &Server{}
	key := resultPackageRootRelayKey{
		controllerID: root.ControllerID,
		deviceID:     root.DeviceID,
	}
	release, err := server.resultRelayRoots.acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, deliverErr := server.deliverPendingResultPackage(context.Background(), record)
		done <- deliverErr
	}()
	select {
	case err := <-done:
		t.Fatalf("pending delivery bypassed its root lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, errResultPackageRelayDeferred) {
			t.Fatalf("delivery after lock release = %v, want deferred offline source", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending delivery did not continue after root lock release")
	}
}

func TestResultPackageRootRelayLockClosesColdStateOrdinalWindow(t *testing.T) {
	var locks resultPackageRootRelayLocks
	key := resultPackageRootRelayKey{controllerID: resultRelayTreeID, deviceID: resultRelayRootPeer}
	firstRelease, err := locks.acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}

	var brokerHighwater atomic.Uint64
	rootCounter := uint64(1)
	secondOrdinal := make(chan uint64, 1)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		release, acquireErr := locks.acquire(context.Background(), key)
		if acquireErr != nil {
			secondOrdinal <- 0
			return
		}
		defer release()
		floor := brokerHighwater.Load()
		rootCounter = max(rootCounter, floor) + 1
		secondOrdinal <- rootCounter
	}()
	<-secondStarted

	// The root loses its local counter after package A became available with
	// ordinal 1, but before the broker records that delivery. Package B must not
	// observe the recreated counter until A's broker mark is durable.
	rootCounter = 0
	select {
	case ordinal := <-secondOrdinal:
		t.Fatalf("second relay allocated ordinal %d before the first broker mark", ordinal)
	case <-time.After(20 * time.Millisecond):
	}
	brokerHighwater.Store(1)
	firstRelease()
	select {
	case ordinal := <-secondOrdinal:
		if ordinal != 2 {
			t.Fatalf("second relay ordinal = %d, want 2", ordinal)
		}
	case <-time.After(time.Second):
		t.Fatal("second relay did not continue after the first broker mark")
	}
}

func TestResultPackageRootRelayLocksArePerRootAndCancelable(t *testing.T) {
	var locks resultPackageRootRelayLocks
	first := resultPackageRootRelayKey{controllerID: resultRelayTreeID, deviceID: resultRelayRootPeer}
	second := resultPackageRootRelayKey{controllerID: resultRelayTreeID, deviceID: resultRelaySourceID}
	releaseFirst, err := locks.acquire(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := locks.acquire(context.Background(), second)
	if err != nil {
		t.Fatal("different root was serialized", err)
	}
	releaseSecond()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := locks.acquire(ctx, first); err == nil {
		t.Fatal("canceled root-lock wait succeeded")
	}
	releaseFirst()
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.locks) != 0 {
		t.Fatalf("released root locks retained %d entries", len(locks.locks))
	}
}
