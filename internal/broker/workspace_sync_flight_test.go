package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/GhostFlying/delegation/internal/store"
)

func TestWorkspaceSyncFlightsAreBoundedContextAwareAndReleased(t *testing.T) {
	flights := newWorkspaceSyncFlights(1)
	firstKey := store.WorkspaceSyncKey{SyncID: "first"}
	secondKey := store.WorkspaceSyncKey{SyncID: "second"}
	releaseFirst, err := flights.acquire(context.Background(), firstKey)
	if err != nil {
		t.Fatal(err)
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := flights.acquire(canceledContext, firstKey); !errors.Is(err, context.Canceled) {
		t.Fatalf("same-key waiter = %v, want cancellation", err)
	}
	if _, err := flights.acquire(canceledContext, secondKey); !errors.Is(err, context.Canceled) {
		t.Fatalf("capacity waiter = %v, want cancellation", err)
	}

	releaseFirst()
	releaseFirst()
	flights.mu.Lock()
	active := len(flights.active)
	flights.mu.Unlock()
	if active != 0 {
		t.Fatalf("active flights after release = %d", active)
	}
	releaseSecond, err := flights.acquire(context.Background(), secondKey)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond()
}
