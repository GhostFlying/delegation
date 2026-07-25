package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/store"
)

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

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

	waitContext := &observedDoneContext{Context: context.Background(), observed: make(chan struct{})}
	type acquireResult struct {
		release func()
		err     error
	}
	waiter := make(chan acquireResult, 1)
	go func() {
		release, err := flights.acquire(waitContext, firstKey)
		waiter <- acquireResult{release: release, err: err}
	}()
	select {
	case <-waitContext.observed:
	case <-time.After(time.Second):
		t.Fatal("same-key waiter did not enter the active-flight wait")
	}
	select {
	case result := <-waiter:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("same-key waiter returned before release: %v", result.err)
	default:
	}

	releaseFirst()
	releaseFirst()
	result := <-waiter
	if result.err != nil {
		t.Fatalf("same-key waiter after release: %v", result.err)
	}
	result.release()
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
