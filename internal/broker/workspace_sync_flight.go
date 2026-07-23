package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/GhostFlying/delegation/internal/store"
)

type workspaceSyncFlight struct {
	done chan struct{}
	once sync.Once
}

type drainCanceledWorkspacePeerCallKey struct{}

func withCanceledWorkspacePeerCallDrain(ctx context.Context) context.Context {
	return context.WithValue(ctx, drainCanceledWorkspacePeerCallKey{}, true)
}

func drainsCanceledWorkspacePeerCalls(ctx context.Context) bool {
	drain, _ := ctx.Value(drainCanceledWorkspacePeerCallKey{}).(bool)
	return drain
}

type workspaceSyncFlights struct {
	mu      sync.Mutex
	active  map[store.WorkspaceSyncKey]*workspaceSyncFlight
	changed chan struct{}
	limit   int
}

func newWorkspaceSyncFlights(limit int) *workspaceSyncFlights {
	return &workspaceSyncFlights{
		active:  make(map[store.WorkspaceSyncKey]*workspaceSyncFlight),
		changed: make(chan struct{}),
		limit:   limit,
	}
}

func (f *workspaceSyncFlights) acquire(
	ctx context.Context,
	key store.WorkspaceSyncKey,
) (func(), error) {
	if f == nil || f.limit <= 0 {
		return nil, errors.New("workspace sync coordinator is unavailable")
	}
	for {
		f.mu.Lock()
		if current := f.active[key]; current != nil {
			done := current.done
			f.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if len(f.active) < f.limit {
			flight := &workspaceSyncFlight{done: make(chan struct{})}
			f.active[key] = flight
			f.mu.Unlock()
			return func() {
				flight.once.Do(func() {
					f.mu.Lock()
					if f.active[key] == flight {
						delete(f.active, key)
						close(flight.done)
						close(f.changed)
						f.changed = make(chan struct{})
					}
					f.mu.Unlock()
				})
			}, nil
		}
		changed := f.changed
		f.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
