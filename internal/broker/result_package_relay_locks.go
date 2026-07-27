package broker

import (
	"context"
	"sync"
)

type resultPackageRootRelayKey struct {
	controllerID string
	deviceID     string
}

type resultPackageRootRelayLock struct {
	token chan struct{}
	refs  int
}

// resultPackageRootRelayLocks serializes retention-ordinal allocation through
// the broker delivery mark for one root peer. Its zero value is ready to use.
type resultPackageRootRelayLocks struct {
	mu    sync.Mutex
	locks map[resultPackageRootRelayKey]*resultPackageRootRelayLock
}

func (l *resultPackageRootRelayLocks) acquire(
	ctx context.Context,
	key resultPackageRootRelayKey,
) (func(), error) {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[resultPackageRootRelayKey]*resultPackageRootRelayLock)
	}
	entry := l.locks[key]
	if entry == nil {
		entry = &resultPackageRootRelayLock{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.dropReference(key, entry)
		return nil, ctx.Err()
	case <-entry.token:
		if err := ctx.Err(); err != nil {
			entry.token <- struct{}{}
			l.dropReference(key, entry)
			return nil, err
		}
	}

	return func() {
		entry.token <- struct{}{}
		l.dropReference(key, entry)
	}, nil
}

func (l *resultPackageRootRelayLocks) dropReference(
	key resultPackageRootRelayKey,
	entry *resultPackageRootRelayLock,
) {
	l.mu.Lock()
	entry.refs--
	if entry.refs == 0 && l.locks[key] == entry {
		delete(l.locks, key)
	}
	l.mu.Unlock()
}
