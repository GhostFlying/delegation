//go:build windows

package store

import "context"

var stateFileLifecyclePermit = func() chan struct{} {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	return permit
}()

func lockStateFileLifecycle(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-stateFileLifecyclePermit:
		return func() { stateFileLifecyclePermit <- struct{}{} }, nil
	}
}
