//go:build !windows

package store

import "context"

func lockStateFileLifecycle(context.Context) (func(), error) {
	return func() {}, nil
}
