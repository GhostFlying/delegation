//go:build !darwin

package workerhost

import "context"

func resolveWorkerGitBinary(_ context.Context, configured string) (string, error) {
	return configured, nil
}
