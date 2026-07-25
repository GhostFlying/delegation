//go:build darwin

package workerhost

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	darwinXcrunBinary          = "/usr/bin/xcrun"
	darwinGitResolutionTimeout = 10 * time.Second
)

func resolveWorkerGitBinary(ctx context.Context, _ string) (string, error) {
	// Homebrew Git loads formula libraries through @loader_path paths containing
	// parent traversals. Making those work in Codex's split filesystem profile
	// requires exposing the whole Homebrew prefix, so managed workers use the
	// developer Git runtime located outside the sandbox by the trusted connector.
	resolveContext, cancel := context.WithTimeout(ctx, darwinGitResolutionTimeout)
	defer cancel()
	output, err := exec.CommandContext(resolveContext, darwinXcrunBinary, "--find", "git").Output()
	if err != nil {
		if errors.Is(resolveContext.Err(), context.DeadlineExceeded) {
			return "", errors.New("locate managed worker Git binary: xcrun timed out")
		}
		return "", fmt.Errorf("locate managed worker Git binary: %w", err)
	}
	path := strings.TrimSpace(string(output))
	if !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n") {
		return "", errors.New("locate managed worker Git binary: xcrun returned an invalid path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve managed worker Git binary: %w", err)
	}
	if err := requireRegularFile(resolved, "managed worker Git binary"); err != nil {
		return "", err
	}
	return resolved, nil
}
