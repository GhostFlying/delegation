//go:build linux

package appserver

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestRepeatedClientLifecycleClosesOutputDescriptors(t *testing.T) {
	baseline := openDescriptorCount(t)
	for range 6 {
		client := startHelperClient(t, "normal", Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := client.Close(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		for range client.Notifications() {
		}
	}
	if current := openDescriptorCount(t); current > baseline+2 {
		t.Fatalf("open descriptors grew from %d to %d", baseline, current)
	}
}

func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
