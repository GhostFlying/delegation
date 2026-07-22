//go:build darwin

package appserver

import (
	"errors"
	"testing"
	"time"
)

func TestAwaitDarwinSupervisorTerminationTimesOutWhenChildCannotBeReaped(t *testing.T) {
	childDone := make(chan darwinSupervisorChildResult, 1)
	terminated := make(chan struct{})
	start := time.Now()
	_, err := awaitDarwinSupervisorTermination(func() error {
		close(terminated)
		return nil
	}, childDone, 20*time.Millisecond)
	if !errors.Is(err, errDarwinSupervisorCleanup) {
		t.Fatalf("termination wait error = %v, want cleanup failure", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("termination wait exceeded its bound")
	}
	select {
	case <-terminated:
	default:
		t.Fatal("termination was not requested")
	}
	childDone <- darwinSupervisorChildResult{}
}

func TestAwaitDarwinSupervisorTerminationPreservesContainmentFailure(t *testing.T) {
	containErr := errors.New("containment failed")
	childDone := make(chan darwinSupervisorChildResult, 1)
	childDone <- darwinSupervisorChildResult{}
	child, err := awaitDarwinSupervisorTermination(func() error {
		return containErr
	}, childDone, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(child.containErr, containErr) {
		t.Fatalf("child containment error = %v", child.containErr)
	}
}
