//go:build darwin

package appserver

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDarwinDescendantTrackerBoundsActiveProcessesNotLifetimeChurn(t *testing.T) {
	tracker := &darwinDescendantTracker{active: make(map[int]darwinProcessIdentity)}
	const extraSequentialProcesses = 128
	for index := range maxDarwinTrackedProcesses + extraSequentialProcesses {
		identity := darwinProcessIdentity{PID: index + 100, StartSec: int64(index + 1)}
		if err := tracker.recordIdentity(identity); err != nil {
			t.Fatalf("record sequential identity %d: %v", index, err)
		}
		delete(tracker.active, identity.PID)
	}
	if want := uint64(maxDarwinTrackedProcesses + extraSequentialProcesses); tracker.revision != want {
		t.Fatalf("tracker revision = %d, want %d", tracker.revision, want)
	}
}

func TestDarwinDescendantTrackerRejectsTooManyActiveProcesses(t *testing.T) {
	tracker := &darwinDescendantTracker{active: make(map[int]darwinProcessIdentity)}
	for index := range maxDarwinTrackedProcesses {
		identity := darwinProcessIdentity{PID: index + 100, StartSec: int64(index + 1)}
		if err := tracker.recordIdentity(identity); err != nil {
			t.Fatalf("record active identity %d: %v", index, err)
		}
	}
	overflow := darwinProcessIdentity{PID: maxDarwinTrackedProcesses + 100, StartSec: 1}
	if err := tracker.recordIdentity(overflow); err == nil {
		t.Fatal("tracker accepted more than the active process limit")
	}
	if tracker.active[overflow.PID] != overflow {
		t.Fatal("overflow identity was not retained for fail-closed containment")
	}
}

func TestDarwinDescendantTrackerRebindsReusedPIDKnote(t *testing.T) {
	oldIdentity := darwinProcessIdentity{PID: 42, StartSec: 1}
	newIdentity := darwinProcessIdentity{PID: 42, StartSec: 2}
	newInfo := darwinProcessInfo{Identity: newIdentity, Parent: 7}
	for _, existing := range []map[int]darwinProcessIdentity{
		{},
		{oldIdentity.PID: oldIdentity},
	} {
		t.Run(fmt.Sprintf("existing-%t", len(existing) != 0), func(t *testing.T) {
			var operations []string
			tracker := &darwinDescendantTracker{
				active: existing,
				readProcessInfo: func(int) (darwinProcessInfo, bool, error) {
					return newInfo, true, nil
				},
				listChildPIDs: func(int) ([]int, error) { return nil, nil },
				unwatchPID: func(pid int) error {
					operations = append(operations, fmt.Sprintf("unwatch:%d", pid))
					return nil
				},
				watchPID: func(pid int) error {
					operations = append(operations, fmt.Sprintf("watch:%d", pid))
					return nil
				},
			}
			if err := tracker.addProcess(newInfo, newInfo.Parent); err != nil {
				t.Fatal(err)
			}
			wantOperations := []string{"unwatch:42", "watch:42"}
			if !reflect.DeepEqual(operations, wantOperations) {
				t.Fatalf("knote operations = %#v, want %#v", operations, wantOperations)
			}
			if tracker.active[newIdentity.PID] != newIdentity {
				t.Fatalf("active identity = %#v, want %#v", tracker.active[newIdentity.PID], newIdentity)
			}
		})
	}
}

func TestDarwinDescendantTrackerIgnoresStaleExitForLiveIdentity(t *testing.T) {
	identity := darwinProcessIdentity{PID: 42, StartSec: 2}
	processExists := true
	tracker := &darwinDescendantTracker{
		active: map[int]darwinProcessIdentity{identity.PID: identity},
		readProcessInfo: func(int) (darwinProcessInfo, bool, error) {
			return darwinProcessInfo{Identity: identity}, processExists, nil
		},
	}
	if err := tracker.removeExitedIdentity(identity); err != nil {
		t.Fatal(err)
	}
	if tracker.active[identity.PID] != identity {
		t.Fatal("stale NOTE_EXIT removed the live reused PID identity")
	}
	processExists = false
	if err := tracker.removeExitedIdentity(identity); err != nil {
		t.Fatal(err)
	}
	if _, found := tracker.active[identity.PID]; found {
		t.Fatal("exited PID identity remained active")
	}
}

func TestDarwinDescendantTrackerResumesStoppedReusedPID(t *testing.T) {
	expected := darwinProcessIdentity{PID: 42, StartSec: 1}
	replacement := darwinProcessIdentity{PID: 42, StartSec: 2}
	reads := []darwinProcessInfo{
		{Identity: expected},
		{Identity: replacement, Status: darwinProcessStopped},
		{Identity: replacement, Status: darwinProcessStopped},
	}
	var signals []unix.Signal
	tracker := &darwinDescendantTracker{
		active:       map[int]darwinProcessIdentity{expected.PID: expected},
		stopRequests: make(map[int]darwinProcessIdentity),
		readProcessInfo: func(int) (darwinProcessInfo, bool, error) {
			if len(reads) == 0 {
				t.Fatal("unexpected process identity read")
			}
			result := reads[0]
			reads = reads[1:]
			return result, true, nil
		},
		signalPID: func(_ int, signal unix.Signal) error {
			signals = append(signals, signal)
			return nil
		},
	}
	stopped, err := tracker.stopIdentity(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !stopped {
		t.Fatal("stopIdentity did not finish after PID reuse")
	}
	if want := []unix.Signal{unix.SIGSTOP, unix.SIGCONT}; !reflect.DeepEqual(signals, want) {
		t.Fatalf("signals = %#v, want %#v", signals, want)
	}
	if _, found := tracker.active[expected.PID]; found {
		t.Fatal("reused PID remained in the managed identity set")
	}
	if _, found := tracker.stopRequests[expected.PID]; found {
		t.Fatal("managed stop request remained after compensation")
	}
}

func TestDarwinDescendantTrackerDoesNotTriggerClosedQueue(t *testing.T) {
	var closed []int
	triggered := false
	tracker := &darwinDescendantTracker{
		queue: 42,
		closeQueue: func(queue int) error {
			closed = append(closed, queue)
			return nil
		},
		triggerQueue: func(int) error {
			triggered = true
			return nil
		},
	}
	if err := tracker.closeControlQueue(); err != nil {
		t.Fatal(err)
	}
	if err := tracker.triggerControlEvent(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(closed, []int{42}) || triggered || tracker.queue != -1 {
		t.Fatalf("closed = %#v, triggered = %t, queue = %d", closed, triggered, tracker.queue)
	}
}

func TestDarwinDescendantTrackerContainsPartialChildEnumeration(t *testing.T) {
	root := darwinProcessIdentity{PID: 40, StartSec: 1}
	child := darwinProcessIdentity{PID: 42, StartSec: 2}
	enumerationErr := fmt.Errorf("injected child enumeration overflow")
	tracker := &darwinDescendantTracker{
		root: root, active: map[int]darwinProcessIdentity{root.PID: root},
		readProcessInfo: func(pid int) (darwinProcessInfo, bool, error) {
			switch pid {
			case root.PID:
				return darwinProcessInfo{Identity: root}, true, nil
			case child.PID:
				return darwinProcessInfo{Identity: child, Parent: root.PID}, true, nil
			default:
				return darwinProcessInfo{}, false, nil
			}
		},
		listChildPIDs: func(pid int) ([]int, error) {
			if pid == root.PID {
				return []int{child.PID}, enumerationErr
			}
			return nil, nil
		},
		watchPID:   func(int) error { return nil },
		unwatchPID: func(int) error { return nil },
	}
	err := tracker.discoverChildren(root)
	if !errors.Is(err, enumerationErr) {
		t.Fatalf("discover children error = %v, want enumeration error", err)
	}
	if tracker.active[child.PID] != child {
		t.Fatal("partially enumerated child was not retained for containment")
	}
}

func TestDarwinDescendantTrackerFailsClosedOnSignalPermissionError(t *testing.T) {
	identity := darwinProcessIdentity{PID: 42, StartSec: 2}
	tracker := &darwinDescendantTracker{
		root: identity, active: map[int]darwinProcessIdentity{identity.PID: identity},
		readProcessInfo: func(int) (darwinProcessInfo, bool, error) {
			return darwinProcessInfo{Identity: identity}, true, nil
		},
		listChildPIDs: func(int) ([]int, error) { return nil, nil },
		signalPID:     func(int, unix.Signal) error { return unix.EPERM },
	}
	if stopped, err := tracker.stopIdentity(identity); stopped || !errors.Is(err, unix.EPERM) {
		t.Fatalf("stop identity = %t, %v; want EPERM", stopped, err)
	}
	now := time.Now()
	err := tracker.containUntil(now, now)
	if !errors.Is(err, unix.EPERM) {
		t.Fatalf("contain error = %v, want EPERM", err)
	}
	if tracker.active[identity.PID] != identity {
		t.Fatal("permission failure removed an uncontained process identity")
	}
}

func TestDarwinDescendantTrackerDiscoversAllBranchesAfterError(t *testing.T) {
	first := darwinProcessIdentity{PID: 41, StartSec: 1}
	second := darwinProcessIdentity{PID: 42, StartSec: 1}
	child := darwinProcessIdentity{PID: 43, StartSec: 2}
	firstErr := errors.New("injected first branch error")
	tracker := &darwinDescendantTracker{
		active: map[int]darwinProcessIdentity{first.PID: first, second.PID: second},
		readProcessInfo: func(pid int) (darwinProcessInfo, bool, error) {
			switch pid {
			case first.PID:
				return darwinProcessInfo{Identity: first}, true, nil
			case second.PID:
				return darwinProcessInfo{Identity: second}, true, nil
			case child.PID:
				return darwinProcessInfo{Identity: child, Parent: second.PID}, true, nil
			default:
				return darwinProcessInfo{}, false, nil
			}
		},
		listChildPIDs: func(pid int) ([]int, error) {
			switch pid {
			case first.PID:
				return nil, firstErr
			case second.PID:
				return []int{child.PID}, nil
			default:
				return nil, nil
			}
		},
		watchPID:   func(int) error { return nil },
		unwatchPID: func(int) error { return nil },
	}
	if err := tracker.discoverAll(); !errors.Is(err, firstErr) {
		t.Fatalf("discover all error = %v, want first branch error", err)
	}
	if tracker.active[child.PID] != child {
		t.Fatal("later branch child was not retained after an earlier branch failed")
	}
}
