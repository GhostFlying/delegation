//go:build darwin

package appserver

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	// proc_info exposes the same child enumeration used by proc_listchildpids
	// without requiring cgo or a libproc dynamic loader.
	darwinProcInfoCallListPIDs = 1
	darwinProcParentPIDOnly    = 6
	darwinProcessStopped       = 4
	darwinProcessZombie        = 5
	darwinTrackerControlIdent  = 1
	darwinTrackerPollInterval  = 100 * time.Millisecond
	darwinFreezeTimeout        = 250 * time.Millisecond
	darwinContainmentTimeout   = time.Second
	maxDarwinTrackedProcesses  = 4096
	maxDarwinEnumeratedPIDs    = 1 << 20
)

type darwinProcessIdentity struct {
	PID       int
	StartSec  int64
	StartUsec int32
}

type darwinProcessInfo struct {
	Identity darwinProcessIdentity
	Parent   int
	Status   int8
}

// darwinDescendantTracker records ancestry while it remains discoverable.
// Process groups are insufficient because Codex shell and MCP children may
// call setsid. An immediately daemonizing, double-forking child can reparent
// before macOS exposes its identity; this is lifecycle cleanup, not a hostile
// same-UID containment boundary.
type darwinDescendantTracker struct {
	queueMu          sync.Mutex
	queue            int
	root             darwinProcessIdentity
	active           map[int]darwinProcessIdentity
	stopRequests     map[int]darwinProcessIdentity
	revision         uint64
	fatal            chan error
	terminateRequest chan struct{}
	done             chan struct{}
	terminateOnce    sync.Once
	resultMu         sync.Mutex
	result           error
	readProcessInfo  func(int) (darwinProcessInfo, bool, error)
	listChildPIDs    func(int) ([]int, error)
	watchPID         func(int) error
	unwatchPID       func(int) error
	signalPID        func(int, unix.Signal) error
	triggerQueue     func(int) error
	closeQueue       func(int) error
}

func newDarwinDescendantTracker(rootPID int) (*darwinDescendantTracker, error) {
	queue, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("create descendant tracker kqueue: %w", err)
	}
	tracker := &darwinDescendantTracker{
		queue: queue, active: map[int]darwinProcessIdentity{},
		stopRequests:     map[int]darwinProcessIdentity{},
		fatal:            make(chan error, 1),
		terminateRequest: make(chan struct{}, 1), done: make(chan struct{}),
	}
	if err := tracker.registerControlEvent(); err != nil {
		_ = unix.Close(queue)
		return nil, fmt.Errorf("register descendant tracker control event: %w", err)
	}
	root, exists, err := readDarwinProcessInfo(rootPID)
	if err != nil {
		_ = unix.Close(queue)
		return nil, fmt.Errorf("inspect managed app-server: %w", err)
	}
	if !exists {
		_ = unix.Close(queue)
		return nil, errors.New("managed app-server exited before descendant tracking started")
	}
	tracker.root = root.Identity
	if err := tracker.addProcess(root, -1); err != nil {
		containErr := tracker.contain()
		_ = unix.Close(queue)
		return nil, errors.Join(fmt.Errorf("track managed app-server descendants: %w", err), containErr)
	}
	go tracker.run()
	return tracker, nil
}

func (t *darwinDescendantTracker) Fatal() <-chan error {
	return t.fatal
}

func (t *darwinDescendantTracker) Terminate() error {
	t.terminateOnce.Do(func() {
		t.terminateRequest <- struct{}{}
		_ = t.triggerControlEvent()
	})
	<-t.done
	t.resultMu.Lock()
	defer t.resultMu.Unlock()
	return t.result
}

func (t *darwinDescendantTracker) run() {
	defer close(t.done)
	defer t.closeControlQueue()
	events := make([]unix.Kevent_t, 32)
	for {
		if t.terminationRequested() {
			t.setResult(t.contain())
			return
		}
		timeout := unix.NsecToTimespec(darwinTrackerPollInterval.Nanoseconds())
		count, err := unix.Kevent(t.queue, nil, events, &timeout)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			t.failClosed(fmt.Errorf("monitor managed process descendants: %w", err))
			return
		}
		for _, event := range events[:count] {
			if event.Filter == unix.EVFILT_USER && event.Ident == darwinTrackerControlIdent {
				continue
			}
			if event.Filter != unix.EVFILT_PROC {
				continue
			}
			pid := int(event.Ident)
			identity, tracked := t.active[pid]
			if !tracked {
				continue
			}
			if event.Flags&unix.EV_ERROR != 0 {
				eventErr := unix.Errno(event.Data)
				if errors.Is(eventErr, unix.ESRCH) {
					if err := t.removeExitedIdentity(identity); err != nil {
						t.failClosed(fmt.Errorf("confirm managed descendant %d exit: %w", pid, err))
						return
					}
					continue
				}
				t.failClosed(fmt.Errorf("monitor managed descendant %d: %w", pid, eventErr))
				return
			}
			if event.Fflags&unix.NOTE_FORK != 0 {
				if err := t.discoverChildren(identity); err != nil {
					t.failClosed(fmt.Errorf("discover managed descendants: %w", err))
					return
				}
			}
			if event.Fflags&unix.NOTE_EXIT != 0 {
				if err := t.removeExitedIdentity(identity); err != nil {
					t.failClosed(fmt.Errorf("confirm managed descendant %d exit: %w", pid, err))
					return
				}
			}
		}
	}
}

func (t *darwinDescendantTracker) failClosed(cause error) {
	containErr := t.contain()
	// Fatal reports why monitoring stopped; Terminate reports only whether the
	// subsequent containment pass could confirm cleanup.
	t.setResult(containErr)
	select {
	case t.fatal <- cause:
	default:
	}
}

func (t *darwinDescendantTracker) contain() error {
	now := time.Now()
	return t.containUntil(now.Add(darwinFreezeTimeout), now.Add(darwinContainmentTimeout))
}

func (t *darwinDescendantTracker) containUntil(freezeDeadline, deadline time.Time) error {
	var result error
	// Stop the root first and then close the transitive child set to a fixed
	// point. Once every known identity is stopped, none can fork while the set
	// is killed.
	stablePasses := 0
	for time.Now().Before(freezeDeadline) {
		revisionBefore := t.revision
		if err := t.discoverAll(); err != nil {
			result = errors.Join(result, fmt.Errorf("refresh managed descendants: %w", err))
		}
		allStopped := true
		for _, identity := range t.activeSnapshot(true) {
			stopped, err := t.stopIdentity(identity)
			if err != nil {
				result = errors.Join(result, err)
			}
			allStopped = allStopped && stopped
		}
		if allStopped && revisionBefore == t.revision {
			stablePasses++
			if stablePasses == 2 {
				break
			}
		} else {
			stablePasses = 0
		}
		time.Sleep(5 * time.Millisecond)
	}
	if stablePasses < 2 && len(t.active) != 0 {
		result = errors.Join(result, errors.New("managed descendants did not reach a stable stopped state"))
	}

	for _, identity := range t.activeSnapshot(false) {
		if err := t.signalIdentity(identity, unix.SIGKILL); err != nil {
			result = errors.Join(result, err)
		}
	}
	for len(t.active) != 0 && time.Now().Before(deadline) {
		for _, identity := range t.activeSnapshot(false) {
			info, exists, err := t.processInfo(identity.PID)
			if err != nil {
				result = errors.Join(result, fmt.Errorf("confirm managed descendant %d exit: %w", identity.PID, err))
				continue
			}
			if !exists || info.Identity != identity || info.Status == darwinProcessZombie {
				var observed *darwinProcessInfo
				if exists {
					observed = &info
				}
				if err := t.forgetIdentity(identity, observed); err != nil {
					result = errors.Join(result, err)
				}
				continue
			}
			if err := t.signalIdentity(identity, unix.SIGKILL); err != nil {
				result = errors.Join(result, err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(t.active) != 0 {
		result = errors.Join(result, fmt.Errorf("%d managed descendants survived forced cleanup", len(t.active)))
	}
	return result
}

func (t *darwinDescendantTracker) stopIdentity(identity darwinProcessIdentity) (bool, error) {
	info, exists, err := t.processInfo(identity.PID)
	if err != nil {
		return false, fmt.Errorf("inspect managed descendant %d before stop: %w", identity.PID, err)
	}
	if !exists || info.Identity != identity || info.Status == darwinProcessZombie {
		var observed *darwinProcessInfo
		if exists {
			observed = &info
		}
		return true, t.forgetIdentity(identity, observed)
	}
	if info.Status == darwinProcessStopped {
		return true, nil
	}
	if err := t.signalProcess(identity.PID, unix.SIGSTOP); err != nil {
		if errors.Is(err, unix.ESRCH) {
			delete(t.active, identity.PID)
			return true, nil
		}
		return false, fmt.Errorf("stop managed descendant %d: %w", identity.PID, err)
	}
	if t.stopRequests == nil {
		t.stopRequests = make(map[int]darwinProcessIdentity)
	}
	t.stopRequests[identity.PID] = identity
	confirmed, exists, err := t.processInfo(identity.PID)
	if err != nil {
		return false, fmt.Errorf("confirm managed descendant %d after stop: %w", identity.PID, err)
	}
	if !exists || confirmed.Identity != identity || confirmed.Status == darwinProcessZombie {
		var observed *darwinProcessInfo
		if exists {
			observed = &confirmed
		}
		return true, t.forgetIdentity(identity, observed)
	}
	return confirmed.Status == darwinProcessStopped, nil
}

func (t *darwinDescendantTracker) signalIdentity(identity darwinProcessIdentity, signal unix.Signal) error {
	info, exists, err := t.processInfo(identity.PID)
	if err != nil {
		return fmt.Errorf("inspect managed descendant %d before signal: %w", identity.PID, err)
	}
	if !exists || info.Identity != identity || info.Status == darwinProcessZombie {
		var observed *darwinProcessInfo
		if exists {
			observed = &info
		}
		return t.forgetIdentity(identity, observed)
	}
	if err := t.signalProcess(identity.PID, signal); err != nil {
		if errors.Is(err, unix.ESRCH) {
			delete(t.active, identity.PID)
			return nil
		}
		return fmt.Errorf("signal managed descendant %d: %w", identity.PID, err)
	}
	return nil
}

func (t *darwinDescendantTracker) discoverAll() error {
	var result error
	for _, identity := range t.activeSnapshot(false) {
		if err := t.discoverChildren(identity); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (t *darwinDescendantTracker) discoverChildren(parent darwinProcessIdentity) error {
	current, exists, err := t.processInfo(parent.PID)
	if err != nil {
		return err
	}
	if !exists || current.Identity != parent || current.Status == darwinProcessZombie {
		var observed *darwinProcessInfo
		if exists {
			observed = &current
		}
		return t.forgetIdentity(parent, observed)
	}
	children, result := t.childPIDs(parent.PID)
	for _, pid := range children {
		child, exists, err := t.processInfo(pid)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if !exists || child.Parent != parent.PID {
			continue
		}
		if err := t.addProcess(child, parent.PID); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (t *darwinDescendantTracker) addProcess(info darwinProcessInfo, expectedParent int) error {
	var replacementErr error
	if existing, found := t.active[info.Identity.PID]; found {
		if existing == info.Identity {
			return nil
		}
		replacementErr = t.forgetIdentity(existing, &info)
	}
	// EVFILT_PROC knotes are keyed only by PID. Delete first even when active
	// has no entry, because a delivered or queued NOTE_EXIT may have removed
	// the identity while leaving the old knote awaiting event consumption.
	if err := t.unwatchProcess(info.Identity.PID); err != nil {
		return fmt.Errorf("replace managed process identity watch: %w", err)
	}
	if expectedParent >= 0 && info.Parent != expectedParent {
		return replacementErr
	}
	if err := t.watchProcess(info.Identity.PID); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return nil
		}
		return err
	}
	confirmed, exists, err := t.processInfo(info.Identity.PID)
	if err != nil {
		return err
	}
	if !exists || confirmed.Identity != info.Identity ||
		(expectedParent >= 0 && confirmed.Parent != expectedParent) {
		_ = t.unwatchProcess(info.Identity.PID)
		return nil
	}
	limitErr := t.recordIdentity(info.Identity)
	discoveryErr := t.discoverChildren(info.Identity)
	return errors.Join(replacementErr, limitErr, discoveryErr)
}

func (t *darwinDescendantTracker) recordIdentity(identity darwinProcessIdentity) error {
	existing, found := t.active[identity.PID]
	if found && existing == identity {
		return nil
	}
	t.active[identity.PID] = identity
	t.revision++
	if !found && len(t.active) > maxDarwinTrackedProcesses {
		return fmt.Errorf("managed process tree exceeds %d active identities", maxDarwinTrackedProcesses)
	}
	return nil
}

func (t *darwinDescendantTracker) activeSnapshot(rootFirst bool) []darwinProcessIdentity {
	identities := make([]darwinProcessIdentity, 0, len(t.active))
	unsortedStart := 0
	if rootFirst {
		if root, ok := t.active[t.root.PID]; ok && root == t.root {
			identities = append(identities, root)
			unsortedStart = 1
		}
	}
	for _, identity := range t.active {
		if rootFirst && identity == t.root {
			continue
		}
		identities = append(identities, identity)
	}
	sort.Slice(identities[unsortedStart:], func(left, right int) bool {
		return identities[unsortedStart+left].PID < identities[unsortedStart+right].PID
	})
	return identities
}

func (t *darwinDescendantTracker) watchProcess(pid int) error {
	if t.watchPID != nil {
		return t.watchPID(pid)
	}
	change := unix.Kevent_t{
		Ident: uint64(pid), Filter: unix.EVFILT_PROC, Flags: unix.EV_ADD | unix.EV_CLEAR,
		Fflags: unix.NOTE_FORK | unix.NOTE_EXEC | unix.NOTE_EXIT,
	}
	_, err := unix.Kevent(t.queue, []unix.Kevent_t{change}, nil, nil)
	return err
}

func (t *darwinDescendantTracker) unwatchProcess(pid int) error {
	if t.unwatchPID != nil {
		return t.unwatchPID(pid)
	}
	change := unix.Kevent_t{Ident: uint64(pid), Filter: unix.EVFILT_PROC, Flags: unix.EV_DELETE}
	_, err := unix.Kevent(t.queue, []unix.Kevent_t{change}, nil, nil)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func (t *darwinDescendantTracker) processInfo(pid int) (darwinProcessInfo, bool, error) {
	if t.readProcessInfo != nil {
		return t.readProcessInfo(pid)
	}
	return readDarwinProcessInfo(pid)
}

func (t *darwinDescendantTracker) childPIDs(pid int) ([]int, error) {
	if t.listChildPIDs != nil {
		return t.listChildPIDs(pid)
	}
	return listDarwinChildPIDs(pid)
}

func (t *darwinDescendantTracker) signalProcess(pid int, signal unix.Signal) error {
	if t.signalPID != nil {
		return t.signalPID(pid, signal)
	}
	return unix.Kill(pid, signal)
}

func (t *darwinDescendantTracker) removeExitedIdentity(identity darwinProcessIdentity) error {
	info, exists, err := t.processInfo(identity.PID)
	if err != nil {
		return err
	}
	if exists && info.Identity == identity && info.Status != darwinProcessZombie {
		return nil
	}
	var observed *darwinProcessInfo
	if exists {
		observed = &info
	}
	return t.forgetIdentity(identity, observed)
}

func (t *darwinDescendantTracker) forgetIdentity(
	identity darwinProcessIdentity,
	observed *darwinProcessInfo,
) error {
	if t.active[identity.PID] == identity {
		delete(t.active, identity.PID)
	}
	stopIdentity, stoppedByTracker := t.stopRequests[identity.PID]
	if !stoppedByTracker || stopIdentity != identity {
		return nil
	}
	delete(t.stopRequests, identity.PID)
	if observed == nil || observed.Identity == identity || observed.Status != darwinProcessStopped {
		return nil
	}
	replacement, exists, err := t.processInfo(identity.PID)
	if err != nil {
		return fmt.Errorf("inspect reused process %d before resume: %w", identity.PID, err)
	}
	if !exists || replacement.Identity != observed.Identity || replacement.Status != darwinProcessStopped {
		return nil
	}
	if err := t.signalProcess(identity.PID, unix.SIGCONT); err != nil && !errors.Is(err, unix.ESRCH) {
		return fmt.Errorf("resume reused process %d after managed stop: %w", identity.PID, err)
	}
	return nil
}

func (t *darwinDescendantTracker) registerControlEvent() error {
	change := unix.Kevent_t{
		Ident: darwinTrackerControlIdent, Filter: unix.EVFILT_USER,
		Flags: unix.EV_ADD | unix.EV_CLEAR,
	}
	_, err := unix.Kevent(t.queue, []unix.Kevent_t{change}, nil, nil)
	return err
}

func (t *darwinDescendantTracker) triggerControlEvent() error {
	t.queueMu.Lock()
	defer t.queueMu.Unlock()
	if t.queue < 0 {
		return nil
	}
	if t.triggerQueue != nil {
		return t.triggerQueue(t.queue)
	}
	change := unix.Kevent_t{
		Ident: darwinTrackerControlIdent, Filter: unix.EVFILT_USER,
		Fflags: unix.NOTE_TRIGGER,
	}
	_, err := unix.Kevent(t.queue, []unix.Kevent_t{change}, nil, nil)
	return err
}

func (t *darwinDescendantTracker) closeControlQueue() error {
	t.queueMu.Lock()
	defer t.queueMu.Unlock()
	if t.queue < 0 {
		return nil
	}
	queue := t.queue
	t.queue = -1
	if t.closeQueue != nil {
		return t.closeQueue(queue)
	}
	return unix.Close(queue)
}

func (t *darwinDescendantTracker) terminationRequested() bool {
	select {
	case <-t.terminateRequest:
		return true
	default:
		return false
	}
}

func (t *darwinDescendantTracker) setResult(err error) {
	t.resultMu.Lock()
	t.result = err
	t.resultMu.Unlock()
}

func readDarwinProcessInfo(pid int) (darwinProcessInfo, bool, error) {
	if pid <= 0 {
		return darwinProcessInfo{}, false, nil
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
		return darwinProcessInfo{}, false, nil
	}
	if err != nil {
		return darwinProcessInfo{}, false, err
	}
	if len(processes) == 0 {
		return darwinProcessInfo{}, false, nil
	}
	if len(processes) != 1 || int(processes[0].Proc.P_pid) != pid {
		return darwinProcessInfo{}, false, errors.New("kernel returned an inconsistent process identity")
	}
	process := processes[0]
	if process.Proc.P_starttime.Sec == 0 && process.Proc.P_starttime.Usec == 0 {
		return darwinProcessInfo{}, false, errors.New("kernel returned a process without a start time")
	}
	return darwinProcessInfo{
		Identity: darwinProcessIdentity{
			PID: pid, StartSec: process.Proc.P_starttime.Sec,
			StartUsec: process.Proc.P_starttime.Usec,
		},
		Parent: int(process.Eproc.Ppid), Status: process.Proc.P_stat,
	}, true, nil
}

func listDarwinChildPIDs(parentPID int) ([]int, error) {
	capacity := 16
	for capacity <= maxDarwinEnumeratedPIDs {
		buffer := make([]int32, capacity)
		bytes, _, errno := unix.Syscall6(
			unix.SYS_PROC_INFO,
			darwinProcInfoCallListPIDs,
			darwinProcParentPIDOnly,
			uintptr(parentPID),
			0,
			uintptr(unsafe.Pointer(&buffer[0])),
			uintptr(len(buffer))*unsafe.Sizeof(buffer[0]),
		)
		runtime.KeepAlive(buffer)
		if errno != 0 {
			return nil, errno
		}
		entrySize := unsafe.Sizeof(buffer[0])
		if bytes%entrySize != 0 || bytes > uintptr(len(buffer))*entrySize {
			return nil, errors.New("kernel returned an invalid child process list length")
		}
		count := int(bytes / entrySize)
		children := make([]int, 0, count)
		for _, pid := range buffer[:count] {
			if pid > 0 {
				children = append(children, int(pid))
			}
		}
		if count < len(buffer) {
			return children, nil
		}
		if capacity == maxDarwinEnumeratedPIDs {
			return children, fmt.Errorf(
				"managed process has at least %d direct children",
				maxDarwinEnumeratedPIDs,
			)
		}
		capacity *= 2
	}
	panic("unreachable Darwin child enumeration capacity")
}
