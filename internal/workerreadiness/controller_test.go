package workerreadiness

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

type testProbe struct {
	err   error
	calls int
}

func (p *testProbe) Qualify(context.Context) error {
	p.calls++
	return p.err
}

type testPublisher struct {
	updates []protocol.WorkerReadiness
}

func (p *testPublisher) UpdateWorkerReadiness(
	_ context.Context, readiness protocol.WorkerReadiness,
) error {
	p.updates = append(p.updates, readiness)
	return nil
}

func TestControllerUsesDurableEpochRelativeScheduleAndExhaustsFifthFailure(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	probe := &testProbe{err: errors.New("transient")}
	publisher := &testPublisher{}
	controller, err := New(Options{
		Store: state, Probe: probe, Publisher: publisher, RuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	epochStart := now.UnixMilli()
	for attempt := 1; attempt <= protocol.MaximumReadinessAttempts; attempt++ {
		readiness, attempted, err := controller.advance(context.Background())
		if err != nil || !attempted {
			t.Fatalf("attempt %d advance = %#v, %t, %v", attempt, readiness, attempted, err)
		}
		if readiness.AttemptCount != attempt {
			t.Fatalf("attempt %d state = %#v", attempt, readiness)
		}
		if attempt == protocol.MaximumReadinessAttempts {
			if readiness.State != protocol.WorkerReadinessInterventionRequired ||
				readiness.FailureCode != protocol.WorkerRequalificationExhausted {
				t.Fatalf("terminal readiness = %#v", readiness)
			}
			break
		}
		offset, err := protocol.WorkerReadinessAttemptOffset(attempt)
		if err != nil {
			t.Fatal(err)
		}
		wantNext := epochStart + offset.Milliseconds()
		if readiness.NextAttemptAt != wantNext {
			t.Fatalf("attempt %d next = %d, want %d", attempt, readiness.NextAttemptAt, wantNext)
		}
		before := *now
		*now = time.UnixMilli(wantNext - 1)
		if _, attempted, err := controller.advance(context.Background()); err != nil || attempted {
			t.Fatalf("early attempt %d = %t, %v", attempt+1, attempted, err)
		}
		*now = before
		*now = time.UnixMilli(wantNext)
	}
	if probe.calls != protocol.MaximumReadinessAttempts {
		t.Fatalf("probe calls = %d", probe.calls)
	}
	if len(publisher.updates) != protocol.MaximumReadinessAttempts {
		t.Fatalf("published updates = %d", len(publisher.updates))
	}
	for index, update := range publisher.updates {
		if update.AttemptCount != index+1 {
			t.Fatalf("published attempt %d = %#v", index+1, update)
		}
	}
}

func TestControllerRestartDoesNotResetAttemptsAndPermanentFailureStopsImmediately(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	firstProbe := &testProbe{err: errors.New("transient")}
	first, err := New(Options{
		Store: state, Probe: firstProbe, Publisher: &testPublisher{},
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, attempted, err := first.advance(context.Background()); err != nil || !attempted {
		t.Fatalf("first advance = %t, %v", attempted, err)
	}
	restartedProbe := &testProbe{err: Permanent(protocol.WorkerManagedHomeInvalid, errors.New("invalid home"))}
	var reports []error
	restarted, err := New(Options{
		Store: state, Probe: restartedProbe, Publisher: &testPublisher{},
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		Now:         func() time.Time { return *now },
		ReportError: func(err error) { reports = append(reports, err) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, attempted, err := restarted.advance(context.Background()); err != nil || attempted {
		t.Fatalf("restart before due = %t, %v", attempted, err)
	}
	*now = now.Add(10 * time.Second)
	readiness, attempted, err := restarted.advance(context.Background())
	if err != nil || !attempted {
		t.Fatalf("permanent advance = %#v, %t, %v", readiness, attempted, err)
	}
	if readiness.AttemptCount != 2 || readiness.State != protocol.WorkerReadinessInterventionRequired ||
		readiness.FailureCode != protocol.WorkerManagedHomeInvalid || restartedProbe.calls != 1 {
		t.Fatalf("permanent readiness = %#v, calls %d", readiness, restartedProbe.calls)
	}
	if len(reports) != 1 || !strings.Contains(reports[0].Error(), "state=intervention_required") ||
		!strings.Contains(reports[0].Error(), "failureCode=managed_home_invalid") {
		t.Fatalf("intervention reports = %v", reports)
	}
	if _, attempted, err := restarted.advance(context.Background()); err != nil || attempted {
		t.Fatalf("terminal restart advance = %t, %v", attempted, err)
	}
	if len(reports) != 1 {
		t.Fatalf("terminal restart reports = %v", reports)
	}
}

func TestControllerExplicitRecheckCreatesAndPublishesNewEpoch(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	publisher := &testPublisher{}
	controller, err := New(Options{
		Store: state, Probe: &testProbe{}, Publisher: publisher,
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Minute)
	readiness, err := controller.Recheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if readiness.Epoch != 2 || readiness.AttemptCount != 0 ||
		readiness.EpochStartedAt != now.UnixMilli() || len(publisher.updates) != 1 ||
		publisher.updates[0] != readiness {
		t.Fatalf("recheck readiness = %#v, publications %#v", readiness, publisher.updates)
	}
}

func TestControllerPublishesOneCompletedSnapshotPerProbe(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	publisher := &testPublisher{}
	probe := &testProbe{err: errors.New("transient")}
	controller, err := New(Options{
		Store: state, Probe: probe, Publisher: publisher, RuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	readiness, attempted, err := controller.advance(context.Background())
	if err != nil ||
		!attempted || readiness.State != protocol.WorkerReadinessPending {
		t.Fatalf("transient advance = %#v, %t, %v", readiness, attempted, err)
	}
	if len(publisher.updates) != 1 || publisher.updates[0] != readiness ||
		publisher.updates[0].AttemptCount != 1 || publisher.updates[0].NextAttemptAt == 0 {
		t.Fatalf("transient advance publications = %#v", publisher.updates)
	}
	*now = now.Add(10 * time.Second)
	probe.err = nil
	readiness, attempted, err = controller.advance(context.Background())
	if err != nil || !attempted || readiness.State != protocol.WorkerReadinessReady {
		t.Fatalf("terminal advance = %#v, %t, %v", readiness, attempted, err)
	}
	if len(publisher.updates) != 2 || publisher.updates[1] != readiness {
		t.Fatalf("terminal publications = %#v, want %#v", publisher.updates, readiness)
	}
}

func TestControllerWaitWorkerInterventionHasNoLostWakeupOrRestartReplay(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	probe := &testProbe{err: Permanent(
		protocol.WorkerManagedHomeInvalid, errors.New("invalid home"),
	)}
	controller, err := New(Options{
		Store: state, Probe: probe, Publisher: &testPublisher{},
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := controller.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	readiness, attempted, err := controller.advance(context.Background())
	if err != nil || !attempted {
		t.Fatalf("advance = %#v, %t, %v", readiness, attempted, err)
	}
	got, err := controller.WaitWorkerIntervention(context.Background(), baseline.Cursor())
	if err != nil || got != readiness {
		t.Fatalf("wait after transition = %#v, %v; want %#v", got, err, readiness)
	}

	restarted, err := New(Options{
		Store: state, Probe: probe, Publisher: &testPublisher{},
		RuntimeDigest: runtimeDigest, ConfigDigest: configDigest,
		Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := restarted.WaitWorkerIntervention(waitCtx, readiness.Cursor()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("terminal restart wait error = %v", err)
	}
}

func TestControllerWaitWorkerInterventionObservesConcurrentTransition(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	controller, err := New(Options{
		Store: state, Probe: &testProbe{err: Permanent(
			protocol.WorkerManagedHomeInvalid, errors.New("invalid home"),
		)}, Publisher: &testPublisher{}, RuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := controller.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan protocol.WorkerReadiness, 1)
	waitError := make(chan error, 1)
	go func() {
		readiness, err := controller.WaitWorkerIntervention(
			context.Background(), baseline.Cursor(),
		)
		if err != nil {
			waitError <- err
			return
		}
		waitResult <- readiness
	}()
	terminal, attempted, err := controller.advance(context.Background())
	if err != nil || !attempted {
		t.Fatalf("advance = %#v, %t, %v", terminal, attempted, err)
	}
	select {
	case err := <-waitError:
		t.Fatal(err)
	case got := <-waitResult:
		if got != terminal {
			t.Fatalf("concurrent wait = %#v, want %#v", got, terminal)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent intervention wait lost the transition")
	}
}

func TestControllerWaitWorkerInterventionSurvivesImmediateRecheck(t *testing.T) {
	state, now, runtimeDigest, configDigest := readinessTestState(t)
	controller, err := New(Options{
		Store: state, Probe: &testProbe{err: Permanent(
			protocol.WorkerManagedHomeInvalid, errors.New("invalid home"),
		)}, Publisher: &testPublisher{}, RuntimeDigest: runtimeDigest,
		ConfigDigest: configDigest, Now: func() time.Time { return *now },
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := controller.WorkerReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	terminal, attempted, err := controller.advance(context.Background())
	if err != nil || !attempted {
		t.Fatalf("advance = %#v, %t, %v", terminal, attempted, err)
	}
	*now = now.Add(time.Second)
	if _, err := controller.Recheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := controller.WaitWorkerIntervention(context.Background(), baseline.Cursor())
	if err != nil || got != terminal {
		t.Fatalf("wait after immediate recheck = %#v, %v; want %#v", got, err, terminal)
	}
}

func readinessTestState(
	t *testing.T,
) (*store.PeerStore, *time.Time, string, string) {
	t.Helper()
	state, err := store.OpenPeer(
		context.Background(), filepath.Join(t.TempDir(), "state", "peer.sqlite3"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	now := time.UnixMilli(1_000_000)
	runtimeDigest := strings.Repeat("a", 64)
	configDigest := strings.Repeat("b", 64)
	if _, err := state.EnsureWorkerReadinessEpoch(
		context.Background(), runtimeDigest, configDigest, now.UnixMilli(),
	); err != nil {
		t.Fatal(err)
	}
	return state, &now, runtimeDigest, configDigest
}
