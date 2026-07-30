package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/hostkind"
	"github.com/GhostFlying/delegation/internal/store"
)

type scriptedResultPackageCompactor struct {
	mu      sync.Mutex
	results []store.ResultPackageDetailCompaction
	err     error
	calls   int
	called  chan struct{}
}

type prepareResultPackageGCRegistry struct {
	Registry
	err error
}

type runningResultPackageGCRegistry struct {
	Registry
	compactor *scriptedResultPackageCompactor
}

func (r *runningResultPackageGCRegistry) BeginBrokerEpoch(
	context.Context,
	string,
	hostkind.Kind,
) (store.PresenceTransition, error) {
	return store.PresenceTransition{}, nil
}

func (r *runningResultPackageGCRegistry) CompactReleasedResultPackageDetails(
	ctx context.Context,
	controllerID string,
	limit int,
) (store.ResultPackageDetailCompaction, error) {
	return r.compactor.CompactReleasedResultPackageDetails(ctx, controllerID, limit)
}

type blockingResultPackageGCRegistry struct {
	Registry
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (r *blockingResultPackageGCRegistry) BeginBrokerEpoch(
	context.Context,
	string,
	hostkind.Kind,
) (store.PresenceTransition, error) {
	return store.PresenceTransition{}, nil
}

func (r *blockingResultPackageGCRegistry) CompactReleasedResultPackageDetails(
	ctx context.Context,
	_ string,
	_ int,
) (store.ResultPackageDetailCompaction, error) {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	close(r.stopped)
	return store.ResultPackageDetailCompaction{}, ctx.Err()
}

func (r *prepareResultPackageGCRegistry) BeginBrokerEpoch(
	context.Context,
	string,
	hostkind.Kind,
) (store.PresenceTransition, error) {
	return store.PresenceTransition{}, r.err
}

func (c *scriptedResultPackageCompactor) CompactReleasedResultPackageDetails(
	_ context.Context,
	controllerID string,
	limit int,
) (store.ResultPackageDetailCompaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if controllerID != brokerTestControllerID {
		return store.ResultPackageDetailCompaction{}, errors.New("unexpected controller")
	}
	if limit != store.MaximumResultPackageDetailCompactionBatch {
		return store.ResultPackageDetailCompaction{}, errors.New("unexpected batch limit")
	}
	c.calls++
	if c.called != nil {
		c.called <- struct{}{}
	}
	if c.err != nil {
		return store.ResultPackageDetailCompaction{}, c.err
	}
	if len(c.results) == 0 {
		return store.ResultPackageDetailCompaction{}, nil
	}
	result := c.results[0]
	c.results = c.results[1:]
	return result, nil
}

func TestResultPackageGCBackgroundLifecycle(t *testing.T) {
	compactor := &scriptedResultPackageCompactor{
		results: []store.ResultPackageDetailCompaction{
			{Compacted: 128, More: true},
			{Compacted: 8},
		},
		called: make(chan struct{}, 2),
	}
	registry := &runningResultPackageGCRegistry{compactor: compactor}
	server, err := New(Options{
		ControllerID: brokerTestControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     registry,
		ReportError: func(err error) {
			t.Errorf("unexpected broker background error: %v", err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	})

	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	for call := 1; call <= 2; call++ {
		select {
		case <-compactor.called:
		case <-time.After(time.Second):
			t.Fatalf("background GC did not reach compaction call %d", call)
		}
	}
	if compactor.callCount() != 2 {
		t.Fatalf("background compaction calls = %d, want 2", compactor.callCount())
	}
}

func TestResultPackageGCBackgroundErrorDoesNotFailPrepare(t *testing.T) {
	injected := errors.New("store unavailable")
	compactor := &scriptedResultPackageCompactor{err: injected, called: make(chan struct{}, 1)}
	registry := &runningResultPackageGCRegistry{compactor: compactor}
	reported := make(chan error, 1)
	server, err := New(Options{
		ControllerID: brokerTestControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     registry,
		ReportError:  func(err error) { reported <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	})

	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare failed because background GC failed: %v", err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, injected) {
			t.Fatalf("reported error = %v, want %v", err, injected)
		}
	case <-time.After(time.Second):
		t.Fatal("background GC failure was not reported")
	}
}

func TestResultPackageGCCloseCancelsAndWaitsForActiveOperation(t *testing.T) {
	registry := &blockingResultPackageGCRegistry{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	server, err := New(Options{
		ControllerID: brokerTestControllerID,
		AuthMode:     config.AuthModeNone,
		Registry:     registry,
		ReportError: func(err error) {
			t.Errorf("unexpected broker background error: %v", err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.started:
	case <-time.After(time.Second):
		t.Fatal("background GC operation did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.stopped:
	default:
		t.Fatal("broker close returned before active GC operation stopped")
	}
}

func (c *scriptedResultPackageCompactor) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestResultPackageGCDrainsBoundedBatches(t *testing.T) {
	registry := &scriptedResultPackageCompactor{results: []store.ResultPackageDetailCompaction{
		{Compacted: 128, More: true},
		{Compacted: 8},
	}}
	server := &Server{
		controllerID:            brokerTestControllerID,
		context:                 context.Background(),
		resultPackageGCRegistry: registry,
		reportError: func(err error) {
			t.Fatalf("unexpected GC error: %v", err)
		},
	}

	server.compactReleasedResultPackageDetails()
	if registry.callCount() != 2 {
		t.Fatalf("compaction calls = %d, want 2", registry.callCount())
	}
}

func TestResultPackageGCReportsFailureWithoutRetryLoop(t *testing.T) {
	injected := errors.New("store unavailable")
	registry := &scriptedResultPackageCompactor{err: injected}
	var reported error
	server := &Server{
		controllerID:            brokerTestControllerID,
		context:                 context.Background(),
		resultPackageGCRegistry: registry,
		reportError:             func(err error) { reported = err },
	}

	server.compactReleasedResultPackageDetails()
	if registry.callCount() != 1 {
		t.Fatalf("compaction calls = %d, want 1", registry.callCount())
	}
	if !errors.Is(reported, injected) ||
		!strings.Contains(reported.Error(), "compact released result package details") {
		t.Fatalf("reported error = %v", reported)
	}
}

func TestResultPackageGCStopsWhenRegistryReportsNoProgress(t *testing.T) {
	registry := &scriptedResultPackageCompactor{results: []store.ResultPackageDetailCompaction{{More: true}}}
	var reported error
	server := &Server{
		controllerID:            brokerTestControllerID,
		context:                 context.Background(),
		resultPackageGCRegistry: registry,
		reportError:             func(err error) { reported = err },
	}

	server.compactReleasedResultPackageDetails()
	if registry.callCount() != 1 {
		t.Fatalf("compaction calls = %d, want 1", registry.callCount())
	}
	if reported == nil || !strings.Contains(reported.Error(), "without progress") {
		t.Fatalf("reported error = %v", reported)
	}
}

func TestResultPackageGCWakeIsCoalesced(t *testing.T) {
	server := &Server{resultPackageGCWake: make(chan struct{}, 1)}
	server.wakeResultPackageGC()
	server.wakeResultPackageGC()
	if len(server.resultPackageGCWake) != 1 {
		t.Fatalf("queued GC wakes = %d, want 1", len(server.resultPackageGCWake))
	}
	(&Server{}).wakeResultPackageGC()
}

func TestBrokerPrepareWakesResultPackageGCOnlyAfterDurableEpoch(t *testing.T) {
	server := &Server{
		controllerID:        brokerTestControllerID,
		registry:            &prepareResultPackageGCRegistry{},
		resultPackageGCWake: make(chan struct{}, 1),
	}
	if _, err := server.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(server.resultPackageGCWake) != 1 {
		t.Fatalf("queued GC wakes = %d, want 1", len(server.resultPackageGCWake))
	}

	injected := errors.New("epoch failed")
	server.registry = &prepareResultPackageGCRegistry{err: injected}
	server.resultPackageGCWake = make(chan struct{}, 1)
	if _, err := server.Prepare(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("Prepare() error = %v, want %v", err, injected)
	}
	if len(server.resultPackageGCWake) != 0 {
		t.Fatalf("failed epoch queued %d GC wakes", len(server.resultPackageGCWake))
	}
}
