package workerreadiness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	transientProbeFailureCode = "worker_probe_failed"
	publicationTimeout        = 10 * time.Second
	publicationRetryInterval  = 250 * time.Millisecond
)

type StateStore interface {
	WorkerReadiness(context.Context) (protocol.WorkerReadiness, error)
	RecheckWorkerReadiness(context.Context, string, string, int64) (protocol.WorkerReadiness, error)
	UpdateWorkerReadiness(context.Context, uint64, protocol.WorkerReadiness) (protocol.WorkerReadiness, error)
}

type Probe interface {
	Qualify(context.Context) error
}

type permanentProbeError interface {
	WorkerReadinessFailureCode() string
}

type Publisher interface {
	UpdateWorkerReadiness(context.Context, protocol.WorkerReadiness) error
}

type Options struct {
	Store         StateStore
	Probe         Probe
	Publisher     Publisher
	RuntimeDigest string
	ConfigDigest  string
	Now           func() time.Time
	ReportError   func(error)
}

type Controller struct {
	store            StateStore
	probe            Probe
	publisher        Publisher
	runtimeDigest    string
	configDigest     string
	now              func() time.Time
	reportError      func(error)
	wake             chan struct{}
	operationMu      sync.Mutex
	interventionMu   sync.Mutex
	interventionCh   chan struct{}
	lastIntervention protocol.WorkerReadiness
	publicationMu    sync.Mutex
	pendingPublish   *protocol.WorkerReadiness
	publicationWake  chan struct{}
}

type PermanentFailure struct {
	Code string
	Err  error
}

func (e *PermanentFailure) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *PermanentFailure) Unwrap() error {
	return e.Err
}

func Permanent(code string, err error) error {
	return &PermanentFailure{Code: code, Err: err}
}

func New(options Options) (*Controller, error) {
	if options.Store == nil {
		return nil, errors.New("worker readiness store is required")
	}
	if options.Probe == nil {
		return nil, errors.New("worker readiness probe is required")
	}
	if options.Publisher == nil {
		return nil, errors.New("worker readiness publisher is required")
	}
	seed := protocol.NewPendingWorkerReadiness(
		options.RuntimeDigest, options.ConfigDigest, 1,
	)
	if err := seed.Validate(); err != nil {
		return nil, err
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	reportError := options.ReportError
	if reportError == nil {
		reportError = func(error) {}
	}
	return &Controller{
		store: options.Store, probe: options.Probe, publisher: options.Publisher,
		runtimeDigest: options.RuntimeDigest, configDigest: options.ConfigDigest,
		now: now, reportError: reportError, wake: make(chan struct{}, 1),
		interventionCh: make(chan struct{}), publicationWake: make(chan struct{}, 1),
	}, nil
}

func (c *Controller) WorkerReadiness(ctx context.Context) (protocol.WorkerReadiness, error) {
	return c.store.WorkerReadiness(ctx)
}

// WaitWorkerIntervention waits for a terminal readiness revision newer than
// after. Taking the notification channel before reading durable state avoids a
// lost wakeup when the transition races with waiter admission.
func (c *Controller) WaitWorkerIntervention(
	ctx context.Context, after protocol.WorkerReadinessCursor,
) (protocol.WorkerReadiness, error) {
	if err := after.Validate(); err != nil {
		return protocol.WorkerReadiness{}, err
	}
	for {
		c.interventionMu.Lock()
		wake := c.interventionCh
		last := c.lastIntervention
		c.interventionMu.Unlock()
		if last.Epoch != 0 && after.Before(last) {
			return last, nil
		}

		readiness, err := c.store.WorkerReadiness(ctx)
		if err != nil {
			return protocol.WorkerReadiness{}, err
		}
		if readiness.State == protocol.WorkerReadinessInterventionRequired &&
			after.Before(readiness) {
			return readiness, nil
		}
		select {
		case <-ctx.Done():
			return protocol.WorkerReadiness{}, ctx.Err()
		case <-wake:
		}
	}
}

func (c *Controller) Run(ctx context.Context) error {
	publicationContext, cancelPublication := context.WithCancel(ctx)
	publicationDone := make(chan struct{})
	go func() {
		defer close(publicationDone)
		c.runPublicationRetries(publicationContext)
	}()
	defer func() {
		cancelPublication()
		<-publicationDone
	}()
	for {
		readiness, attempted, err := c.advance(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return nil
			}
			return err
		}
		if attempted {
			continue
		}
		var timer *time.Timer
		var timerC <-chan time.Time
		if readiness.State == protocol.WorkerReadinessPending && readiness.NextAttemptAt > 0 {
			delay := time.UnixMilli(readiness.NextAttemptAt).Sub(c.now())
			if delay <= 0 {
				continue
			}
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil
		case <-c.wake:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

func (c *Controller) Recheck(ctx context.Context) (protocol.WorkerReadiness, error) {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	nowMillis := positiveMillis(c.now())
	readiness, err := c.store.RecheckWorkerReadiness(
		ctx, c.runtimeDigest, c.configDigest, nowMillis,
	)
	if err != nil {
		return protocol.WorkerReadiness{}, err
	}
	c.publish(readiness)
	c.signal()
	return readiness, nil
}

func (c *Controller) RecheckWorkerReadiness(ctx context.Context) (protocol.WorkerReadiness, error) {
	return c.Recheck(ctx)
}

func (c *Controller) advance(ctx context.Context) (protocol.WorkerReadiness, bool, error) {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	readiness, err := c.store.WorkerReadiness(ctx)
	if err != nil {
		return protocol.WorkerReadiness{}, false, err
	}
	if readiness.State != protocol.WorkerReadinessPending {
		return readiness, false, nil
	}
	if readiness.AttemptCount == protocol.MaximumReadinessAttempts {
		terminal := readiness
		terminal.State = protocol.WorkerReadinessInterventionRequired
		terminal.FailureCode = protocol.WorkerRequalificationExhausted
		terminal.NextAttemptAt = 0
		terminal.UpdatedAt = nextUpdateMillis(readiness, c.now())
		stored, err := c.store.UpdateWorkerReadiness(ctx, readiness.Epoch, terminal)
		if err != nil {
			if errors.Is(err, store.ErrWorkerReadinessStale) {
				return readiness, true, nil
			}
			return protocol.WorkerReadiness{}, false, err
		}
		c.signalIntervention(stored)
		c.publish(stored)
		c.reportIntervention(stored)
		return stored, true, nil
	}
	now := c.now()
	if now.UnixMilli() < readiness.NextAttemptAt {
		return readiness, false, nil
	}
	claimed := readiness
	claimed.AttemptCount++
	claimed.LastAttemptAt = nextUpdateMillis(readiness, now)
	claimed.UpdatedAt = claimed.LastAttemptAt
	claimed.FailureCode = ""
	if claimed.AttemptCount < protocol.MaximumReadinessAttempts {
		offset, err := protocol.WorkerReadinessAttemptOffset(claimed.AttemptCount)
		if err != nil {
			return protocol.WorkerReadiness{}, false, err
		}
		claimed.NextAttemptAt = readiness.EpochStartedAt + offset.Milliseconds()
	} else {
		claimed.NextAttemptAt = 0
	}
	claimed, err = c.store.UpdateWorkerReadiness(ctx, readiness.Epoch, claimed)
	if err != nil {
		if errors.Is(err, store.ErrWorkerReadinessStale) {
			return readiness, true, nil
		}
		return protocol.WorkerReadiness{}, false, err
	}
	probeErr := c.probe.Qualify(ctx)
	completed := claimed
	completed.UpdatedAt = nextUpdateMillis(claimed, c.now())
	switch {
	case probeErr == nil:
		completed.State = protocol.WorkerReadinessReady
		completed.NextAttemptAt = 0
		completed.FailureCode = ""
	default:
		var permanent *PermanentFailure
		var classified permanentProbeError
		if errors.As(probeErr, &permanent) {
			completed.State = protocol.WorkerReadinessInterventionRequired
			completed.NextAttemptAt = 0
			completed.FailureCode = permanent.Code
		} else if errors.As(probeErr, &classified) {
			completed.State = protocol.WorkerReadinessInterventionRequired
			completed.NextAttemptAt = 0
			completed.FailureCode = classified.WorkerReadinessFailureCode()
		} else if completed.AttemptCount == protocol.MaximumReadinessAttempts {
			completed.State = protocol.WorkerReadinessInterventionRequired
			completed.FailureCode = protocol.WorkerRequalificationExhausted
		} else {
			completed.FailureCode = transientProbeFailureCode
		}
	}
	stored, err := c.store.UpdateWorkerReadiness(ctx, readiness.Epoch, completed)
	if err != nil {
		if errors.Is(err, store.ErrWorkerReadinessStale) {
			return completed, true, nil
		}
		return protocol.WorkerReadiness{}, false, err
	}
	c.publish(stored)
	if stored.State == protocol.WorkerReadinessInterventionRequired {
		c.signalIntervention(stored)
		c.reportIntervention(stored)
	}
	return stored, true, nil
}

func (c *Controller) reportIntervention(readiness protocol.WorkerReadiness) {
	c.reportError(fmt.Errorf(
		"worker readiness state=%s failureCode=%s",
		readiness.State, readiness.FailureCode,
	))
}

func (c *Controller) publish(readiness protocol.WorkerReadiness) {
	err := c.publishOnce(context.Background(), readiness)
	if err != nil {
		c.reportError(fmt.Errorf("publish worker readiness: %w", err))
		c.queuePublication(readiness)
		return
	}
	c.discardPublicationThrough(readiness)
}

func (c *Controller) publishOnce(
	parent context.Context, readiness protocol.WorkerReadiness,
) error {
	ctx, cancel := context.WithTimeout(parent, publicationTimeout)
	defer cancel()
	return c.publisher.UpdateWorkerReadiness(ctx, readiness)
}

func (c *Controller) queuePublication(readiness protocol.WorkerReadiness) {
	c.publicationMu.Lock()
	if c.pendingPublish == nil || c.pendingPublish.Cursor().Before(readiness) {
		copy := readiness
		c.pendingPublish = &copy
	}
	c.publicationMu.Unlock()
	c.signalPublication()
}

func (c *Controller) pendingPublication() (protocol.WorkerReadiness, bool) {
	c.publicationMu.Lock()
	defer c.publicationMu.Unlock()
	if c.pendingPublish == nil {
		return protocol.WorkerReadiness{}, false
	}
	return *c.pendingPublish, true
}

func (c *Controller) discardPublicationThrough(readiness protocol.WorkerReadiness) {
	c.publicationMu.Lock()
	if c.pendingPublish != nil && !readiness.Cursor().Before(*c.pendingPublish) {
		c.pendingPublish = nil
	}
	pending := c.pendingPublish != nil
	c.publicationMu.Unlock()
	if pending {
		c.signalPublication()
	}
}

func (c *Controller) runPublicationRetries(ctx context.Context) {
	for {
		readiness, pending := c.pendingPublication()
		if !pending {
			select {
			case <-ctx.Done():
				return
			case <-c.publicationWake:
			}
			continue
		}
		err := c.publishOnce(ctx, readiness)
		if err == nil {
			c.discardPublicationThrough(readiness)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(publicationRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.publicationWake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Controller) signalPublication() {
	select {
	case c.publicationWake <- struct{}{}:
	default:
	}
}

func (c *Controller) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *Controller) signalIntervention(readiness protocol.WorkerReadiness) {
	c.interventionMu.Lock()
	c.lastIntervention = readiness
	close(c.interventionCh)
	c.interventionCh = make(chan struct{})
	c.interventionMu.Unlock()
}

func positiveMillis(now time.Time) int64 {
	millis := now.UnixMilli()
	if millis < 1 {
		return 1
	}
	return millis
}

func nextUpdateMillis(previous protocol.WorkerReadiness, now time.Time) int64 {
	millis := positiveMillis(now)
	if millis <= previous.UpdatedAt {
		return previous.UpdatedAt + 1
	}
	return millis
}
