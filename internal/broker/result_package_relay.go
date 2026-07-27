package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	resultPackageBeginTimeout         = 30 * time.Second
	resultPackageChunkTimeout         = 30 * time.Second
	resultPackageFinishTimeout        = 2 * time.Minute
	resultPackageAcknowledgeTimeout   = 30 * time.Second
	resultPackageReleaseTimeout       = 30 * time.Second
	resultPackageCancelTimeout        = 5 * time.Second
	resultPackageReconnectScanTimeout = 30 * time.Second
	maximumPendingResultPackageRelays = 128
	maximumActiveResultPackageRelays  = 128
	initialResultPackageRelayBackoff  = time.Second
	maximumResultPackageRelayBackoff  = 30 * time.Second
)

var errResultPackageRelayDeferred = errors.New("result package relay is deferred")

type resultPackageRelayRetryError struct {
	err   error
	after time.Duration
}

func (e *resultPackageRelayRetryError) Error() string {
	return fmt.Sprintf("%v; retry after %s", e.err, e.after)
}

func (e *resultPackageRelayRetryError) Unwrap() error {
	return e.err
}

type resultPackageRelayRequest struct {
	source    control.PrincipalIdentity
	packageID string
}

type resultPackageTransferResult struct {
	began                bool
	rootRetentionOrdinal uint64
}

type resultPackageRelayKey struct {
	controllerID string
	treeID       string
	packageID    string
}

func (r resultPackageRelayRequest) key() resultPackageRelayKey {
	return resultPackageRelayKey{
		controllerID: r.source.ControllerID,
		treeID:       r.source.TreeID,
		packageID:    r.packageID,
	}
}

type resultPackageRelayFunc func(context.Context, resultPackageRelayRequest) error
type resultPackageRelayScanPage struct {
	requests  []resultPackageRelayRequest
	nextAfter *store.ResultPackageRelayCursor
}
type resultPackageRelayScanFunc func(
	context.Context,
	string,
	*store.ResultPackageRelayCursor,
) (resultPackageRelayScanPage, error)

type resultPackageRelayFlight struct {
	wake             chan struct{}
	firstAttemptDone chan struct{}
}

type resultPackageRelayPeerScan struct {
	wake    chan struct{}
	restart bool
}

type resultPackageRelayCoordinator struct {
	context context.Context
	relay   resultPackageRelayFunc
	scan    resultPackageRelayScanFunc
	report  func(error)

	mu         sync.Mutex
	active     map[resultPackageRelayKey]*resultPackageRelayFlight
	activeScan map[string]*resultPackageRelayPeerScan
	capacity   chan struct{}
	stopped    bool
	wait       sync.WaitGroup
}

func newResultPackageRelayCoordinator(
	ctx context.Context,
	relay resultPackageRelayFunc,
	scan resultPackageRelayScanFunc,
	report func(error),
) *resultPackageRelayCoordinator {
	if report == nil {
		report = func(error) {}
	}
	return &resultPackageRelayCoordinator{
		context:    ctx,
		relay:      relay,
		scan:       scan,
		report:     report,
		active:     make(map[resultPackageRelayKey]*resultPackageRelayFlight),
		activeScan: make(map[string]*resultPackageRelayPeerScan),
		capacity:   make(chan struct{}),
	}
}

func (c *resultPackageRelayCoordinator) schedulePeerReconnect(deviceID string) bool {
	return c.schedulePeerScan(deviceID, true)
}

func (c *resultPackageRelayCoordinator) schedulePeerScan(
	deviceID string,
	wakeFlights bool,
) bool {
	if c == nil || c.context == nil || c.scan == nil {
		return false
	}
	c.mu.Lock()
	if c.stopped || c.context.Err() != nil {
		c.mu.Unlock()
		return false
	}
	if wakeFlights {
		for _, flight := range c.active {
			select {
			case flight.wake <- struct{}{}:
			default:
			}
		}
	}
	if current := c.activeScan[deviceID]; current != nil {
		current.restart = true
		if wakeFlights {
			select {
			case current.wake <- struct{}{}:
			default:
			}
		}
		c.mu.Unlock()
		return false
	}
	scan := &resultPackageRelayPeerScan{wake: make(chan struct{}, 1)}
	c.activeScan[deviceID] = scan
	c.wait.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wait.Done()
		defer func() {
			c.mu.Lock()
			if c.activeScan[deviceID] == scan {
				delete(c.activeScan, deviceID)
			}
			c.mu.Unlock()
		}()
		var after *store.ResultPackageRelayCursor
		scanAttempt := 0
		for {
			ctx, cancel := context.WithTimeout(c.context, resultPackageReconnectScanTimeout)
			page, err := c.scan(ctx, deviceID, after)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) && c.context.Err() == nil {
					err = fmt.Errorf(
						"%w: pending result package scan timed out",
						errResultPackageRelayDeferred,
					)
				}
				delay, retry := resultPackageRelayRetryDelay(err, scanAttempt)
				if !retry {
					if !errors.Is(err, context.Canceled) {
						c.report(fmt.Errorf("scan pending result packages for peer %s: %w", deviceID, err))
					}
					return
				}
				scanAttempt++
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-scan.wake:
					timer.Stop()
				case <-c.context.Done():
					timer.Stop()
					return
				}
				continue
			}
			scanAttempt = 0
			flights := make([]*resultPackageRelayFlight, 0, len(page.requests))
			for _, request := range page.requests {
				flight, ok := c.scheduleScannedFlight(scan, request)
				if !ok {
					return
				}
				flights = append(flights, flight)
			}
			if !c.waitForResultPackageRelayAttempts(scan, flights) {
				return
			}
			if page.nextAfter == nil {
				c.mu.Lock()
				if scan.restart {
					scan.restart = false
					c.mu.Unlock()
					after = nil
					continue
				}
				if c.activeScan[deviceID] == scan {
					delete(c.activeScan, deviceID)
				}
				c.mu.Unlock()
				return
			}
			after = page.nextAfter
		}
	}()
	return true
}

func (c *resultPackageRelayCoordinator) scheduleScannedFlight(
	scan *resultPackageRelayPeerScan,
	request resultPackageRelayRequest,
) (*resultPackageRelayFlight, bool) {
	for {
		flight, _ := c.scheduleFlight(request)
		if flight != nil {
			return flight, true
		}
		c.mu.Lock()
		if c.stopped || c.context.Err() != nil {
			c.mu.Unlock()
			return nil, false
		}
		if len(c.active) < maximumActiveResultPackageRelays {
			c.mu.Unlock()
			continue
		}
		capacity := c.capacity
		c.mu.Unlock()
		select {
		case <-capacity:
		case <-scan.wake:
		case <-c.context.Done():
			return nil, false
		}
	}
}

func (c *resultPackageRelayCoordinator) waitForResultPackageRelayAttempts(
	scan *resultPackageRelayPeerScan,
	flights []*resultPackageRelayFlight,
) bool {
flights:
	for _, flight := range flights {
		for {
			select {
			case <-flight.firstAttemptDone:
				continue flights
			case <-scan.wake:
				for _, pending := range flights {
					select {
					case pending.wake <- struct{}{}:
					default:
					}
				}
			case <-c.context.Done():
				return false
			}
		}
	}
	return c.context.Err() == nil
}

// schedule starts at most one relay for a package. It never invokes relay on
// the caller's goroutine, because that goroutine may be the only reader for the
// source peer's full-duplex session.
func (c *resultPackageRelayCoordinator) schedule(request resultPackageRelayRequest) bool {
	flight, started := c.scheduleFlight(request)
	if flight == nil {
		c.schedulePeerScan(request.source.DeviceID, false)
	} else if !started {
		select {
		case flight.wake <- struct{}{}:
		default:
		}
	}
	return started
}

func (c *resultPackageRelayCoordinator) scheduleFlight(
	request resultPackageRelayRequest,
) (*resultPackageRelayFlight, bool) {
	if c == nil || c.context == nil || c.relay == nil {
		return nil, false
	}
	if validateResultPackageRelayRequest(request) != nil {
		return nil, false
	}
	key := request.key()
	c.mu.Lock()
	if c.stopped || c.context.Err() != nil {
		c.mu.Unlock()
		return nil, false
	}
	if current := c.active[key]; current != nil {
		c.mu.Unlock()
		return current, false
	}
	if len(c.active) >= maximumActiveResultPackageRelays {
		c.mu.Unlock()
		return nil, false
	}
	flight := &resultPackageRelayFlight{
		wake:             make(chan struct{}, 1),
		firstAttemptDone: make(chan struct{}),
	}
	c.active[key] = flight
	c.wait.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.wait.Done()
		defer func() {
			c.mu.Lock()
			delete(c.active, key)
			close(c.capacity)
			c.capacity = make(chan struct{})
			c.mu.Unlock()
		}()
		firstAttempt := true
		for attempt := 0; ; attempt++ {
			err := c.relay(c.context, request)
			if firstAttempt {
				close(flight.firstAttemptDone)
				firstAttempt = false
			}
			if err == nil {
				return
			}
			delay, retry := resultPackageRelayRetryDelay(err, attempt)
			if !retry {
				if !errors.Is(err, context.Canceled) {
					c.report(fmt.Errorf("relay result package %s: %w", request.packageID, err))
				}
				return
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-flight.wake:
				timer.Stop()
			case <-c.context.Done():
				timer.Stop()
				return
			}
		}
	}()
	return flight, true
}

func resultPackageRelayRetryDelay(err error, attempt int) (time.Duration, bool) {
	if !errors.Is(err, errResultPackageRelayDeferred) {
		return 0, false
	}
	var retry *resultPackageRelayRetryError
	if errors.As(err, &retry) {
		return retry.after, true
	}
	delay := initialResultPackageRelayBackoff << min(attempt, 5)
	return min(delay, maximumResultPackageRelayBackoff), true
}

func retryResultPackageRelayAfter(err error, delay time.Duration) error {
	return &resultPackageRelayRetryError{err: err, after: delay}
}

func (c *resultPackageRelayCoordinator) stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stopped = true
	close(c.capacity)
	c.capacity = make(chan struct{})
	c.mu.Unlock()
}

func (c *resultPackageRelayCoordinator) waitForShutdown() {
	if c != nil {
		c.wait.Wait()
	}
}

type resultPackagePeer interface {
	callPeer(
		context.Context,
		string,
		string,
		control.PrincipalIdentity,
		any,
	) (json.RawMessage, error)
}

type resultPackageSourceAcknowledger interface {
	MarkResultPackageSourceAcknowledged(
		context.Context,
		string,
		control.PrincipalIdentity,
		string,
		time.Time,
	) (store.ResultPackageRecord, error)
	MarkResultPackageSourceReleased(
		context.Context,
		string,
		control.PrincipalIdentity,
		string,
		time.Time,
	) (store.ResultPackageRecord, error)
}

func callResultPackagePeer(
	ctx context.Context,
	timeout time.Duration,
	peer resultPackagePeer,
	method string,
	treeID string,
	source control.PrincipalIdentity,
	params any,
) (json.RawMessage, error) {
	callContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return peer.callPeer(callContext, method, treeID, source, params)
}

func resultPackageReceivingLeaseExpiresAt(now time.Time) int64 {
	return now.Add(store.MaximumResultInboxLease).Unix()
}

func (s *Server) relayResultPackage(
	ctx context.Context,
	request resultPackageRelayRequest,
) error {
	record, err := s.registry.GetResultPackageForDelivery(
		ctx, request.source.DeviceID, request.source, request.packageID,
	)
	if err != nil {
		return resultPackageStoreRelayError("load relay metadata", err)
	}
	if record.SourcePrincipal != request.source {
		return errors.New("stored result package source differs from its relay request")
	}
	if record.State == store.ResultPackageDelivered {
		s.notifyResultPackageDelivered(record)
	}
	if record.SourceReleasedAt != 0 {
		s.wakeResultPackageGC()
		return nil
	}
	if record.State == store.ResultPackageDelivered {
		source := s.connection(record.SourcePrincipal.DeviceID)
		if source == nil {
			return fmt.Errorf("%w: source peer is offline", errResultPackageRelayDeferred)
		}
		return s.finalizeDeliveredResultPackage(
			ctx, source, s.registry, record.SourcePrincipal, record, s.now(),
		)
	}
	delivered, err := s.deliverPendingResultPackage(ctx, record)
	if err != nil {
		return err
	}
	source := s.connection(record.SourcePrincipal.DeviceID)
	if source == nil {
		return fmt.Errorf("%w: source peer is offline", errResultPackageRelayDeferred)
	}
	acknowledged, err := acknowledgeAndMarkResultPackage(
		ctx, source, s.registry, record.SourcePrincipal, delivered, s.now(),
	)
	if err != nil {
		return err
	}
	err = s.releaseAndMarkResultPackage(
		ctx, source, s.registry, record.SourcePrincipal, acknowledged, s.now(),
	)
	return err
}

func (s *Server) deliverPendingResultPackage(
	ctx context.Context,
	record store.ResultPackageRecord,
) (store.ResultPackageRecord, error) {
	release, err := s.resultRelayRoots.acquire(ctx, resultPackageRootRelayKey{
		controllerID: record.RootPrincipal.ControllerID,
		deviceID:     record.RootPrincipal.DeviceID,
	})
	if err != nil {
		return store.ResultPackageRecord{}, err
	}
	defer release()

	source := s.connection(record.SourcePrincipal.DeviceID)
	if source == nil {
		return store.ResultPackageRecord{}, fmt.Errorf(
			"%w: source peer is offline",
			errResultPackageRelayDeferred,
		)
	}
	root := s.connection(record.RootPrincipal.DeviceID)
	if root == nil {
		return store.ResultPackageRecord{}, fmt.Errorf(
			"%w: root peer is offline",
			errResultPackageRelayDeferred,
		)
	}
	retentionFloor, err := s.registry.GetResultPackageRootRetentionHighwater(
		ctx,
		record.RootPrincipal.ControllerID,
		record.RootPrincipal.DeviceID,
	)
	if err != nil {
		return store.ResultPackageRecord{}, resultPackageStoreRelayError(
			"load root result retention highwater",
			err,
		)
	}
	attemptID, err := s.newID()
	if err != nil {
		return store.ResultPackageRecord{}, fmt.Errorf(
			"create result package transfer attempt: %w",
			err,
		)
	}
	transfer, err := transferResultPackage(
		ctx,
		source,
		root,
		record.SourcePrincipal,
		record.RootPrincipal,
		record,
		attemptID,
		retentionFloor,
		resultPackageReceivingLeaseExpiresAt(s.now()),
	)
	if err != nil {
		if transfer.began {
			if !s.cancelResultPackageTransfer(root, record.RootPrincipal, record, attemptID) &&
				errors.Is(err, errResultPackageRelayDeferred) {
				return store.ResultPackageRecord{}, retryResultPackageRelayAfter(
					err,
					store.MaximumResultInboxLease,
				)
			}
		}
		return store.ResultPackageRecord{}, err
	}
	delivered, err := s.markResultPackageDelivered(
		ctx,
		record.RootPrincipal,
		record.Manifest.PackageID,
		transfer.rootRetentionOrdinal,
	)
	if err != nil {
		return store.ResultPackageRecord{}, err
	}
	return delivered, nil
}

func (s *Server) finalizeDeliveredResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	registry resultPackageSourceAcknowledger,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	now time.Time,
) error {
	err := finalizeDeliveredResultPackage(ctx, source, registry, workerPrincipal, record, now)
	if err == nil {
		s.wakeResultPackageGC()
	}
	return err
}

func finalizeDeliveredResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	registry resultPackageSourceAcknowledger,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	now time.Time,
) error {
	var err error
	if record.SourceAcknowledgedAt == 0 {
		record, err = acknowledgeAndMarkResultPackage(
			ctx, source, registry, workerPrincipal, record, now,
		)
		if err != nil {
			return err
		}
	}
	return releaseAndMarkResultPackage(
		ctx, source, registry, workerPrincipal, record, now,
	)
}

func (s *Server) markResultPackageDelivered(
	ctx context.Context,
	root control.PrincipalIdentity,
	packageID string,
	rootRetentionOrdinal uint64,
) (store.ResultPackageRecord, error) {
	delivered, err := s.registry.MarkResultPackageDelivered(
		ctx, root.DeviceID, root, packageID, rootRetentionOrdinal, s.now(),
	)
	if err != nil {
		return store.ResultPackageRecord{}, resultPackageStoreRelayError(
			"record durable result package delivery",
			err,
		)
	}
	s.notifyResultPackageDelivered(delivered)
	s.wakeResultPackageGC()
	return delivered, nil
}

func (s *Server) notifyResultPackageDelivered(record store.ResultPackageRecord) {
	s.resultNotifier.notify(treeKey{
		controllerID: record.Manifest.ControllerID,
		treeID:       record.Manifest.TreeID,
	})
}

func (s *Server) pendingResultPackageRelays(
	ctx context.Context,
	deviceID string,
	after *store.ResultPackageRelayCursor,
) (resultPackageRelayScanPage, error) {
	page, err := s.registry.ListPendingResultPackageRelaysForPeer(
		ctx,
		s.controllerID,
		deviceID,
		store.ResultPackageRelayPageRequest{
			After: after,
			Limit: maximumPendingResultPackageRelays,
		},
	)
	if err != nil {
		return resultPackageRelayScanPage{}, resultPackageStoreRelayError(
			"list pending result package relays",
			err,
		)
	}
	requests := make([]resultPackageRelayRequest, 0, len(page.Packages))
	for _, record := range page.Packages {
		if (record.State != store.ResultPackageDeliveryPending && record.State != store.ResultPackageDelivered) ||
			(record.SourcePrincipal.DeviceID != deviceID && record.RootPrincipal.DeviceID != deviceID) {
			return resultPackageRelayScanPage{}, errors.New(
				"pending result package scan returned unrelated relay state",
			)
		}
		if record.State == store.ResultPackageDelivered && record.SourcePrincipal.DeviceID != deviceID {
			return resultPackageRelayScanPage{}, errors.New(
				"delivered result package scan returned an unrelated root peer",
			)
		}
		request := resultPackageRelayRequest{
			source:    record.SourcePrincipal,
			packageID: record.Manifest.PackageID,
		}
		if err := validateResultPackageRelayRequest(request); err != nil {
			return resultPackageRelayScanPage{}, fmt.Errorf("pending result package relay: %w", err)
		}
		requests = append(requests, request)
	}
	return resultPackageRelayScanPage{requests: requests, nextAfter: page.NextAfter}, nil
}

func transferResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	root resultPackagePeer,
	workerPrincipal control.PrincipalIdentity,
	rootPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	attemptID string,
	retentionFloor uint64,
	leaseExpiresAt int64,
) (resultPackageTransferResult, error) {
	beginParams := protocol.BeginResultPackageParams{
		AttemptID:      attemptID,
		PackageID:      record.Manifest.PackageID,
		RetentionFloor: retentionFloor,
		LeaseExpiresAt: leaseExpiresAt,
		Metadata:       record.Metadata,
	}
	payload, err := callResultPackagePeer(
		ctx,
		resultPackageBeginTimeout,
		root,
		protocol.MethodBeginResultPackage,
		record.Manifest.TreeID,
		rootPrincipal,
		beginParams,
	)
	if err != nil {
		var rpcError *peerRPCError
		if errors.As(err, &rpcError) && rpcError.code == protocol.ErrorConflict {
			deferred := fmt.Errorf(
				"%w: root still owns another receiving attempt",
				errResultPackageRelayDeferred,
			)
			return resultPackageTransferResult{}, retryResultPackageRelayAfter(
				deferred,
				store.MaximumResultInboxLease,
			)
		}
		return resultPackageTransferResult{}, deferResultPackagePeerError("begin root result package", err)
	}
	begin, err := protocol.DecodePayload[protocol.BeginResultPackageResult](payload)
	if err != nil || begin.Validate() != nil || begin.AttemptID != attemptID ||
		begin.PackageID != record.Manifest.PackageID {
		return resultPackageTransferResult{}, errors.New("root returned invalid result package begin metadata")
	}
	result := resultPackageTransferResult{rootRetentionOrdinal: begin.RetentionOrdinal}
	if begin.Outcome == protocol.ResultPackageAlreadyAvailable {
		return result, nil
	}
	result.began = true
	offsets, err := validateResultPackageOffsets(record.Manifest.Parts, begin.Offsets)
	if err != nil {
		return result, err
	}
	for index, part := range record.Manifest.Parts {
		if err := relayResultPackagePart(
			ctx,
			source,
			root,
			workerPrincipal,
			rootPrincipal,
			record.Manifest,
			attemptID,
			part,
			offsets[index],
		); err != nil {
			return result, err
		}
	}
	finishParams := protocol.FinishResultPackageParams{
		AttemptID: attemptID,
		PackageID: record.Manifest.PackageID,
	}
	payload, err = callResultPackagePeer(
		ctx,
		resultPackageFinishTimeout,
		root,
		protocol.MethodFinishResultPackage,
		record.Manifest.TreeID,
		rootPrincipal,
		finishParams,
	)
	if err != nil {
		return result, deferResultPackagePeerError("finish root result package", err)
	}
	finished, err := protocol.DecodePayload[protocol.FinishResultPackageResult](payload)
	if err != nil || finished.Validate() != nil ||
		finished != protocol.FinishResultPackageResult(finishParams) {
		return result, errors.New("root returned invalid result package finish metadata")
	}
	return result, nil
}

func validateResultPackageOffsets(
	parts []protocol.ResultPackagePartDescriptor,
	offsets []protocol.ResultPackagePartOffset,
) ([]int64, error) {
	if len(offsets) != len(parts) {
		return nil, errors.New("root result package offsets do not match the manifest parts")
	}
	result := make([]int64, len(parts))
	for index, part := range parts {
		if offsets[index].Kind != part.Kind || offsets[index].NextOffset > part.Size {
			return nil, errors.New("root result package offset is outside its manifest part")
		}
		result[index] = offsets[index].NextOffset
	}
	return result, nil
}

func relayResultPackagePart(
	ctx context.Context,
	source resultPackagePeer,
	root resultPackagePeer,
	workerPrincipal control.PrincipalIdentity,
	rootPrincipal control.PrincipalIdentity,
	manifest protocol.ResultManifest,
	attemptID string,
	part protocol.ResultPackagePartDescriptor,
	offset int64,
) error {
	for offset < part.Size {
		limit := min(int64(protocol.ResultPackageChunkBytes), part.Size-offset)
		readParams := protocol.ReadResultPackagePartParams{
			PackageID: manifest.PackageID,
			Kind:      part.Kind,
			Offset:    offset,
			Limit:     int(limit),
		}
		payload, err := callResultPackagePeer(
			ctx,
			resultPackageChunkTimeout,
			source,
			protocol.MethodReadResultPackagePart,
			manifest.TreeID,
			workerPrincipal,
			readParams,
		)
		if err != nil {
			return deferResultPackagePeerError("read source result package part", err)
		}
		read, err := protocol.DecodePayload[protocol.ReadResultPackagePartResult](payload)
		if err != nil || read.Validate() != nil || read.PackageID != manifest.PackageID ||
			read.Kind != part.Kind || read.Offset != offset || int64(len(read.Data)) != limit ||
			read.NextOffset != offset+limit {
			return errors.New("source returned a result package chunk outside the requested bounds")
		}
		writeParams := protocol.WriteResultPackagePartParams{
			AttemptID: attemptID,
			PackageID: manifest.PackageID,
			Kind:      part.Kind,
			Offset:    offset,
			Data:      read.Data,
		}
		payload, err = callResultPackagePeer(
			ctx,
			resultPackageChunkTimeout,
			root,
			protocol.MethodWriteResultPackagePart,
			manifest.TreeID,
			rootPrincipal,
			writeParams,
		)
		if err != nil {
			return deferResultPackagePeerError("write root result package part", err)
		}
		written, err := protocol.DecodePayload[protocol.WriteResultPackagePartResult](payload)
		if err != nil || written.Validate() != nil || written.AttemptID != attemptID ||
			written.PackageID != manifest.PackageID || written.Kind != part.Kind ||
			written.NextOffset != read.NextOffset {
			return errors.New("root returned an invalid result package write acknowledgement")
		}
		offset = read.NextOffset
	}
	return nil
}

func acknowledgeResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
) error {
	params := protocol.AcknowledgeResultPackageParams{
		PackageID: record.Manifest.PackageID,
		Sequence:  record.Sequence,
	}
	payload, err := callResultPackagePeer(
		ctx,
		resultPackageAcknowledgeTimeout,
		source,
		protocol.MethodAcknowledgeResultPackage,
		record.Manifest.TreeID,
		workerPrincipal,
		params,
	)
	if err != nil {
		return deferResultPackagePeerError("acknowledge source result package", err)
	}
	acknowledged, err := protocol.DecodePayload[protocol.AcknowledgeResultPackageResult](payload)
	if err != nil || acknowledged.Validate() != nil ||
		acknowledged != protocol.AcknowledgeResultPackageResult(params) {
		return errors.New("source returned an invalid result package delivery acknowledgement")
	}
	return nil
}

func acknowledgeAndMarkResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	registry resultPackageSourceAcknowledger,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	acknowledgedAt time.Time,
) (store.ResultPackageRecord, error) {
	if err := acknowledgeResultPackage(ctx, source, workerPrincipal, record); err != nil {
		return store.ResultPackageRecord{}, err
	}
	acknowledged, err := registry.MarkResultPackageSourceAcknowledged(
		ctx,
		workerPrincipal.DeviceID,
		workerPrincipal,
		record.Manifest.PackageID,
		acknowledgedAt,
	)
	if err != nil {
		return store.ResultPackageRecord{}, resultPackageStoreRelayError(
			"record source result package acknowledgement",
			err,
		)
	}
	return acknowledged, nil
}

func releaseResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
) error {
	params := protocol.ReleaseResultPackageParams{
		PackageID: record.Manifest.PackageID,
		Sequence:  record.Sequence,
	}
	payload, err := callResultPackagePeer(
		ctx,
		resultPackageReleaseTimeout,
		source,
		protocol.MethodReleaseResultPackage,
		record.Manifest.TreeID,
		workerPrincipal,
		params,
	)
	if err != nil {
		return deferResultPackagePeerError("release source result package", err)
	}
	released, err := protocol.DecodePayload[protocol.ReleaseResultPackageResult](payload)
	if err != nil || released.Validate() != nil || released != protocol.ReleaseResultPackageResult(params) {
		return errors.New("source returned an invalid result package release acknowledgement")
	}
	return nil
}

func releaseAndMarkResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	registry resultPackageSourceAcknowledger,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	releasedAt time.Time,
) error {
	if record.SourceAcknowledgedAt == 0 {
		return errors.New("result package release preceded source acknowledgement")
	}
	if err := releaseResultPackage(ctx, source, workerPrincipal, record); err != nil {
		return err
	}
	if _, err := registry.MarkResultPackageSourceReleased(
		ctx,
		workerPrincipal.DeviceID,
		workerPrincipal,
		record.Manifest.PackageID,
		releasedAt,
	); err != nil {
		return resultPackageStoreRelayError("record source result package release", err)
	}
	return nil
}

func (s *Server) releaseAndMarkResultPackage(
	ctx context.Context,
	source resultPackagePeer,
	registry resultPackageSourceAcknowledger,
	workerPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	releasedAt time.Time,
) error {
	err := releaseAndMarkResultPackage(ctx, source, registry, workerPrincipal, record, releasedAt)
	if err == nil {
		s.wakeResultPackageGC()
	}
	return err
}

func resultPackageStoreRelayError(operation string, err error) error {
	if isContextError(err) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if errors.Is(err, store.ErrAuthorizationDenied) ||
		errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrConflict) ||
		errors.Is(err, store.ErrResultPackageCorrupt) ||
		errors.Is(err, store.ErrResultPackageSequenceExhausted) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%w: %s: %v", errResultPackageRelayDeferred, operation, err)
}

func (s *Server) cancelResultPackageTransfer(
	root resultPackagePeer,
	rootPrincipal control.PrincipalIdentity,
	record store.ResultPackageRecord,
	attemptID string,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), resultPackageCancelTimeout)
	defer cancel()
	params := protocol.CancelResultPackageParams{
		AttemptID: attemptID,
		PackageID: record.Manifest.PackageID,
	}
	payload, err := root.callPeer(
		ctx,
		protocol.MethodCancelResultPackage,
		record.Manifest.TreeID,
		rootPrincipal,
		params,
	)
	if err != nil {
		return false
	}
	result, err := protocol.DecodePayload[protocol.CancelResultPackageResult](payload)
	if err != nil || result.Validate() != nil || result != protocol.CancelResultPackageResult(params) {
		s.reportError(errors.New("root returned an invalid result package cancellation acknowledgement"))
		return false
	}
	return true
}

func deferResultPackagePeerError(operation string, err error) error {
	if registrationDenied(err) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	var rpcError *peerRPCError
	if errors.As(err, &rpcError) && rpcError.code != protocol.ErrorUnavailable {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%w: %s: %v", errResultPackageRelayDeferred, operation, err)
}
