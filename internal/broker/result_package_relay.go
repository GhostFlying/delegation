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
	resultPackageRelayRequestTimeout  = 30 * time.Minute
	resultPackageReceivingLease       = 2 * time.Minute
	resultPackageCancelTimeout        = 5 * time.Second
	resultPackageReconnectScanTimeout = 30 * time.Second
	maximumPendingResultPackageRelays = 128
	maximumResultPackageRelayAttempts = 8
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
	wake chan struct{}
	done chan struct{}
}

type resultPackageRelayPeerScan struct {
	wake chan struct{}
}

type resultPackageRelayCoordinator struct {
	context context.Context
	timeout time.Duration
	relay   resultPackageRelayFunc
	scan    resultPackageRelayScanFunc
	report  func(error)

	mu         sync.Mutex
	active     map[resultPackageRelayKey]*resultPackageRelayFlight
	activeScan map[string]*resultPackageRelayPeerScan
	stopped    bool
	wait       sync.WaitGroup
}

func newResultPackageRelayCoordinator(
	ctx context.Context,
	timeout time.Duration,
	relay resultPackageRelayFunc,
	scan resultPackageRelayScanFunc,
	report func(error),
) *resultPackageRelayCoordinator {
	if report == nil {
		report = func(error) {}
	}
	return &resultPackageRelayCoordinator{
		context:    ctx,
		timeout:    timeout,
		relay:      relay,
		scan:       scan,
		report:     report,
		active:     make(map[resultPackageRelayKey]*resultPackageRelayFlight),
		activeScan: make(map[string]*resultPackageRelayPeerScan),
	}
}

func (c *resultPackageRelayCoordinator) schedulePeerReconnect(deviceID string) bool {
	if c == nil || c.context == nil || c.scan == nil {
		return false
	}
	c.mu.Lock()
	if c.stopped || c.context.Err() != nil {
		c.mu.Unlock()
		return false
	}
	if current := c.activeScan[deviceID]; current != nil {
		select {
		case current.wake <- struct{}{}:
		default:
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
		for {
			ctx, cancel := context.WithTimeout(c.context, resultPackageReconnectScanTimeout)
			page, err := c.scan(ctx, deviceID, after)
			cancel()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					c.report(fmt.Errorf("scan pending result packages for peer %s: %w", deviceID, err))
				}
				return
			}
			flights := make([]*resultPackageRelayFlight, 0, len(page.requests))
			for _, request := range page.requests {
				flight, _ := c.scheduleFlight(request)
				if flight != nil {
					flights = append(flights, flight)
				}
			}
			if !c.waitForResultPackageRelayFlights(scan, flights) || page.nextAfter == nil {
				return
			}
			after = page.nextAfter
		}
	}()
	return true
}

func (c *resultPackageRelayCoordinator) waitForResultPackageRelayFlights(
	scan *resultPackageRelayPeerScan,
	flights []*resultPackageRelayFlight,
) bool {
flights:
	for index, flight := range flights {
		for {
			select {
			case <-flight.done:
				continue flights
			case <-scan.wake:
				for _, pending := range flights[index:] {
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
	_, started := c.scheduleFlight(request)
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
		select {
		case current.wake <- struct{}{}:
		default:
		}
		c.mu.Unlock()
		return current, false
	}
	flight := &resultPackageRelayFlight{
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	c.active[key] = flight
	c.wait.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.wait.Done()
		defer func() {
			c.mu.Lock()
			delete(c.active, key)
			close(flight.done)
			c.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(c.context, c.timeout)
		defer cancel()
		var err error
		for attempt := 0; attempt < maximumResultPackageRelayAttempts; attempt++ {
			err = c.relay(ctx, request)
			if err == nil {
				return
			}
			delay, retry := resultPackageRelayRetryDelay(err, attempt)
			if !retry || attempt+1 == maximumResultPackageRelayAttempts {
				break
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-flight.wake:
				timer.Stop()
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		if !errors.Is(err, context.Canceled) {
			c.report(fmt.Errorf("relay result package %s: %w", request.packageID, err))
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
}

func (s *Server) relayResultPackage(
	ctx context.Context,
	request resultPackageRelayRequest,
) error {
	record, err := s.registry.GetResultPackageForDelivery(
		ctx, request.source.DeviceID, request.source, request.packageID,
	)
	if err != nil {
		return fmt.Errorf("load relay metadata: %w", err)
	}
	if record.SourcePrincipal != request.source {
		return errors.New("stored result package source differs from its relay request")
	}
	if record.SourceAcknowledgedAt != 0 {
		return nil
	}
	source := s.connection(record.SourcePrincipal.DeviceID)
	if source == nil {
		return fmt.Errorf("%w: source peer is offline", errResultPackageRelayDeferred)
	}
	if record.State == store.ResultPackageDelivered {
		return acknowledgeAndMarkResultPackage(
			ctx, source, s.registry, record.SourcePrincipal, record, s.now(),
		)
	}
	root := s.connection(record.RootPrincipal.DeviceID)
	if root == nil {
		return fmt.Errorf("%w: root peer is offline", errResultPackageRelayDeferred)
	}
	attemptID, err := s.newID()
	if err != nil {
		return fmt.Errorf("create result package transfer attempt: %w", err)
	}
	began, err := transferResultPackage(
		ctx,
		source,
		root,
		record.SourcePrincipal,
		record.RootPrincipal,
		record,
		attemptID,
		s.now().Add(resultPackageReceivingLease).Unix(),
	)
	if err != nil {
		if began {
			if !s.cancelResultPackageTransfer(root, record.RootPrincipal, record, attemptID) &&
				errors.Is(err, errResultPackageRelayDeferred) {
				return retryResultPackageRelayAfter(err, resultPackageReceivingLease)
			}
		}
		return err
	}
	delivered, err := s.markResultPackageDelivered(ctx, record.RootPrincipal, request.packageID)
	if err != nil {
		return fmt.Errorf("record durable result package delivery: %w", err)
	}
	return acknowledgeAndMarkResultPackage(
		ctx, source, s.registry, record.SourcePrincipal, delivered, s.now(),
	)
}

func (s *Server) markResultPackageDelivered(
	ctx context.Context,
	root control.PrincipalIdentity,
	packageID string,
) (store.ResultPackageRecord, error) {
	delivered, err := s.registry.MarkResultPackageDelivered(
		ctx, root.DeviceID, root, packageID, s.now(),
	)
	if err != nil {
		return store.ResultPackageRecord{}, err
	}
	s.artifactNotifier.notify(treeKey{
		controllerID: delivered.Manifest.ControllerID,
		treeID:       delivered.Manifest.TreeID,
	})
	return delivered, nil
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
		return resultPackageRelayScanPage{}, err
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
	leaseExpiresAt int64,
) (bool, error) {
	beginParams := protocol.BeginResultPackageParams{
		AttemptID:      attemptID,
		PackageID:      record.Manifest.PackageID,
		LeaseExpiresAt: leaseExpiresAt,
		Metadata:       record.Metadata,
	}
	payload, err := root.callPeer(
		ctx,
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
			return false, retryResultPackageRelayAfter(deferred, resultPackageReceivingLease)
		}
		return false, deferResultPackagePeerError("begin root result package", err)
	}
	begin, err := protocol.DecodePayload[protocol.BeginResultPackageResult](payload)
	if err != nil || begin.Validate() != nil || begin.AttemptID != attemptID ||
		begin.PackageID != record.Manifest.PackageID {
		return false, errors.New("root returned invalid result package begin metadata")
	}
	if begin.Outcome == protocol.ResultPackageAlreadyAvailable {
		return false, nil
	}
	offsets, err := validateResultPackageOffsets(record.Manifest.Parts, begin.Offsets)
	if err != nil {
		return true, err
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
			return true, err
		}
	}
	finishParams := protocol.FinishResultPackageParams{
		AttemptID: attemptID,
		PackageID: record.Manifest.PackageID,
	}
	payload, err = root.callPeer(
		ctx,
		protocol.MethodFinishResultPackage,
		record.Manifest.TreeID,
		rootPrincipal,
		finishParams,
	)
	if err != nil {
		return true, deferResultPackagePeerError("finish root result package", err)
	}
	finished, err := protocol.DecodePayload[protocol.FinishResultPackageResult](payload)
	if err != nil || finished.Validate() != nil ||
		finished != protocol.FinishResultPackageResult(finishParams) {
		return true, errors.New("root returned invalid result package finish metadata")
	}
	return true, nil
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
		payload, err := source.callPeer(
			ctx,
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
		payload, err = root.callPeer(
			ctx,
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
	payload, err := source.callPeer(
		ctx,
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
) error {
	if err := acknowledgeResultPackage(ctx, source, workerPrincipal, record); err != nil {
		return err
	}
	if _, err := registry.MarkResultPackageSourceAcknowledged(
		ctx,
		workerPrincipal.DeviceID,
		workerPrincipal,
		record.Manifest.PackageID,
		acknowledgedAt,
	); err != nil {
		return fmt.Errorf(
			"%w: record source result package acknowledgement: %v",
			errResultPackageRelayDeferred,
			err,
		)
	}
	return nil
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
	var rpcError *peerRPCError
	if errors.As(err, &rpcError) && rpcError.code != protocol.ErrorUnavailable {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%w: %s: %v", errResultPackageRelayDeferred, operation, err)
}
