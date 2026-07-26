package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	resultRelayTreeID    = "123e4567-e89b-42d3-a456-426614174800"
	resultRelayRootID    = "123e4567-e89b-42d3-a456-426614174801"
	resultRelayWorkerID  = "123e4567-e89b-42d3-a456-426614174802"
	resultRelaySourceID  = "123e4567-e89b-42d3-a456-426614174803"
	resultRelayRootPeer  = "123e4567-e89b-42d3-a456-426614174804"
	resultRelayPackageID = "123e4567-e89b-42d3-a456-426614174805"
	resultRelayThreadID  = "123e4567-e89b-42d3-a456-426614174806"
	resultRelayTurnID    = "123e4567-e89b-42d3-a456-426614174807"
	resultRelayAttemptID = "123e4567-e89b-42d3-a456-426614174808"
)

type fakeResultPackagePeer struct {
	mu      sync.Mutex
	calls   []resultPackagePeerCall
	handler func(resultPackagePeerCall) (any, error)
}

type resultPackagePeerCall struct {
	method string
	treeID string
	source control.PrincipalIdentity
	params any
}

type fakeResultPackageSourceAcknowledger struct {
	mu      sync.Mutex
	calls   int
	handler func(control.PrincipalIdentity, string, time.Time) (store.ResultPackageRecord, error)
}

func (r *fakeResultPackageSourceAcknowledger) MarkResultPackageSourceAcknowledged(
	_ context.Context,
	_ string,
	source control.PrincipalIdentity,
	packageID string,
	acknowledgedAt time.Time,
) (store.ResultPackageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.handler(source, packageID, acknowledgedAt)
}

type fakeResultPackageDeliveryRegistry struct {
	Registry
	handler func(
		string,
		control.PrincipalIdentity,
		string,
		time.Time,
	) (store.ResultPackageRecord, error)
}

type fakeResultPackageLoadRegistry struct {
	Registry
	record store.ResultPackageRecord
	err    error
}

func (r *fakeResultPackageLoadRegistry) GetResultPackageForDelivery(
	_ context.Context,
	_ string,
	_ control.PrincipalIdentity,
	_ string,
) (store.ResultPackageRecord, error) {
	return r.record, r.err
}

type blockingResultPackagePeer struct{}

func (blockingResultPackagePeer) callPeer(
	ctx context.Context,
	_ string,
	_ string,
	_ control.PrincipalIdentity,
	_ any,
) (json.RawMessage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *fakeResultPackageDeliveryRegistry) MarkResultPackageDelivered(
	_ context.Context,
	connectedDeviceID string,
	root control.PrincipalIdentity,
	packageID string,
	deliveredAt time.Time,
) (store.ResultPackageRecord, error) {
	return r.handler(connectedDeviceID, root, packageID, deliveredAt)
}

func (p *fakeResultPackagePeer) callPeer(
	_ context.Context,
	method, treeID string,
	source control.PrincipalIdentity,
	params any,
) (json.RawMessage, error) {
	call := resultPackagePeerCall{method: method, treeID: treeID, source: source, params: params}
	p.mu.Lock()
	p.calls = append(p.calls, call)
	p.mu.Unlock()
	result, err := p.handler(call)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (p *fakeResultPackagePeer) methods() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	methods := make([]string, 0, len(p.calls))
	for _, call := range p.calls {
		methods = append(methods, call.method)
	}
	return methods
}

func TestTransferResultPackageResumesAndUsesFixedChunks(t *testing.T) {
	data := make([]byte, protocol.ResultPackageChunkBytes+19)
	for index := range data {
		data[index] = byte(index % 251)
	}
	record := resultPackageRelayRecord(t, data)
	worker, root := resultPackageRelayPrincipals()
	const resumedOffset = int64(7)

	source := &fakeResultPackagePeer{}
	source.handler = func(call resultPackagePeerCall) (any, error) {
		if call.method != protocol.MethodReadResultPackagePart || call.treeID != resultRelayTreeID ||
			call.source != worker {
			return nil, fmt.Errorf("unexpected source call %#v", call)
		}
		params := call.params.(protocol.ReadResultPackagePartParams)
		end := params.Offset + int64(params.Limit)
		if end > int64(len(data)) {
			return nil, errors.New("read beyond fixture")
		}
		return protocol.ReadResultPackagePartResult{
			PackageID:  params.PackageID,
			Kind:       params.Kind,
			Offset:     params.Offset,
			Data:       append([]byte(nil), data[params.Offset:end]...),
			NextOffset: end,
		}, nil
	}
	rootPeer := &fakeResultPackagePeer{}
	rootPeer.handler = func(call resultPackagePeerCall) (any, error) {
		if call.treeID != resultRelayTreeID || call.source != root {
			return nil, fmt.Errorf("unexpected root authority %#v", call)
		}
		switch params := call.params.(type) {
		case protocol.BeginResultPackageParams:
			return protocol.BeginResultPackageResult{
				AttemptID: params.AttemptID,
				PackageID: params.PackageID,
				Outcome:   protocol.ResultPackageReceiving,
				Offsets: []protocol.ResultPackagePartOffset{{
					Kind: protocol.ResultPackagePartRollout, NextOffset: resumedOffset,
				}},
			}, nil
		case protocol.WriteResultPackagePartParams:
			end := params.Offset + int64(len(params.Data))
			if string(params.Data) != string(data[params.Offset:end]) {
				return nil, errors.New("relay changed result package bytes")
			}
			return protocol.WriteResultPackagePartResult{
				AttemptID:  params.AttemptID,
				PackageID:  params.PackageID,
				Kind:       params.Kind,
				NextOffset: end,
			}, nil
		case protocol.FinishResultPackageParams:
			return protocol.FinishResultPackageResult(params), nil
		default:
			return nil, fmt.Errorf("unexpected root call %#v", call)
		}
	}

	began, err := transferResultPackage(
		context.Background(),
		source,
		rootPeer,
		worker,
		root,
		record,
		resultRelayAttemptID,
		1_700_000_000,
	)
	if err != nil || !began {
		t.Fatalf("transfer result = began %t, error %v", began, err)
	}
	if got, want := source.methods(), []string{
		protocol.MethodReadResultPackagePart,
		protocol.MethodReadResultPackagePart,
	}; !equalStrings(got, want) {
		t.Fatalf("source methods = %v, want %v", got, want)
	}
	if got, want := rootPeer.methods(), []string{
		protocol.MethodBeginResultPackage,
		protocol.MethodWriteResultPackagePart,
		protocol.MethodWriteResultPackagePart,
		protocol.MethodFinishResultPackage,
	}; !equalStrings(got, want) {
		t.Fatalf("root methods = %v, want %v", got, want)
	}
}

func TestResultPackagePeerCallHasAnOperationDeadline(t *testing.T) {
	const timeout = 20 * time.Millisecond
	started := time.Now()
	_, err := callResultPackagePeer(
		context.Background(),
		timeout,
		blockingResultPackagePeer{},
		protocol.MethodBeginResultPackage,
		resultRelayTreeID,
		resultPackageRelayPrincipalsForRequest(),
		protocol.BeginResultPackageParams{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded peer call error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded peer call took %s", elapsed)
	}
}

func TestTransferResultPackageAcceptsAlreadyAvailableWithoutReadingSource(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	worker, root := resultPackageRelayPrincipals()
	source := &fakeResultPackagePeer{handler: func(resultPackagePeerCall) (any, error) {
		return nil, errors.New("source must not be read")
	}}
	rootPeer := &fakeResultPackagePeer{handler: func(call resultPackagePeerCall) (any, error) {
		params := call.params.(protocol.BeginResultPackageParams)
		return protocol.BeginResultPackageResult{
			AttemptID: params.AttemptID,
			PackageID: params.PackageID,
			Outcome:   protocol.ResultPackageAlreadyAvailable,
			Offsets:   []protocol.ResultPackagePartOffset{},
		}, nil
	}}
	began, err := transferResultPackage(
		context.Background(), source, rootPeer, worker, root, record, resultRelayAttemptID, 1,
	)
	if err != nil || began {
		t.Fatalf("already available result = began %t, error %v", began, err)
	}
	if len(source.methods()) != 0 {
		t.Fatalf("already available package read source: %v", source.methods())
	}
}

func TestTransferResultPackageRejectsRootOffsetOutsideManifest(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	worker, root := resultPackageRelayPrincipals()
	source := &fakeResultPackagePeer{handler: func(resultPackagePeerCall) (any, error) {
		return nil, errors.New("source must not be read")
	}}
	rootPeer := &fakeResultPackagePeer{handler: func(call resultPackagePeerCall) (any, error) {
		params := call.params.(protocol.BeginResultPackageParams)
		return protocol.BeginResultPackageResult{
			AttemptID: params.AttemptID,
			PackageID: params.PackageID,
			Outcome:   protocol.ResultPackageReceiving,
			Offsets: []protocol.ResultPackagePartOffset{{
				Kind:       record.Manifest.Parts[0].Kind,
				NextOffset: record.Manifest.Parts[0].Size + 1,
			}},
		}, nil
	}}
	began, err := transferResultPackage(
		context.Background(), source, rootPeer, worker, root, record, resultRelayAttemptID, 1,
	)
	if !began || err == nil {
		t.Fatalf("out-of-range root offset = began %t, error %v", began, err)
	}
	if len(source.methods()) != 0 {
		t.Fatalf("out-of-range root offset read source: %v", source.methods())
	}
}

func TestResultPackageSourceAcknowledgementRetriesLostBrokerMark(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	record.State = store.ResultPackageDelivered
	record.Sequence = 7
	record.DeliveredAt = 50
	worker, _ := resultPackageRelayPrincipals()
	source := &fakeResultPackagePeer{handler: func(call resultPackagePeerCall) (any, error) {
		if call.method != protocol.MethodAcknowledgeResultPackage || call.source != worker {
			return nil, fmt.Errorf("unexpected source acknowledgement %#v", call)
		}
		return protocol.AcknowledgeResultPackageResult(
			call.params.(protocol.AcknowledgeResultPackageParams),
		), nil
	}}
	markFailed := true
	registry := &fakeResultPackageSourceAcknowledger{
		handler: func(
			source control.PrincipalIdentity,
			packageID string,
			acknowledgedAt time.Time,
		) (store.ResultPackageRecord, error) {
			if source != worker || packageID != resultRelayPackageID || acknowledgedAt.Unix() != 60 {
				return store.ResultPackageRecord{}, errors.New("unexpected broker acknowledgement mark")
			}
			if markFailed {
				markFailed = false
				return store.ResultPackageRecord{}, errors.New("broker commit lost")
			}
			record.SourceAcknowledgedAt = acknowledgedAt.Unix()
			return record, nil
		},
	}
	for attempt := 1; attempt <= 2; attempt++ {
		err := acknowledgeAndMarkResultPackage(
			context.Background(), source, registry, worker, record, time.Unix(60, 0),
		)
		if attempt == 1 && !errors.Is(err, errResultPackageRelayDeferred) {
			t.Fatalf("lost broker mark error = %v, want deferred relay", err)
		}
		if attempt == 2 && err != nil {
			t.Fatalf("source acknowledgement replay: %v", err)
		}
	}
	if got := source.methods(); !equalStrings(got, []string{
		protocol.MethodAcknowledgeResultPackage,
		protocol.MethodAcknowledgeResultPackage,
	}) {
		t.Fatalf("source acknowledgement methods = %v", got)
	}
	registry.mu.Lock()
	markCalls := registry.calls
	registry.mu.Unlock()
	if markCalls != 2 {
		t.Fatalf("broker acknowledgement marks = %d, want 2", markCalls)
	}
}

func TestMarkResultPackageDeliveredWakesTreeWaiters(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	record.State = store.ResultPackageDelivered
	record.Sequence = 1
	record.DeliveredAt = 50
	_, root := resultPackageRelayPrincipals()
	registry := &fakeResultPackageDeliveryRegistry{
		handler: func(
			connectedDeviceID string,
			principal control.PrincipalIdentity,
			packageID string,
			deliveredAt time.Time,
		) (store.ResultPackageRecord, error) {
			if connectedDeviceID != root.DeviceID || principal != root ||
				packageID != resultRelayPackageID || deliveredAt.Unix() != 50 {
				return store.ResultPackageRecord{}, errors.New("unexpected delivery mark")
			}
			return record, nil
		},
	}
	server := &Server{
		registry:       registry,
		resultNotifier: newTreeNotifier(),
		now:            func() time.Time { return time.Unix(50, 0) },
	}
	key := treeKey{
		controllerID: record.Manifest.ControllerID,
		treeID:       record.Manifest.TreeID,
	}
	subscription := server.resultNotifier.subscribe(key)
	defer subscription.release()
	if _, err := server.markResultPackageDelivered(
		context.Background(), root, resultRelayPackageID,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscription.channel():
	case <-time.After(time.Second):
		t.Fatal("durable result package did not wake its root tree")
	}

	failedNotifier := newTreeNotifier()
	failedSubscription := failedNotifier.subscribe(key)
	defer failedSubscription.release()
	server = &Server{
		registry: &fakeResultPackageDeliveryRegistry{
			handler: func(
				string,
				control.PrincipalIdentity,
				string,
				time.Time,
			) (store.ResultPackageRecord, error) {
				return store.ResultPackageRecord{}, errors.New("broker commit failed")
			},
		},
		resultNotifier: failedNotifier,
		now:            func() time.Time { return time.Unix(50, 0) },
	}
	if _, err := server.markResultPackageDelivered(
		context.Background(), root, resultRelayPackageID,
	); !errors.Is(err, errResultPackageRelayDeferred) {
		t.Fatalf("failed delivery mark error = %v, want deferred relay", err)
	}
	select {
	case <-failedSubscription.channel():
		t.Fatal("failed delivery mark woke the root tree")
	default:
	}
}

func TestDeliveredResultPackageReplayWakesTreeBeforeSourceReconnect(t *testing.T) {
	record := resultPackageRelayRecord(t, []byte("payload"))
	worker, root := resultPackageRelayPrincipals()
	record.SourcePrincipal = worker
	record.RootPrincipal = root
	record.State = store.ResultPackageDelivered
	record.Sequence = 1
	record.DeliveredAt = 50
	server := &Server{
		registry:       &fakeResultPackageLoadRegistry{record: record},
		resultNotifier: newTreeNotifier(),
	}
	key := treeKey{
		controllerID: record.Manifest.ControllerID,
		treeID:       record.Manifest.TreeID,
	}
	subscription := server.resultNotifier.subscribe(key)
	defer subscription.release()
	err := server.relayResultPackage(context.Background(), resultPackageRelayRequest{
		source: worker, packageID: resultRelayPackageID,
	})
	if !errors.Is(err, errResultPackageRelayDeferred) {
		t.Fatalf("offline source replay error = %v, want deferred", err)
	}
	select {
	case <-subscription.channel():
	case <-time.After(time.Second):
		t.Fatal("delivered replay did not wake the root before source reconnect")
	}
}

func TestResultPackageRelayCoordinatorIsAsyncDeduplicatedAndRetryable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 3)
	release := make(chan struct{})
	var attempts atomic.Int32
	var reports atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(context.Context, resultPackageRelayRequest) error {
			attempt := int(attempts.Add(1))
			started <- attempt
			if attempt == 1 {
				<-release
				return fmt.Errorf("%w: disconnected", errResultPackageRelayDeferred)
			}
			return nil
		},
		nil,
		func(error) { reports.Add(1) },
	)
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}

	start := time.Now()
	if !coordinator.schedule(request) || time.Since(start) > 100*time.Millisecond {
		t.Fatal("relay scheduling blocked on the relay runner")
	}
	if attempt := <-started; attempt != 1 {
		t.Fatalf("first relay attempt = %d", attempt)
	}
	for index := 0; index < 32; index++ {
		if coordinator.schedule(request) {
			t.Fatal("concurrent package relay was not deduplicated")
		}
	}
	close(release)
	if attempt := <-started; attempt != 2 {
		t.Fatalf("automatic retry relay attempt = %d", attempt)
	}
	coordinator.stop()
	coordinator.waitForShutdown()
	if reports.Load() != 0 {
		t.Fatalf("deferred relay reported %d internal errors", reports.Load())
	}
}

func TestResultPackageRelaySurvivesMoreThanEightUnavailableAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempted := make(chan int, 12)
	var attempts atomic.Int32
	var reports atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(context.Context, resultPackageRelayRequest) error {
			attempt := int(attempts.Add(1))
			attempted <- attempt
			if attempt <= 9 {
				return fmt.Errorf("%w: peer unavailable", errResultPackageRelayDeferred)
			}
			return nil
		},
		nil,
		func(error) { reports.Add(1) },
	)
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}
	if !coordinator.schedule(request) {
		t.Fatal("durable relay was not scheduled")
	}
	for want := 1; want <= 10; want++ {
		select {
		case got := <-attempted:
			if got != want {
				t.Fatalf("relay attempt = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("relay stopped after %d unavailable attempts", want-1)
		}
		if want < 10 && coordinator.schedule(request) {
			t.Fatal("durable relay lost its single-flight while deferred")
		}
	}
	coordinator.stop()
	coordinator.waitForShutdown()
	if reports.Load() != 0 {
		t.Fatalf("recoverable unavailable relay reported %d errors", reports.Load())
	}
}

func TestResultPackageRelayRetriesTransientStoreFailures(t *testing.T) {
	t.Run("metadata load", func(t *testing.T) {
		record := resultPackageRelayRecord(t, []byte("payload"))
		record.SourcePrincipal = resultPackageRelayPrincipalsForRequest()
		record.State = store.ResultPackageDelivered
		record.Sequence = 1
		record.DeliveredAt = 50
		record.SourceAcknowledgedAt = 60
		registry := &fakeResultPackageLoadRegistry{
			record: record,
			err:    errors.New("database busy"),
		}
		server := &Server{registry: registry, resultNotifier: newTreeNotifier()}
		var attempts atomic.Int32
		assertResultPackageRelayRetries(t, func(ctx context.Context) error {
			if attempts.Add(1) == 2 {
				registry.err = nil
			}
			return server.relayResultPackage(ctx, resultPackageRelayRequest{
				source: record.SourcePrincipal, packageID: resultRelayPackageID,
			})
		})
	})

	t.Run("delivery mark", func(t *testing.T) {
		record := resultPackageRelayRecord(t, []byte("payload"))
		record.State = store.ResultPackageDelivered
		record.Sequence = 1
		record.DeliveredAt = 50
		_, root := resultPackageRelayPrincipals()
		var attempts atomic.Int32
		registry := &fakeResultPackageDeliveryRegistry{
			handler: func(
				string,
				control.PrincipalIdentity,
				string,
				time.Time,
			) (store.ResultPackageRecord, error) {
				if attempts.Add(1) == 1 {
					return store.ResultPackageRecord{}, errors.New("database busy")
				}
				return record, nil
			},
		}
		server := &Server{
			registry:       registry,
			resultNotifier: newTreeNotifier(),
			now:            func() time.Time { return time.Unix(50, 0) },
		}
		assertResultPackageRelayRetries(t, func(ctx context.Context) error {
			_, err := server.markResultPackageDelivered(ctx, root, resultRelayPackageID)
			return err
		})
	})
}

func assertResultPackageRelayRetries(t *testing.T, relay func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempted := make(chan int, 2)
	var attempts atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(ctx context.Context, _ resultPackageRelayRequest) error {
			attempt := int(attempts.Add(1))
			attempted <- attempt
			return relay(ctx)
		},
		nil,
		nil,
	)
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}
	if !coordinator.schedule(request) {
		t.Fatal("store retry relay was not scheduled")
	}
	for want := 1; want <= 2; want++ {
		select {
		case got := <-attempted:
			if got != want {
				t.Fatalf("store retry attempt = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("store retry stopped after attempt %d", want-1)
		}
	}
	coordinator.stop()
	coordinator.waitForShutdown()
}

func TestResultPackagePeerReconnectWakesDeferredRelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}
	attempted := make(chan int, 2)
	var attempts atomic.Int32
	var scans atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(context.Context, resultPackageRelayRequest) error {
			attempt := int(attempts.Add(1))
			attempted <- attempt
			if attempt == 1 {
				return retryResultPackageRelayAfter(
					fmt.Errorf("%w: root peer is offline", errResultPackageRelayDeferred),
					time.Hour,
				)
			}
			return nil
		},
		func(
			_ context.Context,
			deviceID string,
			after *store.ResultPackageRelayCursor,
		) (resultPackageRelayScanPage, error) {
			if deviceID != resultRelayRootPeer {
				return resultPackageRelayScanPage{}, fmt.Errorf(
					"unexpected reconnect peer %s",
					deviceID,
				)
			}
			if after != nil {
				return resultPackageRelayScanPage{}, errors.New("unexpected reconnect cursor")
			}
			scans.Add(1)
			return resultPackageRelayScanPage{
				requests: []resultPackageRelayRequest{request},
			}, nil
		},
		nil,
	)
	if !coordinator.schedule(request) {
		t.Fatal("initial offline relay was not scheduled")
	}
	if attempt := <-attempted; attempt != 1 {
		t.Fatalf("initial attempt = %d", attempt)
	}
	if !coordinator.schedulePeerReconnect(resultRelayRootPeer) {
		t.Fatal("root reconnect scan was not scheduled")
	}
	select {
	case attempt := <-attempted:
		if attempt != 2 {
			t.Fatalf("reconnect attempt = %d", attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("root reconnect did not wake the deferred live relay")
	}
	coordinator.stop()
	coordinator.waitForShutdown()
	if scans.Load() != 1 {
		t.Fatalf("root reconnect scans = %d, want 1", scans.Load())
	}
}

func TestResultPackageRelayBoundsResidentFlightsAndDrainsBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := resultPackageRelayPrincipalsForRequest()
	firstPage := make([]resultPackageRelayRequest, maximumPendingResultPackageRelays)
	for index := range firstPage {
		firstPage[index] = resultPackageRelayRequest{
			source:    worker,
			packageID: fmt.Sprintf("123e4567-e89b-42d3-a456-%012d", index+1),
		}
	}
	lastRequest := resultPackageRelayRequest{
		source: worker,
		packageID: fmt.Sprintf(
			"123e4567-e89b-42d3-a456-%012d",
			maximumPendingResultPackageRelays+1,
		),
	}
	next := &store.ResultPackageRelayCursor{
		PublishedAt: 40,
		PackageID:   firstPage[len(firstPage)-1].packageID,
	}
	secondPage := make(chan struct{})
	var secondPageOnce sync.Once
	attempted := make(chan string, maximumPendingResultPackageRelays*3)
	completed := make(chan string, maximumPendingResultPackageRelays+1)
	completedPackages := make(map[string]bool)
	var completedMu sync.Mutex
	var recovered atomic.Bool
	var coordinator *resultPackageRelayCoordinator
	var maximumResident atomic.Int32
	var relays atomic.Int32
	coordinator = newResultPackageRelayCoordinator(
		ctx,
		func(_ context.Context, request resultPackageRelayRequest) error {
			relays.Add(1)
			coordinator.mu.Lock()
			resident := int32(len(coordinator.active))
			coordinator.mu.Unlock()
			for current := maximumResident.Load(); resident > current; current = maximumResident.Load() {
				if maximumResident.CompareAndSwap(current, resident) {
					break
				}
			}
			attempted <- request.packageID
			if !recovered.Load() {
				return retryResultPackageRelayAfter(
					fmt.Errorf("%w: peer offline", errResultPackageRelayDeferred),
					time.Hour,
				)
			}
			completedMu.Lock()
			if !completedPackages[request.packageID] {
				completedPackages[request.packageID] = true
				completed <- request.packageID
			}
			completedMu.Unlock()
			return nil
		},
		func(
			_ context.Context,
			deviceID string,
			after *store.ResultPackageRelayCursor,
		) (resultPackageRelayScanPage, error) {
			if deviceID != worker.DeviceID {
				return resultPackageRelayScanPage{}, fmt.Errorf(
					"unexpected durable scan peer %s",
					deviceID,
				)
			}
			if after == nil {
				completedMu.Lock()
				requests := make([]resultPackageRelayRequest, 0, len(firstPage))
				for _, request := range firstPage {
					if !completedPackages[request.packageID] {
						requests = append(requests, request)
					}
				}
				completedMu.Unlock()
				return resultPackageRelayScanPage{requests: requests, nextAfter: next}, nil
			}
			if *after != *next {
				return resultPackageRelayScanPage{}, fmt.Errorf("unexpected cursor %#v", after)
			}
			secondPageOnce.Do(func() { close(secondPage) })
			completedMu.Lock()
			isCompleted := completedPackages[lastRequest.packageID]
			completedMu.Unlock()
			if isCompleted {
				return resultPackageRelayScanPage{}, nil
			}
			return resultPackageRelayScanPage{requests: []resultPackageRelayRequest{lastRequest}}, nil
		},
		nil,
	)
	for _, request := range firstPage {
		if !coordinator.schedule(request) {
			t.Fatal("initial relay page did not fit within the resident cap")
		}
	}
	if coordinator.schedule(lastRequest) {
		t.Fatal("relay beyond the resident cap started before durable rescan capacity")
	}
	for range maximumPendingResultPackageRelays {
		select {
		case <-attempted:
		case <-time.After(time.Second):
			t.Fatal("initial resident relay page did not start")
		}
	}
	select {
	case <-secondPage:
	case <-time.After(time.Second):
		t.Fatal("bounded relay scan did not continue to the durable backlog")
	}
	coordinator.mu.Lock()
	resident := len(coordinator.active)
	coordinator.mu.Unlock()
	if resident != maximumActiveResultPackageRelays {
		t.Fatalf("resident relays = %d, want cap %d", resident, maximumActiveResultPackageRelays)
	}
	if relays.Load() != maximumActiveResultPackageRelays {
		t.Fatalf("relay attempts before recovery = %d, want %d", relays.Load(), maximumActiveResultPackageRelays)
	}

	recovered.Store(true)
	coordinator.schedulePeerReconnect(worker.DeviceID)
	for range maximumPendingResultPackageRelays + 1 {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatal("bounded durable relay backlog did not drain after reconnect")
		}
	}
	cancel()
	coordinator.stop()
	coordinator.waitForShutdown()
	if maximumResident.Load() > maximumActiveResultPackageRelays {
		t.Fatalf(
			"maximum resident relays = %d, want at most %d",
			maximumResident.Load(),
			maximumActiveResultPackageRelays,
		)
	}
}

func TestResultPackageRelayCoordinatorShutdownStopsPagination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}
	started := make(chan struct{})
	var scans atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(ctx context.Context, _ resultPackageRelayRequest) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		func(
			context.Context,
			string,
			*store.ResultPackageRelayCursor,
		) (resultPackageRelayScanPage, error) {
			scans.Add(1)
			return resultPackageRelayScanPage{
				requests: []resultPackageRelayRequest{request},
				nextAfter: &store.ResultPackageRelayCursor{
					PublishedAt: 40,
					PackageID:   resultRelayPackageID,
				},
			}, nil
		},
		nil,
	)
	if !coordinator.schedulePeerReconnect(resultRelayRootPeer) {
		t.Fatal("reconnect scan was not scheduled")
	}
	<-started
	if coordinator.schedulePeerReconnect(resultRelayRootPeer) {
		t.Fatal("active peer rescan was not coalesced")
	}
	cancel()
	coordinator.stop()
	shutdown := make(chan struct{})
	go func() {
		coordinator.waitForShutdown()
		close(shutdown)
	}()
	select {
	case <-shutdown:
	case <-time.After(time.Second):
		t.Fatal("coordinator shutdown leaked a relay pagination goroutine")
	}
	if coordinator.schedulePeerReconnect(resultRelayRootPeer) {
		t.Fatal("stopped coordinator accepted a reconnect scan")
	}
	if got := scans.Load(); got != 1 {
		t.Fatalf("coalesced reconnect scans = %d, want 1", got)
	}
}

func TestResultPackageCancelLossRetriesAfterLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := resultPackageRelayRequest{
		source:    resultPackageRelayPrincipalsForRequest(),
		packageID: resultRelayPackageID,
	}
	attempted := make(chan time.Time, 2)
	var attempts atomic.Int32
	const leaseDelay = 20 * time.Millisecond
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		func(context.Context, resultPackageRelayRequest) error {
			attempted <- time.Now()
			if attempts.Add(1) == 1 {
				return retryResultPackageRelayAfter(
					fmt.Errorf("%w: cancellation acknowledgement was lost", errResultPackageRelayDeferred),
					leaseDelay,
				)
			}
			return nil
		},
		nil,
		nil,
	)
	if !coordinator.schedule(request) {
		t.Fatal("cancel-loss relay was not scheduled")
	}
	first := <-attempted
	second := <-attempted
	if second.Sub(first) < leaseDelay {
		t.Fatalf("cancel-loss retry delay = %s, want at least %s", second.Sub(first), leaseDelay)
	}
	coordinator.stop()
	coordinator.waitForShutdown()
}

func TestResultPackagePublicationErrorMappingAndConnectionAuthority(t *testing.T) {
	worker := resultPackageRelayPrincipalsForRequest()
	request := protocol.Envelope{
		ControllerID: worker.ControllerID,
		TreeID:       worker.TreeID,
		Source:       &worker,
	}
	if err := validatePublishResultPackageRequest(request, worker.DeviceID); err != nil {
		t.Fatal(err)
	}
	if err := validatePublishResultPackageRequest(request, resultRelayRootPeer); !errors.Is(
		err,
		errResultPackageConnectionMismatch,
	) {
		t.Fatalf("wrong connection error = %v", err)
	}
	root := worker
	root.ParentAgentID = ""
	request.Source = &root
	if err := validatePublishResultPackageRequest(request, root.DeviceID); err == nil {
		t.Fatal("root principal published a worker result package")
	}

	tests := []struct {
		err         error
		wantCode    int
		wantMessage string
		wantReport  bool
	}{
		{store.ErrResultPackageLifecycleNotReady, protocol.ErrorUnavailable, "lifecycle_not_ready", false},
		{store.ErrAuthorizationDenied, protocol.ErrorForbidden, "result package publication denied", false},
		{store.ErrNotFound, protocol.ErrorForbidden, "result package publication denied", false},
		{store.ErrConflict, protocol.ErrorConflict, "result package conflicts with broker state", false},
		{errors.New("disk unavailable"), protocol.ErrorUnavailable, "broker unavailable", true},
	}
	for _, test := range tests {
		code, message, report := mapPublishResultPackageStoreError(test.err)
		if code != test.wantCode || message != test.wantMessage || report != test.wantReport {
			t.Fatalf(
				"map %v = (%d, %q, %t), want (%d, %q, %t)",
				test.err,
				code,
				message,
				report,
				test.wantCode,
				test.wantMessage,
				test.wantReport,
			)
		}
	}
}

func TestResultPackageStoreRelayErrorSeparatesPermanentFailures(t *testing.T) {
	for _, err := range []error{
		store.ErrAuthorizationDenied,
		store.ErrNotFound,
		store.ErrConflict,
		store.ErrResultPackageCorrupt,
		store.ErrResultPackageSequenceExhausted,
		context.Canceled,
	} {
		mapped := resultPackageStoreRelayError("test operation", err)
		if errors.Is(mapped, errResultPackageRelayDeferred) || !errors.Is(mapped, err) {
			t.Fatalf("permanent store error %v mapped to %v", err, mapped)
		}
	}
	transient := errors.New("database temporarily unavailable")
	if mapped := resultPackageStoreRelayError("test operation", transient); !errors.Is(
		mapped,
		errResultPackageRelayDeferred,
	) {
		t.Fatalf("transient store error mapped to %v", mapped)
	}
	if mapped := deferResultPackagePeerError("test peer operation", store.ErrAuthorizationDenied); errors.Is(
		mapped,
		errResultPackageRelayDeferred,
	) {
		t.Fatalf("peer authority error mapped to deferred relay: %v", mapped)
	}
}

func TestResultPackageReceivingLeaseMatchesPeerInboxWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	want := now.Add(store.MaximumResultInboxLease).Unix()
	if got := resultPackageReceivingLeaseExpiresAt(now); got != want ||
		store.MaximumResultInboxLease != 10*time.Minute {
		t.Fatalf("receiving lease expires at %d, want %d", got, want)
	}
}

func resultPackageRelayRecord(t *testing.T, data []byte) store.ResultPackageRecord {
	t.Helper()
	digest := sha256.Sum256(data)
	rawDigest := sha256.Sum256([]byte("raw rollout"))
	manifest := protocol.ResultManifest{
		Version:           protocol.ResultManifestVersion,
		PackageID:         resultRelayPackageID,
		ControllerID:      brokerTestControllerID,
		TreeID:            resultRelayTreeID,
		SourceAgentID:     resultRelayWorkerID,
		SourceDeviceID:    resultRelaySourceID,
		ManagedThreadID:   resultRelayThreadID,
		TurnID:            resultRelayTurnID,
		LifecycleRevision: 1,
		Terminal:          protocol.ResultTerminal{Outcome: protocol.ResultTerminalCompleted},
		CapturedAt:        1,
		Rollout: protocol.ResultRolloutComponent{
			Status:    protocol.ResultRolloutAvailable,
			RawSize:   int64(len("raw rollout")),
			RawSHA256: fmt.Sprintf("%x", rawDigest),
		},
		Workspace: protocol.ResultWorkspaceComponent{
			Status:         protocol.ResultWorkspaceNotManaged,
			BaseWarnings:   []string{},
			ResultWarnings: []string{},
		},
		Parts: []protocol.ResultPackagePartDescriptor{{
			Kind:   protocol.ResultPackagePartRollout,
			Size:   int64(len(data)),
			SHA256: fmt.Sprintf("%x", digest),
		}},
	}
	manifestBytes, descriptor, err := protocol.EncodeResultManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return store.ResultPackageRecord{
		Metadata: protocol.ResultPackageMetadata{
			Manifest: manifestBytes, ManifestDescriptor: descriptor,
		},
		Manifest:     manifest,
		RootDeviceID: resultRelayRootPeer,
		State:        store.ResultPackageDeliveryPending,
		PublishedAt:  1,
	}
}

func resultPackageRelayPrincipals() (control.PrincipalIdentity, control.PrincipalIdentity) {
	worker := resultPackageRelayPrincipalsForRequest()
	root := control.NewRootPrincipal(
		brokerTestControllerID,
		resultRelayTreeID,
		resultRelayRootID,
		resultRelayRootPeer,
	).Identity()
	return worker, root
}

func resultPackageRelayPrincipalsForRequest() control.PrincipalIdentity {
	return control.NewWorkerPrincipal(
		brokerTestControllerID,
		resultRelayTreeID,
		resultRelayWorkerID,
		resultRelayRootID,
		resultRelaySourceID,
	).Identity()
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
