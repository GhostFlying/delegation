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

func TestResultPackageRelayCoordinatorIsAsyncDeduplicatedAndRetryable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 3)
	release := make(chan struct{})
	var attempts atomic.Int32
	var reports atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		time.Second,
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
		time.Minute,
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

func TestResultPackagePeerReconnectContinuesAfterAnExactFullPage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := resultPackageRelayPrincipalsForRequest()
	requests := make([]resultPackageRelayRequest, maximumPendingResultPackageRelays)
	for index := range requests {
		requests[index] = resultPackageRelayRequest{
			source:    worker,
			packageID: fmt.Sprintf("123e4567-e89b-42d3-a456-%012d", index+1),
		}
	}
	next := &store.ResultPackageRelayCursor{
		PublishedAt: 40,
		PackageID:   requests[len(requests)-1].packageID,
	}
	secondPage := make(chan struct{}, 1)
	var relays atomic.Int32
	coordinator := newResultPackageRelayCoordinator(
		ctx,
		time.Second,
		func(context.Context, resultPackageRelayRequest) error {
			relays.Add(1)
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
			if after == nil {
				return resultPackageRelayScanPage{requests: requests, nextAfter: next}, nil
			}
			if *after != *next {
				return resultPackageRelayScanPage{}, fmt.Errorf("unexpected cursor %#v", after)
			}
			secondPage <- struct{}{}
			return resultPackageRelayScanPage{}, nil
		},
		nil,
	)
	if !coordinator.schedulePeerReconnect(resultRelayRootPeer) {
		t.Fatal("full reconnect page was not scheduled")
	}
	select {
	case <-secondPage:
	case <-time.After(time.Second):
		t.Fatal("exact-full reconnect page did not continue with its cursor")
	}
	coordinator.stop()
	coordinator.waitForShutdown()
	if got := relays.Load(); got != maximumPendingResultPackageRelays {
		t.Fatalf("relays = %d, want %d", got, maximumPendingResultPackageRelays)
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
		time.Hour,
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
		time.Second,
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
