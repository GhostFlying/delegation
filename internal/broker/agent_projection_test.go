package broker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/GhostFlying/delegation/internal/store"
)

const (
	agentProjectionThreadID = "123e4567-e89b-42d3-a456-426614174170"
	agentProjectionSpawnID  = "123e4567-e89b-42d3-a456-426614174171"
	agentProjectionAgentID  = "123e4567-e89b-42d3-a456-426614174172"
)

type observedAgentRegistry struct {
	*store.Store
	calls     int
	afterList func(int)
	listErr   error
}

func (r *observedAgentRegistry) ListAgents(
	ctx context.Context,
	source control.PrincipalIdentity,
	request store.AgentPageRequest,
) (store.AgentPage, error) {
	r.calls++
	if r.listErr != nil {
		return store.AgentPage{}, r.listErr
	}
	page, err := r.Store.ListAgents(ctx, source, request)
	if r.afterList != nil {
		r.afterList(r.calls)
	}
	return page, err
}

func TestProjectAgentStateUsesLifecycleFreshnessAndAuthorityPrecedence(t *testing.T) {
	spawn := protocol.AgentSummary{
		SpawnID: agentProjectionSpawnID,
		Principal: control.NewWorkerPrincipal(
			brokerTestControllerID, agentProjectionThreadID, agentProjectionAgentID,
			brokerTestDeviceID, brokerTestDeviceID,
		).Identity(),
		TaskName: "projection", SpawnStatus: protocol.AgentSpawnPending, Sequence: 1,
	}
	running := &store.AgentLifecycleRecord{
		TargetRevision: 7, Phase: protocol.WorkerLifecycleRunning, ObservedAt: 11,
	}
	tests := []struct {
		name             string
		record           store.AgentCurrentRecord
		target           agentTargetSnapshot
		wantFreshness    protocol.AgentLifecycleFreshness
		wantEffective    protocol.AgentEffectiveStatus
		wantFailure      string
		wantSource       protocol.AgentFailureSource
		wantDispatchable bool
	}{
		{
			name: "current running", record: store.AgentCurrentRecord{Spawn: spawn, Lifecycle: running},
			target:        agentTargetSnapshot{connected: true, syncReady: true, dispatchable: true},
			wantFreshness: protocol.AgentLifecycleCurrent, wantEffective: protocol.AgentEffectiveRunning,
			wantDispatchable: true,
		},
		{
			name: "syncing preserves lifecycle", record: store.AgentCurrentRecord{Spawn: spawn, Lifecycle: running},
			target:        agentTargetSnapshot{connected: true},
			wantFreshness: protocol.AgentLifecycleSyncing, wantEffective: protocol.AgentEffectiveIndeterminate,
		},
		{
			name: "offline preserves lifecycle", record: store.AgentCurrentRecord{Spawn: spawn, Lifecycle: running},
			wantFreshness: protocol.AgentLifecycleOffline, wantEffective: protocol.AgentEffectiveIndeterminate,
		},
		{
			name: "missing is explicit", record: store.AgentCurrentRecord{Spawn: spawn},
			target:        agentTargetSnapshot{connected: true, syncReady: true, dispatchable: true},
			wantFreshness: protocol.AgentLifecycleMissing, wantEffective: protocol.AgentEffectiveIndeterminate,
			wantDispatchable: true,
		},
	}
	failedSpawn := spawn
	failedSpawn.SpawnStatus = protocol.AgentSpawnFailed
	failedSpawn.SpawnFailureCode = "dispatch_failed"
	tests = append(tests, struct {
		name             string
		record           store.AgentCurrentRecord
		target           agentTargetSnapshot
		wantFreshness    protocol.AgentLifecycleFreshness
		wantEffective    protocol.AgentEffectiveStatus
		wantFailure      string
		wantSource       protocol.AgentFailureSource
		wantDispatchable bool
	}{
		name: "spawn failure wins conflicting lifecycle failure",
		record: store.AgentCurrentRecord{Spawn: failedSpawn, Lifecycle: &store.AgentLifecycleRecord{
			TargetRevision: 8, Phase: protocol.WorkerLifecycleFailed,
			FailureCode: "thread_start_failed", ObservedAt: 12,
		}},
		target:        agentTargetSnapshot{connected: true, syncReady: true},
		wantFreshness: protocol.AgentLifecycleCurrent, wantEffective: protocol.AgentEffectiveFailed,
		wantFailure: "dispatch_failed", wantSource: protocol.AgentFailureSourceSpawn,
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := projectAgentState(test.record, test.target)
			if got.LifecycleFreshness != test.wantFreshness ||
				got.EffectiveStatus != test.wantEffective ||
				got.EffectiveFailureCode != test.wantFailure ||
				got.FailureSource != test.wantSource ||
				got.TargetDispatchable != test.wantDispatchable {
				t.Fatalf("projected state = %#v", got)
			}
			if test.record.Lifecycle != nil &&
				(got.LifecyclePhase != test.record.Lifecycle.Phase ||
					got.LifecycleTargetRevision != test.record.Lifecycle.TargetRevision ||
					got.LifecycleObservedAt != test.record.Lifecycle.ObservedAt) {
				t.Fatalf("projection lost lifecycle diagnostics: %#v", got)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("projected state is invalid: %v", err)
			}
		})
	}
}

func TestCaptureAgentConnectionsUsesCompleteDispatchGate(t *testing.T) {
	const staleDevice = "123e4567-e89b-42d3-a456-426614174179"
	newSession := func(deviceID string, syncReady, workerReady, compatible, draining bool) *session {
		current := &session{deviceID: deviceID}
		current.revision.Store(1)
		current.workerSyncReady.Store(syncReady)
		current.workerReady.Store(workerReady)
		current.versionCompatible.Store(compatible)
		current.draining.Store(draining)
		return current
	}
	connections := map[string]*session{
		"ready":        newSession("ready", true, true, true, false),
		"syncing":      newSession("syncing", false, true, true, false),
		"unqualified":  newSession("unqualified", true, false, true, false),
		"incompatible": newSession("incompatible", true, true, false, false),
		"draining":     newSession("draining", true, true, true, true),
		staleDevice:    newSession(staleDevice, true, true, true, false),
	}
	server := &Server{connections: connections, latestRevisions: map[string]uint64{
		"ready": 1, "syncing": 1, "unqualified": 1, "incompatible": 1,
		"draining": 1, staleDevice: 2,
	}}
	got := server.captureAgentConnections()
	if !got.targets["ready"].dispatchable {
		t.Fatal("fully ready target was not dispatchable")
	}
	for _, deviceID := range []string{"syncing", "unqualified", "incompatible", "draining"} {
		if !got.targets[deviceID].connected || got.targets[deviceID].dispatchable {
			t.Fatalf("target %q snapshot = %#v", deviceID, got.targets[deviceID])
		}
	}
	if _, found := got.targets[staleDevice]; found {
		t.Fatalf("stale connection was captured: %#v", got.targets[staleDevice])
	}
}

func TestListAgentStatesRetriesConnectionChurnAndStopsAtBound(t *testing.T) {
	newFixture := func(t *testing.T) (*Server, *observedAgentRegistry, control.PrincipalIdentity) {
		t.Helper()
		registry, err := store.Open(
			context.Background(), filepath.Join(t.TempDir(), "state", "broker.sqlite3"),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := registry.Close(); err != nil {
				t.Errorf("close registry: %v", err)
			}
		})
		descriptor := hello().Descriptor()
		device, err := registry.RegisterTrustedDevice(
			context.Background(), descriptor, time.Unix(1, 0),
		)
		if err != nil {
			t.Fatal(err)
		}
		_, root, err := registry.EnsureRootTree(
			context.Background(), brokerTestControllerID, agentProjectionThreadID,
			brokerTestDeviceID, time.Unix(2, 0),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := registry.BeginAgentSpawn(context.Background(), store.AgentSpawnIntent{
			Source: root.Identity(), SpawnID: agentProjectionSpawnID, AgentID: agentProjectionAgentID,
			TargetDeviceID: brokerTestDeviceID, TaskName: "projection_retry",
		}, time.Unix(3, 0)); err != nil {
			t.Fatal(err)
		}
		observed := &observedAgentRegistry{Store: registry}
		current := &session{deviceID: brokerTestDeviceID}
		current.revision.Store(device.Revision)
		current.workerSyncReady.Store(true)
		current.workerReady.Store(true)
		current.versionCompatible.Store(true)
		server := &Server{
			registry: observed, connections: map[string]*session{brokerTestDeviceID: current},
			latestRevisions: map[string]uint64{brokerTestDeviceID: device.Revision},
		}
		return server, observed, root.Identity()
	}

	t.Run("one change retries the whole snapshot", func(t *testing.T) {
		server, registry, source := newFixture(t)
		registry.afterList = func(call int) {
			if call == 1 {
				server.mu.Lock()
				server.statusGeneration++
				server.mu.Unlock()
			}
		}
		result, err := server.listAgentStates(
			context.Background(), source, store.AgentPageRequest{Limit: 1},
		)
		if err != nil {
			t.Fatal(err)
		}
		if registry.calls != 2 || len(result.Agents) != 1 ||
			result.Agents[0].SpawnID != agentProjectionSpawnID {
			t.Fatalf("retried snapshot = %#v after %d reads", result, registry.calls)
		}
	})

	t.Run("continuous changes fail closed", func(t *testing.T) {
		server, registry, source := newFixture(t)
		registry.afterList = func(int) {
			server.mu.Lock()
			server.statusGeneration++
			server.mu.Unlock()
		}
		_, err := server.listAgentStates(
			context.Background(), source, store.AgentPageRequest{Limit: 1},
		)
		if err == nil || !strings.Contains(err.Error(), "connections changed") {
			t.Fatalf("continuous churn error = %v", err)
		}
		if registry.calls != maximumAgentSnapshotAttempts {
			t.Fatalf("agent snapshot reads = %d, want %d", registry.calls, maximumAgentSnapshotAttempts)
		}
	})

	t.Run("store failure is not retried", func(t *testing.T) {
		server, registry, source := newFixture(t)
		injected := errors.New("injected list failure")
		registry.listErr = injected
		_, err := server.listAgentStates(
			context.Background(), source, store.AgentPageRequest{Limit: 1},
		)
		if !errors.Is(err, injected) || registry.calls != 1 {
			t.Fatalf("store failure = %v after %d reads", err, registry.calls)
		}
	})
}
