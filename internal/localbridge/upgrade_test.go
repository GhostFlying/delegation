package localbridge

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestUpgradeManagementUsesProtectedLocalBridge(t *testing.T) {
	manager := &fakeUpgradeManager{
		snapshot: testUpgradeSnapshot(),
		runtimeIdentity: UpgradeRuntimeIdentity{
			Version: "0.1.0-alpha.8", Digest: strings.Repeat("a", 64),
		},
	}
	identity := testServiceIdentity()
	server, err := ListenWithUpgradeManagement(
		testEndpoint(t), identity, &fakeBackend{}, nil, nil, nil, nil, nil, manager,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})
	endpoint := server.listener.Addr().String()

	prepared, err := PrepareUpgrade(
		context.Background(), endpoint, manager.snapshot.TargetVersion, "/service.json", "/service.env",
	)
	if err != nil || !reflect.DeepEqual(prepared, manager.snapshot) || manager.prepareCalls != 1 {
		t.Fatalf("PrepareUpgrade() = %#v, %v, calls=%d", prepared, err, manager.prepareCalls)
	}
	for name, call := range map[string]func(context.Context, string, string) (UpgradeSnapshot, error){
		"arm": ArmUpgrade, "activate": ActivateUpgrade, "cancel": CancelUpgrade,
	} {
		got, err := call(context.Background(), endpoint, manager.snapshot.TransactionID)
		if err != nil || !reflect.DeepEqual(got, manager.snapshot) {
			t.Fatalf("%s = %#v, %v", name, got, err)
		}
	}
	status, err := ReadUpgrade(context.Background(), endpoint)
	if err != nil || status == nil || !reflect.DeepEqual(*status, manager.snapshot) {
		t.Fatalf("ReadUpgrade() = %#v, %v", status, err)
	}
	runtimeIdentity, err := ReadUpgradeRuntime(context.Background(), endpoint)
	if err != nil || !reflect.DeepEqual(runtimeIdentity, manager.runtimeIdentity) {
		t.Fatalf("ReadUpgradeRuntime() = %#v, %v", runtimeIdentity, err)
	}
}

func TestBrokerIdentityRequiresRoleAndNoDevice(t *testing.T) {
	valid := ServiceIdentity{
		Role: "broker", ControllerID: bridgeTestControllerID, InstanceID: "default",
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.DeviceID = bridgeTestDeviceID
	if err := invalid.Validate(); err == nil {
		t.Fatal("broker identity accepted a device ID")
	}
	peer := ServiceIdentity{ControllerID: bridgeTestControllerID, DeviceID: bridgeTestDeviceID}
	if err := peer.Validate(); err != nil || peer.EffectiveRole() != "peer" {
		t.Fatalf("legacy peer identity = %#v, %v", peer, err)
	}
}

func TestControllerUpgradeManagementUsesBrokerProtectedBridge(t *testing.T) {
	manager := &fakeControllerUpgradeManager{snapshot: ControllerUpgradeSnapshot{
		TransactionID: "123e4567-e89b-42d3-a456-426614174399", State: "completed",
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8",
		GlobalCommit: true, Deadline: 2, UpdatedAt: 1, Participants: ControllerUpgradeCounts{},
	}}
	identity := ServiceIdentity{
		Role: "broker", ControllerID: bridgeTestControllerID, InstanceID: "default",
	}
	server, err := ListenWithControllerUpgradeManagement(
		testEndpoint(t), identity, nil, nil, nil, nil, nil, nil, nil, manager,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})
	endpoint := server.listener.Addr().String()

	started, err := StartControllerUpgrade(
		context.Background(), endpoint, manager.snapshot.TargetVersion, 30_000,
	)
	if err != nil || !reflect.DeepEqual(started, manager.snapshot) || manager.startCalls != 1 {
		t.Fatalf("StartControllerUpgrade() = %#v, %v, calls=%d", started, err, manager.startCalls)
	}
	status, err := ReadControllerUpgrade(context.Background(), endpoint)
	if err != nil || status == nil || !reflect.DeepEqual(*status, manager.snapshot) {
		t.Fatalf("ReadControllerUpgrade() = %#v, %v", status, err)
	}
	canceled, err := CancelControllerUpgrade(
		context.Background(), endpoint, manager.snapshot.TransactionID,
	)
	if err != nil || !reflect.DeepEqual(canceled, manager.snapshot) || manager.cancelCalls != 1 {
		t.Fatalf("CancelControllerUpgrade() = %#v, %v, calls=%d", canceled, err, manager.cancelCalls)
	}
}

func TestControllerUpgradeManagementIsUnavailableOnPeerBridge(t *testing.T) {
	manager := &fakeControllerUpgradeManager{snapshot: ControllerUpgradeSnapshot{
		TransactionID: "123e4567-e89b-42d3-a456-426614174399", State: "completed",
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8",
		GlobalCommit: true, Deadline: 2, UpdatedAt: 1, Participants: ControllerUpgradeCounts{},
	}}
	server, err := ListenWithControllerUpgradeManagement(
		testEndpoint(t), testServiceIdentity(), &fakeBackend{}, nil, nil, nil, nil, nil, nil, manager,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() {
		cancel()
		_ = server.Close()
		<-done
	}()
	if _, err := StartControllerUpgrade(
		context.Background(), server.listener.Addr().String(), manager.snapshot.TargetVersion, 1,
	); err == nil {
		t.Fatal("peer bridge exposed broker controller upgrade management")
	}
}

func TestControllerUpgradeClientRejectsInvalidArguments(t *testing.T) {
	if _, err := StartControllerUpgrade(
		context.Background(), "unused", "0.1.0-alpha.8", maximumControllerUpgradeTimeoutMillis+1,
	); err == nil {
		t.Fatal("StartControllerUpgrade accepted a timeout over 30 minutes")
	}
	if _, err := CancelControllerUpgrade(context.Background(), "unused", "not-an-id"); err == nil {
		t.Fatal("CancelControllerUpgrade accepted an invalid transaction ID")
	}
}

type fakeUpgradeManager struct {
	snapshot        UpgradeSnapshot
	runtimeIdentity UpgradeRuntimeIdentity
	prepareCalls    int
}

type fakeControllerUpgradeManager struct {
	snapshot    ControllerUpgradeSnapshot
	startCalls  int
	cancelCalls int
}

func (m *fakeControllerUpgradeManager) StartControllerUpgrade(
	_ context.Context, target string, timeoutMillis int64,
) (ControllerUpgradeSnapshot, error) {
	m.startCalls++
	if target != m.snapshot.TargetVersion || timeoutMillis <= 0 {
		return ControllerUpgradeSnapshot{}, errors.New("unexpected controller upgrade start")
	}
	return m.snapshot, nil
}

func (m *fakeControllerUpgradeManager) CancelControllerUpgrade(
	_ context.Context, transactionID string,
) (ControllerUpgradeSnapshot, error) {
	m.cancelCalls++
	if transactionID != m.snapshot.TransactionID {
		return ControllerUpgradeSnapshot{}, errors.New("unexpected controller upgrade cancel")
	}
	return m.snapshot, nil
}

func (m *fakeControllerUpgradeManager) ControllerUpgradeStatus(
	context.Context,
) (*ControllerUpgradeSnapshot, error) {
	result := m.snapshot
	return &result, nil
}

func (m *fakeUpgradeManager) PrepareLocalUpgrade(
	_ context.Context, target, configPath, environmentFile string,
) (UpgradeSnapshot, error) {
	m.prepareCalls++
	if target != m.snapshot.TargetVersion || configPath != "/service.json" || environmentFile != "/service.env" {
		return UpgradeSnapshot{}, errors.New("unexpected prepare identity")
	}
	return m.snapshot, nil
}

func (m *fakeUpgradeManager) ArmLocalUpgrade(context.Context, string) (UpgradeSnapshot, error) {
	return m.snapshot, nil
}

func (m *fakeUpgradeManager) ActivateLocalUpgrade(context.Context, string) (UpgradeSnapshot, error) {
	return m.snapshot, nil
}

func (m *fakeUpgradeManager) CancelLocalUpgrade(context.Context, string) (UpgradeSnapshot, error) {
	return m.snapshot, nil
}

func (m *fakeUpgradeManager) LocalUpgrade(context.Context) (*UpgradeSnapshot, error) {
	result := m.snapshot
	return &result, nil
}

func (m *fakeUpgradeManager) LocalUpgradeRuntime(context.Context) (UpgradeRuntimeIdentity, error) {
	return m.runtimeIdentity, nil
}

func testUpgradeSnapshot() UpgradeSnapshot {
	return UpgradeSnapshot{
		TransactionID: "123e4567-e89b-42d3-a456-426614174388", State: "prepared",
		SourceVersion: "0.1.0-alpha.7", TargetVersion: "0.1.0-alpha.8", UpdatedAt: 1,
	}
}
