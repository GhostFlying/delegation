package control

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

const (
	testControllerID = "123e4567-e89b-42d3-a456-426614174000"
	testTreeID       = "123e4567-e89b-42d3-a456-426614174001"
	testAgentID      = "123e4567-e89b-42d3-a456-426614174002"
	testDeviceID     = "123e4567-e89b-42d3-a456-426614174003"
)

func TestRootAndWorkerCapabilitiesRemainSeparated(t *testing.T) {
	root := NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	worker := NewWorkerPrincipal(
		testControllerID,
		testTreeID,
		"123e4567-e89b-42d3-a456-426614174004",
		testAgentID,
		testDeviceID,
	)
	if err := root.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := Require(root, CapabilityDeviceRead); err != nil {
		t.Fatalf("root device.read: %v", err)
	}
	if err := Require(worker, CapabilityDeviceRead); err == nil {
		t.Fatal("worker received device.read")
	}
	if !worker.Has(CapabilityMessageSendParent) {
		t.Fatal("worker lacks message.send.parent")
	}
	if root.ParentAgentID != "" {
		t.Fatalf("root parentAgentId = %q, want empty", root.ParentAgentID)
	}
}

func TestPrincipalRejectsCapabilityInjectionAndUnstableOrdering(t *testing.T) {
	principal := NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	principal.Capabilities = append(principal.Capabilities, Capability("device.delete"))
	if err := principal.Validate(); err == nil {
		t.Fatal("principal accepted unknown capability")
	}

	principal = NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	slices.Reverse(principal.Capabilities)
	if err := principal.Validate(); err == nil {
		t.Fatal("principal accepted unsorted capabilities")
	}

	worker := NewWorkerPrincipal(
		testControllerID,
		testTreeID,
		"123e4567-e89b-42d3-a456-426614174004",
		testAgentID,
		testDeviceID,
	)
	worker.Capabilities = RootCapabilities()
	if err := worker.Validate(); err == nil {
		t.Fatal("worker accepted known root capabilities")
	}
}

func TestCapabilityAccessorsReturnCopies(t *testing.T) {
	first := RootCapabilities()
	first[0] = Capability("modified")
	if slices.Equal(first, RootCapabilities()) {
		t.Fatal("RootCapabilities returned shared storage")
	}
}

func TestRootDoesNotReceiveDeferredArtifactApply(t *testing.T) {
	root := NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	if root.Has(CapabilityArtifactApply) {
		t.Fatal("M1 root received deferred artifact.apply capability")
	}
}

func TestWireIdentityDoesNotContainCapabilities(t *testing.T) {
	root := NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	data, err := json.Marshal(root.Identity())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("capabilities")) {
		t.Fatalf("wire identity leaked capabilities: %s", data)
	}
}

func TestDeviceMetadataIsBounded(t *testing.T) {
	descriptor := DeviceDescriptor{
		ControllerID:    testControllerID,
		DeviceID:        testDeviceID,
		Name:            "windows-builder",
		Role:            DeviceRoleWorker,
		OS:              "windows",
		Arch:            "amd64",
		RuntimeVersion:  "0.1.0-alpha.0",
		ProtocolVersion: 1,
		Features:        []string{"deviceRegistryV1", "fullDuplexRpcV1"},
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
	device := Device{
		ControllerID:    descriptor.ControllerID,
		DeviceID:        descriptor.DeviceID,
		Name:            descriptor.Name,
		Role:            descriptor.Role,
		OS:              descriptor.OS,
		Arch:            descriptor.Arch,
		RuntimeVersion:  descriptor.RuntimeVersion,
		ProtocolVersion: descriptor.ProtocolVersion,
		Features:        descriptor.Features,
		Revision:        1,
	}
	if err := device.Validate(); err != nil {
		t.Fatal(err)
	}
	device.Revision = 0
	if err := device.Validate(); err == nil {
		t.Fatal("device accepted a zero revision")
	}
	descriptor.Name = strings.Repeat("n", 129)
	if err := descriptor.Validate(); err == nil {
		t.Fatal("device accepted an oversized name")
	}
}

func TestDeviceDescriptorExcludesBrokerPresence(t *testing.T) {
	device := Device{
		ControllerID:    testControllerID,
		DeviceID:        testDeviceID,
		Name:            "builder",
		Role:            DeviceRoleWorker,
		OS:              "linux",
		Arch:            "amd64",
		RuntimeVersion:  "test",
		ProtocolVersion: 1,
		Features:        []string{"deviceRegistryV1"},
		Online:          true,
		LastSeenAt:      10,
		Revision:        2,
	}
	descriptor := device.Descriptor()
	data, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for _, brokerField := range [][]byte{[]byte("online"), []byte("lastSeenAt"), []byte("revision")} {
		if bytes.Contains(data, brokerField) {
			t.Fatalf("descriptor exposed broker field %q: %s", brokerField, data)
		}
	}
	descriptor.Features[0] = "modified"
	if device.Features[0] == descriptor.Features[0] {
		t.Fatal("descriptor features share device backing storage")
	}
}
