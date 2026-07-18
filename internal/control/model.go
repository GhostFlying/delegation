package control

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/GhostFlying/delegation/internal/identity"
)

type DeviceRole string

const (
	DeviceRoleController DeviceRole = "controller"
	DeviceRoleWorker     DeviceRole = "device"
)

func (r DeviceRole) Validate() error {
	switch r {
	case DeviceRoleController, DeviceRoleWorker:
		return nil
	default:
		return fmt.Errorf("unsupported device role %q", r)
	}
}

type Capability string

const (
	CapabilityDeviceRead             Capability = "device.read"
	CapabilityAgentSpawn             Capability = "agent.spawn"
	CapabilityAgentManageDescendants Capability = "agent.manage.descendants"
	CapabilityWorkspaceSync          Capability = "workspace.sync"
	CapabilityMessageTree            Capability = "message.tree"
	CapabilityArtifactApply          Capability = "artifact.apply"

	CapabilityMessageSendParent   Capability = "message.send.parent"
	CapabilityMessageReceiveSelf  Capability = "message.receive.self"
	CapabilityArtifactPublishSelf Capability = "artifact.publish.self"
)

var rootCapabilities = []Capability{
	CapabilityAgentManageDescendants,
	CapabilityAgentSpawn,
	CapabilityDeviceRead,
	CapabilityMessageTree,
	CapabilityWorkspaceSync,
}

var workerCapabilities = []Capability{
	CapabilityArtifactPublishSelf,
	CapabilityMessageReceiveSelf,
	CapabilityMessageSendParent,
}

func RootCapabilities() []Capability {
	return slices.Clone(rootCapabilities)
}

func WorkerCapabilities() []Capability {
	return slices.Clone(workerCapabilities)
}

type Principal struct {
	PrincipalIdentity
	Capabilities []Capability `json:"capabilities"`
}

type PrincipalIdentity struct {
	ControllerID  string `json:"controllerId"`
	TreeID        string `json:"treeId"`
	AgentID       string `json:"agentId"`
	ParentAgentID string `json:"parentAgentId"`
	DeviceID      string `json:"deviceId"`
}

func NewRootPrincipal(controllerID, treeID, agentID, deviceID string) Principal {
	return Principal{
		PrincipalIdentity: PrincipalIdentity{
			ControllerID: controllerID,
			TreeID:       treeID,
			AgentID:      agentID,
			DeviceID:     deviceID,
		},
		Capabilities: RootCapabilities(),
	}
}

func NewWorkerPrincipal(controllerID, treeID, agentID, parentAgentID, deviceID string) Principal {
	return Principal{
		PrincipalIdentity: PrincipalIdentity{
			ControllerID:  controllerID,
			TreeID:        treeID,
			AgentID:       agentID,
			ParentAgentID: parentAgentID,
			DeviceID:      deviceID,
		},
		Capabilities: WorkerCapabilities(),
	}
}

func (p Principal) Validate() error {
	if err := p.PrincipalIdentity.Validate(); err != nil {
		return err
	}
	expected := RootCapabilities()
	if p.ParentAgentID != "" {
		expected = WorkerCapabilities()
	}
	if !slices.Equal(p.Capabilities, expected) {
		return errors.New("capabilities do not match the principal role")
	}
	return nil
}

func (p Principal) Identity() PrincipalIdentity {
	return p.PrincipalIdentity
}

func (p PrincipalIdentity) Validate() error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: p.ControllerID},
		{name: "treeId", value: p.TreeID},
		{name: "agentId", value: p.AgentID},
		{name: "deviceId", value: p.DeviceID},
	}
	for _, field := range fields {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if p.ParentAgentID != "" {
		if err := identity.ValidateID(p.ParentAgentID); err != nil {
			return fmt.Errorf("parentAgentId %w", err)
		}
		if p.ParentAgentID == p.AgentID {
			return errors.New("parentAgentId must differ from agentId")
		}
	}
	return nil
}

func (c Capability) Validate() error {
	switch c {
	case CapabilityDeviceRead,
		CapabilityAgentSpawn,
		CapabilityAgentManageDescendants,
		CapabilityWorkspaceSync,
		CapabilityMessageTree,
		CapabilityArtifactApply,
		CapabilityMessageSendParent,
		CapabilityMessageReceiveSelf,
		CapabilityArtifactPublishSelf:
		return nil
	default:
		return fmt.Errorf("unsupported capability %q", c)
	}
}

func (p Principal) Has(capability Capability) bool {
	_, found := slices.BinarySearch(p.Capabilities, capability)
	return found
}

func (p Principal) Matches(identity PrincipalIdentity) bool {
	return p.PrincipalIdentity == identity
}

func Require(p Principal, capability Capability) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("invalid principal: %w", err)
	}
	if !p.Has(capability) {
		return fmt.Errorf("principal %s lacks capability %s", p.AgentID, capability)
	}
	return nil
}

type Device struct {
	ControllerID    string     `json:"controllerId"`
	DeviceID        string     `json:"deviceId"`
	Name            string     `json:"name"`
	Role            DeviceRole `json:"role"`
	OS              string     `json:"os"`
	Arch            string     `json:"arch"`
	RuntimeVersion  string     `json:"runtimeVersion"`
	ProtocolVersion int        `json:"protocolVersion"`
	Features        []string   `json:"features"`
	Online          bool       `json:"online"`
	LastSeenAt      int64      `json:"lastSeenAt"`
	Revision        uint64     `json:"revision"`
}

type DeviceDescriptor struct {
	ControllerID    string     `json:"controllerId"`
	DeviceID        string     `json:"deviceId"`
	Name            string     `json:"name"`
	Role            DeviceRole `json:"role"`
	OS              string     `json:"os"`
	Arch            string     `json:"arch"`
	RuntimeVersion  string     `json:"runtimeVersion"`
	ProtocolVersion int        `json:"protocolVersion"`
	Features        []string   `json:"features"`
}

var featurePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,63}$`)

func (d Device) Validate() error {
	if err := d.Descriptor().Validate(); err != nil {
		return err
	}
	if d.LastSeenAt < 0 {
		return errors.New("lastSeenAt must not be negative")
	}
	if d.Revision == 0 {
		return errors.New("revision must be positive")
	}
	return nil
}

func (d Device) Descriptor() DeviceDescriptor {
	return DeviceDescriptor{
		ControllerID:    d.ControllerID,
		DeviceID:        d.DeviceID,
		Name:            d.Name,
		Role:            d.Role,
		OS:              d.OS,
		Arch:            d.Arch,
		RuntimeVersion:  d.RuntimeVersion,
		ProtocolVersion: d.ProtocolVersion,
		Features:        slices.Clone(d.Features),
	}
}

func (d DeviceDescriptor) Validate() error {
	if err := identity.ValidateID(d.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if err := identity.ValidateID(d.DeviceID); err != nil {
		return fmt.Errorf("deviceId %w", err)
	}
	if err := d.Role.Validate(); err != nil {
		return err
	}
	fields := []struct {
		name  string
		value string
		limit int
	}{
		{name: "name", value: d.Name, limit: 128},
		{name: "os", value: d.OS, limit: 32},
		{name: "arch", value: d.Arch, limit: 32},
		{name: "runtimeVersion", value: d.RuntimeVersion, limit: 64},
	}
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" || len(field.value) > field.limit {
			return fmt.Errorf("%s must contain 1 through %d bytes", field.name, field.limit)
		}
		if !utf8.ValidString(field.value) || strings.IndexFunc(field.value, unicode.IsControl) >= 0 {
			return fmt.Errorf("%s must be valid text without control characters", field.name)
		}
	}
	if d.ProtocolVersion < 1 {
		return errors.New("protocolVersion must be positive")
	}
	if len(d.Features) > 64 {
		return errors.New("features exceeds limit of 64")
	}
	previous := ""
	for _, feature := range d.Features {
		if !featurePattern.MatchString(feature) {
			return errors.New("feature names must use letters, digits, dot, underscore, or hyphen")
		}
		if previous != "" && feature <= previous {
			return errors.New("features must be sorted and unique")
		}
		previous = feature
	}
	return nil
}

type Tree struct {
	ControllerID     string `json:"controllerId"`
	TreeID           string `json:"treeId"`
	ExternalThreadID string `json:"externalThreadId"`
	RootAgentID      string `json:"rootAgentId"`
	RootDeviceID     string `json:"rootDeviceId"`
	CreatedAt        int64  `json:"createdAt"`
}

func (t Tree) Validate() error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "controllerId", value: t.ControllerID},
		{name: "treeId", value: t.TreeID},
		{name: "externalThreadId", value: t.ExternalThreadID},
		{name: "rootAgentId", value: t.RootAgentID},
		{name: "rootDeviceId", value: t.RootDeviceID},
	}
	for _, field := range fields {
		if err := identity.ValidateID(field.value); err != nil {
			return fmt.Errorf("%s %w", field.name, err)
		}
	}
	if t.CreatedAt < 0 {
		return errors.New("createdAt must not be negative")
	}
	return nil
}
