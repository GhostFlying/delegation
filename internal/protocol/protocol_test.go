package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/GhostFlying/delegation/internal/control"
)

const (
	testControllerID = "123e4567-e89b-42d3-a456-426614174000"
	testTreeID       = "123e4567-e89b-42d3-a456-426614174001"
	testAgentID      = "123e4567-e89b-42d3-a456-426614174002"
	testDeviceID     = "123e4567-e89b-42d3-a456-426614174003"
	requestID        = "c_123e4567-e89b-42d3-a456-426614174004"
)

func TestEnvelopeAcceptsBoundRootRequest(t *testing.T) {
	principal := control.NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	envelope := Envelope{
		ProtocolVersion: Version,
		Kind:            KindRequest,
		RequestID:       requestID,
		Method:          MethodListDevices,
		ControllerID:    testControllerID,
		TreeID:          testTreeID,
		Source:          ptr(principal.Identity()),
		Payload:         json.RawMessage(`{}`),
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvelopeRejectsCrossTreePrincipal(t *testing.T) {
	principal := control.NewRootPrincipal(testControllerID, testTreeID, testAgentID, testDeviceID)
	envelope := Envelope{
		ProtocolVersion: Version,
		Kind:            KindRequest,
		RequestID:       requestID,
		Method:          MethodListDevices,
		ControllerID:    testControllerID,
		TreeID:          "123e4567-e89b-42d3-a456-426614174009",
		Source:          ptr(principal.Identity()),
	}
	if err := envelope.Validate(); err == nil {
		t.Fatal("Validate accepted a cross-tree principal")
	}
}

func TestEnvelopeRejectsAmbiguousResponse(t *testing.T) {
	envelope := Envelope{
		ProtocolVersion: Version,
		Kind:            KindResponse,
		RequestID:       "b_123e4567-e89b-42d3-a456-426614174005",
		ReplyTo:         requestID,
		ControllerID:    testControllerID,
		Payload:         json.RawMessage(`{"ok":true}`),
		Error:           &Error{Code: ErrorInternal, Message: "failed"},
	}
	if err := envelope.Validate(); err == nil {
		t.Fatal("Validate accepted response with payload and error")
	}
}

func TestNewRequestIDIncludesDirectionAndUUID(t *testing.T) {
	for _, direction := range []RequestDirection{DirectionBroker, DirectionConnector, DirectionLocal} {
		value, err := NewRequestID(direction)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRequestID(value); err != nil {
			t.Fatalf("validateRequestID(%q): %v", value, err)
		}
	}
}

func TestProtocolErrorUsesStandardMethodNotFoundCode(t *testing.T) {
	err := Error{Code: ErrorMethodNotFound, Message: "method not found"}
	if validateErr := err.Validate(); validateErr != nil {
		t.Fatal(validateErr)
	}
	if err.Code != -32601 {
		t.Fatalf("method-not-found code = %d, want -32601", err.Code)
	}
}

func TestProtocolAcceptsImplementationDefinedServerError(t *testing.T) {
	err := Error{Code: -32042, Message: "future server error"}
	if validateErr := err.Validate(); validateErr != nil {
		t.Fatal(validateErr)
	}
}

func TestReadRejectsOversizedMessageBeforeDecoding(t *testing.T) {
	_, err := Read(strings.NewReader(strings.Repeat(" ", MaxMessageSize+1)))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Read oversized message error = %v", err)
	}
}

func TestMarshalAndReadRoundTrip(t *testing.T) {
	envelope := Envelope{
		ProtocolVersion: Version,
		Kind:            KindRequest,
		RequestID:       requestID,
		Method:          MethodHeartbeat,
		ControllerID:    testControllerID,
		Payload:         json.RawMessage(`{"at":1}`),
	}
	data, err := Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.RequestID != envelope.RequestID || decoded.Method != envelope.Method {
		t.Fatalf("Read(Marshal(envelope)) = %#v, want %#v", decoded, envelope)
	}
}

func ptr[T any](value T) *T {
	return &value
}
