package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	Version        = 1
	MaxMessageSize = 256 * 1024
)

type Kind string

const (
	KindRequest      Kind = "request"
	KindResponse     Kind = "response"
	KindNotification Kind = "notification"
)

type RequestDirection string

const (
	DirectionBroker    RequestDirection = "b"
	DirectionConnector RequestDirection = "c"
	DirectionLocal     RequestDirection = "l"
)

type Envelope struct {
	ProtocolVersion int                        `json:"protocolVersion"`
	Kind            Kind                       `json:"kind"`
	RequestID       string                     `json:"requestId"`
	ReplyTo         string                     `json:"replyTo,omitempty"`
	Method          string                     `json:"method,omitempty"`
	ControllerID    string                     `json:"controllerId"`
	TreeID          string                     `json:"treeId,omitempty"`
	Source          *control.PrincipalIdentity `json:"source,omitempty"`
	Sequence        uint64                     `json:"sequence,omitempty"`
	Cursor          uint64                     `json:"cursor,omitempty"`
	Payload         json.RawMessage            `json:"payload,omitempty"`
	Error           *Error                     `json:"error,omitempty"`
}

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

const (
	ErrorParse           = -32700
	ErrorInvalidRequest  = -32600
	ErrorMethodNotFound  = -32601
	ErrorInvalidParams   = -32602
	ErrorInternal        = -32603
	ErrorUnauthenticated = -32001
	ErrorForbidden       = -32003
	ErrorConflict        = -32009
	ErrorUnavailable     = -32010
)

const (
	MethodHello           = "protocol.hello"
	MethodHeartbeat       = "protocol.heartbeat"
	MethodRegistrySummary = "registry.summary"
	MethodEnsureRootTree  = "tree.ensure_root"
	MethodListDevices     = "device.list"
	MethodDescribeDevice  = "device.describe"
)

const (
	FeatureDeviceRegistry = "deviceRegistryV1"
	FeatureFullDuplexRPC  = "fullDuplexRpcV1"
	FeatureRootTree       = "rootTreeV1"
)

var methodPattern = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,63}$`)

func NewRequestID(direction RequestDirection) (string, error) {
	switch direction {
	case DirectionBroker, DirectionConnector, DirectionLocal:
	default:
		return "", fmt.Errorf("unsupported request direction %q", direction)
	}
	id, err := identity.NewID()
	if err != nil {
		return "", err
	}
	return string(direction) + "_" + id, nil
}

func (e Envelope) Validate() error {
	if e.ProtocolVersion != Version {
		return fmt.Errorf("unsupported protocol version %d", e.ProtocolVersion)
	}
	if err := validateRequestID(e.RequestID); err != nil {
		return fmt.Errorf("requestId %w", err)
	}
	if err := identity.ValidateID(e.ControllerID); err != nil {
		return fmt.Errorf("controllerId %w", err)
	}
	if e.TreeID != "" {
		if err := identity.ValidateID(e.TreeID); err != nil {
			return fmt.Errorf("treeId %w", err)
		}
	}
	if e.Source != nil {
		if err := e.Source.Validate(); err != nil {
			return fmt.Errorf("source: %w", err)
		}
		if e.Source.ControllerID != e.ControllerID {
			return errors.New("source controllerId does not match envelope")
		}
		if e.TreeID == "" || e.Source.TreeID != e.TreeID {
			return errors.New("source treeId does not match envelope")
		}
	}

	switch e.Kind {
	case KindRequest:
		if e.ReplyTo != "" || e.Error != nil {
			return errors.New("request must not contain replyTo or error")
		}
		if !methodPattern.MatchString(e.Method) {
			return errors.New("request method is invalid")
		}
	case KindResponse:
		if err := validateRequestID(e.ReplyTo); err != nil {
			return fmt.Errorf("replyTo %w", err)
		}
		if e.Method != "" {
			return errors.New("response must not contain method")
		}
		if e.Error != nil && len(e.Payload) != 0 {
			return errors.New("response must not contain both payload and error")
		}
		if e.Error != nil {
			if err := e.Error.Validate(); err != nil {
				return err
			}
		}
	case KindNotification:
		if e.ReplyTo != "" || e.Error != nil {
			return errors.New("notification must not contain replyTo or error")
		}
		if !methodPattern.MatchString(e.Method) {
			return errors.New("notification method is invalid")
		}
	default:
		return fmt.Errorf("unsupported envelope kind %q", e.Kind)
	}
	return nil
}

func (e Error) Validate() error {
	switch e.Code {
	case ErrorParse,
		ErrorInvalidRequest,
		ErrorMethodNotFound,
		ErrorInvalidParams,
		ErrorInternal,
		ErrorUnauthenticated,
		ErrorForbidden,
		ErrorConflict,
		ErrorUnavailable:
	default:
		if e.Code < -32099 || e.Code > -32000 {
			return fmt.Errorf("unsupported protocol error code %d", e.Code)
		}
	}
	if strings.TrimSpace(e.Message) == "" || len(e.Message) > 512 {
		return errors.New("protocol error message must contain 1 through 512 bytes")
	}
	return nil
}

func Marshal(envelope Envelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode protocol envelope: %w", err)
	}
	if len(data) > MaxMessageSize {
		return nil, fmt.Errorf("protocol message exceeds %d-byte limit", MaxMessageSize)
	}
	return data, nil
}

func Read(reader io.Reader) (Envelope, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxMessageSize+1))
	if err != nil {
		return Envelope{}, fmt.Errorf("read protocol message: %w", err)
	}
	if len(data) > MaxMessageSize {
		return Envelope{}, fmt.Errorf("protocol message exceeds %d-byte limit", MaxMessageSize)
	}
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode protocol message: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func validateRequestID(value string) error {
	if len(value) != 38 || value[1] != '_' {
		return errors.New("must contain a direction prefix and UUID")
	}
	switch RequestDirection(value[:1]) {
	case DirectionBroker, DirectionConnector, DirectionLocal:
	default:
		return errors.New("has an unsupported direction prefix")
	}
	return identity.ValidateID(value[2:])
}

type Hello struct {
	ControllerID   string             `json:"controllerId"`
	DeviceID       string             `json:"deviceId"`
	DeviceName     string             `json:"deviceName"`
	Role           control.DeviceRole `json:"role"`
	OS             string             `json:"os"`
	Arch           string             `json:"arch"`
	RuntimeVersion string             `json:"runtimeVersion"`
	Features       []string           `json:"features"`
	Cursor         uint64             `json:"cursor"`
}

type HelloResult struct {
	ConnectionID        string   `json:"connectionId"`
	Features            []string `json:"features"`
	HeartbeatIntervalMS int64    `json:"heartbeatIntervalMs"`
	Revision            uint64   `json:"revision"`
}

type EnsureRootTreeParams struct {
	ExternalThreadID string `json:"externalThreadId"`
}

type EnsureRootTreeResult struct {
	Tree      control.Tree      `json:"tree"`
	Principal control.Principal `json:"principal"`
}

type ListDevicesResult struct {
	Revision uint64           `json:"revision"`
	Devices  []control.Device `json:"devices"`
}

type DescribeDeviceParams struct {
	DeviceID string `json:"deviceId"`
}

type DescribeDeviceResult struct {
	Revision uint64         `json:"revision"`
	Device   control.Device `json:"device"`
}
