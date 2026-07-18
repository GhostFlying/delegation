package localbridge

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/GhostFlying/delegation/internal/control"
	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const Version = 1

var methodPattern = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,63}$`)

type request struct {
	Version   int                        `json:"version"`
	RequestID string                     `json:"requestId"`
	Method    string                     `json:"method"`
	TreeID    string                     `json:"treeId,omitempty"`
	Source    *control.PrincipalIdentity `json:"source,omitempty"`
	Payload   json.RawMessage            `json:"payload"`
}

type response struct {
	Version   int             `json:"version"`
	RequestID string          `json:"requestId"`
	ReplyTo   string          `json:"replyTo"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     *protocol.Error `json:"error,omitempty"`
}

func (r request) validate() error {
	if r.Version != Version {
		return fmt.Errorf("unsupported local bridge version %d", r.Version)
	}
	if err := validateLocalID(r.RequestID); err != nil {
		return fmt.Errorf("requestId %w", err)
	}
	if !methodPattern.MatchString(r.Method) {
		return errors.New("method is invalid")
	}
	if r.TreeID != "" {
		if err := identity.ValidateID(r.TreeID); err != nil {
			return fmt.Errorf("treeId %w", err)
		}
	}
	if r.Source != nil {
		if err := r.Source.Validate(); err != nil {
			return fmt.Errorf("source: %w", err)
		}
		if r.TreeID == "" || r.Source.TreeID != r.TreeID {
			return errors.New("source treeId does not match request")
		}
	}
	if len(r.Payload) == 0 {
		return errors.New("payload is required")
	}
	return nil
}

func (r response) validate() error {
	if r.Version != Version {
		return fmt.Errorf("unsupported local bridge version %d", r.Version)
	}
	if err := validateLocalID(r.RequestID); err != nil {
		return fmt.Errorf("requestId %w", err)
	}
	if err := validateLocalID(r.ReplyTo); err != nil {
		return fmt.Errorf("replyTo %w", err)
	}
	if r.Error != nil && len(r.Payload) != 0 {
		return errors.New("response must not contain payload and error")
	}
	if r.Error != nil {
		return r.Error.Validate()
	}
	if len(r.Payload) == 0 {
		return errors.New("successful response payload is required")
	}
	return nil
}

func validateLocalID(value string) error {
	if !strings.HasPrefix(value, string(protocol.DirectionLocal)+"_") {
		return errors.New("must use the local request direction")
	}
	return identity.ValidateID(strings.TrimPrefix(value, string(protocol.DirectionLocal)+"_"))
}

func newLocalID() (string, error) {
	return protocol.NewRequestID(protocol.DirectionLocal)
}

func readJSONFrame[T any](reader io.Reader) (T, error) {
	var value T
	var size uint32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return value, fmt.Errorf("read local bridge frame size: %w", err)
	}
	if size == 0 || size > protocol.MaxMessageSize {
		return value, fmt.Errorf("local bridge frame size must be from 1 through %d", protocol.MaxMessageSize)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return value, fmt.Errorf("read local bridge frame: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode local bridge frame: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, errors.New("local bridge frame must contain exactly one JSON value")
		}
		return value, fmt.Errorf("decode trailing local bridge data: %w", err)
	}
	return value, nil
}

func writeJSONFrame(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode local bridge frame: %w", err)
	}
	if len(data) == 0 || len(data) > protocol.MaxMessageSize {
		return fmt.Errorf("local bridge frame size must be from 1 through %d", protocol.MaxMessageSize)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if err := writeFull(writer, header[:]); err != nil {
		return fmt.Errorf("write local bridge frame size: %w", err)
	}
	if err := writeFull(writer, data); err != nil {
		return fmt.Errorf("write local bridge frame: %w", err)
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
