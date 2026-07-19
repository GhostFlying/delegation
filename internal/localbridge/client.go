package localbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/control"
)

type Client struct {
	endpoint string
}

type RPCError struct {
	Code    int
	Message string
}

// Probe verifies that the endpoint serves the expected configured connector.
func Probe(ctx context.Context, endpoint string, expected ServiceIdentity) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("expected local bridge identity: %w", err)
	}
	client, err := NewClient(endpoint)
	if err != nil {
		return err
	}
	var actual ServiceIdentity
	if err := client.Call(ctx, methodIdentity, "", nil, struct{}{}, &actual); err != nil {
		return fmt.Errorf("read local bridge identity: %w", err)
	}
	if err := actual.Validate(); err != nil {
		return fmt.Errorf("invalid local bridge identity: %w", err)
	}
	if actual != expected {
		return fmt.Errorf(
			"local bridge identity mismatch: got controller %s device %s",
			actual.ControllerID,
			actual.DeviceID,
		)
	}
	return nil
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("local bridge RPC %d: %s", e.Code, e.Message)
}

func NewClient(endpoint string) (*Client, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint}, nil
}

func (c *Client) Call(
	ctx context.Context,
	method, treeID string,
	source *control.PrincipalIdentity,
	params, result any,
) error {
	payload, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode local bridge %s params: %w", method, err)
	}
	requestID, err := newLocalID()
	if err != nil {
		return err
	}
	call := request{
		Version: Version, RequestID: requestID, Method: method, TreeID: treeID, Payload: payload,
	}
	if source != nil {
		copy := *source
		call.Source = &copy
	}
	if err := call.validate(); err != nil {
		return fmt.Errorf("invalid local bridge call: %w", err)
	}
	connection, err := dial(ctx, c.endpoint)
	if err != nil {
		return fmt.Errorf("connect to local delegation service: %w", err)
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() {
		_ = connection.Close()
	})
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := connection.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set local bridge deadline: %w", err)
		}
	}
	if err := writeJSONFrame(connection, call); err != nil {
		return err
	}
	reply, err := readJSONFrame[response](connection)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if err := reply.validate(); err != nil {
		return fmt.Errorf("invalid local bridge response: %w", err)
	}
	if reply.ReplyTo != requestID {
		return errors.New("local bridge response does not match its request")
	}
	if reply.Error != nil {
		return &RPCError{Code: reply.Error.Code, Message: reply.Error.Message}
	}
	if result == nil {
		return nil
	}
	if err := decodeResult(reply.Payload, result); err != nil {
		return fmt.Errorf("decode local bridge %s result: %w", method, err)
	}
	return nil
}

func decodeResult(payload json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("result must contain exactly one JSON value")
		}
		return err
	}
	return nil
}
