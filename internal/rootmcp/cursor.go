package rootmcp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/identity"
	"github.com/GhostFlying/delegation/internal/protocol"
)

const maximumCursorBytes = 256

type deviceCursor struct {
	Revision uint64 `json:"revision"`
	DeviceID string `json:"deviceId"`
}

type agentCursor struct {
	TreeID        string `json:"treeId"`
	AfterSequence uint64 `json:"afterSequence"`
}

func encodeCursor(revision uint64, deviceID string) (string, error) {
	cursor := deviceCursor{Revision: revision, DeviceID: deviceID}
	if err := cursor.validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode device cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(value string) (string, *uint64, error) {
	if value == "" {
		return "", nil, nil
	}
	if len(value) > base64.RawURLEncoding.EncodedLen(maximumCursorBytes) {
		return "", nil, errors.New("device cursor is too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(data) == 0 || len(data) > maximumCursorBytes {
		return "", nil, errors.New("device cursor is invalid")
	}
	var cursor deviceCursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return "", nil, errors.New("device cursor is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", nil, errors.New("device cursor is invalid")
	}
	if err := cursor.validate(); err != nil {
		return "", nil, errors.New("device cursor is invalid")
	}
	return cursor.DeviceID, &cursor.Revision, nil
}

func (c deviceCursor) validate() error {
	if c.Revision == 0 {
		return errors.New("cursor revision must be positive")
	}
	if err := identity.ValidateID(c.DeviceID); err != nil {
		return err
	}
	return nil
}

func encodeAgentCursor(treeID string, afterSequence uint64) (string, error) {
	cursor := agentCursor{TreeID: treeID, AfterSequence: afterSequence}
	if err := cursor.validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode agent cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeAgentCursor(value, treeID string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	if len(value) > base64.RawURLEncoding.EncodedLen(maximumCursorBytes) {
		return 0, errors.New("agent cursor is too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(data) == 0 || len(data) > maximumCursorBytes {
		return 0, errors.New("agent cursor is invalid")
	}
	var cursor agentCursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return 0, errors.New("agent cursor is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return 0, errors.New("agent cursor is invalid")
	}
	if cursor.validate() != nil || cursor.TreeID != treeID {
		return 0, errors.New("agent cursor does not belong to this Codex task")
	}
	return cursor.AfterSequence, nil
}

func (c agentCursor) validate() error {
	if err := identity.ValidateID(c.TreeID); err != nil {
		return err
	}
	if c.AfterSequence < 1 || c.AfterSequence > protocol.MaximumAgentsPerTree {
		return errors.New("agent cursor sequence is outside the supported range")
	}
	return nil
}
