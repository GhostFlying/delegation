package appserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	MethodThreadStart         = "thread/start"
	MethodThreadResume        = "thread/resume"
	MethodMCPServerStatusList = "mcpServerStatus/list"
	MethodTurnStart           = "turn/start"
	MethodTurnSteer           = "turn/steer"
	MethodTurnInterrupt       = "turn/interrupt"

	jsonRPCMethodNotFound = -32601
)

type Notification struct {
	Method string
	Params json.RawMessage
}

type RPCError struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("app-server RPC %d: %s", e.Code, e.Message)
}

type response struct {
	result json.RawMessage
	err    error
}

type decodedMessage struct {
	id             json.RawMessage
	responseID     uint64
	method         string
	params         json.RawMessage
	result         json.RawMessage
	rpcError       *RPCError
	isResponse     bool
	isRequest      bool
	isNotification bool
}

func decodeMessage(data []byte) (decodedMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil {
		return decodedMessage{}, fmt.Errorf("decode app-server message: %w", err)
	}
	if fields == nil {
		return decodedMessage{}, errors.New("app-server message must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return decodedMessage{}, errors.New("app-server message contains trailing JSON data")
	}

	id, hasID := fields["id"]
	methodValue, hasMethod := fields["method"]
	result, hasResult := fields["result"]
	errorValue, hasError := fields["error"]
	if hasMethod {
		if hasResult || hasError {
			return decodedMessage{}, errors.New("app-server method message must not contain result or error")
		}
		var method string
		if err := json.Unmarshal(methodValue, &method); err != nil || method == "" {
			return decodedMessage{}, errors.New("app-server method must be a non-empty string")
		}
		params := cloneRaw(fields["params"])
		if hasID {
			if !validRequestID(id) {
				return decodedMessage{}, errors.New("app-server request ID must be a string or number")
			}
			return decodedMessage{
				id: cloneRaw(id), method: method, params: params, isRequest: true,
			}, nil
		}
		return decodedMessage{method: method, params: params, isNotification: true}, nil
	}
	if !hasID || hasResult == hasError {
		return decodedMessage{}, errors.New("app-server response must contain an ID and exactly one of result or error")
	}
	responseID, err := parseResponseID(id)
	if err != nil {
		return decodedMessage{}, err
	}
	message := decodedMessage{responseID: responseID, isResponse: true}
	if hasError {
		var wire struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(errorValue, &wire); err != nil || wire.Message == "" {
			return decodedMessage{}, errors.New("app-server response contains an invalid error")
		}
		message.rpcError = &RPCError{Code: wire.Code, Message: wire.Message, Data: cloneRaw(wire.Data)}
	} else {
		message.result = cloneRaw(result)
	}
	return message, nil
}

func marshalRequest(id uint64, method string, params any) ([]byte, error) {
	message := map[string]any{"id": id, "method": method}
	if params != nil {
		message["params"] = params
	}
	return json.Marshal(message)
}

func marshalNotification(method string, params any) ([]byte, error) {
	message := map[string]any{"method": method}
	if params != nil {
		message["params"] = params
	}
	return json.Marshal(message)
}

func marshalMethodNotFound(id json.RawMessage) ([]byte, error) {
	message := struct {
		ID    json.RawMessage `json:"id"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{ID: cloneRaw(id)}
	message.Error.Code = jsonRPCMethodNotFound
	message.Error.Message = "method not found"
	return json.Marshal(message)
}

func parseResponseID(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 || raw[0] == '"' {
		return 0, errors.New("app-server response ID does not match a numeric client request")
	}
	id, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("app-server response ID does not match a numeric client request")
	}
	return id, nil
}

func validRequestID(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	if raw[0] == '"' {
		var value string
		return json.Unmarshal(raw, &value) == nil
	}
	var value json.Number
	return json.Unmarshal(raw, &value) == nil
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func isLifecycleNotification(method string) bool {
	switch method {
	case "error",
		"mcpServer/startupStatus/updated",
		"thread/closed",
		"thread/started",
		"thread/status/changed",
		"turn/completed",
		"turn/started":
		return true
	default:
		return false
	}
}
