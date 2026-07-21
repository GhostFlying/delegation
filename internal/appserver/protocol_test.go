package appserver

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDecodeMessageKinds(t *testing.T) {
	tests := []struct {
		name  string
		data  string
		check func(t *testing.T, message decodedMessage)
	}{
		{
			name: "response",
			data: `{"id":7,"result":{"ok":true}}`,
			check: func(t *testing.T, message decodedMessage) {
				if !message.isResponse || message.responseID != 7 || string(message.result) != `{"ok":true}` {
					t.Fatalf("message = %+v", message)
				}
			},
		},
		{
			name: "RPC error",
			data: `{"id":8,"error":{"code":-32602,"message":"bad params","data":{"field":"cwd"}}}`,
			check: func(t *testing.T, message decodedMessage) {
				var rpcErr *RPCError
				if !message.isResponse || !errors.As(message.rpcError, &rpcErr) || rpcErr.Code != -32602 {
					t.Fatalf("message = %+v", message)
				}
			},
		},
		{
			name: "notification",
			data: `{"method":"turn/completed","params":{"turn":{"id":"turn-1"}}}`,
			check: func(t *testing.T, message decodedMessage) {
				if !message.isNotification || message.method != "turn/completed" {
					t.Fatalf("message = %+v", message)
				}
			},
		},
		{
			name: "server request",
			data: `{"id":"callback-1","method":"item/commandExecution/requestApproval","params":{}}`,
			check: func(t *testing.T, message decodedMessage) {
				if !message.isRequest || message.method != "item/commandExecution/requestApproval" || string(message.id) != `"callback-1"` {
					t.Fatalf("message = %+v", message)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := decodeMessage([]byte(test.data))
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, message)
		})
	}
}

func TestMarshalMethodNotFoundPreservesRequestID(t *testing.T) {
	data, err := marshalMethodNotFound(json.RawMessage(`"request-1"`))
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		ID    string `json:"id"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != "request-1" || response.Error.Code != jsonRPCMethodNotFound || response.Error.Message != "method not found" {
		t.Fatalf("response = %+v", response)
	}
}

func TestDecodeMessageRejectsTrailingJSON(t *testing.T) {
	if _, err := decodeMessage([]byte(`{"id":1,"result":null} {}`)); err == nil {
		t.Fatal("decodeMessage accepted trailing JSON")
	}
}
