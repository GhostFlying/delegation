package rootmcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/GhostFlying/delegation/internal/localbridge"
	"github.com/GhostFlying/delegation/internal/protocol"
)

func explainBridgeError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var rpcError *localbridge.RPCError
	if errors.As(err, &rpcError) {
		switch rpcError.Code {
		case protocol.ErrorConflict:
			return errors.New("the device registry changed; call list_devices again without a cursor")
		case protocol.ErrorNotFound:
			return errors.New("the requested delegation device was not found")
		case protocol.ErrorForbidden, protocol.ErrorUnauthenticated:
			return errors.New("this Codex task is not authorized to read the delegation device registry")
		case protocol.ErrorUnavailable:
			return errors.New("the delegation connector is offline or cannot reach the broker")
		default:
			return fmt.Errorf("delegation broker rejected the request with code %d", rpcError.Code)
		}
	}
	return errors.New("the local delegation connector is unavailable; run delegation doctor and ensure its service is running")
}
