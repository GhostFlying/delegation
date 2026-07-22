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

func explainEnsureRootError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var rpcError *localbridge.RPCError
	if errors.As(err, &rpcError) && rpcError.Code == protocol.ErrorConflict {
		return errors.New("this Codex task is already bound to another delegation root device and cannot be rebound")
	}
	return explainBridgeError(err)
}

func explainAgentError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var rpcError *localbridge.RPCError
	if errors.As(err, &rpcError) {
		switch rpcError.Code {
		case protocol.ErrorConflict:
			return errors.New("the agent request conflicts with an existing spawn_id, message_id, operation_id, task_name, or tree binding")
		case protocol.ErrorNotFound:
			return errors.New("the requested delegation agent or target device was not found")
		case protocol.ErrorForbidden, protocol.ErrorUnauthenticated:
			return errors.New("this Codex task is not authorized to manage delegation agents")
		case protocol.ErrorUnavailable:
			return errors.New("the delegation connector, broker, or target peer is temporarily unavailable")
		default:
			return fmt.Errorf("delegation broker rejected the agent request with code %d", rpcError.Code)
		}
	}
	return errors.New("the local delegation connector is unavailable; run delegation doctor and ensure its service is running")
}
