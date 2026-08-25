package cli

import (
	"context"
	"net"

	"github.com/GhostFlying/delegation/internal/tailscaleruntime"
)

type embeddedTailscaleRuntime interface {
	Start(context.Context, tailscaleruntime.Config) error
	Listen(context.Context, string, string) (net.Listener, error)
	Dial(context.Context, string, string) (net.Conn, error)
	Close() error
}

type embeddedTailscaleRuntimeFactory func() embeddedTailscaleRuntime

func newEmbeddedTailscaleRuntime() embeddedTailscaleRuntime {
	return tailscaleruntime.New()
}
