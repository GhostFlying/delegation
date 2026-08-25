package tailscaleruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"

	delegationconfig "github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/store"
	"github.com/GhostFlying/delegation/internal/tailscaleauth"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
	"tailscale.com/types/key"
)

const (
	defaultControlURL     = ipn.DefaultControlURL
	forceLoginEnvironment = "TSNET_FORCE_LOGIN"
)

type Config struct {
	Dir         string
	Hostname    string
	AuthKeyFile string
}

type Runtime struct {
	mu           sync.Mutex
	newNode      nodeFactory
	acquireLease leaseFactory
	node         node
	lease        io.Closer
	closed       bool
	closeErr     error
}

type nodeConfig struct {
	Dir        string
	Hostname   string
	AuthKey    string
	ControlURL string
	Ephemeral  bool
	Logf       func(string, ...any)
	UserLogf   func(string, ...any)
}

type node interface {
	Start() error
	ClearAuthKey()
	Up(context.Context) error
	Listen(string, string) (net.Listener, error)
	Status(context.Context) (*ipnstate.Status, error)
	DialPeerTCP(context.Context, netip.AddrPort) (net.Conn, error)
	Close() error
}

type nodeFactory func(nodeConfig) (node, error)
type leaseFactory func(string) (io.Closer, error)

func New() *Runtime {
	return newRuntime(newTSNetNode, func(stateDir string) (io.Closer, error) {
		return store.AcquireTailscaleStateDirLease(stateDir)
	})
}

func newRuntime(newNode nodeFactory, acquireLease leaseFactory) *Runtime {
	return &Runtime{newNode: newNode, acquireLease: acquireLease}
}

func (r *Runtime) Start(ctx context.Context, cfg Config) error {
	if r == nil {
		return errors.New("tailscale runtime is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("tailscale runtime is closed")
	}
	if r.node != nil {
		return errors.New("tailscale runtime is already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rejectForcedLogin(); err != nil {
		return err
	}

	lease, err := r.acquireLease(cfg.Dir)
	if err != nil {
		return fmt.Errorf("acquire tailscale state directory lease: %w", err)
	}
	failLease := func(startErr error) error {
		return errors.Join(startErr, lease.Close())
	}
	if err := delegationconfig.PreparePrivateDirectory(cfg.Dir); err != nil {
		return failLease(fmt.Errorf("prepare private tailscale state directory: %w", err))
	}

	key, err := tailscaleauth.Read(cfg.AuthKeyFile)
	if err != nil {
		return failLease(err)
	}
	nodeCfg := nodeConfig{
		Dir:        cfg.Dir,
		Hostname:   cfg.Hostname,
		AuthKey:    key.AuthKey(),
		ControlURL: defaultControlURL,
		Ephemeral:  false,
		Logf:       discardLog,
		UserLogf:   discardLog,
	}
	key = nil
	startedNode, err := r.newNode(nodeCfg)
	nodeCfg.AuthKey = ""
	if err != nil {
		return failLease(fmt.Errorf("create tailscale node: %w", err))
	}
	if startedNode == nil {
		return failLease(errors.New("create tailscale node: node is nil"))
	}
	if err := startedNode.Start(); err != nil {
		startedNode.ClearAuthKey()
		return failLease(errors.Join(
			fmt.Errorf("start tailscale node: %w", err),
			startedNode.Close(),
		))
	}
	startedNode.ClearAuthKey()
	if err := startedNode.Up(ctx); err != nil {
		return failLease(errors.Join(
			fmt.Errorf("wait for tailscale node readiness: %w", err),
			startedNode.Close(),
		))
	}
	r.node = startedNode
	r.lease = lease
	return nil
}

func (r *Runtime) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	startedNode, err := r.startedNode()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	listener, err := startedNode.Listen(network, address)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	return listener, nil
}

func (r *Runtime) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	startedNode, err := r.startedNode()
	if err != nil {
		return nil, err
	}
	peer, err := classifyPeerAddress(ctx, startedNode, network, address)
	if err != nil {
		return nil, err
	}
	connection, err := startedNode.DialPeerTCP(ctx, peer.address)
	if err != nil {
		return nil, err
	}
	if err := validatePeerStillOnline(ctx, startedNode, peer); err != nil {
		return nil, errors.Join(err, connection.Close())
	}
	return connection, nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	if r.node != nil {
		r.closeErr = errors.Join(r.closeErr, r.node.Close())
		r.node = nil
	}
	if r.lease != nil {
		r.closeErr = errors.Join(r.closeErr, r.lease.Close())
		r.lease = nil
	}
	return r.closeErr
}

func (r *Runtime) startedNode() (node, error) {
	if r == nil {
		return nil, errors.New("tailscale runtime is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("tailscale runtime is closed")
	}
	if r.node == nil {
		return nil, errors.New("tailscale runtime is not started")
	}
	return r.node, nil
}

func discardLog(string, ...any) {}

func rejectForcedLogin() error {
	value, present := os.LookupEnv(forceLoginEnvironment)
	if !present || value == "" {
		return nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("%s must be unset or a false boolean value", forceLoginEnvironment)
	}
	if enabled {
		return fmt.Errorf("%s must be unset or false for embedded tailscale", forceLoginEnvironment)
	}
	return nil
}

type classifiedPeer struct {
	key     key.NodePublic
	address netip.AddrPort
}

func classifyPeerAddress(
	ctx context.Context,
	startedNode node,
	network string,
	address string,
) (classifiedPeer, error) {
	if err := ctx.Err(); err != nil {
		return classifiedPeer{}, err
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return classifiedPeer{}, fmt.Errorf(
			"tailscale runtime dial only supports tcp, tcp4, or tcp6, not %q",
			network,
		)
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return classifiedPeer{}, fmt.Errorf("parse tailscale peer address: %w", err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return classifiedPeer{}, fmt.Errorf("parse tailscale peer port: %w", err)
	}

	status, err := runningStatus(ctx, startedNode)
	if err != nil {
		return classifiedPeer{}, err
	}

	if ip, parseErr := netip.ParseAddr(host); parseErr == nil {
		ip = ip.Unmap()
		if !addressFamilyMatches(network, ip) {
			return classifiedPeer{}, fmt.Errorf(
				"tailscale peer address %q does not match network %q",
				host,
				network,
			)
		}
		peerKey, peer, err := uniqueOnlinePeer(status, func(peer *ipnstate.PeerStatus) bool {
			return peerHasIP(peer, ip)
		})
		if err != nil {
			return classifiedPeer{}, fmt.Errorf("classify tailscale peer address %q: %w", host, err)
		}
		if peer == nil {
			return classifiedPeer{}, fmt.Errorf(
				"tailscale peer address %q is not assigned to an online peer",
				host,
			)
		}
		return classifiedPeer{
			key:     peerKey,
			address: netip.AddrPortFrom(ip, uint16(port)),
		}, nil
	}

	hostname := canonicalDNSName(host)
	if hostname == "" {
		return classifiedPeer{}, errors.New("tailscale peer hostname is empty")
	}
	peerKey, peer, err := uniqueOnlinePeer(status, func(peer *ipnstate.PeerStatus) bool {
		return peerNameMatches(status, peer, hostname)
	})
	if err != nil {
		return classifiedPeer{}, fmt.Errorf("classify tailscale peer hostname %q: %w", host, err)
	}
	if peer == nil {
		return classifiedPeer{}, fmt.Errorf(
			"tailscale peer hostname %q is not an online peer",
			host,
		)
	}
	ip, ok := peerIPForNetwork(peer, network)
	if !ok {
		return classifiedPeer{}, fmt.Errorf(
			"tailscale peer hostname %q has no address for network %q",
			host,
			network,
		)
	}
	return classifiedPeer{
		key:     peerKey,
		address: netip.AddrPortFrom(ip, uint16(port)),
	}, nil
}

func runningStatus(ctx context.Context, startedNode node) (*ipnstate.Status, error) {
	status, err := startedNode.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("read tailscale peer status: %w", err)
	}
	if status == nil {
		return nil, errors.New("read tailscale peer status: status is nil")
	}
	if status.BackendState != "Running" {
		return nil, fmt.Errorf(
			"tailscale peer status is not running: %q",
			status.BackendState,
		)
	}
	return status, nil
}

func validatePeerStillOnline(
	ctx context.Context,
	startedNode node,
	classified classifiedPeer,
) error {
	status, err := runningStatus(ctx, startedNode)
	if err != nil {
		return fmt.Errorf("revalidate tailscale peer: %w", err)
	}
	peer := status.Peer[classified.key]
	if peer == nil || !peer.Online || !peerHasIP(peer, classified.address.Addr()) {
		return errors.New("classified tailscale peer is no longer online at the selected address")
	}
	return nil
}

func uniqueOnlinePeer(
	status *ipnstate.Status,
	matches func(*ipnstate.PeerStatus) bool,
) (key.NodePublic, *ipnstate.PeerStatus, error) {
	var matchKey key.NodePublic
	var match *ipnstate.PeerStatus
	for peerKey, peer := range status.Peer {
		if peer == nil || !peer.Online || !matches(peer) {
			continue
		}
		if match != nil {
			return key.NodePublic{}, nil, errors.New("multiple online peers match")
		}
		matchKey = peerKey
		match = peer
	}
	return matchKey, match, nil
}

func peerNameMatches(
	status *ipnstate.Status,
	peer *ipnstate.PeerStatus,
	hostname string,
) bool {
	peerName := canonicalDNSName(peer.DNSName)
	if peerName == "" {
		return false
	}
	if hostname == peerName {
		return true
	}
	if strings.Contains(hostname, ".") || status.CurrentTailnet == nil {
		return false
	}
	suffix := canonicalDNSName(status.CurrentTailnet.MagicDNSSuffix)
	return suffix != "" && peerName == hostname+"."+suffix
}

func canonicalDNSName(name string) string {
	return strings.ToLower(strings.TrimSuffix(name, "."))
}

func peerHasIP(peer *ipnstate.PeerStatus, target netip.Addr) bool {
	for _, ip := range peer.TailscaleIPs {
		if ip.Unmap() == target {
			return true
		}
	}
	return false
}

func peerIPForNetwork(peer *ipnstate.PeerStatus, network string) (netip.Addr, bool) {
	var ipv6 netip.Addr
	for _, ip := range peer.TailscaleIPs {
		ip = ip.Unmap()
		if !ip.IsValid() || !addressFamilyMatches(network, ip) {
			continue
		}
		if ip.Is4() {
			return ip, true
		}
		if !ipv6.IsValid() {
			ipv6 = ip
		}
	}
	return ipv6, ipv6.IsValid()
}

func addressFamilyMatches(network string, ip netip.Addr) bool {
	switch network {
	case "tcp4":
		return ip.Is4()
	case "tcp6":
		return ip.Is6()
	default:
		return true
	}
}

type tsnetNode struct {
	server *tsnet.Server
}

func newTSNetNode(cfg nodeConfig) (node, error) {
	return &tsnetNode{server: &tsnet.Server{
		Dir:        cfg.Dir,
		Hostname:   cfg.Hostname,
		AuthKey:    cfg.AuthKey,
		ControlURL: cfg.ControlURL,
		Ephemeral:  cfg.Ephemeral,
		Logf:       cfg.Logf,
		UserLogf:   cfg.UserLogf,
	}}, nil
}

func (n *tsnetNode) Start() error {
	return n.server.Start()
}

func (n *tsnetNode) ClearAuthKey() {
	n.server.AuthKey = ""
}

func (n *tsnetNode) Up(ctx context.Context) error {
	_, err := n.server.Up(ctx)
	return err
}

func (n *tsnetNode) Listen(network, address string) (net.Listener, error) {
	return n.server.Listen(network, address)
}

func (n *tsnetNode) Status(ctx context.Context) (*ipnstate.Status, error) {
	localClient, err := n.server.LocalClient()
	if err != nil {
		return nil, err
	}
	return localClient.Status(ctx)
}

func (n *tsnetNode) DialPeerTCP(ctx context.Context, address netip.AddrPort) (net.Conn, error) {
	system := n.server.Sys()
	if system == nil {
		return nil, errors.New("tailscale node system is unavailable")
	}
	dialer, ok := system.Dialer.GetOK()
	if !ok || dialer == nil || dialer.NetstackDialTCP == nil {
		return nil, errors.New("tailscale node netstack dialer is unavailable")
	}
	return dialer.NetstackDialTCP(ctx, address)
}

func (n *tsnetNode) Close() error {
	return n.server.Close()
}
