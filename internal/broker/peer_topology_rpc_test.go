package broker

import (
	"testing"
	"time"

	"github.com/GhostFlying/delegation/internal/config"
	"github.com/GhostFlying/delegation/internal/protocol"
	"github.com/coder/websocket"
)

const (
	peerTopologyDeviceA = "123e4567-e89b-42d3-a456-426614174120"
	peerTopologyDeviceB = "123e4567-e89b-42d3-a456-426614174121"
	peerTopologyDeviceC = "123e4567-e89b-42d3-a456-426614174122"
	peerTopologyThreadA = "123e4567-e89b-42d3-a456-426614174123"
	peerTopologyThreadB = "123e4567-e89b-42d3-a456-426614174124"
	peerTopologyThreadC = "123e4567-e89b-42d3-a456-426614174125"
	peerTopologyThreadD = "123e4567-e89b-42d3-a456-426614174126"
)

func TestEveryPeerCreatesIndependentRootsAndReadsSharedRegistry(t *testing.T) {
	harness := newBrokerHarness(t, config.AuthModeNone, time.Second)
	peers := []struct {
		deviceID string
		threadID string
	}{
		{peerTopologyDeviceA, peerTopologyThreadA},
		{peerTopologyDeviceB, peerTopologyThreadB},
		{peerTopologyDeviceC, peerTopologyThreadC},
	}
	connections := make([]*websocket.Conn, 0, len(peers))
	for _, peer := range peers {
		connection, _, err := dialBroker(harness, nil)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
		hello := hello()
		hello.DeviceID = peer.deviceID
		hello.DeviceName = "peer-" + peer.deviceID[len(peer.deviceID)-1:]
		response := writeAndRead(t, connection, request(t, protocol.MethodHello, hello))
		if response.Error != nil {
			t.Fatalf("peer %s hello = %#v", peer.deviceID, response.Error)
		}
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			connection.CloseNow()
		}
	})

	var roots []protocol.EnsureRootTreeResult
	for index, peer := range peers {
		response := writeAndRead(t, connections[index], request(
			t,
			protocol.MethodEnsureRootTree,
			protocol.EnsureRootTreeParams{ExternalThreadID: peer.threadID},
		))
		if response.Error != nil {
			t.Fatalf("peer %s root = %#v", peer.deviceID, response.Error)
		}
		root := decodeResult[protocol.EnsureRootTreeResult](t, response)
		if root.Tree.RootDeviceID != peer.deviceID || root.Principal.DeviceID != peer.deviceID {
			t.Fatalf("peer %s root binding = %#v", peer.deviceID, root)
		}
		roots = append(roots, root)
		source := root.Principal.Identity()
		listResponse := writeAndRead(t, connections[index], principalRequest(
			t, protocol.MethodListDevices, protocol.ListDevicesParams{Limit: len(peers)}, root.Principal,
		))
		if listResponse.Error != nil {
			t.Fatalf("peer %s registry read = %#v", peer.deviceID, listResponse.Error)
		}
		listed := decodeResult[protocol.ListDevicesResult](t, listResponse)
		if len(listed.Devices) != len(peers) {
			t.Fatalf("peer %s registry = %#v", peer.deviceID, listed)
		}
		if source.DeviceID != peer.deviceID {
			t.Fatalf("peer %s source = %#v", peer.deviceID, source)
		}
	}
	if roots[0].Tree.TreeID == roots[1].Tree.TreeID || roots[1].Tree.TreeID == roots[2].Tree.TreeID {
		t.Fatalf("peer roots were not isolated: %#v", roots)
	}

	secondTask := writeAndRead(t, connections[0], request(
		t,
		protocol.MethodEnsureRootTree,
		protocol.EnsureRootTreeParams{ExternalThreadID: peerTopologyThreadD},
	))
	if secondTask.Error != nil {
		t.Fatalf("same-peer second task = %#v", secondTask.Error)
	}
	secondRoot := decodeResult[protocol.EnsureRootTreeResult](t, secondTask)
	if secondRoot.Tree.TreeID == roots[0].Tree.TreeID || secondRoot.Tree.RootDeviceID != peerTopologyDeviceA {
		t.Fatalf("same-peer task isolation = %#v", secondRoot)
	}

	conflict := writeAndRead(t, connections[1], request(
		t,
		protocol.MethodEnsureRootTree,
		protocol.EnsureRootTreeParams{ExternalThreadID: peerTopologyThreadA},
	))
	if conflict.Error == nil || conflict.Error.Code != protocol.ErrorConflict {
		t.Fatalf("cross-peer thread conflict = %#v", conflict)
	}
}
