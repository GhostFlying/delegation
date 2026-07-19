package userservice

import (
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

func TestRenderSystemdProducesInactiveUserUnit(t *testing.T) {
	descriptor, err := RenderSystemd(ServiceRolePeer, `/opt/${HOME}/Delegation % test/delegation`, `/home/user/${CONFIG} "quoted".json`)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindSystemd || descriptor.Name != SystemdPeerUnitName {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	content := string(descriptor.Content)
	wantExec := `ExecStart="/opt/$${HOME}/Delegation %% test/delegation" service run --config "/home/user/$${CONFIG} \"quoted\".json"`
	if !strings.Contains(content, wantExec) {
		t.Fatalf("systemd unit missing escaped ExecStart %q:\n%s", wantExec, content)
	}
	if strings.Contains(content, "WantedBy=default.target\nAlias=") || !strings.Contains(content, MarkerPeer) {
		t.Fatalf("systemd unit has unexpected activation or marker:\n%s", content)
	}
}

func TestRenderLaunchAgentProducesDisabledValidXML(t *testing.T) {
	descriptor, err := RenderLaunchAgent(ServiceRolePeer, `/Applications/Delegation & Tools/delegation`, `/Users/test/Config <one>.json`)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindLaunchAgent || descriptor.Name != LaunchAgentPeerName {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	assertXMLWellFormed(t, descriptor.Content)
	content := string(descriptor.Content)
	for _, expected := range []string{
		`<string>/Applications/Delegation &amp; Tools/delegation</string>`,
		`<string>/Users/test/Config &lt;one&gt;.json</string>`,
		`<key>Disabled</key>`,
		`<true/>`,
		MarkerPeer,
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("LaunchAgent missing %q:\n%s", expected, content)
		}
	}
}

func TestRenderScheduledTaskProducesDisabledValidXML(t *testing.T) {
	escapedArguments := make([]string, 0, 4)
	escape := func(value string) string {
		escapedArguments = append(escapedArguments, value)
		return "ESC[" + value + "]"
	}
	descriptor, err := RenderScheduledTask(
		ServiceRolePeer,
		"C:\\Program Files\\Delegation \u5de5\u5177 & Tools\\delegation.exe",
		"C:\\Users\\test\\\u914d\u7f6e One.json",
		`S-1-5-21-1000`,
		escape,
	)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindScheduledTask || descriptor.Name != ScheduledTaskPeer {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if !bytes.HasPrefix(descriptor.Content, []byte{0xff, 0xfe}) {
		t.Fatalf("scheduled task does not use UTF-16LE with a BOM: %x", descriptor.Content[:min(8, len(descriptor.Content))])
	}
	encodedContent, err := decodeUTF16(descriptor.Content[2:], binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(encodedContent, `<?xml version="1.0" encoding="UTF-16"?>`) {
		t.Fatalf("scheduled task has an unexpected XML declaration: %q", encodedContent[:min(80, len(encodedContent))])
	}
	assertXMLWellFormed(t, descriptor.Content)
	content := taskXMLText(t, descriptor.Content)
	for _, expected := range []string{
		`<Enabled>false</Enabled>`,
		`<LogonType>InteractiveToken</LogonType>`,
		`<URI>\Delegation Peer</URI>`,
		`<UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>`,
		"<Command>C:\\Program Files\\Delegation \u5de5\u5177 &amp; Tools\\delegation.exe</Command>",
		"<Arguments>ESC[service] ESC[run] ESC[--config] ESC[C:\\Users\\test\\\u914d\u7f6e One.json]</Arguments>",
		MarkerPeer,
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("scheduled task missing %q:\n%s", expected, content)
		}
	}
	for _, omittedDefault := range []string{
		`<Enabled>true</Enabled>`,
		`<RunLevel>LeastPrivilege</RunLevel>`,
		`<AllowHardTerminate>true</AllowHardTerminate>`,
		`<RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>`,
		`<AllowStartOnDemand>true</AllowStartOnDemand>`,
		`<Hidden>false</Hidden>`,
		`<RunOnlyIfIdle>false</RunOnlyIfIdle>`,
		`<WakeToRun>false</WakeToRun>`,
		`<Priority>7</Priority>`,
	} {
		if strings.Contains(content, omittedDefault) {
			t.Fatalf("scheduled task includes exporter-omitted default %q:\n%s", omittedDefault, content)
		}
	}
	wantArguments := []string{"service", "run", "--config", "C:\\Users\\test\\\u914d\u7f6e One.json"}
	if strings.Join(escapedArguments, "\x00") != strings.Join(wantArguments, "\x00") {
		t.Fatalf("escaped arguments = %q, want %q", escapedArguments, wantArguments)
	}
}

func TestRoleSpecificServiceDescriptorsDoNotCollide(t *testing.T) {
	for _, render := range []struct {
		name      string
		scheduled bool
		call      func(ServiceRole) (Descriptor, error)
	}{
		{
			name: "systemd",
			call: func(role ServiceRole) (Descriptor, error) {
				return RenderSystemd(role, "/opt/delegation/delegation", "/home/test/.delegation/config.json")
			},
		},
		{
			name: "launchd",
			call: func(role ServiceRole) (Descriptor, error) {
				return RenderLaunchAgent(role, "/opt/delegation/delegation", "/Users/test/.delegation/config.json")
			},
		},
		{
			name: "scheduled task", scheduled: true,
			call: func(role ServiceRole) (Descriptor, error) {
				return RenderScheduledTask(
					role, `C:\Delegation\delegation.exe`, `C:\Users\test\.delegation\config.json`,
					"S-1-5-21-1000", func(value string) string { return value },
				)
			},
		},
	} {
		t.Run(render.name, func(t *testing.T) {
			broker, err := render.call(ServiceRoleBroker)
			if err != nil {
				t.Fatal(err)
			}
			peer, err := render.call(ServiceRolePeer)
			if err != nil {
				t.Fatal(err)
			}
			if broker.Name == peer.Name || bytes.Equal(broker.Content, peer.Content) {
				t.Fatalf("broker and peer descriptors collide: %#v / %#v", broker, peer)
			}
			brokerText := string(broker.Content)
			peerText := string(peer.Content)
			if render.scheduled {
				brokerText = taskXMLText(t, broker.Content)
				peerText = taskXMLText(t, peer.Content)
			}
			if !strings.Contains(brokerText, MarkerBroker) || strings.Contains(brokerText, MarkerPeer) ||
				!strings.Contains(peerText, MarkerPeer) || strings.Contains(peerText, MarkerBroker) {
				t.Fatalf("role markers are not isolated: %q / %q", brokerText, peerText)
			}
		})
	}
}

func TestRenderersRejectUnsafePaths(t *testing.T) {
	if _, err := RenderSystemd(ServiceRolePeer, "relative", "/config"); err == nil {
		t.Fatal("RenderSystemd() accepted relative binary path")
	}
	if _, err := RenderSystemd(ServiceRolePeer, `C:\delegation.exe`, `C:\config.json`); err == nil {
		t.Fatal("RenderSystemd() accepted Windows binary path")
	}
	if _, err := RenderLaunchAgent(ServiceRolePeer, "/binary", "/config\nline"); err == nil {
		t.Fatal("RenderLaunchAgent() accepted control character")
	}
	if _, err := RenderScheduledTask(ServiceRolePeer, `C:\binary.exe`, `relative`, "S-1-5-21", func(value string) string { return value }); err == nil {
		t.Fatal("RenderScheduledTask() accepted relative config path")
	}
	if _, err := RenderSystemd(ServiceRolePeer, "/binary", "/config\xff"); err == nil {
		t.Fatal("RenderSystemd() accepted invalid UTF-8")
	}
	if _, err := RenderScheduledTask(ServiceRolePeer, `\\server`, `C:\config`, "S-1-5-21", func(value string) string { return value }); err == nil {
		t.Fatal("RenderScheduledTask() accepted incomplete UNC binary path")
	}
	if _, err := RenderScheduledTask(ServiceRole("custom"), `C:\binary.exe`, `C:\config`, "S-1-5-21", func(value string) string { return value }); err == nil {
		t.Fatal("RenderScheduledTask() accepted an arbitrary service role")
	}
}

func assertXMLWellFormed(t *testing.T, content []byte) {
	t.Helper()
	normalized, err := normalizeTaskXML(content)
	if err != nil {
		t.Fatalf("normalize XML: %v", err)
	}
	decoder := xml.NewDecoder(bytes.NewReader(normalized))
	for {
		if _, err := decoder.Token(); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatalf("invalid XML: %v\n%s", err, content)
		}
	}
}

func taskXMLText(t *testing.T, content []byte) string {
	t.Helper()
	normalized, err := normalizeTaskXML(content)
	if err != nil {
		t.Fatalf("normalize scheduled task XML: %v", err)
	}
	return string(normalized)
}
