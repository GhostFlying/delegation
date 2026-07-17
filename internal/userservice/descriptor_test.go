package userservice

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
	"testing"
)

func TestRenderSystemdProducesInactiveUserUnit(t *testing.T) {
	descriptor, err := RenderSystemd(`/opt/${HOME}/Delegation % test/delegation`, `/home/user/${CONFIG} "quoted".json`)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindSystemd || descriptor.Name != SystemdUnitName {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	content := string(descriptor.Content)
	wantExec := `ExecStart="/opt/$${HOME}/Delegation %% test/delegation" service run --config "/home/user/$${CONFIG} \"quoted\".json"`
	if !strings.Contains(content, wantExec) {
		t.Fatalf("systemd unit missing escaped ExecStart %q:\n%s", wantExec, content)
	}
	if strings.Contains(content, "WantedBy=default.target\nAlias=") || !strings.Contains(content, Marker) {
		t.Fatalf("systemd unit has unexpected activation or marker:\n%s", content)
	}
}

func TestRenderLaunchAgentProducesDisabledValidXML(t *testing.T) {
	descriptor, err := RenderLaunchAgent(`/Applications/Delegation & Tools/delegation`, `/Users/test/Config <one>.json`)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindLaunchAgent || descriptor.Name != LaunchAgentName {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	assertXMLWellFormed(t, descriptor.Content)
	content := string(descriptor.Content)
	for _, expected := range []string{
		`<string>/Applications/Delegation &amp; Tools/delegation</string>`,
		`<string>/Users/test/Config &lt;one&gt;.json</string>`,
		`<key>Disabled</key>`,
		`<true/>`,
		Marker,
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
		`C:\Program Files\Delegation & Tools\delegation.exe`,
		`C:\Users\test\Config One.json`,
		`S-1-5-21-1000`,
		escape,
	)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Kind != KindScheduledTask || descriptor.Name != ScheduledTask {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	assertXMLWellFormed(t, descriptor.Content)
	content := string(descriptor.Content)
	for _, expected := range []string{
		`<Enabled>false</Enabled>`,
		`<LogonType>InteractiveToken</LogonType>`,
		`<RunLevel>LeastPrivilege</RunLevel>`,
		`<Command>C:\Program Files\Delegation &amp; Tools\delegation.exe</Command>`,
		`<Arguments>ESC[service] ESC[run] ESC[--config] ESC[C:\Users\test\Config One.json]</Arguments>`,
		Marker,
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("scheduled task missing %q:\n%s", expected, content)
		}
	}
	wantArguments := []string{"service", "run", "--config", `C:\Users\test\Config One.json`}
	if strings.Join(escapedArguments, "\x00") != strings.Join(wantArguments, "\x00") {
		t.Fatalf("escaped arguments = %q, want %q", escapedArguments, wantArguments)
	}
}

func TestRenderersRejectUnsafePaths(t *testing.T) {
	if _, err := RenderSystemd("relative", "/config"); err == nil {
		t.Fatal("RenderSystemd() accepted relative binary path")
	}
	if _, err := RenderLaunchAgent("/binary", "/config\nline"); err == nil {
		t.Fatal("RenderLaunchAgent() accepted control character")
	}
	if _, err := RenderScheduledTask(`C:\binary.exe`, `relative`, "S-1-5-21", func(value string) string { return value }); err == nil {
		t.Fatal("RenderScheduledTask() accepted relative config path")
	}
	if _, err := RenderSystemd("/binary", "/config\xff"); err == nil {
		t.Fatal("RenderSystemd() accepted invalid UTF-8")
	}
	if _, err := RenderScheduledTask(`\\server`, `C:\config`, "S-1-5-21", func(value string) string { return value }); err == nil {
		t.Fatal("RenderScheduledTask() accepted incomplete UNC binary path")
	}
}

func assertXMLWellFormed(t *testing.T, content []byte) {
	t.Helper()
	decoder := xml.NewDecoder(bytes.NewReader(content))
	for {
		if _, err := decoder.Token(); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatalf("invalid XML: %v\n%s", err, content)
		}
	}
}
