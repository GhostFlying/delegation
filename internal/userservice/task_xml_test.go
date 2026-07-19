package userservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf16"
)

func TestTaskDefinitionAcceptsUTF16ByteOrders(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	want, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		t.Fatal(err)
	}
	document := taskXMLText(t, descriptor.Content)
	withSurrogatePair := strings.Replace(
		document,
		"<Description>",
		"<Author>\U0001f600</Author><Description>",
		1,
	)
	for name, encoded := range map[string][]byte{
		"little endian":        encodeTaskUTF16(document, binary.LittleEndian, []byte{0xff, 0xfe}),
		"big endian":           encodeTaskUTF16(document, binary.BigEndian, []byte{0xfe, 0xff}),
		"valid surrogate pair": encodeTaskUTF16(withSurrogatePair, binary.LittleEndian, []byte{0xff, 0xfe}),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseTaskDefinition(encoded)
			if err != nil || got != want {
				t.Fatalf("parseTaskDefinition() = %#v, %v; want %#v", got, err, want)
			}
		})
	}
}

func TestTaskDefinitionDetectsBehaviorDrift(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	want, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		t.Fatal(err)
	}
	document := taskXMLText(t, descriptor.Content)
	mutations := map[string]func(string) string{
		"extra trigger": func(document string) string {
			return strings.Replace(document, "</Triggers>", "<TimeTrigger><Enabled>true</Enabled></TimeTrigger></Triggers>", 1)
		},
		"extra action": func(document string) string {
			return strings.Replace(document, "</Actions>", "<ComHandler><ClassId>{00000000-0000-0000-0000-000000000000}</ClassId></ComHandler></Actions>", 1)
		},
		"changed setting": func(document string) string {
			return strings.Replace(document, "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>", "<MultipleInstancesPolicy>Parallel</MultipleInstancesPolicy>", 1)
		},
		"enabled task": func(document string) string {
			return strings.Replace(document, "<Enabled>false</Enabled>", "<Enabled>true</Enabled>", 1)
		},
		"disabled unified engine": func(document string) string {
			return strings.Replace(document, "<UseUnifiedSchedulingEngine>true</UseUnifiedSchedulingEngine>", "<UseUnifiedSchedulingEngine>false</UseUnifiedSchedulingEngine>", 1)
		},
		"elevated principal": func(document string) string {
			return strings.Replace(document, "</LogonType>", "</LogonType><RunLevel>HighestAvailable</RunLevel>", 1)
		},
		"changed command": func(document string) string {
			return strings.Replace(document, "<Command>C:\\Delegation\\delegation.exe</Command>", "<Command>C:\\Other\\delegation.exe</Command>", 1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			got, err := parseTaskDefinition([]byte(mutate(document)))
			if err != nil {
				t.Fatal(err)
			}
			equivalent, err := taskDefinitionsEquivalent(want, got, func(left, right string) (bool, error) {
				return left == right, nil
			})
			if err != nil || equivalent {
				t.Fatalf("taskDefinitionsEquivalent() = %v, %v; want drift", equivalent, err)
			}
		})
	}
}

func TestTaskDefinitionsEquivalentNormalizesSchedulerRepresentation(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	desired, err := parseTaskDefinition(descriptor.Content)
	if err != nil {
		t.Fatal(err)
	}
	document := taskXMLText(t, descriptor.Content)
	document = replaceTaskFixture(t, document, "<UserId>S-1-5-21-test</UserId>", `<UserId>HOST\runner</UserId>`)
	document = replaceTaskFixture(
		t,
		document,
		"    <LogonTrigger>\n      <UserId>",
		"    <LogonTrigger>\n      <Enabled>true</Enabled>\n      <ExecutionTimeLimit>PT72H</ExecutionTimeLimit>\n      <Delay>PT0M</Delay>\n      <UserId>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"      <LogonType>InteractiveToken</LogonType>",
		"      <LogonType>InteractiveToken</LogonType>\n      <RunLevel>LeastPrivilege</RunLevel>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>\n    <AllowStartOnDemand>true</AllowStartOnDemand>\n    <AllowHardTerminate>true</AllowHardTerminate>\n    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>\n    <Hidden>false</Hidden>\n    <WakeToRun>false</WakeToRun>\n    <RunOnlyIfIdle>false</RunOnlyIfIdle>\n    <Priority>7</Priority>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"      <RestartOnIdle>false</RestartOnIdle>",
		"      <RestartOnIdle>false</RestartOnIdle>\n      <Duration>PT10M</Duration>\n      <WaitTimeout>PT1H</WaitTimeout>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>\n    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
		"    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>\n    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"      <StopOnIdleEnd>false</StopOnIdleEnd>\n      <RestartOnIdle>false</RestartOnIdle>",
		"      <RestartOnIdle>false</RestartOnIdle>\n      <StopOnIdleEnd>false</StopOnIdleEnd>",
	)
	document = replaceTaskFixture(
		t,
		document,
		"      <UserId>S-1-5-21-test</UserId>\n      <LogonType>InteractiveToken</LogonType>",
		"      <LogonType>InteractiveToken</LogonType>\n      <UserId>S-1-5-21-test</UserId>",
	)
	existing, err := parseTaskDefinition([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	resolveAlias := func(left, right string) (bool, error) {
		aliases := map[string]string{
			"S-1-5-21-test": "S-1-5-21-test",
			`HOST\runner`:   "S-1-5-21-test",
			`OTHER\runner`:  "S-1-5-21-other",
		}
		return aliases[left] != "" && aliases[left] == aliases[right], nil
	}
	equivalent, err := taskDefinitionsEquivalent(desired, existing, resolveAlias)
	if err != nil || !equivalent {
		t.Fatalf("taskDefinitionsEquivalent() = %v, %v", equivalent, err)
	}

	existing.TriggerUserID = `OTHER\runner`
	equivalent, err = taskDefinitionsEquivalent(desired, existing, resolveAlias)
	if err != nil || equivalent {
		t.Fatalf("taskDefinitionsEquivalent() accepted another trigger identity: %v, %v", equivalent, err)
	}
	existing.TriggerUserID = `HOST\runner`
	identityErr := errors.New("identity lookup failed")
	_, err = taskDefinitionsEquivalent(desired, existing, func(string, string) (bool, error) {
		return false, identityErr
	})
	if !errors.Is(err, identityErr) {
		t.Fatalf("taskDefinitionsEquivalent() error = %v, want %v", err, identityErr)
	}
}

func TestTaskDefinitionRejectsEnvelopeExtensions(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	document := taskXMLText(t, descriptor.Content)
	tests := map[string]string{
		"root data":           strings.Replace(document, "</Task>", "<Data>foreign</Data></Task>", 1),
		"security descriptor": strings.Replace(document, "</RegistrationInfo>", "<SecurityDescriptor>D:(A;;GA;;;WD)</SecurityDescriptor></RegistrationInfo>", 1),
		"root attribute":      strings.Replace(document, `<Task version="1.4"`, `<Task version="1.4" foreign="true"`, 1),
		"duplicate actions":   strings.Replace(document, "</Task>", "<Actions Context=\"Author\"/></Task>", 1),
		"duplicate setting":   strings.Replace(document, "</Settings>", "<Enabled>true</Enabled></Settings>", 1),
		"duplicate trigger user": strings.Replace(
			document,
			"</LogonTrigger>",
			"<UserId>S-1-5-21-other</UserId></LogonTrigger>",
			1,
		),
		"duplicate principal user": strings.Replace(
			document,
			"</Principal>",
			"<UserId>S-1-5-21-other</UserId></Principal>",
			1,
		),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTaskDefinition([]byte(document)); err == nil {
				t.Fatal("parseTaskDefinition() accepted an envelope extension")
			}
		})
	}
}

func TestTaskDefinitionRejectsResourceExhaustion(t *testing.T) {
	deep := `<Task>` + strings.Repeat(`<Node>`, maxTaskXMLDepth) +
		strings.Repeat(`</Node>`, maxTaskXMLDepth) + `</Task>`
	manyNodes := `<Task>` + strings.Repeat(`<Node/>`, maxTaskXMLNodes) + `</Task>`
	var manyAttributes strings.Builder
	manyAttributes.WriteString(`<Task`)
	for i := 0; i <= maxTaskXMLAttributes; i++ {
		fmt.Fprintf(&manyAttributes, ` a%d="x"`, i)
	}
	manyAttributes.WriteString(`/>`)

	tests := map[string]struct {
		document []byte
		want     string
	}{
		"size":       {document: bytes.Repeat([]byte(" "), maxTaskXMLSize+1), want: "size limit"},
		"depth":      {document: []byte(deep), want: "depth limit"},
		"nodes":      {document: []byte(manyNodes), want: "node limit"},
		"attributes": {document: []byte(manyAttributes.String()), want: "attribute limit"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := parseTaskDefinition(test.document)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parseTaskDefinition() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTaskDefinitionRejectsUnpairedUTF16Surrogates(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	document := strings.Replace(
		taskXMLText(t, descriptor.Content),
		"<Description>",
		"<Author>X</Author><Description>",
		1,
	)
	for name, malformed := range map[string]uint16{
		"lone high surrogate": 0xd800,
		"lone low surrogate":  0xdc00,
	} {
		t.Run(name, func(t *testing.T) {
			units := utf16.Encode([]rune(document))
			for i, unit := range units {
				if unit == 'X' {
					units[i] = malformed
					break
				}
			}
			encoded := make([]byte, 2+len(units)*2)
			copy(encoded, []byte{0xff, 0xfe})
			for i, unit := range units {
				binary.LittleEndian.PutUint16(encoded[2+i*2:], unit)
			}
			_, err := parseTaskDefinition(encoded)
			if err == nil || !strings.Contains(err.Error(), "unpaired surrogate") {
				t.Fatalf("parseTaskDefinition() error = %v, want unpaired surrogate", err)
			}
		})
	}
}

func TestTaskOwnershipRequiresExactDescriptionAndURI(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	document := strings.Replace(taskXMLText(t, descriptor.Content), "<Description>"+MarkerPeer+"</Description>", "<Description>foreign "+MarkerPeer+"</Description>", 1)
	definition, err := parseTaskDefinition([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if taskOwned(definition, ServiceRolePeer) {
		t.Fatal("taskOwned() accepted a marker substring")
	}
}

func TestEncodeTaskXMLUTF16LERejectsInvalidUTF8(t *testing.T) {
	if _, err := encodeTaskXMLUTF16LE("<Task>\xff</Task>"); err == nil {
		t.Fatal("encodeTaskXMLUTF16LE() accepted invalid UTF-8")
	}
}

func replaceTaskFixture(t *testing.T, document, old, replacement string) string {
	t.Helper()
	if !strings.Contains(document, old) {
		t.Fatalf("task fixture does not contain %q", old)
	}
	return strings.Replace(document, old, replacement, 1)
}

func testTaskDescriptor(t *testing.T) Descriptor {
	t.Helper()
	descriptor, err := RenderScheduledTask(
		ServiceRolePeer,
		`C:\Delegation\delegation.exe`,
		`C:\Users\test\config.json`,
		"S-1-5-21-test",
		func(value string) string { return value },
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func encodeTaskUTF16(value string, order binary.ByteOrder, bom []byte) []byte {
	units := utf16.Encode([]rune(value))
	encoded := make([]byte, len(bom)+len(units)*2)
	copy(encoded, bom)
	for i, unit := range units {
		order.PutUint16(encoded[len(bom)+i*2:], unit)
	}
	return encoded
}
