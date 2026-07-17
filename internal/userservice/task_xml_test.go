package userservice

import (
	"bytes"
	"encoding/binary"
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
	withSurrogatePair := strings.Replace(
		string(descriptor.Content),
		"<Description>",
		"<Author>\U0001f600</Author><Description>",
		1,
	)
	for name, encoded := range map[string][]byte{
		"little endian":        encodeTaskUTF16(string(descriptor.Content), binary.LittleEndian, []byte{0xff, 0xfe}),
		"big endian":           encodeTaskUTF16(string(descriptor.Content), binary.BigEndian, []byte{0xfe, 0xff}),
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
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			got, err := parseTaskDefinition([]byte(mutate(string(descriptor.Content))))
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatal("behavior drift produced an identical task definition")
			}
		})
	}
}

func TestTaskDefinitionRejectsEnvelopeExtensions(t *testing.T) {
	descriptor := testTaskDescriptor(t)
	tests := map[string]string{
		"root data":           strings.Replace(string(descriptor.Content), "</Task>", "<Data>foreign</Data></Task>", 1),
		"security descriptor": strings.Replace(string(descriptor.Content), "</RegistrationInfo>", "<SecurityDescriptor>D:(A;;GA;;;WD)</SecurityDescriptor></RegistrationInfo>", 1),
		"root attribute":      strings.Replace(string(descriptor.Content), `<Task version="1.4"`, `<Task version="1.4" foreign="true"`, 1),
		"duplicate actions":   strings.Replace(string(descriptor.Content), "</Task>", "<Actions Context=\"Author\"/></Task>", 1),
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
		string(descriptor.Content),
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
	document := strings.Replace(string(descriptor.Content), "<Description>"+Marker+"</Description>", "<Description>foreign "+Marker+"</Description>", 1)
	definition, err := parseTaskDefinition([]byte(document))
	if err != nil {
		t.Fatal(err)
	}
	if taskOwned(definition) {
		t.Fatal("taskOwned() accepted a marker substring")
	}
}

func testTaskDescriptor(t *testing.T) Descriptor {
	t.Helper()
	descriptor, err := RenderScheduledTask(
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
