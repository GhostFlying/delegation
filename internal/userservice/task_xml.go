package userservice

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	taskXMLNamespace     = "http://schemas.microsoft.com/windows/2004/02/mit/task"
	maxTaskXMLSize       = 1 << 20
	maxTaskXMLDepth      = 64
	maxTaskXMLNodes      = 4096
	maxTaskXMLAttributes = 1024
)

type taskDefinition struct {
	Description string
	URI         string
	Triggers    string
	Principals  string
	Settings    string
	Actions     string
}

type taskXMLAttribute struct {
	Space string
	Local string
	Value string
}

type taskXMLNode struct {
	Space      string
	Local      string
	Attributes []taskXMLAttribute
	Text       string
	Children   []taskXMLNode
}

type taskXMLBudget struct {
	nodes      int
	attributes int
}

func taskOwned(task taskDefinition) bool {
	return task.Description == Marker && task.URI == `\Delegation\Connector`
}

func parseTaskDefinition(data []byte) (taskDefinition, error) {
	if len(data) > maxTaskXMLSize {
		return taskDefinition{}, errors.New("scheduled task XML exceeds the size limit")
	}
	normalized, err := normalizeTaskXML(data)
	if err != nil {
		return taskDefinition{}, err
	}
	root, err := parseTaskXMLTree(normalized)
	if err != nil {
		return taskDefinition{}, err
	}
	if root.Space != taskXMLNamespace || root.Local != "Task" {
		return taskDefinition{}, errors.New("scheduled task XML has an unexpected Task root")
	}
	if err := validateTaskEnvelope(root); err != nil {
		return taskDefinition{}, err
	}
	registration, err := uniqueTaskChild(root, "RegistrationInfo")
	if err != nil {
		return taskDefinition{}, err
	}
	description, err := uniqueTaskLeaf(registration, "Description")
	if err != nil {
		return taskDefinition{}, err
	}
	uri, err := uniqueTaskLeaf(registration, "URI")
	if err != nil {
		return taskDefinition{}, err
	}
	canonical := make(map[string]string, 4)
	for _, name := range []string{"Triggers", "Principals", "Settings", "Actions"} {
		child, err := uniqueTaskChild(root, name)
		if err != nil {
			return taskDefinition{}, err
		}
		encoded, err := json.Marshal(child)
		if err != nil {
			return taskDefinition{}, fmt.Errorf("canonicalize scheduled task %s: %w", name, err)
		}
		canonical[name] = string(encoded)
	}
	return taskDefinition{
		Description: description,
		URI:         uri,
		Triggers:    canonical["Triggers"],
		Principals:  canonical["Principals"],
		Settings:    canonical["Settings"],
		Actions:     canonical["Actions"],
	}, nil
}

func validateTaskEnvelope(root taskXMLNode) error {
	attributes := map[string]int{"version": 0, "namespace": 0}
	for _, attribute := range root.Attributes {
		switch {
		case attribute.Space == "" && attribute.Local == "version" && attribute.Value == "1.4":
			attributes["version"]++
		case attribute.Space == "" && attribute.Local == "xmlns" && attribute.Value == taskXMLNamespace:
			attributes["namespace"]++
		case attribute.Space == "xmlns" && attribute.Local == "" && attribute.Value == taskXMLNamespace:
			attributes["namespace"]++
		default:
			return fmt.Errorf("scheduled task XML has unexpected Task attribute %#v", attribute)
		}
	}
	if attributes["version"] != 1 || attributes["namespace"] > 1 {
		return errors.New("scheduled task XML has invalid Task version or namespace declaration")
	}
	rootFields := map[string]int{
		"RegistrationInfo": 0,
		"Triggers":         0,
		"Principals":       0,
		"Settings":         0,
		"Actions":          0,
	}
	for _, child := range root.Children {
		if child.Space != taskXMLNamespace {
			return fmt.Errorf("scheduled task XML has foreign root child %s", child.Local)
		}
		count, ok := rootFields[child.Local]
		if !ok {
			return fmt.Errorf("scheduled task XML has unexpected root child %s", child.Local)
		}
		rootFields[child.Local] = count + 1
	}
	for name, count := range rootFields {
		if count != 1 {
			return fmt.Errorf("scheduled task XML must contain exactly one %s", name)
		}
	}

	registration, err := uniqueTaskChild(root, "RegistrationInfo")
	if err != nil {
		return err
	}
	if len(registration.Attributes) != 0 {
		return errors.New("scheduled task RegistrationInfo has unexpected attributes")
	}
	registrationFields := map[string]int{
		"Description": 0,
		"URI":         0,
		"Author":      0,
		"Date":        0,
	}
	for _, child := range registration.Children {
		if child.Space != taskXMLNamespace {
			return fmt.Errorf("scheduled task RegistrationInfo has foreign child %s", child.Local)
		}
		count, ok := registrationFields[child.Local]
		if !ok {
			return fmt.Errorf("scheduled task RegistrationInfo has unexpected child %s", child.Local)
		}
		if len(child.Attributes) != 0 || len(child.Children) != 0 {
			return fmt.Errorf("scheduled task RegistrationInfo %s is not scalar", child.Local)
		}
		registrationFields[child.Local] = count + 1
	}
	for _, name := range []string{"Description", "URI"} {
		if registrationFields[name] != 1 {
			return fmt.Errorf("scheduled task RegistrationInfo must contain exactly one %s", name)
		}
	}
	for _, name := range []string{"Author", "Date"} {
		if registrationFields[name] > 1 {
			return fmt.Errorf("scheduled task RegistrationInfo contains duplicate %s", name)
		}
	}
	return nil
}

func parseTaskXMLTree(data []byte) (taskXMLNode, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	budget := taskXMLBudget{}
	for {
		token, err := decoder.Token()
		if err != nil {
			return taskXMLNode{}, err
		}
		switch element := token.(type) {
		case xml.StartElement:
			root, err := readTaskXMLNode(decoder, element, 1, &budget)
			if err != nil {
				return taskXMLNode{}, err
			}
			for {
				token, err := decoder.Token()
				if err == io.EOF {
					return root, nil
				}
				if err != nil {
					return taskXMLNode{}, err
				}
				if text, ok := token.(xml.CharData); !ok || strings.TrimSpace(string(text)) != "" {
					return taskXMLNode{}, errors.New("scheduled task XML has trailing content")
				}
			}
		case xml.CharData:
			if strings.TrimSpace(string(element)) != "" {
				return taskXMLNode{}, errors.New("scheduled task XML has content before Task root")
			}
		default:
			return taskXMLNode{}, errors.New("scheduled task XML has unsupported content before Task root")
		}
	}
}

func readTaskXMLNode(decoder *xml.Decoder, start xml.StartElement, depth int, budget *taskXMLBudget) (taskXMLNode, error) {
	if depth > maxTaskXMLDepth {
		return taskXMLNode{}, errors.New("scheduled task XML exceeds the depth limit")
	}
	budget.nodes++
	if budget.nodes > maxTaskXMLNodes {
		return taskXMLNode{}, errors.New("scheduled task XML exceeds the node limit")
	}
	budget.attributes += len(start.Attr)
	if budget.attributes > maxTaskXMLAttributes {
		return taskXMLNode{}, errors.New("scheduled task XML exceeds the attribute limit")
	}
	node := taskXMLNode{Space: start.Name.Space, Local: start.Name.Local}
	for _, attribute := range start.Attr {
		node.Attributes = append(node.Attributes, taskXMLAttribute{
			Space: attribute.Name.Space,
			Local: attribute.Name.Local,
			Value: attribute.Value,
		})
	}
	sort.Slice(node.Attributes, func(i, j int) bool {
		left := node.Attributes[i]
		right := node.Attributes[j]
		if left.Space != right.Space {
			return left.Space < right.Space
		}
		if left.Local != right.Local {
			return left.Local < right.Local
		}
		return left.Value < right.Value
	})
	var text strings.Builder
	for {
		token, err := decoder.Token()
		if err != nil {
			return taskXMLNode{}, err
		}
		switch element := token.(type) {
		case xml.StartElement:
			child, err := readTaskXMLNode(decoder, element, depth+1, budget)
			if err != nil {
				return taskXMLNode{}, err
			}
			node.Children = append(node.Children, child)
		case xml.CharData:
			text.Write(element)
		case xml.EndElement:
			if element.Name != start.Name {
				return taskXMLNode{}, errors.New("scheduled task XML has mismatched elements")
			}
			if len(node.Children) != 0 {
				if strings.TrimSpace(text.String()) != "" {
					return taskXMLNode{}, errors.New("scheduled task XML has mixed element content")
				}
			} else {
				node.Text = text.String()
			}
			return node, nil
		default:
			return taskXMLNode{}, errors.New("scheduled task XML contains unsupported nodes")
		}
	}
}

func uniqueTaskChild(parent taskXMLNode, name string) (taskXMLNode, error) {
	var matched *taskXMLNode
	for i := range parent.Children {
		child := &parent.Children[i]
		if child.Space != taskXMLNamespace || child.Local != name {
			continue
		}
		if matched != nil {
			return taskXMLNode{}, fmt.Errorf("scheduled task XML contains duplicate %s", name)
		}
		matched = child
	}
	if matched == nil {
		return taskXMLNode{}, fmt.Errorf("scheduled task XML is missing %s", name)
	}
	return *matched, nil
}

func uniqueTaskLeaf(parent taskXMLNode, name string) (string, error) {
	child, err := uniqueTaskChild(parent, name)
	if err != nil {
		return "", err
	}
	if len(child.Attributes) != 0 || len(child.Children) != 0 {
		return "", fmt.Errorf("scheduled task XML %s is not a scalar", name)
	}
	return child.Text, nil
}

func normalizeTaskXML(data []byte) ([]byte, error) {
	var text string
	switch {
	case bytes.HasPrefix(data, []byte{0xff, 0xfe}):
		decoded, err := decodeUTF16(data[2:], binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		text = decoded
	case bytes.HasPrefix(data, []byte{0xfe, 0xff}):
		decoded, err := decodeUTF16(data[2:], binary.BigEndian)
		if err != nil {
			return nil, err
		}
		text = decoded
	case len(data) >= 2 && data[0] == '<' && data[1] == 0:
		decoded, err := decodeUTF16(data, binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		text = decoded
	case len(data) >= 2 && data[0] == 0 && data[1] == '<':
		decoded, err := decodeUTF16(data, binary.BigEndian)
		if err != nil {
			return nil, err
		}
		text = decoded
	default:
		data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
		if !utf8.Valid(data) {
			return nil, errors.New("scheduled task XML has unsupported text encoding")
		}
		text = string(data)
	}
	trimmed := strings.TrimSpace(text)
	if strings.HasPrefix(trimmed, "<?xml") {
		end := strings.Index(trimmed, "?>")
		if end < 0 {
			return nil, errors.New("scheduled task XML declaration is incomplete")
		}
		trimmed = strings.TrimSpace(trimmed[end+2:])
	}
	return []byte(trimmed), nil
}

func decodeUTF16(data []byte, order binary.ByteOrder) (string, error) {
	if len(data)%2 != 0 {
		return "", errors.New("scheduled task UTF-16 XML has odd byte length")
	}
	units := make([]uint16, len(data)/2)
	for i := range units {
		units[i] = order.Uint16(data[i*2:])
	}
	for i := 0; i < len(units); i++ {
		unit := units[i]
		switch {
		case unit >= 0xd800 && unit <= 0xdbff:
			if i+1 >= len(units) || units[i+1] < 0xdc00 || units[i+1] > 0xdfff {
				return "", errors.New("scheduled task UTF-16 XML has an unpaired surrogate")
			}
			i++
		case unit >= 0xdc00 && unit <= 0xdfff:
			return "", errors.New("scheduled task UTF-16 XML has an unpaired surrogate")
		}
	}
	return string(utf16.Decode(units)), nil
}
