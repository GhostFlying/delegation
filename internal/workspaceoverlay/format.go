package workspaceoverlay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	FormatVersion                   = 1
	MaximumEntries                  = 32 * 1024
	MaximumPayloads                 = 32 * 1024
	MaximumPathBytes                = 4 * 1024
	MaximumPathComponentBytes       = 255
	MaximumTotalPathBytes           = 8 * 1024 * 1024
	MaximumManifestBytes      int64 = 16 * 1024 * 1024
	MaximumPayloadBytes       int64 = 128 * 1024 * 1024
	MaximumPayloadTotal       int64 = 256 * 1024 * 1024
	MaximumExpandedBytes      int64 = 256 * 1024 * 1024
	MaximumDecodedBytes       int64 = MaximumManifestBytes + MaximumPayloadTotal + 32*1024*1024
)

const zeroSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"

const (
	emptyBlobSHA1   = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
	emptyBlobSHA256 = "473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813"
)

type NodeKind string

const (
	NodeAbsent  NodeKind = "absent"
	NodeFile    NodeKind = "file"
	NodeSymlink NodeKind = "symlink"
)

type Payload struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type IndexState struct {
	Mode          string `json:"mode"`
	OID           string `json:"oid"`
	PayloadSHA256 string `json:"payloadSha256,omitempty"`
	IntentToAdd   bool   `json:"intentToAdd,omitempty"`
}

type WorktreeState struct {
	Kind          NodeKind `json:"kind"`
	Mode          string   `json:"mode,omitempty"`
	PayloadSHA256 string   `json:"payloadSha256,omitempty"`
}

type Entry struct {
	Path     string        `json:"path"`
	Index    *IndexState   `json:"index,omitempty"`
	Worktree WorktreeState `json:"worktree"`
}

type Manifest struct {
	Version            int       `json:"version"`
	HeadOID            string    `json:"headOid"`
	ObjectFormat       string    `json:"objectFormat"`
	WorkingDirectory   string    `json:"workingDirectory"`
	SourceSnapshotHash string    `json:"sourceSnapshotHash"`
	Entries            []Entry   `json:"entries"`
	Payloads           []Payload `json:"payloads"`
}

func NewManifest(
	headOID, objectFormat, workingDirectory string,
	entries []Entry,
	payloads []Payload,
) (Manifest, error) {
	manifest := Manifest{
		Version: FormatVersion, HeadOID: headOID, ObjectFormat: objectFormat,
		WorkingDirectory: workingDirectory, SourceSnapshotHash: zeroSHA256,
		Entries: slices.Clone(entries), Payloads: slices.Clone(payloads),
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	digest, err := manifest.semanticHash()
	if err != nil {
		return Manifest{}, err
	}
	manifest.SourceSnapshotHash = digest
	return manifest, manifest.Validate()
}

func (m Manifest) Validate() error {
	if m.Version != FormatVersion {
		return fmt.Errorf("unsupported workspace overlay version %d", m.Version)
	}
	if err := validateObjectID(m.HeadOID, m.ObjectFormat); err != nil {
		return err
	}
	if m.WorkingDirectory != "" {
		if err := ValidatePath(m.WorkingDirectory); err != nil {
			return fmt.Errorf("working directory: %w", err)
		}
	}
	if !validSHA256(m.SourceSnapshotHash) {
		return errors.New("source snapshot hash must be a lowercase SHA-256 digest")
	}
	if len(m.Entries) > MaximumEntries {
		return fmt.Errorf("workspace overlay entries exceed limit of %d", MaximumEntries)
	}
	if len(m.Payloads) > MaximumPayloads {
		return fmt.Errorf("workspace overlay payloads exceed limit of %d", MaximumPayloads)
	}
	payloads := make(map[string]Payload, len(m.Payloads))
	var payloadBytes int64
	previousDigest := ""
	for _, payload := range m.Payloads {
		if !validSHA256(payload.SHA256) || payload.SHA256 <= previousDigest {
			return errors.New("workspace overlay payloads must be sorted unique SHA-256 digests")
		}
		if payload.Size < 0 || payload.Size > MaximumPayloadBytes ||
			payloadBytes > MaximumPayloadTotal-payload.Size {
			return errors.New("workspace overlay payload sizes exceed their configured limits")
		}
		payloadBytes += payload.Size
		payloads[payload.SHA256] = payload
		previousDigest = payload.SHA256
	}
	var pathBytes int
	var expandedBytes int64
	previousPath := ""
	portablePaths := make(map[string]string, len(m.Entries))
	indexPaths := make(map[string]string, len(m.Entries))
	worktreePaths := make(map[string]string, len(m.Entries))
	referencedPayloads := make(map[string]struct{}, len(m.Payloads))
	for _, entry := range m.Entries {
		if err := ValidatePath(entry.Path); err != nil {
			return fmt.Errorf("overlay path %q: %w", entry.Path, err)
		}
		if entry.Path <= previousPath {
			return errors.New("workspace overlay entries must be sorted by unique path")
		}
		pathBytes += len(entry.Path)
		if pathBytes > MaximumTotalPathBytes {
			return fmt.Errorf("workspace overlay paths exceed %d bytes", MaximumTotalPathBytes)
		}
		if err := validateEntry(entry, m.ObjectFormat, payloads); err != nil {
			return fmt.Errorf("overlay path %q: %w", entry.Path, err)
		}
		portable := portablePathKey(entry.Path)
		if entry.Index != nil || entry.Worktree.Kind != NodeAbsent {
			if prior, exists := portablePaths[portable]; exists {
				return fmt.Errorf("active paths %q and %q collide on a supported target", prior, entry.Path)
			}
			portablePaths[portable] = entry.Path
		}
		if entry.Index != nil {
			indexPaths[portable] = entry.Path
		}
		if entry.Worktree.Kind != NodeAbsent {
			worktreePaths[portable] = entry.Path
		}
		for _, digest := range entryPayloadReferences(entry) {
			payload := payloads[digest]
			if expandedBytes > MaximumExpandedBytes-payload.Size {
				return fmt.Errorf("workspace overlay expanded payload references exceed %d bytes", MaximumExpandedBytes)
			}
			expandedBytes += payload.Size
			referencedPayloads[digest] = struct{}{}
		}
		previousPath = entry.Path
	}
	if len(referencedPayloads) != len(payloads) {
		return errors.New("workspace overlay contains an unreferenced payload")
	}
	if err := validateActivePathPrefixes("index", indexPaths); err != nil {
		return err
	}
	if err := validateActivePathPrefixes("worktree", worktreePaths); err != nil {
		return err
	}
	return nil
}

func (m Manifest) VerifySemanticHash() error {
	if err := m.Validate(); err != nil {
		return err
	}
	digest, err := m.semanticHash()
	if err != nil {
		return err
	}
	if digest != m.SourceSnapshotHash {
		return errors.New("workspace overlay semantic hash does not match its contents")
	}
	return nil
}

func (m Manifest) CanonicalJSON() ([]byte, error) {
	if err := m.VerifySemanticHash(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func (m Manifest) semanticHash() (string, error) {
	m.SourceSnapshotHash = zeroSHA256
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func ValidatePath(value string) error {
	if value == "" || len(value) > MaximumPathBytes || !utf8.ValidString(value) ||
		path.IsAbs(value) || path.Clean(value) != value || strings.ContainsAny(value, "\\:\x00<>\"|?*") {
		return errors.New("path must be bounded, normalized, relative UTF-8 text")
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return errors.New("path must not contain control characters")
		}
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." ||
			len(component) > MaximumPathComponentBytes {
			return errors.New("path contains an invalid component")
		}
		if component != norm.NFC.String(component) {
			return errors.New("path components must use NFC normalization")
		}
		if aliasesGitAdminDirectory(component) {
			return errors.New("path must not address Git administrative data")
		}
		if strings.HasSuffix(component, ".") || strings.HasSuffix(component, " ") ||
			windowsReservedComponent(component) {
			return errors.New("path is not portable to Windows")
		}
	}
	return nil
}

func aliasesGitAdminDirectory(component string) bool {
	if asciiEqualFold(component, ".git") || asciiEqualFold(component, "git~1") {
		return true
	}
	var filtered strings.Builder
	filtered.Grow(len(component))
	for _, character := range component {
		if !hfsIgnoredCodepoint(character) {
			filtered.WriteRune(character)
		}
	}
	return asciiEqualFold(filtered.String(), ".git")
}

func asciiEqualFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range len(left) {
		leftByte := left[index]
		rightByte := right[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}
	return true
}

func hfsIgnoredCodepoint(character rune) bool {
	switch character {
	case '\u200c', '\u200d', '\u200e', '\u200f',
		'\u202a', '\u202b', '\u202c', '\u202d', '\u202e',
		'\u206a', '\u206b', '\u206c', '\u206d', '\u206e', '\u206f',
		'\ufeff':
		return true
	default:
		return false
	}
}

func PortablePathKey(value string) (string, error) {
	if err := ValidatePath(value); err != nil {
		return "", err
	}
	return portablePathKey(value), nil
}

func portablePathKey(value string) string {
	return norm.NFC.String(cases.Fold().String(value))
}

func windowsReservedComponent(component string) bool {
	base := component
	if before, _, found := strings.Cut(base, "."); found {
		base = before
	}
	base = strings.ToLower(strings.TrimRight(base, " ."))
	if base == "con" || base == "prn" || base == "aux" || base == "nul" {
		return true
	}
	if base == "conin$" || base == "conout$" {
		return true
	}
	runes := []rune(base)
	if len(runes) == 4 && (string(runes[:3]) == "com" || string(runes[:3]) == "lpt") {
		return runes[3] >= '1' && runes[3] <= '9' || runes[3] == '¹' || runes[3] == '²' || runes[3] == '³'
	}
	return false
}

func entryPayloadReferences(entry Entry) []string {
	references := make([]string, 0, 2)
	if entry.Index != nil && entry.Index.PayloadSHA256 != "" {
		references = append(references, entry.Index.PayloadSHA256)
	}
	if entry.Worktree.PayloadSHA256 != "" {
		references = append(references, entry.Worktree.PayloadSHA256)
	}
	return references
}

func validateActivePathPrefixes(namespace string, active map[string]string) error {
	for portable, original := range active {
		ancestor := portable
		for {
			separator := strings.LastIndexByte(ancestor, '/')
			if separator < 0 {
				break
			}
			ancestor = ancestor[:separator]
			if prior, exists := active[ancestor]; exists {
				return fmt.Errorf("%s paths %q and %q have an active file ancestor conflict", namespace, prior, original)
			}
		}
	}
	return nil
}

func validateEntry(entry Entry, objectFormat string, payloads map[string]Payload) error {
	if entry.Index != nil {
		if err := validateIndexState(*entry.Index, objectFormat, payloads); err != nil {
			return err
		}
	}
	switch entry.Worktree.Kind {
	case NodeAbsent:
		if entry.Worktree.Mode != "" || entry.Worktree.PayloadSHA256 != "" {
			return errors.New("absent worktree state must not contain mode or payload")
		}
	case NodeFile:
		if entry.Worktree.Mode != "100644" && entry.Worktree.Mode != "100755" {
			return errors.New("worktree file mode must be 100644 or 100755")
		}
		if _, exists := payloads[entry.Worktree.PayloadSHA256]; !exists {
			return errors.New("worktree file references an unknown payload")
		}
	case NodeSymlink:
		if entry.Worktree.Mode != "120000" {
			return errors.New("worktree symlink mode must be 120000")
		}
		payload, exists := payloads[entry.Worktree.PayloadSHA256]
		if !exists {
			return errors.New("worktree symlink references an unknown payload")
		}
		if payload.Size < 1 || payload.Size > MaximumPathBytes {
			return errors.New("worktree symlink payload must contain a bounded target")
		}
	default:
		return fmt.Errorf("unsupported worktree node kind %q", entry.Worktree.Kind)
	}
	return nil
}

func validateIndexState(state IndexState, objectFormat string, payloads map[string]Payload) error {
	if state.Mode != "100644" && state.Mode != "100755" && state.Mode != "120000" {
		return fmt.Errorf("unsupported index mode %q", state.Mode)
	}
	if err := validateObjectID(state.OID, objectFormat); err != nil {
		return err
	}
	if state.IntentToAdd {
		emptyBlob := emptyBlobSHA1
		if objectFormat == "sha256" {
			emptyBlob = emptyBlobSHA256
		}
		if state.PayloadSHA256 != "" || state.OID != emptyBlob {
			return errors.New("intent-to-add index state must use Git's exact empty-blob object ID")
		}
		return nil
	}
	if state.OID == strings.Repeat("0", len(state.OID)) {
		return errors.New("ordinary index state must not use an empty object ID")
	}
	if state.PayloadSHA256 != "" {
		payload, exists := payloads[state.PayloadSHA256]
		if !exists {
			return errors.New("index state references an unknown payload")
		}
		if state.Mode == "120000" && (payload.Size < 1 || payload.Size > MaximumPathBytes) {
			return errors.New("index symlink payload must contain a bounded target")
		}
	}
	return nil
}

func validateObjectID(oid, objectFormat string) error {
	want := 0
	switch objectFormat {
	case "sha1":
		want = 40
	case "sha256":
		want = 64
	default:
		return fmt.Errorf("unsupported Git object format %q", objectFormat)
	}
	if len(oid) != want {
		return fmt.Errorf("Git object ID does not match %s", objectFormat)
	}
	for _, character := range oid {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return errors.New("Git object ID must contain lowercase hexadecimal characters")
		}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
