package workspaceoverlay

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestArchiveIsDeterministicAndRoundTrips(t *testing.T) {
	payloadData := map[string][]byte{}
	for _, data := range [][]byte{nil, []byte("index\n"), []byte("worktree\x00binary\n")} {
		digest := sha256.Sum256(data)
		payloadData[hex.EncodeToString(digest[:])] = data
	}
	digests := make([]string, 0, len(payloadData))
	for digest := range payloadData {
		digests = append(digests, digest)
	}
	slicesSort(digests)
	payloads := make([]Payload, 0, len(digests))
	for _, digest := range digests {
		payloads = append(payloads, Payload{SHA256: digest, Size: int64(len(payloadData[digest]))})
	}
	manifest, err := NewManifest(
		strings.Repeat("a", 40), "sha1", "nested",
		[]Entry{
			{
				Path: "nested/empty",
				Worktree: WorktreeState{
					Kind: NodeFile, Mode: "100644", PayloadSHA256: digests[0],
				},
			},
			{
				Path: "nested/file", Index: &IndexState{
					Mode: "100644", OID: strings.Repeat("b", 40), PayloadSHA256: digests[1],
				},
				Worktree: WorktreeState{Kind: NodeFile, Mode: "100644", PayloadSHA256: digests[2]},
			},
		},
		payloads,
	)
	if err != nil {
		t.Fatal(err)
	}
	write := func() []byte {
		var archive bytes.Buffer
		err := WriteArchive(context.Background(), &archive, manifest, func(digest string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(payloadData[digest])), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return archive.Bytes()
	}
	first := write()
	second := write()
	if !bytes.Equal(first, second) {
		t.Fatal("identical overlays produced different archive bytes")
	}
	rootPath := t.TempDir()
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	extracted, err := ExtractArchive(context.Background(), bytes.NewReader(first), root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(extracted.Manifest, manifest) {
		t.Fatalf("extracted manifest = %#v, want %#v", extracted.Manifest, manifest)
	}
	for digest, name := range extracted.PayloadNames {
		data, err := root.ReadFile(name)
		if err != nil || !bytes.Equal(data, payloadData[digest]) {
			t.Fatalf("extracted payload %s = %q, %v", digest, data, err)
		}
	}
}

func TestExtractRejectsUnexpectedArchiveEntryMetadata(t *testing.T) {
	manifest, err := NewManifest(strings.Repeat("a", 40), "sha1", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	encoder, err := zstd.NewWriter(&archive, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(encoder)
	if err := writer.WriteHeader(&tar.Header{
		Name: manifestEntryName, Mode: 0o777, Size: int64(len(manifestBytes)),
		ModTime: archiveTimestamp, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(manifestBytes); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := ExtractArchive(context.Background(), bytes.NewReader(archive.Bytes()), root); err == nil {
		t.Fatal("ExtractArchive accepted invalid tar metadata")
	}
}

func TestValidateHeaderRejectsNonCanonicalMetadata(t *testing.T) {
	canonical := func() tar.Header {
		return tar.Header{
			Name: manifestEntryName, Mode: 0o600, Size: 1,
			ModTime: archiveTimestamp, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
		}
	}
	tests := map[string]func(*tar.Header){
		"name":         func(header *tar.Header) { header.Name = "other" },
		"type":         func(header *tar.Header) { header.Typeflag = tar.TypeSymlink },
		"link name":    func(header *tar.Header) { header.Linkname = "target" },
		"mode":         func(header *tar.Header) { header.Mode = 0o644 },
		"size":         func(header *tar.Header) { header.Size = 2 },
		"uid":          func(header *tar.Header) { header.Uid = 1 },
		"gid":          func(header *tar.Header) { header.Gid = 1 },
		"user name":    func(header *tar.Header) { header.Uname = "user" },
		"group name":   func(header *tar.Header) { header.Gname = "group" },
		"device major": func(header *tar.Header) { header.Devmajor = 1 },
		"device minor": func(header *tar.Header) { header.Devminor = 1 },
		"format":       func(header *tar.Header) { header.Format = tar.FormatPAX },
		"modified":     func(header *tar.Header) { header.ModTime = archiveTimestamp.Add(time.Second) },
		"accessed":     func(header *tar.Header) { header.AccessTime = archiveTimestamp },
		"changed":      func(header *tar.Header) { header.ChangeTime = archiveTimestamp },
		"pax records":  func(header *tar.Header) { header.PAXRecords = map[string]string{"key": "value"} },
		"xattrs":       func(header *tar.Header) { header.Xattrs = map[string]string{"key": "value"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			header := canonical()
			mutate(&header)
			if err := validateHeader(&header, manifestEntryName, 1, 1); err == nil {
				t.Fatal("validateHeader accepted non-canonical tar metadata")
			}
		})
	}
	header := canonical()
	header.Size = -1
	if err := validateHeader(&header, manifestEntryName, -1, 1); err == nil {
		t.Fatal("validateHeader accepted a negative tar entry size")
	}
	header = canonical()
	header.Size = 2
	if err := validateHeader(&header, manifestEntryName, 2, 1); err == nil {
		t.Fatal("validateHeader accepted a tar entry over its size limit")
	}
}

func TestExtractRejectsDecodedDataAfterTarTerminator(t *testing.T) {
	manifest, err := NewManifest(strings.Repeat("a", 40), "sha1", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded bytes.Buffer
	writer := tar.NewWriter(&decoded)
	if err := writer.WriteHeader(&tar.Header{
		Name: manifestEntryName, Mode: 0o600, Size: int64(len(manifestBytes)),
		ModTime: archiveTimestamp, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(manifestBytes); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	decoded.WriteString("trailing decoded bytes")
	var archive bytes.Buffer
	encoder, err := zstd.NewWriter(&archive, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(decoded.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := ExtractArchive(context.Background(), bytes.NewReader(archive.Bytes()), root); err == nil ||
		!strings.Contains(err.Error(), "data after the tar terminator") {
		t.Fatalf("ExtractArchive trailing decoded data error = %v", err)
	}
}

func TestExtractRejectsTrailingTarEntry(t *testing.T) {
	manifest, err := NewManifest(strings.Repeat("a", 40), "sha1", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	encoder, err := zstd.NewWriter(&archive, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(encoder)
	for _, entry := range []struct {
		name string
		data []byte
	}{
		{name: manifestEntryName, data: manifestBytes},
		{name: "payload/" + strings.Repeat("f", 64), data: []byte("extra")},
	} {
		if err := writer.WriteHeader(&tar.Header{
			Name: entry.name, Mode: 0o600, Size: int64(len(entry.data)),
			ModTime: archiveTimestamp, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := ExtractArchive(context.Background(), bytes.NewReader(archive.Bytes()), root); err == nil {
		t.Fatal("ExtractArchive accepted an extra tar entry")
	}
}

func TestExtractRejectsNonCanonicalAndUnknownManifestJSON(t *testing.T) {
	manifest, err := NewManifest(strings.Repeat("a", 40), "sha1", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{
		append([]byte(" "), canonical...),
		append([]byte(`{"unknown":true,`), canonical[1:]...),
	} {
		archive := encodeTestArchive(t, []testArchiveEntry{{name: manifestEntryName, data: data}})
		root, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		_, extractErr := ExtractArchive(context.Background(), bytes.NewReader(archive), root)
		closeErr := root.Close()
		if extractErr == nil || closeErr != nil {
			t.Fatalf("ExtractArchive errors = %v, %v", extractErr, closeErr)
		}
	}
}

func TestExtractRejectsPayloadDigestMismatchAndCleansFiles(t *testing.T) {
	payloadData := map[string][]byte{}
	for _, data := range [][]byte{[]byte("first"), []byte("second")} {
		digest := sha256.Sum256(data)
		payloadData[hex.EncodeToString(digest[:])] = data
	}
	digests := make([]string, 0, len(payloadData))
	for digest := range payloadData {
		digests = append(digests, digest)
	}
	slicesSort(digests)
	payloads := make([]Payload, 0, len(digests))
	entries := make([]Entry, 0, len(digests))
	for index, digest := range digests {
		payloads = append(payloads, Payload{SHA256: digest, Size: int64(len(payloadData[digest]))})
		entries = append(entries, Entry{
			Path:     string(rune('a' + index)),
			Worktree: WorktreeState{Kind: NodeFile, Mode: "100644", PayloadSHA256: digest},
		})
	}
	manifest, err := NewManifest(strings.Repeat("a", 40), "sha1", "", entries, payloads)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	archiveEntries := []testArchiveEntry{{name: manifestEntryName, data: manifestData}}
	for index, payload := range manifest.Payloads {
		data := payloadData[payload.SHA256]
		if index == len(manifest.Payloads)-1 {
			data = bytes.Repeat([]byte{'x'}, len(data))
		}
		archiveEntries = append(archiveEntries, testArchiveEntry{
			name: path.Join("payload", payload.SHA256), data: data,
		})
	}
	archive := encodeTestArchive(t, archiveEntries)
	destination := t.TempDir()
	root, err := os.OpenRoot(destination)
	if err != nil {
		t.Fatal(err)
	}
	_, extractErr := ExtractArchive(context.Background(), bytes.NewReader(archive), root)
	closeErr := root.Close()
	if extractErr == nil || closeErr != nil {
		t.Fatalf("ExtractArchive errors = %v, %v", extractErr, closeErr)
	}
	remaining, err := os.ReadDir(destination)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("failed extraction retained files = %v, %v", remaining, err)
	}
}

func TestExtractRejectsOversizedZstdWindow(t *testing.T) {
	payloadData := bytes.Repeat([]byte{0}, 9*1024*1024)
	digest := sha256.Sum256(payloadData)
	digestText := hex.EncodeToString(digest[:])
	manifest, err := NewManifest(
		strings.Repeat("a", 40), "sha1", "",
		[]Entry{{
			Path: "window-fixture",
			Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: digestText,
			},
		}},
		[]Payload{{SHA256: digestText, Size: int64(len(payloadData))}},
	)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	archive := encodeTestArchive(
		t,
		[]testArchiveEntry{
			{name: manifestEntryName, data: manifestData},
			{name: path.Join("payload", digestText), data: payloadData},
		},
		zstd.WithWindowSize(16*1024*1024),
		zstd.WithSingleSegment(false),
	)
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := ExtractArchive(context.Background(), bytes.NewReader(archive), root); !errors.Is(err, zstd.ErrWindowSizeExceeded) {
		t.Fatalf("ExtractArchive oversized window error = %v", err)
	}
}

func TestExtractRejectsHighlyCompressibleDecodedExpansion(t *testing.T) {
	payloadData := bytes.Repeat([]byte{0}, 64*1024)
	digest := sha256.Sum256(payloadData)
	digestText := hex.EncodeToString(digest[:])
	manifest, err := NewManifest(
		strings.Repeat("a", 40), "sha1", "",
		[]Entry{{
			Path: "large-zero-file",
			Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: digestText,
			},
		}},
		[]Payload{{SHA256: digestText, Size: int64(len(payloadData))}},
	)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	archive := encodeTestArchive(t, []testArchiveEntry{
		{name: manifestEntryName, data: manifestData},
		{name: path.Join("payload", digestText), data: payloadData},
	})
	const decodedLimit = 8 * 1024
	if len(archive) >= decodedLimit {
		t.Fatalf("compressed fixture = %d bytes, want less than %d", len(archive), decodedLimit)
	}
	destination := t.TempDir()
	root, err := os.OpenRoot(destination)
	if err != nil {
		t.Fatal(err)
	}
	_, extractErr := extractArchiveWithDecodedLimit(
		context.Background(), bytes.NewReader(archive), root, decodedLimit,
	)
	closeErr := root.Close()
	if extractErr == nil || !strings.Contains(extractErr.Error(), "decoded data exceeds its byte limit") || closeErr != nil {
		t.Fatalf("ExtractArchive decoded expansion errors = %v, %v", extractErr, closeErr)
	}
	remaining, err := os.ReadDir(destination)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("failed expansion retained files = %v, %v", remaining, err)
	}
}

func TestWriteArchiveRejectsPayloadMutation(t *testing.T) {
	want := []byte("expected")
	digest := sha256.Sum256(want)
	payload := Payload{SHA256: hex.EncodeToString(digest[:]), Size: int64(len(want))}
	manifest, err := NewManifest(
		strings.Repeat("a", 40), "sha1", "",
		[]Entry{{
			Path: "file", Worktree: WorktreeState{
				Kind: NodeFile, Mode: "100644", PayloadSHA256: payload.SHA256,
			},
		}},
		[]Payload{payload},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = WriteArchive(context.Background(), io.Discard, manifest, func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("mutated!"))), nil
	})
	if err == nil {
		t.Fatal("WriteArchive accepted changed payload bytes")
	}
}

func slicesSort(values []string) {
	for index := 1; index < len(values); index++ {
		for current := index; current > 0 && values[current] < values[current-1]; current-- {
			values[current], values[current-1] = values[current-1], values[current]
		}
	}
}

type testArchiveEntry struct {
	name string
	data []byte
}

func encodeTestArchive(t *testing.T, entries []testArchiveEntry, options ...zstd.EOption) []byte {
	t.Helper()
	var archive bytes.Buffer
	encoderOptions := append([]zstd.EOption{zstd.WithEncoderConcurrency(1)}, options...)
	encoder, err := zstd.NewWriter(&archive, encoderOptions...)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(encoder)
	for _, entry := range entries {
		if err := writer.WriteHeader(&tar.Header{
			Name: entry.name, Mode: 0o600, Size: int64(len(entry.data)),
			ModTime: archiveTimestamp, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}
