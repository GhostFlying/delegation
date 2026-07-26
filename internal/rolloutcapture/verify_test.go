package rolloutcapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestVerifyCompressedSegment(t *testing.T) {
	raw := []byte("{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\"}}\n")
	digest := sha256.Sum256(raw)
	for _, checksum := range []bool{true, false} {
		compressed := encodeRollout(t, raw, zstd.WithEncoderCRC(checksum))
		source := &eofRecordingReader{reader: bytes.NewReader(compressed)}
		if err := VerifyCompressedSegment(
			context.Background(), source, int64(len(raw)), hex.EncodeToString(digest[:]),
		); err != nil {
			t.Fatal(err)
		}
		if !source.sawEOF {
			t.Fatal("rollout verifier did not consume the compressed source to EOF")
		}
	}
}

func TestVerifyCompressedSegmentRejectsExtraFramesAndBytes(t *testing.T) {
	raw := []byte("rollout\n")
	compressed := encodeRollout(t, raw)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])
	skippable := make([]byte, 8)
	copy(skippable, []byte{0x50, 0x2a, 0x4d, 0x18})
	binary.LittleEndian.PutUint32(skippable[4:], 0)
	tests := []struct {
		name string
		data []byte
	}{
		{name: "concatenated", data: append(append([]byte{}, compressed...), compressed...)},
		{name: "trailing byte", data: append(append([]byte{}, compressed...), 0)},
		{name: "leading skippable", data: append(skippable, compressed...)},
		{name: "skippable only", data: skippable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := VerifyCompressedSegment(
				context.Background(), bytes.NewReader(test.data), int64(len(raw)), digestText,
			)
			if !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("error = %v, want invalid frame", err)
			}
		})
	}
}

func TestVerifyCompressedSegmentRejectsTruncatedAndCorruptFrames(t *testing.T) {
	raw := []byte("rollout\n")
	compressed := encodeRollout(t, raw)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])
	corrupt := append([]byte{}, compressed...)
	corrupt[len(corrupt)-1] ^= 0xff
	tests := []struct {
		name string
		data []byte
	}{
		{name: "header", data: compressed[:3]},
		{name: "block or checksum", data: compressed[:len(compressed)-1]},
		{name: "checksum", data: corrupt},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := VerifyCompressedSegment(
				context.Background(), bytes.NewReader(test.data), int64(len(raw)), digestText,
			)
			if err == nil {
				t.Fatal("VerifyCompressedSegment accepted corrupt data")
			}
		})
	}
}

func TestVerifyCompressedSegmentEnforcesCompressedAndRawLimits(t *testing.T) {
	raw := bytes.Repeat([]byte("a"), 2048)
	compressed := encodeRollout(t, raw)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])
	limits := testVerifierLimits()
	limits.compressedBytes = int64(len(compressed) - 1)
	if err := verifyCompressedSegment(
		context.Background(), bytes.NewReader(compressed), 1, digestText, limits,
	); !errors.Is(err, ErrCompressedTooLarge) {
		t.Fatalf("compressed limit error = %v", err)
	}

	limits = testVerifierLimits()
	limits.rawBytes = int64(len(raw) - 1)
	if err := verifyCompressedSegment(
		context.Background(), bytes.NewReader(compressed), limits.rawBytes, digestText, limits,
	); !errors.Is(err, ErrRawTooLarge) {
		t.Fatalf("raw limit error = %v", err)
	}
}

func TestVerifyCompressedSegmentEnforcesDecoderWindowAndMemory(t *testing.T) {
	raw := bytes.Repeat([]byte("window"), 64*1024)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])

	windowFrame := encodeRollout(
		t, raw, zstd.WithWindowSize(1<<20), zstd.WithSingleSegment(false),
	)
	limits := testVerifierLimits()
	limits.rawBytes = int64(len(raw))
	limits.decoderMemory = uint64(len(raw) * 2)
	limits.decoderWindow = 64 * 1024
	if err := verifyCompressedSegment(
		context.Background(), bytes.NewReader(windowFrame), int64(len(raw)), digestText, limits,
	); !errors.Is(err, zstd.ErrWindowSizeExceeded) {
		t.Fatalf("window limit error = %v", err)
	}

	memoryFrame := encodeRollout(t, raw, zstd.WithSingleSegment(true))
	limits.decoderWindow = 1 << 20
	limits.decoderMemory = 64 * 1024
	if err := verifyCompressedSegment(
		context.Background(), bytes.NewReader(memoryFrame), int64(len(raw)), digestText, limits,
	); !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Fatalf("memory limit error = %v", err)
	}
}

func TestVerifyCompressedSegmentChecksRawDescriptor(t *testing.T) {
	raw := []byte("rollout\n")
	compressed := encodeRollout(t, raw)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])
	tests := []struct {
		name   string
		size   int64
		digest string
	}{
		{name: "size mismatch", size: int64(len(raw) - 1), digest: digestText},
		{name: "digest mismatch", size: int64(len(raw)), digest: strings.Repeat("0", 64)},
		{name: "uppercase digest", size: int64(len(raw)), digest: strings.ToUpper(digestText)},
		{name: "short digest", size: int64(len(raw)), digest: digestText[:63]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := VerifyCompressedSegment(
				context.Background(), bytes.NewReader(compressed), test.size, test.digest,
			)
			if !errors.Is(err, ErrDescriptorMismatch) {
				t.Fatalf("descriptor error = %v", err)
			}
		})
	}
}

func TestVerifyCompressedSegmentPropagatesReadAndContextErrors(t *testing.T) {
	raw := []byte("rollout\n")
	compressed := encodeRollout(t, raw)
	digest := sha256.Sum256(raw)
	digestText := hex.EncodeToString(digest[:])
	wantErr := errors.New("read failed")
	if err := VerifyCompressedSegment(
		context.Background(), &terminalErrorReader{data: compressed, err: wantErr},
		int64(len(raw)), digestText,
	); !errors.Is(err, wantErr) {
		t.Fatalf("source error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := VerifyCompressedSegment(
		canceled, bytes.NewReader(compressed), int64(len(raw)), digestText,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("context error = %v", err)
	}
}

func encodeRollout(t *testing.T, raw []byte, options ...zstd.EOption) []byte {
	t.Helper()
	encoderOptions := []zstd.EOption{
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
		zstd.WithWindowSize(1 << 10),
		zstd.WithSingleSegment(false),
	}
	encoderOptions = append(encoderOptions, options...)
	encoder, err := zstd.NewWriter(nil, encoderOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(raw, nil)
}

func testVerifierLimits() verifierLimits {
	return verifierLimits{
		compressedBytes: 1 << 20,
		rawBytes:        1 << 20,
		decoderMemory:   1 << 20,
		decoderWindow:   1 << 20,
	}
}

type eofRecordingReader struct {
	reader *bytes.Reader
	sawEOF bool
}

func (r *eofRecordingReader) Read(destination []byte) (int, error) {
	count, err := r.reader.Read(destination)
	if errors.Is(err, io.EOF) {
		r.sawEOF = true
	}
	return count, err
}

type terminalErrorReader struct {
	data []byte
	err  error
}

func (r *terminalErrorReader) Read(destination []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	count := copy(destination, r.data)
	r.data = r.data[count:]
	return count, nil
}
