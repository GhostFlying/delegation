package rolloutcapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestCaptureCompressedSegmentRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name     string
		terminal string
		outcome  Outcome
	}{
		{name: "completed", terminal: "task_complete", outcome: OutcomeCompleted},
		{name: "aborted", terminal: "turn_aborted", outcome: OutcomeAborted},
	} {
		t.Run(test.name, func(t *testing.T) {
			beforeOffset := "outside saved offset\n"
			preStart := rolloutLine("event_msg", "thread_settings_applied", "")
			segment := rolloutLine("event_msg", "task_started", testThreadID) +
				rolloutLine("response_item", "", "") +
				rolloutLine("event_msg", test.terminal, testThreadID)
			var output bytes.Buffer
			got, err := CaptureCompressedSegment(
				context.Background(),
				strings.NewReader(beforeOffset+preStart+segment),
				int64(len(beforeOffset)),
				testThreadID,
				&output,
			)
			if err != nil {
				t.Fatal(err)
			}
			rawDigest := sha256.Sum256([]byte(segment))
			compressedDigest := sha256.Sum256(output.Bytes())
			want := CompressedSegment{
				Outcome:          test.outcome,
				RawBytes:         int64(len(segment)),
				RawSHA256:        hex.EncodeToString(rawDigest[:]),
				CompressedBytes:  int64(output.Len()),
				CompressedSHA256: hex.EncodeToString(compressedDigest[:]),
			}
			if got != want {
				t.Fatalf("compressed segment = %#v, want %#v", got, want)
			}
			if err := validateSingleFrame(output.Bytes()); err != nil {
				t.Fatalf("compressed output is not one ordinary frame: %v", err)
			}
			if err := VerifyCompressedSegment(
				context.Background(), bytes.NewReader(output.Bytes()), got.RawBytes, got.RawSHA256,
			); err != nil {
				t.Fatalf("verify compressed output: %v", err)
			}
		})
	}
}

func TestCaptureCompressedSegmentEnforcesExactRawAndCompressedLimits(t *testing.T) {
	prefix := rolloutLine("event_msg", "thread_settings_applied", "")
	segment := rolloutSegmentWithSize(t, 4096)
	limits := testCaptureLimits()
	limits.rawBytes = int64(len(segment))

	var baseline bytes.Buffer
	got, err := captureCompressedSegment(
		context.Background(), strings.NewReader(prefix+segment), 0, testThreadID, &baseline, limits,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.RawBytes != limits.rawBytes {
		t.Fatalf("raw bytes = %d, want exact limit %d", got.RawBytes, limits.rawBytes)
	}

	limits.compressedBytes = got.CompressedBytes
	var exact bytes.Buffer
	exactResult, err := captureCompressedSegment(
		context.Background(), strings.NewReader(prefix+segment), 0, testThreadID, &exact, limits,
	)
	if err != nil {
		t.Fatalf("exact compressed limit: %v", err)
	}
	if exactResult.CompressedBytes != limits.compressedBytes {
		t.Fatalf(
			"compressed bytes = %d, want exact limit %d",
			exactResult.CompressedBytes,
			limits.compressedBytes,
		)
	}

	limits.compressedBytes--
	if _, err := captureCompressedSegment(
		context.Background(), strings.NewReader(prefix+segment), 0, testThreadID, io.Discard, limits,
	); !errors.Is(err, ErrCompressedTooLarge) {
		t.Fatalf("compressed overflow error = %v, want %v", err, ErrCompressedTooLarge)
	}

	limits = testCaptureLimits()
	limits.rawBytes = int64(len(segment) - 1)
	if _, err := captureCompressedSegment(
		context.Background(), strings.NewReader(prefix+segment), 0, testThreadID, io.Discard, limits,
	); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("raw overflow error = %v, want %v", err, ErrTooLarge)
	}
}

func TestCaptureCompressedSegmentRejectsTruncatedAndFailedDestinations(t *testing.T) {
	input := rolloutLine("event_msg", "task_started", testThreadID) +
		rolloutLine("event_msg", "task_complete", testThreadID)
	wantErr := errors.New("destination failed")
	for _, test := range []struct {
		name        string
		destination io.Writer
		wantError   error
	}{
		{name: "short write", destination: shortWriter{}, wantError: io.ErrShortWrite},
		{
			name:        "partial error",
			destination: &partialErrorWriter{err: wantErr},
			wantError:   wantErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := CaptureCompressedSegment(
				context.Background(), strings.NewReader(input), 0, testThreadID, test.destination,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("destination error = %v, want %v", err, test.wantError)
			}
			if got != (CompressedSegment{}) {
				t.Fatalf("failed capture returned metadata %#v", got)
			}
		})
	}
}

func TestCaptureCompressedSegmentHonorsContextCancellation(t *testing.T) {
	input := rolloutLine("event_msg", "task_started", testThreadID) +
		rolloutLine("event_msg", "task_complete", testThreadID)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := CaptureCompressedSegment(
		canceled, strings.NewReader(input), 0, testThreadID, io.Discard,
	); !errors.Is(err, context.Canceled) || got != (CompressedSegment{}) {
		t.Fatalf("pre-canceled capture = %#v, error %v", got, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	destination := &cancelingWriter{cancel: cancel}
	if got, err := CaptureCompressedSegment(
		ctx, strings.NewReader(input), 0, testThreadID, destination,
	); !errors.Is(err, context.Canceled) || got != (CompressedSegment{}) {
		t.Fatalf("mid-write canceled capture = %#v, error %v", got, err)
	}
}

func testCaptureLimits() captureLimits {
	return captureLimits{
		rawBytes:        1 << 20,
		preStartBytes:   1 << 10,
		compressedBytes: 1 << 20,
		encoderWindow:   zstdMinimumTestWindow,
	}
}

const zstdMinimumTestWindow = 1024

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return len(data) - 1, nil
}

type partialErrorWriter struct {
	destination bytes.Buffer
	err         error
}

func (w *partialErrorWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, w.err
	}
	count := len(data) / 2
	if count == 0 {
		count = 1
	}
	_, _ = w.destination.Write(data[:count])
	return count, w.err
}

type cancelingWriter struct {
	destination bytes.Buffer
	cancel      context.CancelFunc
}

func (w *cancelingWriter) Write(data []byte) (int, error) {
	w.cancel()
	return w.destination.Write(data)
}
