package rolloutcapture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
)

const maximumEncoderWindowSize = 1024 * 1024

// CompressedSegment contains the metadata needed to describe both the raw
// managed rollout segment and its compressed result-package part.
type CompressedSegment struct {
	Outcome          Outcome
	RawBytes         int64
	RawSHA256        string
	CompressedBytes  int64
	CompressedSHA256 string
}

type captureLimits struct {
	rawBytes        int64
	preStartBytes   int64
	compressedBytes int64
	encoderWindow   int
}

// CaptureCompressedSegment writes the exact managed rollout turn segment as
// one ordinary zstd frame. The caller owns destination cleanup after any
// error; bytes written before an error must not be used as a result-package
// part.
func CaptureCompressedSegment(
	ctx context.Context,
	source io.ReadSeeker,
	offset int64,
	turnID string,
	destination io.Writer,
) (CompressedSegment, error) {
	return captureCompressedSegment(
		ctx,
		source,
		offset,
		turnID,
		destination,
		captureLimits{
			rawBytes:        MaximumRawBytes,
			preStartBytes:   maximumPreStartBytes,
			compressedBytes: MaximumCompressedBytes,
			encoderWindow:   maximumEncoderWindowSize,
		},
	)
}

func captureCompressedSegment(
	ctx context.Context,
	source io.ReadSeeker,
	offset int64,
	turnID string,
	destination io.Writer,
	limits captureLimits,
) (CompressedSegment, error) {
	if ctx == nil || source == nil || destination == nil {
		return CompressedSegment{}, errors.New("rollout context, source, and destination are required")
	}
	if limits.rawBytes < 1 || limits.preStartBytes < 0 || limits.compressedBytes < 1 ||
		limits.encoderWindow < zstd.MinWindowSize ||
		limits.encoderWindow > int(maximumDecoderWindowSize) {
		return CompressedSegment{}, errors.New("compressed rollout capture limits are invalid")
	}

	compressedDigest := sha256.New()
	compressed := &boundedDigestWriter{
		ctx:         ctx,
		destination: destination,
		digest:      compressedDigest,
		maximum:     limits.compressedBytes,
	}
	encoder, err := zstd.NewWriter(
		compressed,
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(limits.encoderWindow),
		zstd.WithLowerEncoderMem(true),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
		zstd.WithSingleSegment(false),
	)
	if err != nil {
		return CompressedSegment{}, fmt.Errorf("create managed rollout encoder: %w", err)
	}

	segment, captureErr := captureSegment(
		ctx,
		source,
		offset,
		turnID,
		encoder,
		segmentLimits{rawBytes: limits.rawBytes, preStartBytes: limits.preStartBytes},
	)
	closeErr := encoder.Close()
	if captureErr != nil {
		if closeErr != nil {
			return CompressedSegment{}, errors.Join(
				captureErr, fmt.Errorf("close managed rollout encoder: %w", closeErr),
			)
		}
		return CompressedSegment{}, captureErr
	}
	if closeErr != nil {
		return CompressedSegment{}, fmt.Errorf("close managed rollout encoder: %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return CompressedSegment{}, err
	}
	return CompressedSegment{
		Outcome:          segment.Outcome,
		RawBytes:         segment.RawBytes,
		RawSHA256:        segment.SHA256,
		CompressedBytes:  compressed.written,
		CompressedSHA256: hex.EncodeToString(compressedDigest.Sum(nil)),
	}, nil
}

type boundedDigestWriter struct {
	ctx         context.Context
	destination io.Writer
	digest      hash.Hash
	maximum     int64
	written     int64
}

func (w *boundedDigestWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	allowed := len(data)
	overflow := int64(allowed) > w.maximum-w.written
	if overflow {
		allowed = int(w.maximum - w.written)
	}
	if allowed == 0 {
		if overflow {
			return 0, ErrCompressedTooLarge
		}
		return 0, nil
	}

	count, err := w.destination.Write(data[:allowed])
	if count < 0 || count > allowed {
		return 0, errors.New("compressed rollout destination returned an invalid write count")
	}
	if count > 0 {
		_, _ = w.digest.Write(data[:count])
		w.written += int64(count)
	}
	if err != nil {
		return count, err
	}
	if count != allowed {
		return count, io.ErrShortWrite
	}
	if overflow {
		return count, ErrCompressedTooLarge
	}
	return count, nil
}
