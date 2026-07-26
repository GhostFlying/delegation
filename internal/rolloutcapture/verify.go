package rolloutcapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	MaximumCompressedBytes   = int64(64 * 1024 * 1024)
	maximumDecoderWindowSize = uint64(8 * 1024 * 1024)
)

var (
	ErrCompressedTooLarge = errors.New("compressed managed rollout exceeds its byte limit")
	ErrRawTooLarge        = errors.New("decoded managed rollout exceeds its byte limit")
	ErrInvalidFrame       = errors.New("managed rollout is not exactly one zstd frame")
	ErrDescriptorMismatch = errors.New("managed rollout does not match its raw descriptor")
)

type verifierLimits struct {
	compressedBytes int64
	rawBytes        int64
	decoderMemory   uint64
	decoderWindow   uint64
}

// VerifyCompressedSegment validates a root-inbox rollout component without
// retaining decoded rollout bytes. It accepts exactly one ordinary zstd frame.
func VerifyCompressedSegment(
	ctx context.Context,
	source io.Reader,
	expectedRawBytes int64,
	expectedRawSHA256 string,
) error {
	return verifyCompressedSegment(
		ctx,
		source,
		expectedRawBytes,
		expectedRawSHA256,
		verifierLimits{
			compressedBytes: MaximumCompressedBytes,
			rawBytes:        MaximumRawBytes,
			decoderMemory:   uint64(MaximumRawBytes),
			decoderWindow:   maximumDecoderWindowSize,
		},
	)
}

func verifyCompressedSegment(
	ctx context.Context,
	source io.Reader,
	expectedRawBytes int64,
	expectedRawSHA256 string,
	limits verifierLimits,
) error {
	if ctx == nil || source == nil {
		return errors.New("rollout verification context and source are required")
	}
	if limits.compressedBytes < 1 || limits.rawBytes < 0 ||
		limits.decoderMemory < 1 || limits.decoderWindow < zstd.MinWindowSize {
		return errors.New("rollout verifier limits are invalid")
	}
	if expectedRawBytes < 0 || expectedRawBytes > limits.rawBytes ||
		!validSHA256(expectedRawSHA256) {
		return ErrDescriptorMismatch
	}

	compressed, err := readCompressed(ctx, source, limits.compressedBytes)
	if err != nil {
		return err
	}
	if err := validateSingleFrame(compressed); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	decoder, err := zstd.NewReader(
		bytes.NewReader(compressed),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(limits.decoderMemory),
		zstd.WithDecoderMaxWindow(limits.decoderWindow),
		zstd.WithDecodeBuffersBelow(0),
	)
	if err != nil {
		return fmt.Errorf("create managed rollout decoder: %w", err)
	}
	defer decoder.Close()

	digest := sha256.New()
	decoded := &io.LimitedReader{R: decoder, N: limits.rawBytes + 1}
	written, err := io.CopyBuffer(
		&contextWriter{ctx: ctx, destination: digest}, decoded, make([]byte, 32*1024),
	)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return fmt.Errorf("%w: decode stream: %w", ErrInvalidFrame, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if written > limits.rawBytes {
		return ErrRawTooLarge
	}
	if written != expectedRawBytes || hex.EncodeToString(digest.Sum(nil)) != expectedRawSHA256 {
		return ErrDescriptorMismatch
	}
	return nil
}

func readCompressed(ctx context.Context, source io.Reader, maximum int64) ([]byte, error) {
	limited := &io.LimitedReader{
		R: &contextReader{ctx: ctx, source: source},
		N: maximum + 1,
	}
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read compressed managed rollout: %w", err)
	}
	if int64(len(data)) > maximum {
		return nil, ErrCompressedTooLarge
	}
	return data, nil
}

func validateSingleFrame(data []byte) error {
	var header zstd.Header
	if err := header.Decode(data); err != nil {
		return fmt.Errorf("%w: decode header: %w", ErrInvalidFrame, err)
	}
	if header.Skippable {
		return fmt.Errorf("%w: skippable frame", ErrInvalidFrame)
	}
	offset := header.HeaderSize
	for {
		if len(data)-offset < 3 {
			return fmt.Errorf("%w: truncated block header", ErrInvalidFrame)
		}
		blockHeader := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16
		offset += 3
		last := blockHeader&1 != 0
		blockType := (blockHeader >> 1) & 3
		payloadBytes := int(blockHeader >> 3)
		switch blockType {
		case 0, 2:
		case 1:
			payloadBytes = 1
		case 3:
			return fmt.Errorf("%w: %w", ErrInvalidFrame, zstd.ErrReservedBlockType)
		}
		if payloadBytes > len(data)-offset {
			return fmt.Errorf("%w: truncated block payload", ErrInvalidFrame)
		}
		offset += payloadBytes
		if last {
			break
		}
	}
	if header.HasCheckSum {
		if len(data)-offset < 4 {
			return fmt.Errorf("%w: truncated checksum", ErrInvalidFrame)
		}
		offset += 4
	}
	if offset != len(data) {
		return fmt.Errorf("%w: trailing or concatenated data", ErrInvalidFrame)
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range []byte(value) {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r *contextReader) Read(destination []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(destination)
}

type contextWriter struct {
	ctx         context.Context
	destination io.Writer
}

func (w *contextWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.destination.Write(data)
}
