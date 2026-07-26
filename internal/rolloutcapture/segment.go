package rolloutcapture

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/GhostFlying/delegation/internal/identity"
)

const (
	MaximumRawBytes      = int64(64 * 1024 * 1024)
	maximumPreStartBytes = int64(1024 * 1024)
)

var (
	ErrIncomplete = errors.New("managed rollout turn segment is incomplete")
	ErrConflict   = errors.New("managed rollout turn segment conflicts with the expected turn")
	ErrTooLarge   = errors.New("managed rollout turn segment exceeds its byte limit")
	errStartScan  = errors.New("managed rollout pre-start scan exceeds its byte limit")
)

type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeAborted   Outcome = "aborted"
)

type Segment struct {
	Outcome  Outcome
	RawBytes int64
	SHA256   string
}

type segmentLimits struct {
	rawBytes      int64
	preStartBytes int64
}

// CaptureSegment copies the exact inclusive task_started-to-terminal JSONL
// segment for turnID. The caller owns destination cleanup after any error.
func CaptureSegment(
	ctx context.Context,
	source io.ReadSeeker,
	offset int64,
	turnID string,
	destination io.Writer,
) (Segment, error) {
	return captureSegment(
		ctx,
		source,
		offset,
		turnID,
		destination,
		segmentLimits{rawBytes: MaximumRawBytes, preStartBytes: maximumPreStartBytes},
	)
}

func captureSegment(
	ctx context.Context,
	source io.ReadSeeker,
	offset int64,
	turnID string,
	destination io.Writer,
	limits segmentLimits,
) (Segment, error) {
	if err := identity.ValidateID(turnID); err != nil {
		return Segment{}, fmt.Errorf("turnId %w", err)
	}
	if ctx == nil || source == nil || destination == nil {
		return Segment{}, errors.New("rollout context, source, and destination are required")
	}
	if offset < 0 {
		return Segment{}, errors.New("rollout offset must not be negative")
	}
	if limits.rawBytes < 1 || limits.preStartBytes < 0 ||
		limits.preStartBytes > int64(^uint64(0)>>1)-limits.rawBytes-1 {
		return Segment{}, errors.New("rollout capture limits are invalid")
	}
	position, err := source.Seek(offset, io.SeekStart)
	if err != nil {
		return Segment{}, fmt.Errorf("seek managed rollout: %w", err)
	}
	if position != offset {
		return Segment{}, errors.New("managed rollout seek returned a mismatched offset")
	}

	limited := &io.LimitedReader{R: source, N: limits.preStartBytes + limits.rawBytes + 1}
	reader := bufio.NewReaderSize(limited, 64*1024)
	digest := sha256.New()
	written := int64(0)
	preStartBytes := int64(0)
	started := false
	for {
		if err := ctx.Err(); err != nil {
			return Segment{}, err
		}
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && errors.Is(readErr, io.EOF) {
			return Segment{}, ErrIncomplete
		}
		if len(line) == 0 && readErr != nil {
			return Segment{}, fmt.Errorf("read managed rollout: %w", readErr)
		}
		if line[len(line)-1] != '\n' {
			if limited.N == 0 {
				return Segment{}, ErrTooLarge
			}
			return Segment{}, ErrIncomplete
		}
		event, err := decodeEvent(line[:len(line)-1])
		if err != nil {
			return Segment{}, err
		}
		if !started {
			if event.kind != "task_started" {
				preStartBytes += int64(len(line))
				if preStartBytes > limits.preStartBytes {
					return Segment{}, errStartScan
				}
				continue
			}
			if event.turnID != turnID {
				return Segment{}, ErrConflict
			}
			started = true
		} else if event.kind == "task_started" {
			return Segment{}, ErrConflict
		}
		if started {
			if written > limits.rawBytes-int64(len(line)) {
				return Segment{}, ErrTooLarge
			}
			if _, err := io.MultiWriter(destination, digest).Write(line); err != nil {
				return Segment{}, fmt.Errorf("write managed rollout segment: %w", err)
			}
			written += int64(len(line))
		}
		if started && (event.kind == "task_complete" || event.kind == "turn_aborted") {
			if event.turnID != "" && event.turnID != turnID {
				return Segment{}, ErrConflict
			}
			outcome := OutcomeCompleted
			if event.kind == "turn_aborted" {
				outcome = OutcomeAborted
			}
			return Segment{
				Outcome: outcome, RawBytes: written, SHA256: hex.EncodeToString(digest.Sum(nil)),
			}, nil
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return Segment{}, ErrIncomplete
			}
			return Segment{}, fmt.Errorf("read managed rollout: %w", readErr)
		}
	}
}

type rolloutEvent struct {
	kind   string
	turnID string
}

func decodeEvent(line []byte) (rolloutEvent, error) {
	var envelope struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return rolloutEvent{}, fmt.Errorf("decode managed rollout line: %w", err)
	}
	if envelope.Type != "event_msg" {
		return rolloutEvent{}, nil
	}
	var payload struct {
		Type   string `json:"type"`
		TurnID string `json:"turn_id"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return rolloutEvent{}, fmt.Errorf("decode managed rollout event: %w", err)
	}
	switch payload.Type {
	case "task_started", "task_complete":
		if err := identity.ValidateID(payload.TurnID); err != nil {
			return rolloutEvent{}, fmt.Errorf("managed rollout %s turnId %w", payload.Type, err)
		}
		return rolloutEvent{kind: payload.Type, turnID: payload.TurnID}, nil
	case "turn_aborted":
		if payload.TurnID != "" {
			if err := identity.ValidateID(payload.TurnID); err != nil {
				return rolloutEvent{}, fmt.Errorf("managed rollout %s turnId %w", payload.Type, err)
			}
		}
		return rolloutEvent{kind: payload.Type, turnID: payload.TurnID}, nil
	default:
		return rolloutEvent{}, nil
	}
}
