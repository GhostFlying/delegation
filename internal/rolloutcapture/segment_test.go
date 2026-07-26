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

const otherTurnID = "123e4567-e89b-42d3-a456-426614174801"

func TestCaptureSegmentCopiesExactTurn(t *testing.T) {
	prefix := rolloutLine("event_msg", "task_complete", otherTurnID) +
		rolloutLine("response_item", "", "")
	segment := rolloutLine("event_msg", "task_started", testThreadID) +
		rolloutLine("response_item", "", "") +
		rolloutLine("event_msg", "task_complete", testThreadID)
	source := strings.NewReader(prefix + segment + rolloutLine("response_item", "", ""))
	var output bytes.Buffer
	got, err := CaptureSegment(context.Background(), source, 0, testThreadID, &output)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(segment))
	want := Segment{
		Outcome: OutcomeCompleted, RawBytes: int64(len(segment)), SHA256: hex.EncodeToString(digest[:]),
	}
	if got != want {
		t.Fatalf("segment = %#v, want %#v", got, want)
	}
	if output.String() != segment {
		t.Fatalf("output = %q, want %q", output.String(), segment)
	}
}

func TestCaptureSegmentReportsAbortedTurn(t *testing.T) {
	for _, abortedTurnID := range []string{testThreadID, ""} {
		input := rolloutLine("event_msg", "task_started", testThreadID) +
			rolloutLine("event_msg", "turn_aborted", abortedTurnID)
		var output bytes.Buffer
		got, err := CaptureSegment(context.Background(), strings.NewReader(input), 0, testThreadID, &output)
		if err != nil {
			t.Fatal(err)
		}
		if got.Outcome != OutcomeAborted || output.String() != input {
			t.Fatalf("aborted segment = %#v, output %q", got, output.String())
		}
	}
}

func TestCaptureSegmentReportsFailedTurn(t *testing.T) {
	input := rolloutLine("event_msg", "task_started", testThreadID) +
		failedRolloutLine(testThreadID)
	var output bytes.Buffer
	got, err := CaptureSegment(
		context.Background(), strings.NewReader(input), 0, testThreadID, &output,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != OutcomeFailed || output.String() != input {
		t.Fatalf("failed segment = %#v, output %q", got, output.String())
	}
}

func TestCaptureSegmentRejectsIncompleteAndConflictingTurns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{name: "no start", input: rolloutLine("response_item", "", ""), want: ErrIncomplete},
		{name: "no terminal", input: rolloutLine("event_msg", "task_started", testThreadID), want: ErrIncomplete},
		{name: "partial line", input: strings.TrimSuffix(rolloutLine("event_msg", "task_started", testThreadID), "\n"), want: ErrIncomplete},
		{name: "different start", input: rolloutLine("event_msg", "task_started", otherTurnID), want: ErrConflict},
		{
			name: "nested start",
			input: rolloutLine("event_msg", "task_started", testThreadID) +
				rolloutLine("event_msg", "task_started", otherTurnID),
			want: ErrConflict,
		},
		{
			name: "different terminal",
			input: rolloutLine("event_msg", "task_started", testThreadID) +
				rolloutLine("event_msg", "task_complete", otherTurnID),
			want: ErrConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			_, err := CaptureSegment(
				context.Background(), strings.NewReader(test.input), 0, testThreadID, &output,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCaptureSegmentAcceptsOnlyAbortedTerminalWithoutTurnID(t *testing.T) {
	input := rolloutLine("event_msg", "task_started", testThreadID) +
		"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\"}}\n"
	var output bytes.Buffer
	if _, err := CaptureSegment(
		context.Background(), strings.NewReader(input), 0, testThreadID, &output,
	); err == nil {
		t.Fatal("CaptureSegment accepted a terminal event without a turn ID")
	}
}

func TestCaptureSegmentHonorsOffsetAndContext(t *testing.T) {
	prefix := "ignored\n"
	input := prefix + rolloutLine("event_msg", "task_started", testThreadID) +
		rolloutLine("event_msg", "task_complete", testThreadID)
	var output bytes.Buffer
	if _, err := CaptureSegment(
		context.Background(), strings.NewReader(input), int64(len(prefix)), testThreadID, &output,
	); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CaptureSegment(canceled, strings.NewReader(input), int64(len(prefix)), testThreadID, &output); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestCaptureSegmentRawLimitExcludesPreStartRecords(t *testing.T) {
	prefix := rolloutLine("event_msg", "thread_settings_applied", "")
	for _, test := range []struct {
		name      string
		rawBytes  int64
		wantError error
	}{
		{name: "exact limit", rawBytes: MaximumRawBytes},
		{name: "over limit", rawBytes: MaximumRawBytes + 1, wantError: ErrTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			segment := rolloutSegmentWithSize(t, test.rawBytes)
			var output bytes.Buffer
			got, err := CaptureSegment(
				context.Background(), strings.NewReader(prefix+segment), 0, testThreadID, &output,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("error = %v, want %v", err, test.wantError)
			}
			if test.wantError == nil && (got.RawBytes != MaximumRawBytes || output.String() != segment) {
				t.Fatalf("segment metadata = %#v, output bytes = %d", got, output.Len())
			}
		})
	}
}

func TestCaptureSegmentBoundsPreStartScan(t *testing.T) {
	prefix := "{\"type\":\"response_item\",\"payload\":{\"padding\":\"" +
		strings.Repeat("x", int(maximumPreStartBytes)) + "\"}}\n"
	input := prefix + rolloutLine("event_msg", "task_started", testThreadID)
	if _, err := CaptureSegment(
		context.Background(), strings.NewReader(input), 0, testThreadID, io.Discard,
	); !errors.Is(err, errStartScan) {
		t.Fatalf("error = %v, want %v", err, errStartScan)
	}
}

func rolloutSegmentWithSize(t *testing.T, size int64) string {
	t.Helper()
	start := rolloutLine("event_msg", "task_started", testThreadID)
	terminal := rolloutLine("event_msg", "task_complete", testThreadID)
	prefix := "{\"type\":\"response_item\",\"payload\":{\"padding\":\""
	suffix := "\"}}\n"
	padding := size - int64(len(start)+len(terminal)+len(prefix)+len(suffix))
	if padding < 0 || padding > int64(int(padding)) {
		t.Fatalf("invalid test rollout size %d", size)
	}
	return start + prefix + strings.Repeat("x", int(padding)) + suffix + terminal
}

func rolloutLine(outerType, eventType, turnID string) string {
	if outerType != "event_msg" {
		return "{\"type\":\"" + outerType + "\",\"payload\":{}}\n"
	}
	return "{\"type\":\"event_msg\",\"payload\":{\"type\":\"" + eventType +
		"\",\"turn_id\":\"" + turnID + "\"}}\n"
}

func failedRolloutLine(turnID string) string {
	return "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"" +
		turnID + "\",\"error\":{\"message\":\"managed turn failed\"}}}\n"
}
