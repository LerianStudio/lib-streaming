//go:build unit

package dlqheader

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// TestTruncateErrorMessage_BoundsTheHeaderBudget pins the bound on
// x-lerian-dlq-error-message.
//
// It is the ONE unbounded input on a DLQ record: the sanitized handler error
// string, appended to a copy that already carries the full original payload and
// every original header. A near-cap source record whose handler returns a long
// error produced a DLQ copy strictly larger than the source — which the broker
// rejects, which fails the quarantine, which halts the partition forever.
func TestTruncateErrorMessage_BoundsTheHeaderBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want func(t *testing.T, got string)
	}{
		{
			name: "short message travels verbatim",
			in:   "boom",
			want: func(t *testing.T, got string) {
				if got != "boom" {
					t.Errorf("got %q; want %q", got, "boom")
				}
			},
		},
		{
			name: "exactly at the budget travels verbatim",
			in:   strings.Repeat("a", MaxErrorMessageBytes),
			want: func(t *testing.T, got string) {
				if len(got) != MaxErrorMessageBytes {
					t.Errorf("len = %d; want %d", len(got), MaxErrorMessageBytes)
				}
			},
		},
		{
			name: "over the budget is cut and marked",
			in:   strings.Repeat("a", MaxErrorMessageBytes*3),
			want: func(t *testing.T, got string) {
				if len(got) > MaxErrorMessageBytes {
					t.Errorf("len = %d; want <= %d", len(got), MaxErrorMessageBytes)
				}

				if !strings.Contains(got, "truncated") {
					t.Errorf("got %q; want an explicit truncation marker", got)
				}

				if !strings.Contains(got, fmt.Sprintf("%d", MaxErrorMessageBytes*3)) {
					t.Errorf("got %q; want the original byte count in the marker", got)
				}
			},
		},
		{
			name: "multi-byte runes are not split",
			in:   strings.Repeat("é", MaxErrorMessageBytes),
			want: func(t *testing.T, got string) {
				if len(got) > MaxErrorMessageBytes {
					t.Errorf("len = %d; want <= %d", len(got), MaxErrorMessageBytes)
				}

				if strings.ContainsRune(got, '�') {
					t.Errorf("got %q; want no replacement rune (a split UTF-8 sequence)", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.want(t, TruncateErrorMessage(tt.in))
		})
	}
}

// TestIsSizeError_DetectsEveryTransportsOversizeVerdict pins the detection that
// decides whether a failed DLQ publish is worth one payload-omitted retry.
//
// Getting it wrong in either direction is bad: a missed detection wedges the
// partition, and a false positive drops the payload from a DLQ entry that would
// have fit.
func TestIsSizeError_DetectsEveryTransportsOversizeVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"kafka broker verdict", kerr.MessageTooLarge, true},
		{"kafka client-side verdict, wrapped", fmt.Errorf("produce: %w (uncompressed_bytes=1234)", kerr.MessageTooLarge), true},
		{"library payload cap (sqs / eventbridge)", contract.ErrPayloadTooLarge, true},
		{"library payload cap, wrapped", fmt.Errorf("sqs: %w", contract.ErrPayloadTooLarge), true},
		{"broker unavailable is not a size verdict", errors.New("dial tcp: connection refused"), false},
		{"topic authorization is not a size verdict", kerr.TopicAuthorizationFailed, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsSizeError(tt.err); got != tt.want {
				t.Errorf("IsSizeError(%v) = %v; want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestSlimRetryHeaderValuesFrozen pins the two header keys that mark a DLQ entry
// whose payload was dropped to make it fit. Replay tooling branches on them, so
// they are a wire contract like the rest.
func TestSlimRetryHeaderValuesFrozen(t *testing.T) {
	t.Parallel()

	if PayloadOmitted != "x-lerian-dlq-payload-omitted" {
		t.Errorf("PayloadOmitted = %q; want %q", PayloadOmitted, "x-lerian-dlq-payload-omitted")
	}

	if PayloadBytes != "x-lerian-dlq-payload-bytes" {
		t.Errorf("PayloadBytes = %q; want %q", PayloadBytes, "x-lerian-dlq-payload-bytes")
	}
}
