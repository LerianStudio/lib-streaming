//go:build unit

package contract

import (
	"errors"
	"strings"
	"testing"
)

func TestOutboxEventTypeForSource_QualifiesTheStableType(t *testing.T) {
	t.Parallel()

	got, err := OutboxEventTypeForSource("svc-a")
	if err != nil {
		t.Fatalf("OutboxEventTypeForSource() error = %v", err)
	}

	if want := "lerian.streaming.publish.svc-a"; got != want {
		t.Fatalf("OutboxEventTypeForSource() = %q; want %q", got, want)
	}
}

func TestOutboxEventTypeForSource_RefusesAnIllegalSource(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		source string
		want   error
	}{
		"empty":            {source: "", want: ErrMissingSource},
		"dotted":           {source: "svc.a", want: ErrInvalidSource},
		"uppercase":        {source: "Svc", want: ErrInvalidSource},
		"one byte too big": {source: strings.Repeat("a", maxSourceSegmentBytes+1), want: ErrInvalidSource},
	}

	for name, tc := range cases {
		got, err := OutboxEventTypeForSource(tc.source)
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: error = %v; want %v", name, err, tc.want)
		}

		if got != "" {
			t.Fatalf("%s: event type = %q; want empty on refusal", name, got)
		}
	}
}

// The longest legal source must still fit lib-commons' event_type column, or
// a source-scoped producer with a long name would fail every outbox write.
func TestOutboxEventTypeForSource_LongestSourceFitsTheEventTypeColumn(t *testing.T) {
	t.Parallel()

	got, err := OutboxEventTypeForSource(strings.Repeat("a", maxSourceSegmentBytes))
	if err != nil {
		t.Fatalf("OutboxEventTypeForSource() error = %v", err)
	}

	if len(got) > MaxOutboxEventTypeBytes {
		t.Fatalf("event type is %d bytes; the column holds %d", len(got), MaxOutboxEventTypeBytes)
	}
}
