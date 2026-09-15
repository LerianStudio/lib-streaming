//go:build unit

package dlqheader

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestParseRecord_NeverRejects is the posture the whole type exists for.
//
// A DLQ reader's ce-source is the application whose ".dlq" it drains, so a
// parse failure that became a terminal verdict would quarantine the failure back
// onto the topic being drained — a self-feeding loop that grows without bound
// and empties nothing. Every malformed shape below therefore has to yield a
// usable record, not an error.
func TestParseRecord_NeverRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers []kgo.RecordHeader
		want    func(t *testing.T, got DiscardRecord)
	}{
		{
			name:    "no headers at all",
			headers: nil,
			want: func(t *testing.T, got DiscardRecord) {
				if got.SourceTopic != "" || got.CauseKind != "" {
					t.Errorf("got %+v; want every forensic field at its zero value", got)
				}

				if got.EnvelopeError == nil {
					t.Error("EnvelopeError is nil; an absent envelope must be reported, not hidden")
				}
			},
		},
		{
			name: "partition and offset are not numbers",
			headers: []kgo.RecordHeader{
				{Key: SourceTopic, Value: []byte("lerian.streaming.gateway")},
				{Key: SourcePartition, Value: []byte("three")},
				{Key: SourceOffset, Value: []byte("")},
			},
			want: func(t *testing.T, got DiscardRecord) {
				if got.SourceTopic != "lerian.streaming.gateway" {
					t.Errorf("SourceTopic = %q; want the readable half to survive the unreadable half", got.SourceTopic)
				}

				if got.SourcePartition != 0 || got.SourceOffset != 0 {
					t.Errorf("partition/offset = %d/%d; want 0/0", got.SourcePartition, got.SourceOffset)
				}
			},
		},
		{
			name: "partition overflows int32",
			headers: []kgo.RecordHeader{
				{Key: SourcePartition, Value: []byte("99999999999999")},
			},
			want: func(t *testing.T, got DiscardRecord) {
				if got.SourcePartition != 0 {
					t.Errorf("SourcePartition = %d; want 0 rather than a silently wrapped value", got.SourcePartition)
				}
			},
		},
		{
			name: "first-failure stamp is not a timestamp",
			headers: []kgo.RecordHeader{
				{Key: FirstFailureAt, Value: []byte("yesterday")},
			},
			want: func(t *testing.T, got DiscardRecord) {
				if !got.FirstFailureAt.IsZero() {
					t.Errorf("FirstFailureAt = %v; want the zero time", got.FirstFailureAt)
				}
			},
		},
		{
			name: "a cause kind this version does not know travels through",
			headers: []kgo.RecordHeader{
				{Key: CauseKind, Value: []byte("something_new")},
			},
			want: func(t *testing.T, got DiscardRecord) {
				if got.CauseKind != "something_new" {
					t.Errorf("CauseKind = %q; want it verbatim — normalizing an unknown writer's value away loses the only clue", got.CauseKind)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.want(t, ParseRecord(tt.headers, nil))
		})
	}
}

// TestParseRecord_PayloadOmittedIsDistinguishable pins the marker pair a reader
// needs to tell "this payload is genuinely gone" from "I failed to read it".
//
// Without it a reader sees an empty payload and cannot say whether the record
// was published slim on purpose (near the broker's size cap) or whether its own
// parse went wrong — and the two have opposite remediations: fetch the original
// from the origin triple, versus fix the reader.
func TestParseRecord_PayloadOmittedIsDistinguishable(t *testing.T) {
	t.Parallel()

	t.Run("omitted", func(t *testing.T) {
		t.Parallel()

		got := ParseRecord([]kgo.RecordHeader{
			{Key: PayloadOmitted, Value: []byte("true")},
			{Key: PayloadBytes, Value: []byte("1048000")},
		}, nil)

		if !got.PayloadOmitted {
			t.Error("PayloadOmitted = false; want true")
		}

		if got.PayloadBytes != 1048000 {
			t.Errorf("PayloadBytes = %d; want 1048000 — the only trace of how big the dropped payload was", got.PayloadBytes)
		}
	})

	t.Run("present", func(t *testing.T) {
		t.Parallel()

		got := ParseRecord(nil, []byte(`{"ok":true}`))

		if got.PayloadOmitted {
			t.Error("PayloadOmitted = true with the marker absent; want false")
		}

		if string(got.Payload) != `{"ok":true}` {
			t.Errorf("Payload = %q; want it verbatim", got.Payload)
		}
	})
}

// TestParseRecord_ReadsEveryForensicHeader walks the full set once, with values
// chosen so no two fields could be satisfied by the same string.
func TestParseRecord_ReadsEveryForensicHeader(t *testing.T) {
	t.Parallel()

	stamp := "2026-09-15T12:34:56.789Z"

	got := ParseRecord([]kgo.RecordHeader{
		{Key: SourceTopic, Value: []byte("lerian.streaming.gateway")},
		{Key: SourcePartition, Value: []byte("3")},
		{Key: SourceOffset, Value: []byte("42")},
		{Key: CauseKind, Value: []byte(CauseSourceMismatch)},
		{Key: ErrorClass, Value: []byte("validation")},
		{Key: ErrorMessage, Value: []byte("ce-source is not an expected producer")},
		{Key: RetryCount, Value: []byte("2")},
		{Key: FirstFailureAt, Value: []byte(stamp)},
		{Key: ProducerID, Value: []byte("lender-consumer-group")},
	}, []byte(`{}`))

	want, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		t.Fatalf("parse stamp: %v", err)
	}

	checks := []struct {
		field string
		got   any
		want  any
	}{
		{"SourceTopic", got.SourceTopic, "lerian.streaming.gateway"},
		{"SourcePartition", got.SourcePartition, int32(3)},
		{"SourceOffset", got.SourceOffset, int64(42)},
		{"CauseKind", got.CauseKind, CauseSourceMismatch},
		{"ErrorClass", got.ErrorClass, "validation"},
		{"ErrorMessage", got.ErrorMessage, "ce-source is not an expected producer"},
		{"RetryCount", got.RetryCount, 2},
		{"FirstFailureAt", got.FirstFailureAt, want.UTC()},
		{"ProducerID", got.ProducerID, "lender-consumer-group"},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v; want %v", c.field, c.got, c.want)
		}
	}
}

// TestCauseKinds_AreFrozen pins the four values. They are a wire contract: an
// operator alerts on them and a reader buckets by them, so changing one
// silently reclassifies every entry rather than failing anything.
func TestCauseKinds_AreFrozen(t *testing.T) {
	t.Parallel()

	frozen := map[string]string{
		"codec":           CauseCodec,
		"handler":         CauseHandler,
		"source_mismatch": CauseSourceMismatch,
		"unhandled_key":   CauseUnhandledKey,
	}

	for want, got := range frozen {
		if got != want {
			t.Errorf("cause kind = %q; want %q (frozen wire value)", got, want)
		}
	}
}
