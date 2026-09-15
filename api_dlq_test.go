//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
)

// noopDiscardHandler is a do-nothing DiscardHandler for builder-construction
// tests.
type noopDiscardHandler struct{}

func (noopDiscardHandler) HandleDiscard(context.Context, streaming.DiscardRecord) error { return nil }

// TestDLQHeaderKeys_AreFrozen pins the exported keys to their literal wire
// values.
//
// The whole point of exporting them is that a consuming service stops restating
// the strings and drifting. They are declared at the facade as literals rather
// than as aliases so the actual string renders on the public documentation page
// — an alias shows a name, and a reader who cannot see the string restates it.
func TestDLQHeaderKeys_AreFrozen(t *testing.T) {
	t.Parallel()

	frozen := map[string]string{
		"x-lerian-dlq-source-topic":     streaming.DLQHeaderSourceTopic,
		"x-lerian-dlq-source-partition": streaming.DLQHeaderSourcePartition,
		"x-lerian-dlq-source-offset":    streaming.DLQHeaderSourceOffset,
		"x-lerian-dlq-cause-kind":       streaming.DLQHeaderCauseKind,
		"x-lerian-dlq-error-class":      streaming.DLQHeaderErrorClass,
		"x-lerian-dlq-error-message":    streaming.DLQHeaderErrorMessage,
		"x-lerian-dlq-retry-count":      streaming.DLQHeaderRetryCount,
		"x-lerian-dlq-first-failure-at": streaming.DLQHeaderFirstFailureAt,
		"x-lerian-dlq-producer-id":      streaming.DLQHeaderProducerID,
		"x-lerian-dlq-payload-omitted":  streaming.DLQHeaderPayloadOmitted,
		"x-lerian-dlq-payload-bytes":    streaming.DLQHeaderPayloadBytes,
		"codec":                         streaming.DLQCauseCodec,
		"handler":                       streaming.DLQCauseHandler,
		"source_mismatch":               streaming.DLQCauseSourceMismatch,
		"unhandled_key":                 streaming.DLQCauseUnhandledKey,
	}

	for want, got := range frozen {
		if got != want {
			t.Errorf("exported constant = %q; want %q", got, want)
		}
	}
}

// TestDLQHeaderConstants_MatchTheWriter is what makes the facade's literals safe
// to declare twice: they are pinned, one by one, to the constants the DLQ
// publisher actually stamps.
//
// Without it the two copies could drift, and a reader would filter on a header
// the library never writes — seeing an empty DLQ forever while the topic fills.
func TestDLQHeaderConstants_MatchTheWriter(t *testing.T) {
	t.Parallel()

	pairs := map[string][2]string{
		"source topic":     {streaming.DLQHeaderSourceTopic, dlqheader.SourceTopic},
		"source partition": {streaming.DLQHeaderSourcePartition, dlqheader.SourcePartition},
		"source offset":    {streaming.DLQHeaderSourceOffset, dlqheader.SourceOffset},
		"cause kind":       {streaming.DLQHeaderCauseKind, dlqheader.CauseKind},
		"error class":      {streaming.DLQHeaderErrorClass, dlqheader.ErrorClass},
		"error message":    {streaming.DLQHeaderErrorMessage, dlqheader.ErrorMessage},
		"retry count":      {streaming.DLQHeaderRetryCount, dlqheader.RetryCount},
		"first failure at": {streaming.DLQHeaderFirstFailureAt, dlqheader.FirstFailureAt},
		"producer id":      {streaming.DLQHeaderProducerID, dlqheader.ProducerID},
		"payload omitted":  {streaming.DLQHeaderPayloadOmitted, dlqheader.PayloadOmitted},
		"payload bytes":    {streaming.DLQHeaderPayloadBytes, dlqheader.PayloadBytes},
		"cause codec":      {streaming.DLQCauseCodec, dlqheader.CauseCodec},
		"cause handler":    {streaming.DLQCauseHandler, dlqheader.CauseHandler},
		"cause mismatch":   {streaming.DLQCauseSourceMismatch, dlqheader.CauseSourceMismatch},
		"cause unhandled":  {streaming.DLQCauseUnhandledKey, dlqheader.CauseUnhandledKey},
	}

	for name, pair := range pairs {
		if pair[0] != pair[1] {
			t.Errorf("%s: facade has %q, the writer stamps %q", name, pair[0], pair[1])
		}
	}
}

// forensicHeaders builds a complete, well-formed DLQ header block.
func forensicHeaders() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: streaming.DLQHeaderSourceTopic, Value: []byte("lerian.streaming.gateway")},
		{Key: streaming.DLQHeaderSourcePartition, Value: []byte("3")},
		{Key: streaming.DLQHeaderSourceOffset, Value: []byte("42")},
		{Key: streaming.DLQHeaderCauseKind, Value: []byte(streaming.DLQCauseSourceMismatch)},
		{Key: streaming.DLQHeaderErrorClass, Value: []byte("validation")},
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte("ce-source is not an expected producer")},
		{Key: streaming.DLQHeaderRetryCount, Value: []byte("2")},
		{Key: streaming.DLQHeaderFirstFailureAt, Value: []byte("2026-09-15T12:34:56.789Z")},
		{Key: streaming.DLQHeaderProducerID, Value: []byte("lender-consumer-group")},
	}
}

// TestParseDiscardRecord_ReadsEveryForensicHeader walks the full set once, with
// values chosen so no two fields could be satisfied by the same string.
//
// The origin coordinates come from the HEADERS, never from the record's own
// topic/partition/offset — those are the DLQ's, not the poison record's.
func TestParseDiscardRecord_ReadsEveryForensicHeader(t *testing.T) {
	t.Parallel()

	got := streaming.ParseDiscardRecord(forensicHeaders(), []byte(`{"loanId":"l-1"}`))

	stamp, err := time.Parse(time.RFC3339Nano, "2026-09-15T12:34:56.789Z")
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
		{"CauseKind", got.CauseKind, streaming.DLQCauseSourceMismatch},
		{"ErrorClass", got.ErrorClass, "validation"},
		{"ErrorMessage", got.ErrorMessage, "ce-source is not an expected producer"},
		{"RetryCount", got.RetryCount, 2},
		{"FirstFailureAt", got.FirstFailureAt, stamp.UTC()},
		{"ProducerID", got.ProducerID, "lender-consumer-group"},
		{"Payload", string(got.Payload), `{"loanId":"l-1"}`},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v; want %v", c.field, c.got, c.want)
		}
	}

	if got.HeaderError != nil {
		t.Errorf("HeaderError = %v; want nil for a well-formed block", got.HeaderError)
	}
}

// TestParseDiscardRecord_DropOneHeaderLosesExactlyThatField is the mutation
// proof: every field must come from its OWN header, and dropping one must not
// disturb the other eight.
//
// The origin triple matters most. A reader that silently fell back to the DLQ
// record's own topic/partition/offset would look correct in every happy-path
// assertion and point an operator at the quarantine queue instead of at the
// poison record — the coordinates are always populated, so nothing would show.
// Asserting only that the dropped field zeroes would pass against a parser with
// cross-contamination, so this asserts the survivors too.
func TestParseDiscardRecord_DropOneHeaderLosesExactlyThatField(t *testing.T) {
	t.Parallel()

	type fields map[string]any

	snapshot := func(r streaming.DiscardRecord) fields {
		return fields{
			streaming.DLQHeaderSourceTopic:     r.SourceTopic,
			streaming.DLQHeaderSourcePartition: r.SourcePartition,
			streaming.DLQHeaderSourceOffset:    r.SourceOffset,
			streaming.DLQHeaderCauseKind:       r.CauseKind,
			streaming.DLQHeaderErrorClass:      r.ErrorClass,
			streaming.DLQHeaderErrorMessage:    r.ErrorMessage,
			streaming.DLQHeaderRetryCount:      r.RetryCount,
			streaming.DLQHeaderFirstFailureAt:  r.FirstFailureAt,
			streaming.DLQHeaderProducerID:      r.ProducerID,
		}
	}

	zero := fields{
		streaming.DLQHeaderSourceTopic:     "",
		streaming.DLQHeaderSourcePartition: int32(0),
		streaming.DLQHeaderSourceOffset:    int64(0),
		streaming.DLQHeaderCauseKind:       "",
		streaming.DLQHeaderErrorClass:      "",
		streaming.DLQHeaderErrorMessage:    "",
		streaming.DLQHeaderRetryCount:      0,
		streaming.DLQHeaderFirstFailureAt:  time.Time{},
		streaming.DLQHeaderProducerID:      "",
	}

	whole := snapshot(streaming.ParseDiscardRecord(forensicHeaders(), nil))

	// Sanity: no field reads as zero with every header present, or the table
	// below would prove nothing.
	for key, value := range whole {
		if value == zero[key] {
			t.Fatalf("%s is zero with every header present; the table would prove nothing", key)
		}
	}

	for _, dropped := range forensicHeaders() {
		t.Run("without "+dropped.Key, func(t *testing.T) {
			t.Parallel()

			got := snapshot(streaming.ParseDiscardRecord(dropHeader(forensicHeaders(), dropped.Key), nil))

			if got[dropped.Key] != zero[dropped.Key] {
				t.Errorf("dropping %s left its field at %v; it is not sourced from its own header",
					dropped.Key, got[dropped.Key])
			}

			for key, value := range got {
				if key == dropped.Key {
					continue
				}

				if value != whole[key] {
					t.Errorf("dropping %s also changed %s: %v -> %v (cross-contamination)",
						dropped.Key, key, whole[key], value)
				}
			}
		})
	}
}

func dropHeader(headers []kgo.RecordHeader, key string) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(headers))

	for _, h := range headers {
		if h.Key != key {
			out = append(out, h)
		}
	}

	return out
}

// TestParseDiscardRecord_MalformedOriginFailsClosedAsAUnit is the guard against
// the worst output this parser could produce: a plausible-looking coordinate
// pointing at the wrong record.
//
// A half-parsed triple is worse than none. "lerian.streaming.gateway/0/42" looks
// exactly like a real coordinate, so an operator follows it to the wrong
// partition; and since the triple is the documented dedup key, a silently
// collapsed partition merges quarantines that are genuinely distinct. Present
// but unreadable therefore discards all three.
func TestParseDiscardRecord_MalformedOriginFailsClosedAsAUnit(t *testing.T) {
	t.Parallel()

	corrupt := func(key, value string) []kgo.RecordHeader {
		out := dropHeader(forensicHeaders(), key)

		return append(out, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	tests := []struct {
		name    string
		headers []kgo.RecordHeader
	}{
		{"partition is not a number", corrupt(streaming.DLQHeaderSourcePartition, "three")},
		{"partition overflows int32", corrupt(streaming.DLQHeaderSourcePartition, "99999999999999")},
		{"partition is negative", corrupt(streaming.DLQHeaderSourcePartition, "-1")},
		{"offset is not a number", corrupt(streaming.DLQHeaderSourceOffset, "")},
		{"offset is negative", corrupt(streaming.DLQHeaderSourceOffset, "-42")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := streaming.ParseDiscardRecord(tt.headers, nil)

			if got.SourceTopic != "" || got.SourcePartition != 0 || got.SourceOffset != 0 {
				t.Errorf("origin = %s/%d/%d; want the whole triple discarded, never a half-parsed coordinate",
					got.SourceTopic, got.SourcePartition, got.SourceOffset)
			}

			if !errors.Is(got.HeaderError, streaming.ErrMalformedOriginCoordinates) {
				t.Errorf("HeaderError = %v; want it to wrap ErrMalformedOriginCoordinates", got.HeaderError)
			}

			// The rest of the forensics still has to arrive: discarding the
			// route back is not a reason to lose why the record died.
			if got.CauseKind != streaming.DLQCauseSourceMismatch || got.ProducerID != "lender-consumer-group" {
				t.Errorf("cause/producer = %q/%q; want the non-origin forensics to survive", got.CauseKind, got.ProducerID)
			}
		})
	}
}

// TestParseDiscardRecord_AbsentCoordinatesAreNotAFailure pins the other half of
// that rule. A PRODUCER-side quarantine legitimately carries an origin topic and
// no partition or offset — the producer quarantines before any broker assigns
// them — so absence must not discard the topic or raise an error.
func TestParseDiscardRecord_AbsentCoordinatesAreNotAFailure(t *testing.T) {
	t.Parallel()

	headers := dropHeader(dropHeader(forensicHeaders(), streaming.DLQHeaderSourcePartition), streaming.DLQHeaderSourceOffset)

	got := streaming.ParseDiscardRecord(headers, nil)

	if got.SourceTopic != "lerian.streaming.gateway" {
		t.Errorf("SourceTopic = %q; want it kept — a producer-side quarantine has no coordinates", got.SourceTopic)
	}

	if got.HeaderError != nil {
		t.Errorf("HeaderError = %v; want nil — absent is not malformed", got.HeaderError)
	}
}

// TestParseDiscardRecord_NeverFails proves the never-reject posture at the
// public surface. A DLQ reader's ce-source is the application whose quarantines
// it drains, so treating an unreadable entry as an error turns the topic's
// normal content into a failure.
func TestParseDiscardRecord_NeverFails(t *testing.T) {
	t.Parallel()

	t.Run("no headers at all", func(t *testing.T) {
		t.Parallel()

		got := streaming.ParseDiscardRecord(nil, nil)

		if got.EnvelopeError == nil {
			t.Error("EnvelopeError is nil; a reader must tell a dead envelope from a single-tenant one")
		}

		if got.SourceTopic != "" {
			t.Errorf("SourceTopic = %q; want empty, the sentinel for no usable triple", got.SourceTopic)
		}
	})

	t.Run("an unknown cause kind travels through verbatim", func(t *testing.T) {
		t.Parallel()

		got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
			{Key: streaming.DLQHeaderCauseKind, Value: []byte("something_new")},
		}, nil)

		if got.CauseKind != "something_new" {
			t.Errorf("CauseKind = %q; normalizing an unknown writer's value away loses the only clue", got.CauseKind)
		}
	})

	nonOrigin := []struct {
		name string
		key  string
		bad  string
	}{
		{"retry count is not a number", streaming.DLQHeaderRetryCount, "many"},
		{"retry count is negative", streaming.DLQHeaderRetryCount, "-1"},
		{"payload bytes is negative", streaming.DLQHeaderPayloadBytes, "-8"},
		{"first-failure stamp is not a timestamp", streaming.DLQHeaderFirstFailureAt, "yesterday"},
	}

	for _, tt := range nonOrigin {
		t.Run(tt.name+" is reported without wrecking the triple", func(t *testing.T) {
			t.Parallel()

			got := streaming.ParseDiscardRecord(append(dropHeader(forensicHeaders(), tt.key),
				kgo.RecordHeader{Key: tt.key, Value: []byte(tt.bad)}), nil)

			if got.HeaderError == nil {
				t.Fatal("HeaderError is nil; a header that failed to parse must be reported")
			}

			if errors.Is(got.HeaderError, streaming.ErrMalformedOriginCoordinates) {
				t.Errorf("HeaderError wraps ErrMalformedOriginCoordinates; %s is not an origin coordinate", tt.key)
			}

			if !strings.Contains(got.HeaderError.Error(), tt.key) {
				t.Errorf("HeaderError = %v; want it to name %s", got.HeaderError, tt.key)
			}

			if got.SourceTopic != "lerian.streaming.gateway" {
				t.Errorf("SourceTopic = %q; a non-origin failure must not discard the route back", got.SourceTopic)
			}
		})
	}
}

// TestParseDiscardRecord_PayloadOmittedIsDistinguishable pins the marker pair a
// reader needs to tell "this payload is genuinely gone" from "I failed to read
// it". The two have opposite remediations: fetch the original from the origin
// triple, versus fix the reader.
func TestParseDiscardRecord_PayloadOmittedIsDistinguishable(t *testing.T) {
	t.Parallel()

	omitted := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderPayloadOmitted, Value: []byte("true")},
		{Key: streaming.DLQHeaderPayloadBytes, Value: []byte("1048000")},
	}, nil)

	if !omitted.PayloadOmitted || omitted.PayloadBytes != 1048000 {
		t.Errorf("omitted=%v bytes=%d; want true/1048000", omitted.PayloadOmitted, omitted.PayloadBytes)
	}

	present := streaming.ParseDiscardRecord(nil, []byte(`{"ok":true}`))

	if present.PayloadOmitted || string(present.Payload) != `{"ok":true}` {
		t.Errorf("omitted=%v payload=%q; want false and the verbatim payload", present.PayloadOmitted, present.Payload)
	}
}

// TestTruncatedErrorMessageBytes_DetectsACutMessage covers the half of the
// truncation contract a READER needs.
//
// The length bound alone does not answer it: the cut output is SHORTER than
// DLQMaxErrorMessageBytes whenever a split multi-byte rune is dropped, so
// comparing lengths silently reports a truncated message as whole.
func TestTruncatedErrorMessageBytes_DetectsACutMessage(t *testing.T) {
	t.Parallel()

	t.Run("a whole message", func(t *testing.T) {
		t.Parallel()

		if _, cut := streaming.TruncatedErrorMessageBytes("loan already settled"); cut {
			t.Error("reported a whole message as truncated")
		}
	})

	t.Run("a cut message reports its original length", func(t *testing.T) {
		t.Parallel()

		// Three rune widths, because the cut lands differently in each: a
		// 2-byte rune divides the budget evenly and the result is exactly the
		// cap, while 3- and 4-byte runes leave a partial rune that is dropped,
		// so the cut message is SHORTER than the cap. That spread is the point —
		// it is why comparing len() to the bound is not a truncation test, and
		// the detector has to work for all three.
		underTheCap := 0

		for _, r := range []string{"é", "€", "\U0001D11E"} {
			original := strings.Repeat(r, streaming.DLQMaxErrorMessageBytes)
			message := dlqheader.TruncateErrorMessage(original)

			size, cut := streaming.TruncatedErrorMessageBytes(message)
			if !cut {
				t.Errorf("rune %q: a %d-byte cut message was not reported as truncated", r, len(message))

				continue
			}

			if size != len(original) {
				t.Errorf("rune %q: original size = %d; want %d", r, size, len(original))
			}

			if len(message) < streaming.DLQMaxErrorMessageBytes {
				underTheCap++
			}
		}

		if underTheCap == 0 {
			t.Errorf("no case landed under the %d-byte cap; the test no longer shows why the bound is not a truncation test",
				streaming.DLQMaxErrorMessageBytes)
		}
	})
}

// TestNewConsumer_DiscardHandlerBuilds proves the seam reaches a real runtime,
// which is the half the exported constants alone never gave a service: reading a
// ".dlq" topic with this library instead of a raw franz-go client.
func TestNewConsumer_DiscardHandlerBuilds(t *testing.T) {
	t.Parallel()

	c, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender-dlq-desk").
		Topics("lerian.streaming.lender.dlq").
		DiscardHandler(noopDiscardHandler{}).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = c.Close() }()

	// A real runtime is not ready until its first poll; the no-op always is.
	if err := c.Healthy(context.Background()); err == nil {
		t.Error("Healthy() = nil; the builder produced a silent no-op instead of a DLQ reader")
	}
}

// TestNewConsumer_DiscardHandlerIsMutuallyExclusive pins the third answer to
// "who selects events" against the other two, in BOTH orders.
//
// Both modes write the same builder field, so the last call used to win
// silently. The dangerous order is DiscardHandler then Handler: the reader is
// demoted to a plain handler while still subscribed to a ".dlq" topic, which
// re-arms the codec-fault quarantine on it — a loop nobody chose and no error
// announced.
func TestNewConsumer_DiscardHandlerIsMutuallyExclusive(t *testing.T) {
	t.Parallel()

	base := func() *streaming.ConsumerBuilder {
		return streaming.NewConsumer().
			Brokers("localhost:9092").
			Group("lender-dlq-desk").
			Source("lender-dlq-desk")
	}

	tests := []struct {
		name  string
		build func() *streaming.ConsumerBuilder
	}{
		{
			"DiscardHandler then Handler",
			func() *streaming.ConsumerBuilder {
				return base().Topics("lerian.streaming.lender.dlq").
					DiscardHandler(noopDiscardHandler{}).Handler(noopHandler{})
			},
		},
		{
			"Handler then DiscardHandler",
			func() *streaming.ConsumerBuilder {
				return base().Topics("lerian.streaming.lender.dlq").
					Handler(noopHandler{}).DiscardHandler(noopDiscardHandler{})
			},
		},
		{
			"DiscardHandler with On",
			func() *streaming.ConsumerBuilder {
				return base().Apps("gateway").
					DiscardHandler(noopDiscardHandler{}).
					On("loan.created", func(context.Context, streaming.Event, []byte) error { return nil })
			},
		},
		{
			"DiscardHandler with Commands",
			func() *streaming.ConsumerBuilder {
				return base().Commands("gateway").DiscardHandler(noopDiscardHandler{})
			},
		},
		{
			// UnmatchedPolicy decides what the DISPATCHER does with an
			// unregistered key. A DLQ reader has no registry to ask, so the knob
			// would sit inert while an operator believed unknown keys were being
			// quarantined.
			"DiscardHandler with UnmatchedPolicy",
			func() *streaming.ConsumerBuilder {
				return base().Topics("lerian.streaming.lender.dlq").
					DiscardHandler(noopDiscardHandler{}).UnmatchedPolicy(streaming.UnmatchedError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := tt.build().Build(context.Background()); !errors.Is(err, streaming.ErrDiscardHandlerAndHandlerBothSet) {
				t.Errorf("Build err = %v; want ErrDiscardHandlerAndHandlerBothSet", err)
			}
		})
	}
}

// TestNewConsumer_SelfQuarantineTopic pins the asymmetry at the public surface.
//
// Source(...) names where a consumer quarantines. Subscribing there republishes
// onto the topic it just read from and is redelivered, quarantined, redelivered
// — forever, while reporting healthy and growing the topic without bound.
//
// A DLQ reader is refused: the seam is new API, so no deployment can be broken
// by refusing it. A plain Handler is only warned: that shape is one the released
// library accepts and that drains clean today, so refusing it would turn a minor
// upgrade into a startup outage on a running service.
func TestNewConsumer_SelfQuarantineTopic(t *testing.T) {
	t.Parallel()

	ownDLQ, err := streaming.AppDLQTopic("lender")
	if err != nil {
		t.Fatalf("AppDLQTopic: %v", err)
	}

	base := func() *streaming.ConsumerBuilder {
		return streaming.NewConsumer().Brokers("localhost:9092").Group("g").Source("lender").Topics(ownDLQ)
	}

	t.Run("a DLQ reader on its own quarantine topic is refused", func(t *testing.T) {
		t.Parallel()

		_, err := base().DiscardHandler(noopDiscardHandler{}).Build(context.Background())
		if !errors.Is(err, streaming.ErrSubscribedToOwnQuarantineTopic) {
			t.Errorf("Build err = %v; want ErrSubscribedToOwnQuarantineTopic", err)
		}
	})

	t.Run("a plain handler on the same shape still builds", func(t *testing.T) {
		t.Parallel()

		c, err := base().Handler(noopHandler{}).Build(context.Background())
		if err != nil {
			t.Fatalf("Build refused a shape the released library accepts: %v", err)
		}

		_ = c.Close()
	})

	t.Run("the reader's documented fix builds", func(t *testing.T) {
		t.Parallel()

		c, err := streaming.NewConsumer().Brokers("localhost:9092").Group("lender-dlq-desk").
			Source("lender-dlq-desk").Topics(ownDLQ).DiscardHandler(noopDiscardHandler{}).
			Build(context.Background())
		if err != nil {
			t.Fatalf("Build with a distinct ce-source: %v", err)
		}

		_ = c.Close()
	})
}

// TestNewConsumer_NilDiscardHandlerIsRefused proves a typed-nil does not sneak
// past as a wired handler and dispatch into nothing.
func TestNewConsumer_NilDiscardHandlerIsRefused(t *testing.T) {
	t.Parallel()

	var nilHandler *nilDiscardHandler

	_, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender-dlq-desk").
		Topics("lerian.streaming.lender.dlq").
		DiscardHandler(nilHandler).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrNilHandler) {
		t.Fatalf("Build err = %v; want ErrNilHandler", err)
	}
}

type nilDiscardHandler struct{}

func (*nilDiscardHandler) HandleDiscard(context.Context, streaming.DiscardRecord) error { return nil }
