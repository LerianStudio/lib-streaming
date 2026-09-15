//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// noopDiscardHandler is a do-nothing DiscardHandler for builder-construction
// tests.
type noopDiscardHandler struct{}

func (noopDiscardHandler) HandleDiscard(context.Context, streaming.DiscardRecord) error { return nil }

// TestDLQHeaderKeys_AreFrozen pins the eleven exported header keys to their
// literal wire values.
//
// The whole point of exporting them is that a consuming service stops restating
// the strings and drifting. An alias that pointed at the wrong internal constant
// would be invisible — every name still compiles, every test that compares
// constant-to-constant still passes — and the service would read a header the
// library never writes, seeing an empty DLQ forever while the topic fills.
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
	}

	for want, got := range frozen {
		if got != want {
			t.Errorf("exported header key = %q; want %q", got, want)
		}
	}

	causes := map[string]string{
		"codec":           streaming.DLQCauseCodec,
		"handler":         streaming.DLQCauseHandler,
		"source_mismatch": streaming.DLQCauseSourceMismatch,
		"unhandled_key":   streaming.DLQCauseUnhandledKey,
	}

	for want, got := range causes {
		if got != want {
			t.Errorf("exported cause kind = %q; want %q", got, want)
		}
	}
}

// TestParseDiscardRecord_ReadsTheOriginFromHeadersNotCoordinates is the trap
// worth a test at the public surface: the origin triple lives in the HEADERS,
// not in the record's own topic/partition/offset, which are the DLQ's.
func TestParseDiscardRecord_ReadsTheOriginFromHeadersNotCoordinates(t *testing.T) {
	t.Parallel()

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderSourceTopic, Value: []byte("lerian.streaming.gateway")},
		{Key: streaming.DLQHeaderSourcePartition, Value: []byte("3")},
		{Key: streaming.DLQHeaderSourceOffset, Value: []byte("42")},
		{Key: streaming.DLQHeaderCauseKind, Value: []byte(streaming.DLQCauseHandler)},
	}, []byte(`{"loanId":"l-1"}`))

	if got.SourceTopic != "lerian.streaming.gateway" || got.SourcePartition != 3 || got.SourceOffset != 42 {
		t.Errorf("origin = %s/%d/%d; want lerian.streaming.gateway/3/42",
			got.SourceTopic, got.SourcePartition, got.SourceOffset)
	}

	if got.CauseKind != streaming.DLQCauseHandler {
		t.Errorf("CauseKind = %q; want %q", got.CauseKind, streaming.DLQCauseHandler)
	}

	if string(got.Payload) != `{"loanId":"l-1"}` {
		t.Errorf("Payload = %q; want it verbatim", got.Payload)
	}
}

// TestParseDiscardRecord_NeverFails proves the public parser carries the
// never-reject posture, not just the internal one.
func TestParseDiscardRecord_NeverFails(t *testing.T) {
	t.Parallel()

	got := streaming.ParseDiscardRecord(nil, nil)

	if got.EnvelopeError == nil {
		t.Error("EnvelopeError is nil for headerless input; a reader must be able to tell a dead envelope from a single-tenant one")
	}

	if got.SourceTopic != "" {
		t.Errorf("SourceTopic = %q; want empty, the sentinel for an absent origin triple", got.SourceTopic)
	}
}

// TestNewConsumer_DiscardHandlerBuilds proves the seam reaches a real runtime,
// which is the half the exported constants alone never gave a service: reading
// its own ".dlq" with this library instead of a raw franz-go client.
func TestNewConsumer_DiscardHandlerBuilds(t *testing.T) {
	t.Parallel()

	dlqTopic, err := streaming.AppDLQTopic("lender")
	if err != nil {
		t.Fatalf("AppDLQTopic: %v", err)
	}

	c, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender").
		Topics(dlqTopic).
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

// TestNewConsumer_DiscardHandlerIsMutuallyExclusiveWithDispatch pins the third
// answer to "who selects events" against the other two. Silently preferring one
// would drop the other's handlers without a word.
func TestNewConsumer_DiscardHandlerIsMutuallyExclusiveWithDispatch(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender").
		Apps("gateway").
		DiscardHandler(noopDiscardHandler{}).
		On("loan.created", func(context.Context, streaming.Event, []byte) error { return nil }).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrHandlerAndDispatchBothSet) {
		t.Errorf("Build err = %v; want ErrHandlerAndDispatchBothSet", err)
	}
}

// TestNewConsumer_NilDiscardHandlerIsRefused proves a typed-nil does not sneak
// past as a wired handler and dispatch into nothing.
func TestNewConsumer_NilDiscardHandlerIsRefused(t *testing.T) {
	t.Parallel()

	var nilHandler *nilDiscardHandler

	_, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender").
		Topics("lerian.streaming.lender.dlq").
		DiscardHandler(nilHandler).
		Build(context.Background())

	if err == nil {
		t.Fatal("Build succeeded with a typed-nil DiscardHandler; want a refusal")
	}
}

type nilDiscardHandler struct{}

func (*nilDiscardHandler) HandleDiscard(context.Context, streaming.DiscardRecord) error { return nil }
