//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// --- GROUP D: payload-size boundary + caller non-mutation. ---

// TestProducer_EmitPreFlight_PayloadAtExactBoundary pins the three-point
// acceptance shape around maxPayloadBytes (1 MiB). The existing
// TestProducer_EmitPreFlight_PayloadTooLarge only covers the +1 case; this
// test adds the exactly-at-limit and limit-minus-1 cases so a future refactor
// that flips the boundary operator (> vs >=) breaks the test loudly.
//
// The test asserts on preFlight directly (not through Emit) because the kfake
// broker has its own max.message.bytes that is strictly below our 1 MiB
// payload cap once CloudEvents headers are added. preFlight is the boundary
// under test — whether kfake accepts the wire-level message is an orthogonal
// concern that TestIntegration_DLQRouting already exercises on a real Redpanda.
//
// JSON payload shape: a quoted string `"<padding>"`. To reach len == N we
// need padding of N-2 bytes.
func TestProducer_EmitPreFlight_PayloadAtExactBoundary(t *testing.T) {
	tests := []struct {
		name        string
		payloadSize int
		wantErr     error // nil means preFlight accepts
	}{
		{
			name:        "at exact boundary (1 MiB)",
			payloadSize: maxPayloadBytes,
			wantErr:     nil,
		},
		{
			name:        "one byte below boundary",
			payloadSize: maxPayloadBytes - 1,
			wantErr:     nil,
		},
		{
			name:        "one byte above boundary",
			payloadSize: maxPayloadBytes + 1,
			wantErr:     ErrPayloadTooLarge,
		},
	}

	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Construct a JSON string literal whose byte length equals
			// tt.payloadSize. Quote characters contribute 2 bytes.
			padding := strings.Repeat("x", tt.payloadSize-2)
			payload := []byte(`"` + padding + `"`)

			if len(payload) != tt.payloadSize {
				t.Fatalf("test bug: payload len = %d; want %d", len(payload), tt.payloadSize)
			}

			// Assert json.Valid on the happy-path cases. An invalid payload
			// would surface ErrNotJSON instead of ErrPayloadTooLarge and mask
			// the assertion.
			if tt.wantErr == nil && !json.Valid(payload) {
				t.Fatalf("test bug: payload is not valid JSON at size %d", tt.payloadSize)
			}

			event := sampleEvent()
			event.Payload = json.RawMessage(payload)
			(&event).ApplyDefaults()

			err := p.preFlightWithPayload(context.Background(), event, true)

			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("preFlight err = %v; want nil for payload size %d", err, tt.payloadSize)
				}
				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("preFlight err = %v; want errors.Is(%v)", err, tt.wantErr)
			}
		})
	}
}

// TestProducer_EmitPreFlight_TopicAtExactBoundary pins Kafka's 249-byte
// topic-name limit at the ONE place it can still be hit in v3: the source.
//
// The topic is "lerian.streaming." + Source and nothing else, so the bound is
// entirely a bound on the source — and it is derived from the COMMANDS topic
// (the longest derived name), not the base topic or the DLQ. Resource type and
// event type no longer contribute a single byte, which is why the v2 cases that
// blew the limit by concatenating a 120-byte resource with a 120-byte event
// type are gone: that failure mode does not exist any more.
// The numbers below are HARDCODED. Computing maxSource from TopicPrefix and
// CommandsTopicSuffix — the same constants the production code computes with —
// made the assertion agree with itself: shorten the prefix and both sides move
// together while every deployed topic name silently changes. 223 and 249 are
// the contract, and they belong in the test as literals.
func TestProducer_EmitPreFlight_TopicAtExactBoundary(t *testing.T) {
	const (
		maxSource      = 223 // 249 - len("lerian.streaming.") - len(".commands")
		kafkaNameLimit = 249
	)

	tests := []struct {
		name            string
		source          string
		wantErr         error
		wantCommandsLen int
	}{
		{
			name:            "longest legal source: commands topic lands exactly on the limit",
			source:          strings.Repeat("s", maxSource),
			wantCommandsLen: kafkaNameLimit,
		},
		{
			name:            "one byte over: rejected before any broker call",
			source:          strings.Repeat("s", maxSource+1),
			wantErr:         ErrInvalidSource,
			wantCommandsLen: kafkaNameLimit + 1,
		},
	}

	p := &Producer{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := len(contract.AppCommandsTopic(tt.source)); got != tt.wantCommandsLen {
				t.Fatalf("commands topic length = %d; want %d", got, tt.wantCommandsLen)
			}

			event := sampleEvent()
			event.Source = tt.source
			event.ResourceType = strings.Repeat("r", 120)
			event.EventType = strings.Repeat("e", 120)
			(&event).ApplyDefaults()

			err := p.preFlightWithPayload(context.Background(), event, true)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("preFlight err = %v; want errors.Is(%v)", err, tt.wantErr)
			}
		})
	}
}

// TestProducer_Emit_DoesNotMutateCaller pins the value-semantics invariant:
// Emit receives Event by value and must never touch the caller's struct.
// ApplyDefaults runs on a LOCAL copy inside Emit — this test proves the
// caller's zero-valued EventID / Timestamp / SchemaVersion / DataContentType
// remain zero after Emit returns.
//
// A future refactor that changes the Emit signature to `Emit(ctx, *Event)`
// or that accidentally mutates the parameter would break this test. That
// is the intended failure mode: callers (including our integration tests)
// rely on "Emit is read-only on the Event" to safely reuse event templates.
func TestProducer_Emit_DoesNotMutateCaller(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	// Construct an Event with explicit zero values on every field
	// ApplyDefaults would populate. Only the fields preFlight requires
	// (TenantID, ResourceType, EventType, Source, Payload) are populated.
	event := Event{
		TenantID:        "t-caller-mut",
		ResourceType:    "transaction",
		EventType:       "created",
		EventID:         "",          // zero — ApplyDefaults would fill uuid
		Timestamp:       time.Time{}, // zero — ApplyDefaults would fill now UTC
		SchemaVersion:   "",          // zero — ApplyDefaults would fill "1.0.0"
		Source:          "test-caller-mut",
		DataContentType: "", // zero — ApplyDefaults would fill "application/json"
		Payload:         json.RawMessage(`{"k":"v"}`),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := eventToRequest(event)
	if err := emitter.Emit(ctx, request); err != nil {
		t.Fatalf("Emit err = %v", err)
	}

	// Caller's struct MUST remain zero on the four defaulted fields.
	if event.EventID != "" {
		t.Errorf("caller EventID mutated: %q; want empty", event.EventID)
	}

	if !event.Timestamp.IsZero() {
		t.Errorf("caller Timestamp mutated: %v; want zero", event.Timestamp)
	}

	if event.SchemaVersion != "" {
		t.Errorf("caller SchemaVersion mutated: %q; want empty", event.SchemaVersion)
	}

	if event.DataContentType != "" {
		t.Errorf("caller DataContentType mutated: %q; want empty", event.DataContentType)
	}
}
