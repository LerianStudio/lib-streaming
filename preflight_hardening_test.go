//go:build unit

package streaming

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// --- Group 1.1: WithAllowSystemEvents opt-in ----------------------------------

// TestProducer_EmitPreFlight_SystemEventWithoutOpt_Rejected verifies that a
// catalog containing a SystemEvent=true definition, when passed to New
// without WithAllowSystemEvents, fails at CONSTRUCTION with
// ErrSystemEventsNotAllowed. The earlier preflight-level rejection is now
// dead code for catalog-resolved events — the construction gate added in
// Task #12 pre-empts it so a misconfigured bootstrap fails fast at startup.
func TestProducer_EmitPreFlight_SystemEventWithoutOpt_Rejected(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	systemCatalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
		SystemEvent:  true,
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	_, err = New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(systemCatalog))
	if !errors.Is(err, ErrSystemEventsNotAllowed) {
		t.Fatalf("New err = %v; want ErrSystemEventsNotAllowed at construction", err)
	}
}

// TestProducer_EmitPreFlight_SystemEventWithOpt_Accepted verifies that
// WithAllowSystemEvents lets SystemEvent=true through preflight.
func TestProducer_EmitPreFlight_SystemEventWithOpt_Accepted(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	systemCatalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
		SystemEvent:  true,
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	emitter, err = New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(systemCatalog),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	event := EmitRequest{DefinitionKey: "transaction.created", Payload: json.RawMessage(`{}`)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, event); err != nil {
		t.Errorf("Emit with WithAllowSystemEvents err = %v; want nil", err)
	}
}

// TestIsCallerError_SystemEventsNotAllowed_ReturnsTrue wires the new sentinel
// into the IsCallerError truth table.
func TestIsCallerError_SystemEventsNotAllowed_ReturnsTrue(t *testing.T) {
	t.Parallel()

	if !IsCallerError(ErrSystemEventsNotAllowed) {
		t.Errorf("IsCallerError(ErrSystemEventsNotAllowed) = false; want true")
	}
}

// --- Group 1.3: Header sanitization --------------------------------------------

// TestProducer_EmitPreFlight_HeaderSanitization_RejectsInjections covers the
// five sanitized fields with a matrix of control chars and over-limits. Table
// entries include a newline injection (CRLF), NUL byte, DEL byte, and a
// string that exceeds the ceiling.
func TestProducer_EmitPreFlight_HeaderSanitization_RejectsInjections(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	tests := []struct {
		name    string
		mutate  func(*Event)
		wantErr error
	}{
		{
			name:    "tenant id with newline",
			mutate:  func(e *Event) { e.TenantID = "t-abc\n.evil" },
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "tenant id with CR",
			mutate:  func(e *Event) { e.TenantID = "t-abc\r\n" },
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "tenant id with NUL",
			mutate:  func(e *Event) { e.TenantID = "t\x00abc" },
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "tenant id with DEL",
			mutate:  func(e *Event) { e.TenantID = "t\x7fabc" },
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "tenant id over limit",
			mutate:  func(e *Event) { e.TenantID = strings.Repeat("x", maxTenantIDBytes+1) },
			wantErr: ErrInvalidTenantID,
		},
		{
			name:    "subject with newline",
			mutate:  func(e *Event) { e.Subject = "tx-123\nmalicious" },
			wantErr: ErrInvalidSubject,
		},
		{
			name:    "subject over limit",
			mutate:  func(e *Event) { e.Subject = strings.Repeat("x", maxSubjectBytes+1) },
			wantErr: ErrInvalidSubject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := sampleEvent()
			tt.mutate(&e)

			err := emitter.Emit(context.Background(), eventToRequest(e))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Emit err = %v; want errors.Is(%v)", err, tt.wantErr)
			}

			if !IsCallerError(err) {
				t.Errorf("IsCallerError(%v) = false; want true", err)
			}
		})
	}
}

// TestProducer_EmitPreFlight_HeaderSanitization_AcceptsAtLimits asserts that
// fields exactly at their byte ceiling are ACCEPTED by preFlight. Checks
// preFlight directly so the long-but-valid ResourceType / EventType don't
// need to resolve to a pre-created kfake topic.
func TestProducer_EmitPreFlight_HeaderSanitization_AcceptsAtLimits(t *testing.T) {
	t.Parallel()

	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	e := sampleEvent()
	e.TenantID = strings.Repeat("t", maxTenantIDBytes)
	e.ResourceType = strings.Repeat("r", maxResourceTypeBytes)
	e.EventType = strings.Repeat("e", maxEventTypeBytes)
	e.Source = "//" + strings.Repeat("s", maxSourceBytes-2)
	e.Subject = strings.Repeat("j", maxSubjectBytes)
	(&e).ApplyDefaults()

	if err := p.preFlightWithPayload(e, true); err != nil {
		t.Errorf("preFlight at-limit fields err = %v; want nil", err)
	}
}

// TestHasControlChar covers the scanner helper in isolation. Tab is rejected
// — CloudEvents header values never contain tabs in practice.
func TestHasControlChar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", false},
		{"plain ascii", "abc-123.xyz", false},
		{"utf8 no controls", "tenant-ùñiçödé", false},
		{"newline", "a\nb", true},
		{"cr", "a\rb", true},
		{"tab", "a\tb", true},
		{"nul", "a\x00b", true},
		{"del", "a\x7fb", true},
		{"escape", "a\x1bb", true},
		{"bell", "a\x07b", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := hasControlChar(tt.in); got != tt.want {
				t.Errorf("hasControlChar(%q) = %v; want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestIsCallerError_HeaderSentinels walks every new header sentinel.
func TestIsCallerError_HeaderSentinels(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrInvalidTenantID,
		ErrInvalidResourceType,
		ErrInvalidEventType,
		ErrInvalidSource,
		ErrInvalidSubject,
	}

	for _, s := range sentinels {
		s := s
		t.Run(s.Error(), func(t *testing.T) {
			t.Parallel()

			if !IsCallerError(s) {
				t.Errorf("IsCallerError(%v) = false; want true", s)
			}
		})
	}
}

// --- Group 1.2: handleOutboxRow re-runs preFlight ------------------------------

// outboxHandlerStubProducer is a minimal harness that builds a *Producer
// without a kfake broker — we need to exercise handleOutboxRow on an
// oversized payload without triggering any broker I/O (preFlight rejects
// before publishDirect runs, but defense-in-depth is the whole point of this
// test). The kfake-backed Producer is adequate; publishDirect is never
// reached because preFlight rejects first.

// TestProducer_HandleOutboxRow_RunsPreFlight_OversizePayload: when an outbox
// row's serialized Event carries an oversized payload, preFlight MUST reject
// it with ErrPayloadTooLarge wrapped in "outbox replay preflight rejected".
// This proves publishDirect did NOT run on corrupted input.
func TestProducer_HandleOutboxRow_RunsPreFlight_OversizePayload(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	event := sampleEvent()
	event.Payload = json.RawMessage(`"` + strings.Repeat("x", maxPayloadBytes+1) + `"`)
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err == nil {
		t.Fatal("handleOutboxRow err = nil; want ErrPayloadTooLarge via preflight")
	}

	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("handleOutboxRow err = %v; want errors.Is ErrPayloadTooLarge", err)
	}

	if !strings.Contains(err.Error(), "outbox replay preflight rejected") {
		t.Errorf("handleOutboxRow err = %q; want substring %q", err.Error(), "outbox replay preflight rejected")
	}
}

// TestProducer_HandleOutboxRow_RunsPreFlight_SystemEventWithoutOpt: a row
// persisted with SystemEvent=true must be rejected if the relay Producer was
// constructed without WithAllowSystemEvents. This is the outbox-replay
// specific application of the capability gate.
func TestProducer_HandleOutboxRow_RunsPreFlight_SystemEventWithoutOpt(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	event := sampleEvent()
	event.SystemEvent = true
	event.TenantID = ""
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	err = p.handleOutboxRow(context.Background(), row)
	if !errors.Is(err, ErrSystemEventsNotAllowed) {
		t.Errorf("handleOutboxRow err = %v; want ErrSystemEventsNotAllowed", err)
	}
}

// --- Group 5: outcome=outbox_failed metric label -------------------------------

// --- Group 5: outcome=outbox_failed metric label -------------------------------

// TestProducer_Emit_CircuitOpen_OutboxFailure_MetricsOutboxFailed asserts the
// emitted counter carries outcome=outbox_failed (NOT caller_error) when an
// outbox write fails under an OPEN circuit. This is the dashboard signal
// operators use to distinguish outbox-infrastructure failures from caller
// input errors.
func TestProducer_Emit_CircuitOpen_OutboxFailure_MetricsOutboxFailed(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	factory, snapshot := newManualMeterSetup(t)

	fakeMgr := newFakeCBManager()
	fakeRepo := &fakeOutboxRepo{
		createErr: errors.New("db down"),
	}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithMetricsFactory(factory),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	event := sampleRequest()
	topic := "lerian.streaming.transaction.created"

	if err := emitter.Emit(context.Background(), event); err == nil {
		t.Fatal("Emit err = nil; want non-nil (outbox failure must surface)")
	}

	rm := snapshot()

	m, ok := findMetric(rm, metricNameEmitted)
	if !ok {
		t.Fatalf("metric %q not found", metricNameEmitted)
	}

	_, attrSets := sumInt64DataPoints(t, m)

	var found bool
	for _, attrs := range attrSets {
		if attrs["outcome"] == outcomeOutboxFailed && attrs["topic"] == topic {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("outcome=%q not recorded for topic=%q; attrSets=%v",
			outcomeOutboxFailed, topic, attrSets)
	}
}
