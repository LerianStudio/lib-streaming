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
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// --- Outbox handler registration + relay tests. ---

func TestProducer_RegisterOutboxRelay_RelaysStableEnvelope(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	registry := outbox.NewHandlerRegistry()
	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal envelope err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := registry.Handle(ctx, row); err != nil {
		t.Fatalf("registry.Handle err = %v", err)
	}

	consumer := newConsumer(t, cluster, event.Topic())
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)
	var gotRec *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if gotRec == nil {
			gotRec = r
		}
	})
	if gotRec == nil {
		t.Fatal("no record on broker after stable outbox relay")
	}
	if string(gotRec.Value) != string(event.Payload) {
		t.Errorf("relayed payload = %q; want %q", gotRec.Value, event.Payload)
	}
}

// TestProducer_OutboxHandler_PublishDirectFailure_SurfacesError asserts
// that when publishDirect fails (broker down, bad topic, etc.), the
// handler returns a non-nil wrapped error. The outbox Dispatcher needs
// this signal to mark the row FAILED and schedule a retry with backoff.
// Swallowing the error here would leak events into the void.
func TestProducer_OutboxHandler_PublishDirectFailure_SurfacesError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	p := asProducer(t, emitter)

	// Close the producer BEFORE running the handler. publishDirect checks
	// the closed flag and returns ErrEmitterClosed — a deterministic
	// failure that doesn't require sabotaging the broker.
	if err := emitter.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err == nil {
		t.Fatal("handleOutboxRow err = nil; want non-nil (closed producer must surface)")
	}

	if !errors.Is(err, ErrEmitterClosed) {
		t.Errorf("handleOutboxRow err = %v; want errors.Is ErrEmitterClosed", err)
	}

	// Row ID should appear in the wrap context so operators can correlate.
	if !strings.Contains(err.Error(), row.ID.String()) {
		t.Errorf("handleOutboxRow err = %q; want substring %q (row ID for correlation)", err.Error(), row.ID.String())
	}
}

// TestProducer_OutboxHandler_InvalidPayload_ReturnsError: a row whose
// Payload is not valid JSON must surface an error (not panic). The
// Dispatcher will mark it FAILED; operators can then MarkInvalid it from
// the DB so future cycles skip it.
func TestProducer_OutboxHandler_InvalidPayload_ReturnsError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: uuid.New(),
		Payload:     []byte("{not-json"),
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err == nil {
		t.Fatal("handleOutboxRow err = nil; want error for malformed payload")
	}

	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("err = %q; want substring %q", err.Error(), "unmarshal")
	}
}

// TestProducer_OutboxHandler_NonStreamingEventType_NoOp: the handler's
// prefix guard returns nil (not an error) for rows whose EventType is
// NOT a streaming event. This is the defensive branch that protects
// against bootstrap misconfiguration: if someone registers our handler
// for "payment.created" by mistake, we must NOT publish that payload to
// a streaming topic and we must NOT mark the row FAILED (which would
// discard or retry someone else's event).
func TestProducer_OutboxHandler_NonStreamingEventType_NoOp(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   "payment.created", // NOT our prefix
		AggregateID: uuid.New(),
		Payload:     []byte(`{"ok":true}`),
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err != nil {
		t.Errorf("handleOutboxRow(non-streaming row) err = %v; want nil (silent skip)", err)
	}
}

func TestProducer_RegisterOutboxRelay_DuplicateSurfacesError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	registry := outbox.NewHandlerRegistry()

	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("first RegisterOutboxRelay err = %v", err)
	}

	err = p.RegisterOutboxRelay(registry)
	if err == nil {
		t.Fatal("second RegisterOutboxRelay err = nil; want duplicate-registration error")
	}

	if !errors.Is(err, outbox.ErrHandlerAlreadyRegistered) {
		t.Errorf("err = %v; want errors.Is outbox.ErrHandlerAlreadyRegistered", err)
	}
}

func TestProducer_RegisterOutboxRelay_NilReceiver(t *testing.T) {
	t.Parallel()

	var p *Producer

	err := p.RegisterOutboxRelay(outbox.NewHandlerRegistry())
	if !errors.Is(err, ErrNilProducer) {
		t.Errorf("nil.RegisterOutboxRelay err = %v; want ErrNilProducer", err)
	}
}

func TestProducer_RegisterOutboxRelay_NilRegistry(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	err = p.RegisterOutboxRelay(nil)
	if !errors.Is(err, ErrNilOutboxRegistry) {
		t.Errorf("RegisterOutboxRelay(nil registry) err = %v; want ErrNilOutboxRegistry", err)
	}
}

// TestProducer_HandleOutboxRow_NilReceiver: defensive guard. Unreachable
// in practice but promised in the godoc contract.
func TestProducer_HandleOutboxRow_NilReceiver(t *testing.T) {
	t.Parallel()

	var p *Producer
	err := p.handleOutboxRow(context.Background(), &outbox.OutboxEvent{})
	if !errors.Is(err, ErrNilProducer) {
		t.Errorf("nil.handleOutboxRow err = %v; want ErrNilProducer", err)
	}
}

// TestProducer_HandleOutboxRow_NilRow: nil *OutboxEvent must surface the
// outbox sentinel, not panic.
func TestProducer_HandleOutboxRow_NilRow(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	err = p.handleOutboxRow(context.Background(), nil)
	if !errors.Is(err, outbox.ErrOutboxEventRequired) {
		t.Errorf("handleOutboxRow(nil row) err = %v; want outbox.ErrOutboxEventRequired", err)
	}
}

// TestDecodeOutboxRow_LegacyV01Payload_SynthesizedAndReplayed proves the
// v0.1.0 backward-compat shim: a row whose Payload is the bare
// json.Marshal(Event) (no envelope wrapper, no version field) must be
// synthesized into a valid OutboxEnvelope and a WARN must be logged so
// operators see the residue and can drain it.
func TestDecodeOutboxRow_LegacyV01Payload_SynthesizedAndReplayed(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	spy := &spyLogger{}

	emitter, err := New(context.Background(), cfg,
		WithLogger(spy),
		WithCatalog(sampleCatalog()),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	legacyEvent := sampleEvent()
	legacyEvent.Payload = json.RawMessage(`{}`)
	legacyEvent.ApplyDefaults()

	legacyPayload, err := json.Marshal(legacyEvent)
	if err != nil {
		t.Fatalf("json.Marshal legacyEvent err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: uuid.New(),
		Payload:     legacyPayload,
	}

	envelope, err := p.decodeOutboxRow(context.Background(), row)
	if err != nil {
		t.Fatalf("decodeOutboxRow err = %v; want synthesized envelope", err)
	}

	if envelope.Version != outboxEnvelopeVersion {
		t.Errorf("envelope.Version = %d; want %d", envelope.Version, outboxEnvelopeVersion)
	}

	if envelope.Topic != legacyEvent.Topic() {
		t.Errorf("envelope.Topic = %q; want %q", envelope.Topic, legacyEvent.Topic())
	}

	if envelope.Event.EventID != legacyEvent.EventID {
		t.Errorf("envelope.Event.EventID = %q; want %q", envelope.Event.EventID, legacyEvent.EventID)
	}

	if envelope.Policy != DefaultDeliveryPolicy() {
		t.Errorf("envelope.Policy = %+v; want DefaultDeliveryPolicy", envelope.Policy)
	}

	// Assert the WARN was emitted.
	spy.mu.Lock()
	defer spy.mu.Unlock()

	var warnEntry *spyEntry
	for i := range spy.entries {
		e := &spy.entries[i]
		if e.level == log.LevelWarn && strings.Contains(e.msg, "legacy v0.1.0 outbox row migrated") {
			warnEntry = e
			break
		}
	}
	if warnEntry == nil {
		t.Fatalf("expected WARN entry about legacy migration; got %d entries", len(spy.entries))
	}

	if got, ok := warnEntry.fields["row_id"].(string); !ok || got != row.ID.String() {
		t.Errorf("warn row_id = %v; want %q", warnEntry.fields["row_id"], row.ID.String())
	}
}

// TestDecodeOutboxRow_LegacyEmptyPayload_StillRejects proves the shim does
// NOT synthesize a bogus envelope for a zero-value Event. An empty legacy
// payload is indistinguishable from garbage and must surface the original
// validation error so the Dispatcher marks the row FAILED.
func TestDecodeOutboxRow_LegacyEmptyPayload_StillRejects(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog()),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	emptyPayload, err := json.Marshal(Event{})
	if err != nil {
		t.Fatalf("json.Marshal(Event{}) err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: uuid.New(),
		Payload:     emptyPayload,
	}

	_, err = p.decodeOutboxRow(context.Background(), row)
	if err == nil {
		t.Fatal("decodeOutboxRow err = nil; want validation error for zero-value Event")
	}

	if !strings.Contains(err.Error(), "invalid outbox envelope") {
		t.Errorf("err = %q; want substring %q", err.Error(), "invalid outbox envelope")
	}
}

// TestProducer_EndToEnd_OutboxFallbackAndRelay wires the full T4 happy
// path as callers would experience it:
//
//  1. Emit with the circuit OPEN + outbox wired → row lands in fake repo.
//  2. Register the streaming handler against the outbox registry.
//  3. Manually invoke the handler with the captured row (simulating what
//     the Dispatcher does after the broker recovers).
//  4. Assert the broker now has the record — proving the end-to-end
//     flow works without a loop back into the outbox.
func TestProducer_EndToEnd_OutboxFallbackAndRelay(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	fakeMgr := newFakeCBManager()
	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog()),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	// Phase 1: CB OPEN, Emit routes to outbox.
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	event := sampleRequest()
	event.Subject = "end-to-end-subject"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, event); err != nil {
		t.Fatalf("Emit (CB open) err = %v", err)
	}

	row := fakeRepo.firstCreated()
	if row == nil {
		t.Fatal("outbox row was not created")
	}

	// Phase 2: Broker recovers (CB back to CLOSED). Register handler and
	// invoke it with the captured row.
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateClosed)

	registry := outbox.NewHandlerRegistry()
	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	if err := registry.Handle(ctx, row); err != nil {
		t.Fatalf("registry.Handle err = %v", err)
	}

	// Phase 3: The record is now on the broker.
	consumer := newConsumer(t, cluster, "lerian.streaming.transaction.created")

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)

	var gotRec *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		if gotRec == nil {
			gotRec = r
		}
	})

	if gotRec == nil {
		t.Fatal("no record on broker after replay; end-to-end flow broken")
	}

	// Payload round-trip: broker record's value must equal the original
	// event.Payload. This also proves the JSON marshal+unmarshal path
	// through the outbox preserved the bytes exactly.
	if string(gotRec.Value) != string(event.Payload) {
		t.Errorf("relayed record.Value = %q; want %q", gotRec.Value, event.Payload)
	}
}
