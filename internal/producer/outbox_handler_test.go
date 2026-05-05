//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// newTestUUIDv7 returns a time-ordered UUID via commons.GenerateUUIDv7. Used
// for persisted outbox row IDs and aggregate IDs so test rows match the
// production ordering contract (see outbox_writer.go outboxRowFromEnvelope).
func newTestUUIDv7(tb testing.TB) uuid.UUID {
	tb.Helper()
	id, err := commons.GenerateUUIDv7()
	if err != nil {
		tb.Fatalf("commons.GenerateUUIDv7 err = %v", err)
	}
	return id
}

// --- Outbox handler registration + relay tests. ---

func TestProducer_RegisterOutboxRelay_RelaysStableEnvelope(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
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
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), newTestUUIDv7(t))
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal envelope err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
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

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
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
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), newTestUUIDv7(t))

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err == nil {
		t.Fatal("handleOutboxRow err = nil; want non-nil (closed producer must surface)")
	}

	// Multi-target dispatch goes through the closed adapter directly, so
	// the surfaced error originates from the kafka client (e.g. "client
	// closed" or "context canceled" depending on race timing). What the
	// test pins is the loud non-silence: replay errors must propagate up
	// to the Dispatcher, never be swallowed.
	if !strings.Contains(err.Error(), "replay outbox row") {
		t.Errorf("handleOutboxRow err = %q; want wrapped with 'replay outbox row' context", err.Error())
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

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: newTestUUIDv7(t),
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
// equality guard returns nil (not an error) for rows whose EventType is
// NOT the streaming relay type. This is the defensive branch that protects
// against bootstrap misconfiguration: if someone registers our handler
// for "payment.created" by mistake, we must NOT publish that payload to
// a streaming topic and we must NOT mark the row FAILED (which would
// discard or retry someone else's event).
func TestProducer_OutboxHandler_NonStreamingEventType_NoOp(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   "payment.created", // NOT our relay type
		AggregateID: newTestUUIDv7(t),
		Payload:     []byte(`{"ok":true}`),
	}

	err = p.handleOutboxRow(context.Background(), row)
	if err != nil {
		t.Errorf("handleOutboxRow(non-streaming row) err = %v; want nil (silent skip)", err)
	}
}

func TestProducer_RegisterOutboxRelay_DuplicateSurfacesError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
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

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
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

// TestHandleOutboxRow_BypassesCBWhenOpen locks TRD §C7: the outbox relay
// handler MUST use publishDirect — not Emit — so that an in-flight replay
// during a sustained broker outage cannot re-enqueue itself when the
// breaker is OPEN. The fake CB manager is forced OPEN; handleOutboxRow
// must still reach the broker AND must never invoke cb.Execute.
func TestHandleOutboxRow_BypassesCBWhenOpen(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	fakeMgr := newFakeCBManager()

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	// Force the breaker OPEN. A synchronous Emit would now short-circuit with
	// ErrCircuitOpen (or route to outbox if one were wired). handleOutboxRow
	// must bypass that logic entirely.
	primaryService := p.targets["primary"].cbServiceName
	fakeMgr.ForceTransition(primaryService, circuitbreaker.StateOpen)

	if got := p.targetState("primary"); got != flagCBOpen {
		t.Fatalf("primary target state after OPEN = %d; want %d", got, flagCBOpen)
	}

	// Retrieve the fake breaker instance so we can assert Execute call count
	// at the end. GetOrCreate returns the same instance already registered
	// for this producer's primary target.
	cbIface, err := fakeMgr.GetOrCreate(primaryService, circuitbreaker.HTTPServiceConfig())
	if err != nil {
		t.Fatalf("GetOrCreate err = %v", err)
	}

	fake, ok := cbIface.(*fakeCB)
	if !ok {
		t.Fatalf("GetOrCreate returned %T; want *fakeCB", cbIface)
	}

	baseline := fake.executeCalls()

	// Build a valid v0.2.0 envelope around sampleEvent and feed it to the
	// handler. sampleEvent()'s topic matches the catalog, so preflight will
	// accept it.
	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), newTestUUIDv7(t))

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal envelope err = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.handleOutboxRow(ctx, row); err != nil {
		t.Fatalf("handleOutboxRow err = %v; want nil (replay must succeed even with CB OPEN)", err)
	}

	// Assertion 1: broker received the record. publishDirect did run.
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
		t.Fatalf("no record on broker after replay with CB OPEN; publishDirect was not invoked")
	}

	// Assertion 2: cb.Execute was NEVER called. If this grew, it would mean
	// handleOutboxRow drifted from publishDirect to the Emit → cb.Execute
	// path and the loop-prevention property is broken.
	if got := fake.executeCalls(); got != baseline {
		t.Errorf("cb.Execute call count = %d; want %d (replay must bypass CB entirely per TRD §C7)", got, baseline)
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

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
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
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	primaryService := p.targets["primary"].cbServiceName

	// Phase 1: CB OPEN, Emit routes to outbox.
	fakeMgr.ForceTransition(primaryService, circuitbreaker.StateOpen)

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
	fakeMgr.ForceTransition(primaryService, circuitbreaker.StateClosed)

	registry := outbox.NewHandlerRegistry()
	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	if err := registry.Handle(ctx, row); err != nil {
		t.Fatalf("registry.Handle err = %v", err)
	}

	// Phase 3: The record is now on the broker. Derive the topic from the
	// persisted envelope so the assertion cannot silently drift from what
	// the relay actually produced.
	var decodedEnvelope contract.OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &decodedEnvelope); err != nil {
		t.Fatalf("unmarshal envelope err = %v", err)
	}
	consumer := newConsumer(t, cluster, decodedEnvelope.Destination.Name)

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
