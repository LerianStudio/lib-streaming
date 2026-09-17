//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// legacyEnvelopeJSON builds the wire bytes of a version-1 outbox row, the
// shape lib-streaming v2 persisted.
//
// It is assembled as raw JSON rather than by marshalling the current struct so
// the row does not silently inherit today's semantics. Two fields carry the
// whole difference:
//
//   - "version": 1
//   - "destination.name": the v2-era PER-EVENT topic, which v2 derived as
//     sanitizeSourceSegment(Source) + "." + ResourceType + "." + EventType
//     (plus ".v<major>" once SchemaVersion reached 2.0.0). See
//     internal/contract/source_topic.go at tag v2.1.0.
//
// Every other field name and value is identical to a version-2 row: the json
// tags in outbox_envelope.go and route.go have not changed since v2.1.0, and
// Event carries no tags at all in either major.
func legacyEnvelopeJSON(tb testing.TB, source, resourceType, eventType string, aggregateID string) []byte {
	tb.Helper()

	legacyTopic := source + "." + resourceType + "." + eventType

	raw := `{
	  "version": 1,
	  "route_key": "primary.kafka",
	  "definition_key": "transaction.created",
	  "target": "primary",
	  "transport": "kafka",
	  "destination": {"kind": "kafka", "name": "` + legacyTopic + `"},
	  "aggregate_id": "` + aggregateID + `",
	  "requirement": "required",
	  "policy": {"enabled": true, "direct": "direct", "outbox": "fallback_on_circuit_open", "dlq": "on_routable_failure"},
	  "event": {
	    "TenantID": "t-abc",
	    "ResourceType": "` + resourceType + `",
	    "EventType": "` + eventType + `",
	    "Source": "` + source + `",
	    "Subject": "tx-legacy-1",
	    "SchemaVersion": "1.0.0",
	    "Payload": {"amount": 100}
	  }
	}`

	if !json.Valid([]byte(raw)) {
		tb.Fatalf("legacy fixture is not valid JSON:\n%s", raw)
	}

	return []byte(raw)
}

// TestOutboxRelay_LegacyRowLandsOnApplicationTopic is the end-to-end proof
// against a real Kafka protocol implementation: a version-1 row written by
// lib-streaming v2 is drained by a v4 relay and arrives on the v4 application
// topic, NOT on the per-event topic the row itself names.
//
// Before this change the same bytes were refused at the version gate and the
// event was lost at the deploy boundary.
func TestOutboxRelay_LegacyRowLandsOnApplicationTopic(t *testing.T) {
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

	// Source "test" matches kfakeConfig's CloudEventsSource, so the v4
	// application topic is "lerian.streaming.test" while the row names the
	// v2-era "test.transaction.created".
	aggregateID := newTestUUIDv7(t)
	payload := legacyEnvelopeJSON(t, "test", "transaction", "created", aggregateID.String())

	const legacyTopic = "test.transaction.created"

	appTopic := contract.AppTopic("test")
	if appTopic == legacyTopic {
		t.Fatalf("test is vacuous: legacy and application topics are both %q", appTopic)
	}

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := registry.Handle(ctx, row); err != nil {
		t.Fatalf("registry.Handle err = %v, want nil (a v1 row must be dispatched, not refused)", err)
	}

	consumer := newConsumer(t, cluster, appTopic)
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fetchCancel()

	var got *kgo.Record

	consumer.PollFetches(fetchCtx).EachRecord(func(r *kgo.Record) {
		if got == nil {
			got = r
		}
	})

	if got == nil {
		t.Fatalf("no record on %q: the legacy row did not reach the broker", appTopic)
	}

	if got.Topic != appTopic {
		t.Errorf("topic = %q, want %q", got.Topic, appTopic)
	}

	if string(got.Value) != `{"amount": 100}` {
		t.Errorf("payload = %q, want the original v2-era payload byte-for-byte", got.Value)
	}

	// The CloudEvent must be indistinguishable from one a v3 relay would have
	// produced: same ce-source, same ce-type, built by the same helper.
	headers := map[string]string{}
	for _, h := range got.Headers {
		headers[h.Key] = string(h.Value)
	}

	if headers["ce-source"] != "test" {
		t.Errorf("ce-source = %q, want %q", headers["ce-source"], "test")
	}

	if headers["ce-resourcetype"] != "transaction" {
		t.Errorf("ce-resourcetype = %q, want %q", headers["ce-resourcetype"], "transaction")
	}

	if headers["ce-eventtype"] != "created" {
		t.Errorf("ce-eventtype = %q, want %q", headers["ce-eventtype"], "created")
	}
}

// TestOutboxRelay_CurrentRowStillWorks pins the no-regression half: a
// version-2 row keeps going exactly where it was persisted to go.
func TestOutboxRelay_CurrentRowStillWorks(t *testing.T) {
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
	if envelope.Version != contract.OutboxEnvelopeVersion {
		t.Fatalf("fixture version = %d, want the current written version %d", envelope.Version, contract.OutboxEnvelopeVersion)
	}

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

	var got *kgo.Record

	consumer.PollFetches(fetchCtx).EachRecord(func(r *kgo.Record) {
		if got == nil {
			got = r
		}
	})

	if got == nil {
		t.Fatalf("no record on %q for a current-version row", event.Topic())
	}
}

// TestOutboxRelay_UnroutableLegacyRowStaysRetryable is the structural-failure
// case. A v2-era source that v2's sanitizer would have folded is rejected
// outright by the current ValidateSource, so the row cannot be re-derived.
//
// The assertion that matters is IsCallerError: a caller error is what the
// lib-commons dispatcher turns into an immediate INVALID, destroying a durable
// row on its first attempt with no operator in the loop.
func TestOutboxRelay_UnroutableLegacyRowStaysRetryable(t *testing.T) {
	cfg, _ := kfakeConfig(t)

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

	aggregateID := newTestUUIDv7(t)
	// Legal under v2's lossy sanitizer, rejected outright by ValidateSource.
	payload := legacyEnvelopeJSON(t, "//lerian.midaz/transaction-service", "transaction", "created", aggregateID.String())

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = registry.Handle(ctx, row)
	if err == nil {
		t.Fatal("registry.Handle err = nil; returning nil would mark the row PUBLISHED and lose it silently")
	}

	if !errors.Is(err, contract.ErrLegacyOutboxRowUnroutable) {
		t.Errorf("err = %v, want it to wrap ErrLegacyOutboxRowUnroutable", err)
	}

	if contract.IsCallerError(err) {
		t.Errorf("contract.IsCallerError(%v) = true; a caller error sends the row straight to INVALID on attempt one", err)
	}

	// The row id must be in the error so it reaches outbox_events.last_error.
	if !strings.Contains(err.Error(), row.ID.String()) {
		t.Errorf("err = %q, want it to name row id %s so an operator can find the row", err, row.ID)
	}
}

// TestOutboxRelay_UnknownVersionStillRejected pins the boundary: read-compat
// covers version 1 and nothing else. A future or corrupt version stays a
// caller error so a wired classifier still invalidates it immediately.
func TestOutboxRelay_UnknownVersionStillRejected(t *testing.T) {
	cfg, _ := kfakeConfig(t)

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

	for _, version := range []string{"0", "3", "99"} {
		t.Run("version="+version, func(t *testing.T) {
			aggregateID := newTestUUIDv7(t)
			payload := legacyEnvelopeJSON(t, "test", "transaction", "created", aggregateID.String())
			payload = []byte(strings.Replace(string(payload), `"version": 1`, `"version": `+version, 1))

			row := &outbox.OutboxEvent{
				ID:          newTestUUIDv7(t),
				EventType:   StreamingOutboxEventType,
				AggregateID: aggregateID,
				Payload:     payload,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := registry.Handle(ctx, row)
			if err == nil {
				t.Fatalf("registry.Handle err = nil for version %s, want rejection", version)
			}

			if !errors.Is(err, contract.ErrInvalidOutboxEnvelope) {
				t.Errorf("err = %v, want ErrInvalidOutboxEnvelope", err)
			}

			if !contract.IsCallerError(err) {
				t.Errorf("contract.IsCallerError(%v) = false; an undispatchable row must stay non-retryable "+
					"so a wired classifier invalidates it instead of burning the whole budget", err)
			}
		})
	}
}
