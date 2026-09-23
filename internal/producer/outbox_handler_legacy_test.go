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

	// Tenant identity travels inside the persisted envelope, not in the
	// relay's ambient context — the relay may be draining a row written by a
	// different request, on a different pod, for a different tenant. Asserted
	// here in the CI-resident suite and not only in the container test, so a
	// regression that strips the tenant cannot reach a release on the strength
	// of a Docker-less run.
	if headers["ce-tenantid"] != "t-abc" {
		t.Errorf("ce-tenantid = %q, want %q from the persisted envelope", headers["ce-tenantid"], "t-abc")
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

// newLegacyRelayRegistry builds a kfake-backed producer and registers its
// outbox relay, the shared setup of the preflight-classification tests.
func newLegacyRelayRegistry(t *testing.T) *outbox.HandlerRegistry {
	t.Helper()

	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	registry := outbox.NewHandlerRegistry()
	if err := asProducer(t, emitter).RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	return registry
}

// rewriteLegacyFixture applies one textual edit to a legacy fixture and fails
// loudly if the edit did not land, so a drifted fixture cannot make a test
// assert nothing.
func rewriteLegacyFixture(t *testing.T, payload []byte, old, replacement string) []byte {
	t.Helper()

	if !strings.Contains(string(payload), old) {
		t.Fatalf("fixture rewrite failed: %q not found; the test would assert nothing", old)
	}

	return []byte(strings.Replace(string(payload), old, replacement, 1))
}

func handleLegacyRow(t *testing.T, registry *outbox.HandlerRegistry, payload []byte) error {
	t.Helper()

	aggregateID := newTestUUIDv7(t)
	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return registry.Handle(ctx, row)
}

// TestOutboxRelay_LegacyRowWithInvalidSourceFailingPreflightStaysRetryable
// covers the one preflight refusal that is a v2 -> v4 contract difference: a
// ce-source v2's lossy sanitizer accepted and v4's ValidateSource rejects.
//
// A version-1 KAFKA row with such a source never reaches preflight —
// ResolveDestination refuses it first. A non-Kafka row keeps its persisted
// destination, so preflight is where its source is checked, and that refusal
// must be re-cast exactly like the resolution one. The fixture uses an
// EventBridge destination because its shape validates locally; an SQS queue
// URL is DNS-resolved during envelope validation, which would make the test
// depend on the network before it ever reached preflight.
func TestOutboxRelay_LegacyRowWithInvalidSourceFailingPreflightStaysRetryable(t *testing.T) {
	registry := newLegacyRelayRegistry(t)

	payload := legacyEnvelopeJSON(t, "//lerian.midaz/transaction-service", "transaction", "created", newTestUUIDv7(t).String())
	payload = rewriteLegacyFixture(t, payload,
		`"destination": {"kind": "kafka", "name": "//lerian.midaz/transaction-service.transaction.created"}`,
		`"destination": {"kind": "eventbridge", "name": "lerian-bus"}`)
	payload = rewriteLegacyFixture(t, payload, `"transport": "kafka"`, `"transport": "eventbridge"`)

	err := handleLegacyRow(t, registry, payload)
	if err == nil {
		t.Fatal("registry.Handle err = nil; the row must not be reported as published")
	}

	if !errors.Is(err, contract.ErrLegacyOutboxRowUnroutable) {
		t.Errorf("err = %v, want it to wrap ErrLegacyOutboxRowUnroutable", err)
	}

	if contract.IsCallerError(err) {
		t.Errorf("IsCallerError(%v) = true; the source sentinel must not survive into the chain "+
			"or the dispatcher invalidates the row on attempt one", err)
	}
}

// TestOutboxRelay_LegacyRowFailingNonSourcePreflightIsInvalid pins the
// boundary of the re-cast. v2 enforced the system-event gate, the empty-payload
// and size caps, and the JSON check exactly as v4 does, so a version-1 row
// failing one of them is not a contract difference v4 introduced — it is the
// same permanently-unpublishable row a version-2 row would be, and it must stay
// a caller error so a wired classifier sends it to INVALID immediately instead
// of burning the retry budget under a misleading legacy_unroutable reason.
func TestOutboxRelay_LegacyRowFailingNonSourcePreflightIsInvalid(t *testing.T) {
	registry := newLegacyRelayRegistry(t)

	cases := []struct {
		name      string
		old, repl string
		wantErr   error
	}{
		{
			name:    "system event without opt-in",
			old:     `"Subject": "tx-legacy-1",`,
			repl:    `"Subject": "tx-legacy-1", "SystemEvent": true,`,
			wantErr: ErrSystemEventsNotAllowed,
		},
		{
			name: "empty payload",
			old: `,
	    "Payload": {"amount": 100}`,
			repl:    ``,
			wantErr: contract.ErrEmptyPayload,
		},
		{
			name:    "non-JSON payload under JSON content type",
			old:     `"Payload": {"amount": 100}`,
			repl:    `"PayloadOpaque": "bm90IGpzb24="`,
			wantErr: ErrNotJSON,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := legacyEnvelopeJSON(t, "test", "transaction", "created", newTestUUIDv7(t).String())
			payload = rewriteLegacyFixture(t, payload, tc.old, tc.repl)

			err := handleLegacyRow(t, registry, payload)
			if err == nil {
				t.Fatal("registry.Handle err = nil; the row must not be reported as published")
			}

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want it to wrap %v", err, tc.wantErr)
			}

			if errors.Is(err, contract.ErrLegacyOutboxRowUnroutable) {
				t.Errorf("err = %v wraps ErrLegacyOutboxRowUnroutable; only source errors are re-cast", err)
			}

			if !contract.IsCallerError(err) {
				t.Errorf("IsCallerError(%v) = false; the row would retry instead of landing in INVALID", err)
			}
		})
	}
}

// TestOutboxRelay_RejectionMetricTargetLabelIsBounded pins the cardinality
// guard on streaming_outbox_relay_rejected_total.
//
// ValidateShape checks the envelope VERSION first and returns immediately, so
// a row rejected for its version has had no other field validated — Target is
// whatever bytes the row happened to hold. Passing it straight to a counter
// label would make cardinality accident- or attacker-driven on a metric whose
// godoc promises it is bounded, so an unregistered name is recorded as
// "unknown" while the raw value still reaches the ERROR log.
func TestOutboxRelay_RejectionMetricTargetLabelIsBounded(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	factory, snapshot := newManualMeterSetup(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithMetricsRecorder(factory),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	registry := outbox.NewHandlerRegistry()
	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	// An unknown envelope version, so ValidateShape bails before it ever looks
	// at Target — exactly the path where Target is untrusted.
	const attackerTarget = "attacker-controlled-target-value-0xdeadbeef"

	aggregateID := newTestUUIDv7(t)
	payload := legacyEnvelopeJSON(t, "test", "transaction", "created", aggregateID.String())
	payload = []byte(strings.Replace(string(payload), `"version": 1`, `"version": 77`, 1))
	payload = []byte(strings.Replace(string(payload), `"target": "primary"`, `"target": "`+attackerTarget+`"`, 1))

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := registry.Handle(ctx, row); err == nil {
		t.Fatal("registry.Handle err = nil; an unknown envelope version must be rejected")
	}

	metric, ok := findMetric(snapshot(), metricNameOutboxRelayRejected)
	if !ok {
		t.Fatalf("metric %s was never recorded", metricNameOutboxRelayRejected)
	}

	_, attrSets := sumInt64DataPoints(t, metric)
	if len(attrSets) != 1 {
		t.Fatalf("attribute sets = %d, want exactly 1", len(attrSets))
	}

	got := attrSets[0]
	if got[labelTarget] == attackerTarget {
		t.Errorf("target label = %q; an unvalidated row field must never become a metric label", got[labelTarget])
	}

	if got[labelTarget] != relayTargetUnknownLabel {
		t.Errorf("target label = %q, want %q", got[labelTarget], relayTargetUnknownLabel)
	}

	if got["reason"] != relayRejectVersionUnsupported {
		t.Errorf("reason label = %q, want %q", got["reason"], relayRejectVersionUnsupported)
	}
}

// TestOutboxRelay_RejectionMetricKeepsRegisteredTarget is the other half: a
// target this producer actually registered is operator-controlled and bounded,
// so it must survive as the label rather than collapsing to "unknown".
func TestOutboxRelay_RejectionMetricKeepsRegisteredTarget(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	factory, snapshot := newManualMeterSetup(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithMetricsRecorder(factory),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	if _, ok := p.targets["primary"]; !ok {
		t.Fatal("precondition: the default producer must register a 'primary' target")
	}

	registry := outbox.NewHandlerRegistry()
	if err := p.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay err = %v", err)
	}

	// Legacy row on the registered target, with a source that cannot be
	// re-derived -> legacy_unroutable.
	aggregateID := newTestUUIDv7(t)
	payload := legacyEnvelopeJSON(t, "//lerian.midaz/transaction-service", "transaction", "created", aggregateID.String())

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := registry.Handle(ctx, row); err == nil {
		t.Fatal("registry.Handle err = nil; an unroutable legacy row must be reported")
	}

	metric, ok := findMetric(snapshot(), metricNameOutboxRelayRejected)
	if !ok {
		t.Fatalf("metric %s was never recorded", metricNameOutboxRelayRejected)
	}

	_, attrSets := sumInt64DataPoints(t, metric)
	if len(attrSets) != 1 {
		t.Fatalf("attribute sets = %d, want exactly 1", len(attrSets))
	}

	if got := attrSets[0][labelTarget]; got != "primary" {
		t.Errorf("target label = %q, want the registered target %q", got, "primary")
	}

	if got := attrSets[0]["reason"]; got != relayRejectLegacyUnroutable {
		t.Errorf("reason label = %q, want %q", got, relayRejectLegacyUnroutable)
	}
}

// TestOutboxRelay_UndecodableEnvelopeIsCallerError pins that a row whose
// payload is valid JSON (so it survived the JSONB column) but does not decode
// into an outbox envelope is classified as a caller error. Nothing can ever
// publish it, so a wired classifier must send it to INVALID immediately instead
// of burning the retry budget.
func TestOutboxRelay_UndecodableEnvelopeIsCallerError(t *testing.T) {
	registry := newLegacyRelayRegistry(t)

	for name, payload := range map[string]string{
		"event is not an object":    `{"version": 2, "event": "not an object"}`,
		"version is not a number":   `{"version": "two"}`,
		"envelope is not an object": `["not", "an", "envelope"]`,
	} {
		t.Run(name, func(t *testing.T) {
			err := handleLegacyRow(t, registry, []byte(payload))
			if err == nil {
				t.Fatal("registry.Handle err = nil; the row must not be reported as published")
			}

			if !errors.Is(err, contract.ErrInvalidOutboxEnvelope) {
				t.Errorf("err = %v, want it to wrap ErrInvalidOutboxEnvelope", err)
			}

			if !contract.IsCallerError(err) {
				t.Errorf("IsCallerError(%v) = false; the row would retry instead of landing in INVALID", err)
			}

			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				t.Errorf("err = %v, want the decode error kept in the chain", err)
			}
		})
	}
}
