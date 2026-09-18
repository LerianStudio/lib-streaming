//go:build integration

package producer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v7/commons"
	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// TestIntegration_LegacyOutboxRowDrainsToApplicationTopic drains a version-1
// outbox row — the shape lib-streaming v2 persisted — through a v4 relay
// against a real Redpanda broker, and asserts the record arrives on the v4
// application topic.
//
// This is the deploy-boundary case the change exists for: a service upgrading
// off lib-streaming v2 with rows still PENDING (the normal state during a
// broker hiccup, which is when deploys happen) used to have every one of those
// rows refused at the version gate and invalidated. The measured exposure on
// br-sfn services/slc is four production estates running OUTBOX_ENABLED=true
// with STREAMING_ENABLED unset, accumulating version-1 rows that would be
// refused the day the flag flips on a v4 binary.
//
// The kfake-backed unit test covers the same path on every CI run; this one
// proves it against a real broker and a real topic-creation path.
func TestIntegration_LegacyOutboxRowDrainsToApplicationTopic(t *testing.T) {
	seed, rpContainer := startRedpanda(t)
	if rpContainer == nil {
		return
	}

	brokers := []string{seed}
	p := newTestProducer(t, brokers)

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, p.RegisterOutboxRelay(registry), "RegisterOutboxRelay")

	appTopic := contract.AppTopic(integrationSource)
	ensureTopics(t, brokers[0], 1, appTopic)

	// The v2-era per-event topic this row names. It is deliberately NOT
	// created on the broker: if the relay regressed to publishing the
	// persisted destination verbatim, the publish would fail loudly here
	// rather than quietly landing somewhere nobody reads.
	legacyTopic := integrationSource + ".payment.authorized"
	require.NotEqual(t, appTopic, legacyTopic, "test would be vacuous")
	assertTopicsAbsent(t, brokers[0], legacyTopic)

	aggregateID := newIntegrationUUIDv7(t)

	// Raw version-1 bytes. Field names and values are identical to a
	// version-2 row (every json tag in the contract package is unchanged
	// since tag v2.1.0, and Event carries none at all); only "version" and
	// the MEANING of destination.name differ.
	payload := []byte(`{
	  "version": 1,
	  "route_key": "primary.kafka",
	  "definition_key": "payment.authorized",
	  "target": "primary",
	  "transport": "kafka",
	  "destination": {"kind": "kafka", "name": "` + legacyTopic + `"},
	  "aggregate_id": "` + aggregateID.String() + `",
	  "requirement": "required",
	  "policy": {"enabled": true, "direct": "direct", "outbox": "fallback_on_circuit_open", "dlq": "on_routable_failure"},
	  "event": {
	    "TenantID": "tenant-legacy-drain",
	    "ResourceType": "payment",
	    "EventType": "authorized",
	    "Source": "` + integrationSource + `",
	    "Subject": "pmt-legacy-1",
	    "SchemaVersion": "1.0.0",
	    "Payload": {"amount": "100.00", "currency": "BRL"}
	  }
	}`)
	require.True(t, json.Valid(payload), "legacy fixture must be valid JSON")

	row := &outbox.OutboxEvent{
		ID:          newIntegrationUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, registry.Handle(ctx, row),
		"a version-1 row must be dispatched by a v4 relay, not refused")

	consumer := newConsumerClient(t, brokers, appTopic)
	records := pollRecords(t, consumer, 1, 30*time.Second)
	require.Len(t, records, 1, "expected the drained legacy row on the application topic")

	got := records[0]
	assert.Equal(t, appTopic, got.Topic,
		"a v1 row must be re-derived onto the topic current consumers subscribe to")

	headers := headerMap(got.Headers)
	assert.Equal(t, integrationSource, headers["ce-source"])
	assert.Equal(t, "payment", headers["ce-resourcetype"])
	assert.Equal(t, "authorized", headers["ce-eventtype"])
	assert.Equal(t, "tenant-legacy-drain", headers["ce-tenantid"],
		"tenant identity travels in the persisted envelope, not the relay's context")

	var body map[string]string
	require.NoError(t, json.Unmarshal(got.Value, &body))
	assert.Equal(t, "100.00", body["amount"], "payload must survive the drain byte-for-byte")
	assert.Equal(t, "BRL", body["currency"])
}

// TestIntegration_UnroutableLegacyRowIsKeptNotInvalidated pins the
// structural-failure posture against a real broker: nothing is published, the
// handler reports failure so the row is never marked PUBLISHED, and the error
// is classified RETRYABLE so the lib-commons dispatcher takes its MarkFailed
// branch instead of sending the row straight to INVALID.
func TestIntegration_UnroutableLegacyRowIsKeptNotInvalidated(t *testing.T) {
	seed, rpContainer := startRedpanda(t)
	if rpContainer == nil {
		return
	}

	brokers := []string{seed}
	p := newTestProducer(t, brokers)

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, p.RegisterOutboxRelay(registry), "RegisterOutboxRelay")

	aggregateID := newIntegrationUUIDv7(t)

	// "//lerian.midaz/transaction-service" is the sanitizer example documented
	// at tag v2.1.0: legal to emit under v2, rejected outright by the current
	// ValidateSource, so no application topic can be derived from it.
	payload := []byte(`{
	  "version": 1,
	  "route_key": "primary.kafka",
	  "definition_key": "payment.authorized",
	  "target": "primary",
	  "transport": "kafka",
	  "destination": {"kind": "kafka", "name": "lerian.midaz-transaction-service.payment.authorized"},
	  "aggregate_id": "` + aggregateID.String() + `",
	  "requirement": "required",
	  "policy": {"enabled": true, "direct": "direct", "outbox": "fallback_on_circuit_open", "dlq": "on_routable_failure"},
	  "event": {
	    "TenantID": "tenant-legacy-drain",
	    "ResourceType": "payment",
	    "EventType": "authorized",
	    "Source": "//lerian.midaz/transaction-service",
	    "Subject": "pmt-legacy-2",
	    "SchemaVersion": "1.0.0",
	    "Payload": {"amount": "100.00"}
	  }
	}`)
	require.True(t, json.Valid(payload), "legacy fixture must be valid JSON")

	row := &outbox.OutboxEvent{
		ID:          newIntegrationUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: aggregateID,
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	err := registry.Handle(ctx, row)
	require.Error(t, err, "returning nil would mark the row PUBLISHED and lose it silently")
	assert.ErrorIs(t, err, contract.ErrLegacyOutboxRowUnroutable)

	assert.False(t, contract.IsCallerError(err),
		"a caller error is what the dispatcher turns into an immediate INVALID; "+
			"this row must survive its retry budget so an operator can rewrite it")

	assert.Contains(t, err.Error(), row.ID.String(),
		"the row id must reach outbox_events.last_error so an operator can find the row")
}

// newIntegrationUUIDv7 mints a time-ordered row/aggregate id. The unit suite's
// equivalent lives behind the `unit` build tag and is not visible here.
func newIntegrationUUIDv7(tb testing.TB) uuid.UUID {
	tb.Helper()

	id, err := commons.GenerateUUIDv7()
	require.NoError(tb, err, "commons.GenerateUUIDv7")

	return id
}
