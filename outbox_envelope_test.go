//go:build unit

package streaming

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func testOutboxEnvelope(event Event, topic, definitionKey string, policy DeliveryPolicy, aggregateID uuid.UUID) OutboxEnvelope {
	return OutboxEnvelope{
		Version:       outboxEnvelopeVersion,
		Topic:         topic,
		DefinitionKey: definitionKey,
		AggregateID:   aggregateID,
		Policy:        policy.Normalize(),
		Event:         event,
	}
}

func TestOutboxEnvelopeRoundTrip(t *testing.T) {
	event := sampleEvent()
	event.ApplyDefaults()
	aggregateID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(event.TenantID))

	envelope := testOutboxEnvelope(
		event,
		event.Topic(),
		"transaction.created",
		DefaultDeliveryPolicy(),
		aggregateID,
	)

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal err = %v", err)
	}

	var got OutboxEnvelope
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("json.Unmarshal err = %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("Validate err = %v", err)
	}

	if got.Version != outboxEnvelopeVersion {
		t.Errorf("Version = %d; want %d", got.Version, outboxEnvelopeVersion)
	}
	if got.Topic != event.Topic() {
		t.Errorf("Topic = %q; want %q", got.Topic, event.Topic())
	}
	if got.DefinitionKey != "transaction.created" {
		t.Errorf("DefinitionKey = %q; want transaction.created", got.DefinitionKey)
	}
	if got.AggregateID != aggregateID {
		t.Errorf("AggregateID = %v; want %v", got.AggregateID, aggregateID)
	}
	if got.Policy != DefaultDeliveryPolicy() {
		t.Errorf("Policy = %+v; want %+v", got.Policy, DefaultDeliveryPolicy())
	}
	if string(got.Event.Payload) != string(event.Payload) {
		t.Errorf("Event.Payload = %s; want %s", got.Event.Payload, event.Payload)
	}
}

func TestOutboxEnvelopeValidateRejectsInvalidPolicy(t *testing.T) {
	envelope := testOutboxEnvelope(
		sampleEvent(),
		"lerian.streaming.transaction.created",
		"transaction.created",
		DeliveryPolicy{Enabled: true, Direct: DirectMode("bogus")},
		uuid.New(),
	)

	if err := envelope.Validate(); !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("Validate err = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

func TestOutboxEnvelopeValidateRejectsTopicMismatch(t *testing.T) {
	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(
		event,
		"lerian.streaming.payment.authorized",
		"transaction.created",
		DefaultDeliveryPolicy(),
		uuid.New(),
	)

	if err := envelope.Validate(); !errors.Is(err, ErrInvalidEventDefinition) {
		t.Fatalf("Validate err = %v; want ErrInvalidEventDefinition", err)
	}
}
