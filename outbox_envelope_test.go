//go:build unit

package streaming

import (
	"encoding/json"
	"errors"
	"strings"
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

// TestOutboxEnvelope_RoundTrip_PreservesEventID asserts that a populated
// envelope survives a JSON encode/decode cycle byte-for-byte across the
// load-bearing Event fields (EventID, TenantID, Timestamp, Payload). A
// drift here would corrupt replays.
func TestOutboxEnvelope_RoundTrip_PreservesEventID(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults() // populate EventID (UUIDv7), Timestamp, etc.

	original := testOutboxEnvelope(
		event,
		event.Topic(),
		"transaction.created",
		DefaultDeliveryPolicy(),
		uuid.New(),
	)

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal err = %v", err)
	}

	var decoded OutboxEnvelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal err = %v", err)
	}

	if decoded.Event.EventID != original.Event.EventID {
		t.Errorf("EventID round-trip: got %q; want %q", decoded.Event.EventID, original.Event.EventID)
	}

	if decoded.Event.TenantID != original.Event.TenantID {
		t.Errorf("TenantID round-trip: got %q; want %q", decoded.Event.TenantID, original.Event.TenantID)
	}

	if !decoded.Event.Timestamp.Equal(original.Event.Timestamp) {
		t.Errorf("Timestamp round-trip: got %v; want %v", decoded.Event.Timestamp, original.Event.Timestamp)
	}

	if string(decoded.Event.Payload) != string(original.Event.Payload) {
		t.Errorf("Payload round-trip: got %s; want %s", decoded.Event.Payload, original.Event.Payload)
	}
}

// TestOutboxEnvelope_Validate_RejectsZeroVersion asserts that an envelope
// with Version=0 (the legacy unversioned shape) is rejected with an error
// that names the version field.
func TestOutboxEnvelope_Validate_RejectsZeroVersion(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults()

	envelope := OutboxEnvelope{
		Version:     0, // invalid
		Topic:       event.Topic(),
		AggregateID: uuid.New(),
		Policy:      DefaultDeliveryPolicy(),
		Event:       event,
	}

	err := envelope.Validate()
	if err == nil {
		t.Fatal("Validate() err = nil; want non-nil for Version=0")
	}

	if !strings.Contains(err.Error(), "version") {
		t.Errorf("Validate() err = %v; want message mentioning \"version\"", err)
	}
}

// TestOutboxEnvelope_Validate_RejectsEmptyTopic asserts that an envelope
// with an empty Topic is rejected even if the rest of the fields are valid.
func TestOutboxEnvelope_Validate_RejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults()

	envelope := OutboxEnvelope{
		Version:     outboxEnvelopeVersion,
		Topic:       "",
		AggregateID: uuid.New(),
		Policy:      DefaultDeliveryPolicy(),
		Event:       event,
	}

	if err := envelope.Validate(); err == nil {
		t.Fatal("Validate() err = nil; want non-nil for empty Topic")
	}
}

// TestOutboxEnvelope_Validate_RejectsNilAggregateID asserts that an
// envelope with a uuid.Nil AggregateID is rejected — the outbox Dispatcher
// keys replay ordering on AggregateID and a nil value corrupts that.
func TestOutboxEnvelope_Validate_RejectsNilAggregateID(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults()

	envelope := OutboxEnvelope{
		Version:     outboxEnvelopeVersion,
		Topic:       event.Topic(),
		AggregateID: uuid.Nil,
		Policy:      DefaultDeliveryPolicy(),
		Event:       event,
	}

	if err := envelope.Validate(); err == nil {
		t.Fatal("Validate() err = nil; want non-nil for uuid.Nil AggregateID")
	}
}
