package producer

import (
	"testing"

	"github.com/google/uuid"
)

func sampleCatalog(tb testing.TB) Catalog {
	tb.Helper()

	catalog, err := NewCatalog(
		EventDefinition{
			Key:             "transaction.created",
			ResourceType:    "transaction",
			EventType:       "created",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
			DataSchema:      "https://schemas.lerian.test/transaction/created.json",
		},
		EventDefinition{
			Key:             "order.submitted",
			ResourceType:    "order",
			EventType:       "submitted",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
		},
		EventDefinition{
			Key:             "ledger.overflow",
			ResourceType:    "ledger",
			EventType:       "overflow",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
		},
		EventDefinition{
			Key:             "payment.authorized",
			ResourceType:    "payment",
			EventType:       "authorized",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
		},
		EventDefinition{
			Key:             "transaction.cb_organic",
			ResourceType:    "transaction",
			EventType:       "cb_organic",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
		},
		EventDefinition{
			Key:             "chaos.event",
			ResourceType:    "chaos",
			EventType:       "event",
			SchemaVersion:   "1.0.0",
			DataContentType: defaultDataContentType,
		},
	)
	if err != nil {
		tb.Fatalf("sampleCatalog: NewCatalog err = %v", err)
	}

	return catalog
}

func eventToRequest(event Event) EmitRequest {
	return EmitRequest{
		DefinitionKey: event.ResourceType + "." + event.EventType,
		TenantID:      event.TenantID,
		Subject:       event.Subject,
		EventID:       event.EventID,
		Timestamp:     event.Timestamp,
		Payload:       event.Payload,
	}
}

func testOutboxEnvelope(event Event, _topic, definitionKey string, policy DeliveryPolicy, aggregateID uuid.UUID) OutboxEnvelope {
	_ = _topic // retained for caller-site readability; envelope derives the destination from the event topic

	return OutboxEnvelope{
		Version:       outboxEnvelopeVersion,
		RouteKey:      canonicalRouteKey(definitionKey) + ".kafka.primary",
		DefinitionKey: definitionKey,
		Target:        "primary",
		Transport:     TransportKafkaLike,
		Destination:   Destination{Kind: TransportKafkaLike, Name: event.Topic()},
		AggregateID:   aggregateID,
		Requirement:   "required",
		Policy:        policy.Normalize(),
		Event:         event,
	}
}
