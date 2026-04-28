// This file is intentionally tagless: sampleCatalog(tb) and eventToRequest()
// are shared fixtures used by unit, integration, and chaos test suites.
// Adding a build tag here would leave the other suites unable to build. Any
// tag-unused static-analysis findings on these helpers are expected.

package contract

import (
	"encoding/json"
	"testing"
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

func sampleEvent() Event {
	return Event{
		TenantID:      "t-abc",
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
		Source:        "//test/service",
		Subject:       "tx-123",
		Payload:       json.RawMessage(`{"amount":100}`),
	}
}
