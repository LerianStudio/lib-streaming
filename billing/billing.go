// Package billing defines the shared streaming contract for billable usage
// events. Producers (billing-api) and any downstream tooling import this
// package for the single source of truth on the event's payload shape,
// catalog definition, and transport route — so the wire contract stays
// consistent across every service that emits or consumes it.
package billing

import (
	"encoding/json"
	"fmt"
	"time"

	streaming "github.com/LerianStudio/lib-streaming/v2"
)

// Topic is the Kafka-like topic billable usage events are published to. It is
// also the destination Name carried by Route so the catalog route and the
// physical topic never drift apart.
const Topic = "lerian.streaming.billing.recorded"

// definitionKey is the catalog key that ties Definition and Route together.
// Route.DefinitionKey must equal Definition().Key for the route to resolve
// against the event it describes.
const definitionKey = "billing_recorded"

// BillablePayload is the wire payload for a billable usage event. Field
// serialization mirrors the Lago ingestion contract: metric and subscription
// are always present; timestamp, properties, and the precise amount are
// optional and omitted from the wire when unset.
type BillablePayload struct {
	Metric                  string         `json:"metric"`
	SubscriptionID          string         `json:"subscriptionId"`
	Timestamp               *time.Time     `json:"timestamp,omitempty"`
	Properties              map[string]any `json:"properties,omitempty"`
	PreciseTotalAmountCents *string        `json:"preciseTotalAmountCents,omitempty"`
}

// Definition returns the static catalog contract for the billable usage event.
// It is a plain descriptor value (not a validated constructor result) so it can
// be composed into a streaming.Catalog by the producing service at bootstrap.
func Definition() streaming.EventDefinition {
	return streaming.EventDefinition{
		Key:          definitionKey,
		ResourceType: "billing",
		EventType:    "recorded",
		Description:  "Billable usage event for Lago metering",
	}
}

// Route returns the transport route mapping the billable usage event
// definition to its Kafka-like destination. The destination Name is Topic and
// the route is Required, so a failed publish fails the logical emit rather than
// degrading silently.
func Route() streaming.RouteDefinition {
	return streaming.RouteDefinition{
		Key:           "billing-recorded.kafka.primary",
		DefinitionKey: definitionKey,
		Target:        "primary",
		Destination: streaming.Destination{
			Kind: streaming.TransportKafkaLike,
			Name: Topic,
		},
		Requirement: streaming.RouteRequired,
	}
}

// MustMarshal validates p and returns its JSON encoding, panicking on error.
//
// This is a construction/init-time convenience helper in the spirit of
// regexp.MustCompile: it panics ONLY on a caller bug — a payload that fails
// Validate or a value the standard library cannot encode — both of which are
// deterministic and surface at the first call during development, never as a
// runtime condition on the hot path. Production emit paths that build payloads
// from dynamic input MUST call p.Validate() and json.Marshal directly and
// handle the returned error instead of using MustMarshal.
func MustMarshal(p BillablePayload) json.RawMessage {
	if err := p.Validate(); err != nil {
		panic(fmt.Errorf("billing: MustMarshal on invalid payload: %w", err))
	}

	// Defensive belt-and-suspenders. Validate guarantees marshalability — it
	// rejects the only inputs json.Marshal would choke on (non-finite floats,
	// malformed json.Number, out-of-RFC-3339-range Timestamp) — so this branch
	// is not reachable through a Validate-passing payload and is intentionally
	// left uncovered. It stays as a hard stop against a future construction bug
	// where Validate and the payload shape drift apart.
	raw, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Errorf("billing: MustMarshal encode failed: %w", err))
	}

	return raw
}
