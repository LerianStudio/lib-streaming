// Package billing defines the shared streaming contract for billable usage
// events. Producers (billing-api) and any downstream tooling import this
// package for the single source of truth on the event's payload shape,
// catalog definition, and transport route — so the wire contract stays
// consistent across every service that emits or consumes it.
//
// The payload type is the generated protobuf message
// (billing/gen/lerian/streaming/billing/v1). This package re-exports it under
// the ergonomic name producers already import and adds small constructor
// helpers so callers build the property bag without hand-assembling the proto
// oneof.
package billing

import (
	streaming "github.com/LerianStudio/lib-streaming/v3"
	billingv1 "github.com/LerianStudio/lib-streaming/v3/billing/gen/lerian/streaming/billing/v1"
)

// Topic is the Kafka-like topic billable usage events are published to. It is
// also the destination Name carried by Route so the catalog route and the
// physical topic never drift apart.
const Topic = "lerian.streaming.billing.recorded"

// DataContentType is the CloudEvents datacontenttype for a billable usage event.
// The payload is Confluent-framed binary protobuf (see Serializer), not JSON, so
// Definition advertises this content type: the produce path's JSON-validity
// preflight is content-type-aware and skips the opaque, non-JSON payload,
// shipping the Confluent bytes to the broker verbatim.
const DataContentType = "application/vnd.confluent.protobuf"

// definitionKey is the catalog key that ties Definition and Route together.
// Route.DefinitionKey must equal Definition().Key for the route to resolve
// against the event it describes.
const definitionKey = "billing_recorded"

// BillablePayload is the wire payload for a billable usage event. It is a type
// alias for the generated protobuf message: the generated type IS the wire
// contract, and the alias keeps the ergonomic billing.BillablePayload name that
// producers already import. Serialization mirrors the Lago ingestion contract —
// metric and subscription_id are always present; timestamp, properties, and
// precise_total_amount_cents are optional and omitted from the wire when unset.
type BillablePayload = billingv1.BillablePayload

// PropertyValue is a single Lago metric property value, constrained by the proto
// schema to a string OR a number. It aliases the generated oneof message; build
// one with StringProperty or NumberProperty rather than assembling the oneof by
// hand.
type PropertyValue = billingv1.PropertyValue

// StringProperty builds a string-valued PropertyValue for the properties bag,
// hiding the generated oneof wrapper from callers.
func StringProperty(s string) *PropertyValue {
	return &PropertyValue{Value: &billingv1.PropertyValue_StringValue{StringValue: s}}
}

// NumberProperty builds a number-valued PropertyValue for the properties bag,
// hiding the generated oneof wrapper from callers.
func NumberProperty(n float64) *PropertyValue {
	return &PropertyValue{Value: &billingv1.PropertyValue_NumberValue{NumberValue: n}}
}

// Definition returns the static catalog contract for the billable usage event.
// It is a plain descriptor value (not a validated constructor result) so it can
// be composed into a streaming.Catalog by the producing service at bootstrap.
func Definition() streaming.EventDefinition {
	return streaming.EventDefinition{
		Key:             definitionKey,
		ResourceType:    "billing",
		EventType:       "recorded",
		DataContentType: DataContentType,
		Description:     "Billable usage event for Lago metering",
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
