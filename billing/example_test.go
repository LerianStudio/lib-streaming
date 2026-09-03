//go:build unit

package billing_test

import (
	"context"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/billing"
)

// Example_billingSerializer is the documented producer-side handoff for the
// "emit produces Confluent-Protobuf bytes" contract. The library provides the
// pieces; the producer wires them: load config, build the hardened Schema
// Registry client, construct the billing Serializer, serialize a payload, and
// hang the resulting Confluent-framed bytes on an EmitRequest for the producer
// to publish.
//
// It has no Output comment on purpose: it is compiled as living documentation
// but not executed, because NewSchemaRegistryClient and NewSerializer expect a
// reachable registry endpoint that a hermetic example cannot assume.
func Example_billingSerializer() {
	ctx := context.Background()

	// 1. Load STREAMING_* configuration (includes STREAMING_SCHEMA_REGISTRY_*).
	cfg, _, err := streaming.LoadConfig()
	if err != nil {
		return
	}

	// 2. Build the Schema Registry client through the public, hardened
	//    constructor (fails closed on empty URL or a partial credential).
	client, err := streaming.NewSchemaRegistryClient(cfg)
	if err != nil {
		return
	}

	// 3. Construct the Serializer — resolves the schema id once, up front.
	serializer, err := billing.NewSerializer(ctx, client)
	if err != nil {
		return
	}

	// 4. Serialize a billable payload into Confluent-framed Protobuf bytes.
	encoded, err := serializer.Serialize(&billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionId: "sub_123",
		Properties: map[string]*billing.PropertyValue{
			"region": billing.StringProperty("br"),
			"count":  billing.NumberProperty(42),
		},
	})
	if err != nil {
		return
	}

	// 5. Hand the bytes to the producer via EmitRequest.Payload. The billing
	//    Definition's DataContentType marks the payload as opaque Confluent
	//    Protobuf, so the produce path ships these bytes verbatim.
	_ = streaming.EmitRequest{
		DefinitionKey: billing.Definition().Key,
		Subject:       "sub_123",
		Payload:       encoded,
	}
}
