//go:build unit

package billing_test

import (
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/billing"
)

func TestBilling_Topic(t *testing.T) {
	t.Parallel()

	const want = "lerian.streaming.billing.recorded"
	if billing.Topic != want {
		t.Fatalf("Topic = %q, want %q", billing.Topic, want)
	}
}

func TestBilling_Definition(t *testing.T) {
	t.Parallel()

	def := billing.Definition()

	cases := []struct {
		name string
		got  string
		want string
	}{
		{name: "Key", got: def.Key, want: "billing_recorded"},
		{name: "ResourceType", got: def.ResourceType, want: "billing"},
		{name: "EventType", got: def.EventType, want: "recorded"},
		{name: "DataContentType", got: def.DataContentType, want: "application/vnd.confluent.protobuf"},
		{name: "Description", got: def.Description, want: "Billable usage event for Lago metering"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.got != tc.want {
				t.Fatalf("Definition().%s = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestBilling_Route(t *testing.T) {
	t.Parallel()

	route := billing.Route()

	strCases := []struct {
		name string
		got  string
		want string
	}{
		{name: "Key", got: route.Key, want: "billing-recorded.kafka.primary"},
		{name: "DefinitionKey", got: route.DefinitionKey, want: "billing_recorded"},
		{name: "Target", got: route.Target, want: "primary"},
		{name: "Destination.Name", got: route.Destination.Name, want: billing.Topic},
	}

	for _, tc := range strCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.got != tc.want {
				t.Fatalf("Route().%s = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}

	if route.Destination.Kind != streaming.TransportKafkaLike {
		t.Errorf("Route().Destination.Kind = %q, want %q", route.Destination.Kind, streaming.TransportKafkaLike)
	}

	if route.Requirement != streaming.RouteRequired {
		t.Errorf("Route().Requirement = %q, want %q", route.Requirement, streaming.RouteRequired)
	}
	// DefinitionKey must match the catalog Definition's Key so the route
	// resolves against the event it describes.
	if route.DefinitionKey != billing.Definition().Key {
		t.Errorf("Route().DefinitionKey = %q, want Definition().Key = %q", route.DefinitionKey, billing.Definition().Key)
	}
}

func TestBilling_StringProperty(t *testing.T) {
	t.Parallel()

	pv := billing.StringProperty("br")
	if got := pv.GetStringValue(); got != "br" {
		t.Errorf("StringProperty().GetStringValue() = %q, want %q", got, "br")
	}
	// A string property must not read back as a number.
	if got := pv.GetNumberValue(); got != 0 {
		t.Errorf("StringProperty().GetNumberValue() = %v, want 0", got)
	}
}

func TestBilling_NumberProperty(t *testing.T) {
	t.Parallel()

	pv := billing.NumberProperty(12.5)
	if got := pv.GetNumberValue(); got != 12.5 {
		t.Errorf("NumberProperty().GetNumberValue() = %v, want %v", got, 12.5)
	}
}

// Serialization is provided by the Schema-Registry Confluent-Protobuf
// Serializer; its coverage lives in serializer_test.go.
