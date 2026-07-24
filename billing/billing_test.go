//go:build unit

package billing_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/lib-streaming/v2/billing"
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

func TestBilling_MustMarshal_RoundTrips(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, time.July, 23, 10, 30, 0, 0, time.UTC)
	cents := "12345"
	payload := billing.BillablePayload{
		Metric:                  "api_calls",
		SubscriptionID:          "sub_123",
		Timestamp:               &ts,
		Properties:              map[string]any{"region": "br"},
		PreciseTotalAmountCents: &cents,
	}

	raw := billing.MustMarshal(payload)

	var got billing.BillablePayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if got.Metric != payload.Metric {
		t.Errorf("Metric = %q, want %q", got.Metric, payload.Metric)
	}

	if got.SubscriptionID != payload.SubscriptionID {
		t.Errorf("SubscriptionID = %q, want %q", got.SubscriptionID, payload.SubscriptionID)
	}

	if got.Timestamp == nil || !got.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, ts)
	}

	if got.PreciseTotalAmountCents == nil || *got.PreciseTotalAmountCents != cents {
		t.Errorf("PreciseTotalAmountCents = %v, want %q", got.PreciseTotalAmountCents, cents)
	}

	if !reflect.DeepEqual(got.Properties, payload.Properties) {
		t.Errorf("Properties = %#v, want %#v", got.Properties, payload.Properties)
	}
}

func TestBilling_MustMarshal_PanicsOnInvalidJSONNumber(t *testing.T) {
	t.Parallel()

	// An invalid json.Number literal is rejected by Validate (which now
	// guarantees marshalability), so MustMarshal panics at the Validate gate.
	// This locks that MustMarshal refuses a payload json.Marshal would choke
	// on rather than emitting a broken record.
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("MustMarshal did not panic on an invalid json.Number payload")
		}
	}()

	_ = billing.MustMarshal(billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionID: "sub_123",
		Properties:     map[string]any{"bad": json.Number("not-a-number")},
	})
}

func TestBilling_MustMarshal_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()

	raw := billing.MustMarshal(billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionID: "sub_123",
	})

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	for _, key := range []string{"timestamp", "properties", "preciseTotalAmountCents"} {
		if _, present := envelope[key]; present {
			t.Errorf("expected optional field %q to be omitted, but it was present", key)
		}
	}

	if _, present := envelope["metric"]; !present {
		t.Errorf("expected required field %q to be present", "metric")
	}

	if _, present := envelope["subscriptionId"]; !present {
		t.Errorf("expected required field %q to be present", "subscriptionId")
	}
}
