//go:build unit

package streaming_test

import (
	"errors"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
)

func TestFacade_RouteHelpersReturnExpectedDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  streaming.Destination
		want streaming.Destination
	}{
		{
			name: "kafka topic",
			got:  streaming.KafkaTopic("topic-a"),
			want: streaming.Destination{Kind: streaming.TransportKafkaLike, Name: "topic-a"},
		},
		{
			name: "sqs queue url",
			got:  streaming.SQSQueueURL("https://sqs.us-east-1.amazonaws.com/123/q"),
			want: streaming.Destination{Kind: streaming.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
		},
		{
			name: "rabbitmq route",
			got:  streaming.RabbitMQRoute("events", "transaction.created"),
			want: streaming.Destination{Kind: streaming.TransportRabbitMQ, Name: "events", Address: "transaction.created"},
		},
		{
			name: "eventbridge bus",
			got:  streaming.EventBridgeBus("default"),
			want: streaming.Destination{Kind: streaming.TransportEventBridge, Name: "default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got.Kind != tt.want.Kind || tt.got.Name != tt.want.Name || tt.got.Address != tt.want.Address {
				t.Fatalf("helper destination = %#v; want %#v", tt.got, tt.want)
			}
		})
	}
}

func TestFacade_NewRouteTableExportsSentinels(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewRouteTable()
	if !errors.Is(err, streaming.ErrNoRoutesConfigured) {
		t.Fatalf("NewRouteTable() error = %v; want ErrNoRoutesConfigured", err)
	}

	_, err = streaming.NewRouteDefinition(streaming.RouteDefinition{
		Key:           "transaction.created.kafka",
		DefinitionKey: "transaction.created",
		Destination:   streaming.KafkaTopic("topic-a"),
	})
	if !errors.Is(err, streaming.ErrMissingTarget) {
		t.Fatalf("NewRouteDefinition() error = %v; want ErrMissingTarget", err)
	}
}

func TestFacade_NewRouteDefinitionHappyPath(t *testing.T) {
	t.Parallel()

	route, err := streaming.NewRouteDefinition(streaming.RouteDefinition{
		Key:           "transaction.created.kafka",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v", err)
	}
	if route.Requirement != streaming.RouteRequired {
		t.Fatalf("Requirement = %q; want %q", route.Requirement, streaming.RouteRequired)
	}
}

func TestFacade_NewRouteTableHappyPath(t *testing.T) {
	t.Parallel()

	route := streaming.RouteDefinition{
		Key:           "transaction.created.kafka",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
	}

	table, err := streaming.NewRouteTable(route)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("Len() = %d; want 1", table.Len())
	}
}

func TestFacade_RouteSentinelsAreExported(t *testing.T) {
	t.Parallel()

	route := streaming.RouteDefinition{
		Key:           "transaction.created.kafka",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
	}

	_, err := streaming.NewRouteTable(route, route)
	if !errors.Is(err, streaming.ErrDuplicateRouteDefinition) {
		t.Fatalf("NewRouteTable() error = %v; want ErrDuplicateRouteDefinition", err)
	}

	route.Destination = streaming.Destination{Kind: streaming.TransportSQS, Address: "not-a-url"}
	_, err = streaming.NewRouteDefinition(route)
	if !errors.Is(err, streaming.ErrInvalidDestination) {
		t.Fatalf("NewRouteDefinition() error = %v; want ErrInvalidDestination", err)
	}
}

// TestManifestVersion pins the wire-version exported via the public
// facade. A bump in this constant is a wire-version bump and MUST land
// with a CHANGELOG entry plus a downstream consumer migration note.
// This test fails-loud on any silent change.
func TestManifestVersion(t *testing.T) {
	t.Parallel()

	if got := streaming.ManifestVersion; got != "1.0.0" {
		t.Errorf("streaming.ManifestVersion = %q; want %q", got, "1.0.0")
	}
}
