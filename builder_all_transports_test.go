//go:build unit

package streaming_test

import (
	"context"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport/fake"
)

// fakeSQSClient is a test double for streaming.SQSPublisherClient that
// records every SendMessage call.
type fakeSQSClient struct {
	calls int
}

func (f *fakeSQSClient) SendMessage(_ context.Context, _ string, _ []byte, _ []streaming.SQSAttribute) error {
	f.calls++
	return nil
}

// fakeRabbitMQPublisher is a test double for streaming.RabbitMQPublisher.
type fakeRabbitMQPublisher struct {
	calls int
}

func (f *fakeRabbitMQPublisher) Publish(_ context.Context, _, _, _ string, _ []byte, _ map[string]any) error {
	f.calls++
	return nil
}

// fakeEventBridgeClient is a test double for streaming.EventBridgePutEventsClient.
type fakeEventBridgeClient struct {
	calls int
}

func (f *fakeEventBridgeClient) PutEvents(_ context.Context, entries []streaming.EventBridgeEntry) error {
	f.calls += len(entries)
	return nil
}

// TestBuilder_AllFourTransports_Build_Emit_AllTargetsReceiveMessage drives
// a Builder with all four transport kinds wired through the public
// SQSTarget / RabbitMQTarget / EventBridgeTarget helpers (plus a Kafka
// target via fake transport). One Emit call, four targets, four publishes
// — the wiring contract must be honored end to end without any SDK
// dependency.
func TestBuilder_AllFourTransports_Build_Emit_AllTargetsReceiveMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	routes := []streaming.RouteDefinition{
		{
			Key:           "transaction.created.kafka.primary",
			DefinitionKey: "transaction.created",
			Target:        "kafka-primary",
			Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
		},
		{
			Key:           "transaction.created.sqs.shadow",
			DefinitionKey: "transaction.created",
			Target:        "sqs-shadow",
			Destination:   streaming.SQSQueueURL("https://sqs.us-east-1.amazonaws.com/123/q"),
			Requirement:   streaming.RouteOptional,
		},
		{
			Key:           "transaction.created.rabbitmq.bus",
			DefinitionKey: "transaction.created",
			Target:        "rabbitmq-bus",
			Destination:   streaming.RabbitMQRoute("events", "tx.created"),
			Requirement:   streaming.RouteOptional,
		},
		{
			Key:           "transaction.created.eventbridge.bus",
			DefinitionKey: "transaction.created",
			Target:        "eventbridge-bus",
			Destination:   streaming.EventBridgeBus("default-bus"),
			Requirement:   streaming.RouteOptional,
		},
	}

	kafkaAdapter := fake.NewAdapter(streaming.TransportKafkaLike)
	sqsClient := &fakeSQSClient{}
	rabbitClient := &fakeRabbitMQPublisher{}
	ebClient := &fakeEventBridgeClient{}

	emitter, err := streaming.NewBuilder().
		Source("svc://all-transports").
		Catalog(catalog).
		Routes(routes...).
		Target(streaming.TargetConfig{Name: "kafka-primary", Kind: streaming.TransportKafkaLike, Brokers: []string{"127.0.0.1:9092"}}).
		RegisterTransport(streaming.TransportKafkaLike, fixedAdapterFactory(kafkaAdapter)).
		SQSTarget("sqs-shadow", sqsClient, "https://sqs.us-east-1.amazonaws.com/123/q").
		RabbitMQTarget("rabbitmq-bus", rabbitClient).
		EventBridgeTarget("eventbridge-bus", ebClient).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	if err := emitter.Emit(ctx, streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       []byte(`{"amount":100}`),
	}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if got := len(kafkaAdapter.Messages()); got != 1 {
		t.Errorf("kafka publishes = %d; want 1", got)
	}
	if sqsClient.calls != 1 {
		t.Errorf("sqs calls = %d; want 1", sqsClient.calls)
	}
	if rabbitClient.calls != 1 {
		t.Errorf("rabbitmq calls = %d; want 1", rabbitClient.calls)
	}
	if ebClient.calls != 1 {
		t.Errorf("eventbridge calls = %d; want 1", ebClient.calls)
	}
}
