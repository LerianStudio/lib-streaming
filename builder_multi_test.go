//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport/fake"
)

// TestBuilder_MultiTargetBuildWithFakeTransports drives the multi-target
// Build path end-to-end using the in-memory fake adapter for two targets
// of different transport kinds. No SDKs, no brokers — purely the wiring
// contract.
func TestBuilder_MultiTargetBuildWithFakeTransports(t *testing.T) {
	t.Parallel()

	primary := fake.NewAdapter(streaming.TransportKafkaLike)
	secondary := fake.NewAdapter(streaming.TransportSQS)

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	primaryRoute := streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
		Requirement:   streaming.RouteRequired,
	}
	secondaryRoute := streaming.RouteDefinition{
		Key:           "transaction.created.sqs.secondary",
		DefinitionKey: "transaction.created",
		Target:        "secondary",
		Destination:   streaming.SQSQueueURL("https://sqs.us-east-1.amazonaws.com/123/q"),
		Requirement:   streaming.RouteOptional,
	}

	emitter, err := streaming.NewBuilder().
		Source("svc://multi-builder-test").
		Catalog(catalog).
		Routes(primaryRoute, secondaryRoute).
		Target(streaming.TargetConfig{Name: "primary", Kind: streaming.TransportKafkaLike, Brokers: []string{"127.0.0.1:9092"}}).
		Target(streaming.TargetConfig{Name: "secondary", Kind: streaming.TransportSQS}).
		RegisterTransport(streaming.TransportKafkaLike, fixedAdapterFactory(primary)).
		RegisterTransport(streaming.TransportSQS, fixedAdapterFactory(secondary)).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	if err := emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       []byte(`{"amount":100}`),
	}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published = %d; want 1", got)
	}
	if got := len(secondary.Messages()); got != 1 {
		t.Fatalf("secondary published = %d; want 1", got)
	}

	if dest := primary.Messages()[0].Destination; dest.Kind != streaming.TransportKafkaLike || dest.Name != "lerian.streaming.transaction.created" {
		t.Fatalf("primary message destination = %#v; want kafka topic", dest)
	}
	if dest := secondary.Messages()[0].Destination; dest.Kind != streaming.TransportSQS {
		t.Fatalf("secondary message destination kind = %q; want SQS", dest.Kind)
	}
}

// TestBuilder_MultiTargetSurfacesRequiredFailure asserts that a required
// route's failure aggregates into a *MultiEmitError when other required
// routes succeed.
func TestBuilder_MultiTargetSurfacesRequiredFailure(t *testing.T) {
	t.Parallel()

	primary := fake.NewAdapter(streaming.TransportKafkaLike)
	secondary := fake.NewAdapter(streaming.TransportKafkaLike)
	secondaryErr := errors.New("secondary down")
	secondary.SetPublishError(secondaryErr)

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	primaryRoute := streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
		Requirement:   streaming.RouteRequired,
	}
	secondaryRoute := streaming.RouteDefinition{
		Key:           "transaction.created.kafka.secondary",
		DefinitionKey: "transaction.created",
		Target:        "secondary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created.replica"),
		Requirement:   streaming.RouteRequired,
	}

	emitter, err := streaming.NewBuilder().
		Source("svc://multi-required-fail").
		Catalog(catalog).
		Routes(primaryRoute, secondaryRoute).
		Target(streaming.TargetConfig{Name: "primary", Kind: streaming.TransportKafkaLike, Brokers: []string{"127.0.0.1:9092"}}).
		Target(streaming.TargetConfig{Name: "secondary", Kind: streaming.TransportKafkaLike, Brokers: []string{"127.0.0.1:9093"}}).
		RegisterTransport(streaming.TransportKafkaLike, namedAdapterFactory(map[string]streaming.TransportAdapter{
			"primary":   primary,
			"secondary": secondary,
		})).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	emitErr := emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       []byte(`{"amount":100}`),
	})
	if emitErr == nil {
		t.Fatal("Emit() error = nil; want MultiEmitError")
	}

	var multi *streaming.MultiEmitError
	if !errors.As(emitErr, &multi) {
		t.Fatalf("Emit() error = %T; want *MultiEmitError (%v)", emitErr, emitErr)
	}
	if !errors.Is(emitErr, secondaryErr) {
		t.Fatalf("errors.Is(Emit, secondaryErr) = false; want true (chain: %v)", emitErr)
	}
	if len(primary.Messages()) != 1 {
		t.Fatalf("primary published = %d; want 1 (sibling success preserved)", len(primary.Messages()))
	}
}

// fixedAdapterFactory returns a TransportAdapterFactory that always returns
// the supplied adapter regardless of options.
func fixedAdapterFactory(adapter streaming.TransportAdapter) streaming.TransportAdapterFactory {
	return func(_ context.Context, _ streaming.TransportAdapterOptions) (streaming.TransportAdapter, error) {
		return adapter, nil
	}
}

// namedAdapterFactory returns a TransportAdapterFactory that selects an
// adapter by target name from the supplied lookup map. Used to give two
// Kafka targets distinct fake adapters in the same test.
func namedAdapterFactory(lookup map[string]streaming.TransportAdapter) streaming.TransportAdapterFactory {
	return func(_ context.Context, opts streaming.TransportAdapterOptions) (streaming.TransportAdapter, error) {
		adapter, ok := lookup[opts.Name]
		if !ok {
			return nil, errors.New("no adapter registered for " + opts.Name)
		}
		return adapter, nil
	}
}
