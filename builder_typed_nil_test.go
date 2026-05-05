//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/internal/producer"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// The three tests below pin a single contract — and a single notorious Go
// gotcha — across the public Builder.SQSTarget / RabbitMQTarget /
// EventBridgeTarget helpers:
//
//	var c *fakeSQSClient // typed-nil pointer
//	streaming.NewBuilder().SQSTarget("name", c, "https://...").Build(ctx)
//
// The interface header carries a non-nil type tag so a naive `client == nil`
// check returns FALSE — yet calling any method on the wrapped value would
// NPE. The Builder must reject typed-nil at Build time so the failure
// surfaces with the precise target name and the documented ErrNilProducer
// sentinel, not as a runtime crash on first Emit.
//
// We assert errors.Is(err, ErrNilProducer) — the chain wraps the helper's
// fmt.Errorf("%w: …, ErrNilProducer)" so the sentinel is reachable through
// the chain regardless of how the Builder surfaces factory failures.

// TestBuilder_SQSTarget_RejectsTypedNilClient ensures a typed-nil
// SQSPublisherClient surfaces as ErrNilProducer at Build time instead of
// constructing an emitter that will NPE on first Emit.
func TestBuilder_SQSTarget_RejectsTypedNilClient(t *testing.T) {
	t.Parallel()

	catalog := mustCatalogForTypedNil(t)

	var c *fakeSQSClient // typed-nil

	_, err := streaming.NewBuilder().
		Source("svc://typed-nil-sqs").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.sqs.shadow",
			DefinitionKey: "transaction.created",
			Target:        "sqs-shadow",
			Destination:   streaming.SQSQueueURL("https://sqs.us-east-1.amazonaws.com/123/q"),
		}).
		SQSTarget("sqs-shadow", c, "https://sqs.us-east-1.amazonaws.com/123/q").
		Build(context.Background())

	if !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("Build() with typed-nil SQS client error = %v; want errors.Is(ErrNilProducer)", err)
	}
}

// TestBuilder_RabbitMQTarget_RejectsTypedNilPublisher mirrors the SQS test
// for the RabbitMQ events-only helper.
func TestBuilder_RabbitMQTarget_RejectsTypedNilPublisher(t *testing.T) {
	t.Parallel()

	catalog := mustCatalogForTypedNil(t)

	var p *fakeRabbitMQPublisher

	_, err := streaming.NewBuilder().
		Source("svc://typed-nil-rabbit").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.rabbitmq.bus",
			DefinitionKey: "transaction.created",
			Target:        "rabbitmq-bus",
			Destination:   streaming.RabbitMQRoute("events", "tx.created"),
		}).
		RabbitMQTarget("rabbitmq-bus", p).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("Build() with typed-nil RabbitMQ publisher error = %v; want errors.Is(ErrNilProducer)", err)
	}
}

// TestBuilder_EventBridgeTarget_RejectsTypedNilClient mirrors the SQS test
// for the EventBridge helper.
func TestBuilder_EventBridgeTarget_RejectsTypedNilClient(t *testing.T) {
	t.Parallel()

	catalog := mustCatalogForTypedNil(t)

	var c *fakeEventBridgeClient

	_, err := streaming.NewBuilder().
		Source("svc://typed-nil-eb").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.eventbridge.bus",
			DefinitionKey: "transaction.created",
			Target:        "eventbridge-bus",
			Destination:   streaming.EventBridgeBus("default-bus"),
		}).
		EventBridgeTarget("eventbridge-bus", c).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("Build() with typed-nil EventBridge client error = %v; want errors.Is(ErrNilProducer)", err)
	}
}

// mustCatalogForTypedNil returns a minimal catalog with the single
// "transaction.created" definition the typed-nil tests above route on.
func mustCatalogForTypedNil(t *testing.T) streaming.Catalog {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

// fakeAdapterForTypedNil is a minimal adapter type used purely so a
// typed-nil pointer can be returned from a TransportAdapterFactory. The
// methods are unreachable in this test (Build rejects the typed-nil
// before any method is called); they exist only to satisfy the
// transport.TransportAdapter interface at compile time.
type fakeAdapterForTypedNil struct{}

func (f *fakeAdapterForTypedNil) Kind() streaming.TransportKind { return streaming.TransportCustom }
func (f *fakeAdapterForTypedNil) Publish(context.Context, transport.TransportMessage) error {
	return nil
}
func (f *fakeAdapterForTypedNil) Healthy(context.Context) error { return nil }
func (f *fakeAdapterForTypedNil) Flush(context.Context) error   { return nil }
func (f *fakeAdapterForTypedNil) Close(context.Context) error   { return nil }
func (f *fakeAdapterForTypedNil) Classify(error) streaming.ErrorClass {
	return streaming.ClassBrokerUnavailable
}

// TestBuilder_RegisterTransport_FactoryReturnsTypedNil_RejectedAtBuild
// pins the most adversarial typed-nil case: a custom transport factory
// registered via RegisterTransport returns a typed-nil adapter. The
// Builder's buildTargetSpecs MUST detect this through
// transport.IsNilInterface (NOT a naive `adapter == nil` check, which
// would miss typed-nil) and surface
// ErrMultiTransportRuntimeNotConfigured.
//
// Without this gate, the constructed Producer would store a typed-nil
// adapter in its targets map and NPE on the first Emit's Publish call.
//
// The sentinel used here is ErrMultiTransportRuntimeNotConfigured (per
// builder.go:636) — different from the SQS / RabbitMQ / EventBridge
// helper typed-nil tests above, which surface ErrNilProducer at a
// different code site in the Builder. The two paths classify the same
// gotcha through different lenses by design: the helper paths reject
// the typed-nil CLIENT before factory invocation, while the generic
// RegisterTransport path can only inspect the adapter the factory
// returned, by which point the misconfiguration is "runtime not
// configured" rather than "nil producer".
func TestBuilder_RegisterTransport_FactoryReturnsTypedNil_RejectedAtBuild(t *testing.T) {
	t.Parallel()

	catalog := mustCatalogForTypedNil(t)

	_, err := streaming.NewBuilder().
		Source("svc://typed-nil-custom").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.custom.bus",
			DefinitionKey: "transaction.created",
			Target:        "custom-target",
			Destination: streaming.Destination{
				Kind: streaming.TransportCustom,
				Name: "custom-sink",
			},
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:    "custom-target",
			Kind:    streaming.TransportCustom,
			Brokers: []string{"localhost:9999"},
		}).
		RegisterTransport(streaming.TransportCustom, func(_ context.Context, _ producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
			// The notorious typed-nil case: declare a *fakeAdapterForTypedNil,
			// leave it nil, return it as transport.TransportAdapter. The
			// interface header carries the concrete type pointer so a
			// naive nil-check would FALSE-PASS this; only IsNilInterface
			// catches it.
			var c *fakeAdapterForTypedNil
			return c, nil
		}).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrMultiTransportRuntimeNotConfigured) {
		t.Fatalf("Build() with typed-nil custom adapter error = %v; want errors.Is(ErrMultiTransportRuntimeNotConfigured)", err)
	}
}
