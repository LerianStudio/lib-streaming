package streaming

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-streaming/internal/transport"
	"github.com/LerianStudio/lib-streaming/internal/transport/eventbridge"
	"github.com/LerianStudio/lib-streaming/internal/transport/rabbitmq"
	"github.com/LerianStudio/lib-streaming/internal/transport/sqs"
)

// SQSPublisherClient is the caller-supplied client interface used by the
// built-in SQS adapter. Implementations adapt aws-sdk-go-v2 (or any other
// SDK) and own the per-message AWS API call.
//
// See package documentation for the canonical attribute encoding the
// adapter forwards on every SendMessage call.
type SQSPublisherClient = sqs.SQSPublisherClient

// SQSPingClient is the optional health capability production SQS clients
// should implement. SQS Adapter.Healthy fails closed when the caller-supplied
// client does not expose this probe.
type SQSPingClient = sqs.SQSPingClient

// SQSAttribute is one (key, value) message attribute the adapter passes to
// SendMessage. Values are bytes so binary-mode CloudEvents headers stay
// intact through the adapter -> SDK boundary.
type SQSAttribute = sqs.Attribute

// SQSAdapter constructs a built-in SQS TransportAdapter bound to the
// supplied client. defaultQueueURL is used only when the route's
// Destination.Address is empty; an explicit Destination always wins.
//
// The adapter rejects SQS wire messages larger than 256 KiB (body plus String
// message attributes) with ErrPayloadTooLarge before issuing any network call.
func SQSAdapter(client SQSPublisherClient, defaultQueueURL string) (TransportAdapter, error) {
	return sqs.New(client, defaultQueueURL)
}

// sqsHelperBinding is the per-target wiring captured by SQSTarget. Stored
// in Builder.sqsHelpers keyed on target name so a single shared closure
// registered against TransportSQS can resolve the right (client,
// defaultQueueURL) pair at adapter-construction time.
//
// Without this indirection, a Builder calling SQSTarget twice (e.g.
// "us-east" and "us-west" with different clients) would have the SECOND
// call's RegisterTransport closure overwrite the first's, causing BOTH
// targets to publish through the second client's wiring at Build time.
// See HIGH #1 in the Builder API correctness audit.
type sqsHelperBinding struct {
	client          SQSPublisherClient
	defaultQueueURL string
}

// rabbitmqHelperBinding mirrors sqsHelperBinding for RabbitMQ targets.
type rabbitmqHelperBinding struct {
	publisher RabbitMQPublisher
}

// eventbridgeHelperBinding mirrors sqsHelperBinding for EventBridge targets.
type eventbridgeHelperBinding struct {
	client EventBridgePutEventsClient
}

// SQSTarget appends an SQS target plus a registration of the SQS transport
// factory so Builder.Build wires the adapter automatically. The factory
// constructs one adapter per target, looking up the registered client by
// target name so multiple SQSTarget calls with different clients on the
// same Builder all dispatch correctly.
//
// Use this helper when you have a small number of SQS clients and named
// targets. Calling SQSTarget twice with the same name is treated as a
// wiring error and surfaces at Build time. For multi-account / cross-
// region setups with many distinct clients, register a custom factory via
// Builder.RegisterTransport directly.
func (b *Builder) SQSTarget(name string, client SQSPublisherClient, defaultQueueURL string) *Builder {
	if b == nil {
		return b
	}

	b.Target(TargetConfig{Name: name, Kind: TransportSQS})

	if b.sqsHelpers == nil {
		b.sqsHelpers = make(map[string]sqsHelperBinding)
	}

	b.sqsHelpers[name] = sqsHelperBinding{client: client, defaultQueueURL: defaultQueueURL}

	// Idempotent: the closure resolves bindings by opts.Name at adapter
	// construction time (not by capture-at-registration), so repeated
	// SQSTarget calls would re-register a functionally-identical
	// closure. Skip when a TransportSQS factory is already registered.
	if !b.hasRegisteredTransportFactory(TransportSQS) {
		b.RegisterTransport(TransportSQS, func(_ context.Context, opts TransportAdapterOptions) (TransportAdapter, error) {
			binding, ok := b.sqsHelpers[opts.Name]
			if !ok {
				return nil, fmt.Errorf("%w: sqs target %q has no registered helper binding", ErrMultiTransportRuntimeNotConfigured, opts.Name)
			}

			// Reject typed-nil here in addition to untyped nil so a caller
			// passing `var c *FakeClient` cannot smuggle a NPE-ready adapter
			// past the factory. sqs.New also guards via IsNilInterface, but
			// failing fast at the Builder layer surfaces the wiring bug at
			// Build() time with the precise target name.
			if transport.IsNilInterface(binding.client) {
				return nil, fmt.Errorf("%w: sqs target %q requires a non-nil client", ErrNilProducer, opts.Name)
			}

			return sqs.New(binding.client, binding.defaultQueueURL)
		})
	}

	return b
}

// RabbitMQPublisher is the caller-supplied publisher interface used by the
// built-in RabbitMQ adapter. Implementations are typically a small wrapper
// around amqp.Channel.PublishWithContext (or amqp091-go) that pins delivery
// mode and confirms.
//
// The lib-streaming RabbitMQ transport is for *business events* aimed at
// third-party / SaaS subscribers. Internal command queues remain on
// `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`.
type RabbitMQPublisher = rabbitmq.RabbitMQPublisher

// RabbitMQPingClient is the optional health capability production RabbitMQ
// publishers should implement. RabbitMQ Adapter.Healthy fails closed when the
// caller-supplied publisher does not expose this probe.
type RabbitMQPingClient = rabbitmq.RabbitMQPingClient

// RabbitMQAdapter constructs a built-in RabbitMQ TransportAdapter bound to
// the supplied publisher.
func RabbitMQAdapter(publisher RabbitMQPublisher) (TransportAdapter, error) {
	return rabbitmq.New(publisher)
}

// RabbitMQTarget appends a RabbitMQ target plus a registration of the
// RabbitMQ transport factory so Builder.Build wires the adapter
// automatically. Multiple RabbitMQTarget calls on the same Builder are
// resolved per-target-name at construction time — see SQSTarget for the
// rationale.
func (b *Builder) RabbitMQTarget(name string, publisher RabbitMQPublisher) *Builder {
	if b == nil {
		return b
	}

	b.Target(TargetConfig{Name: name, Kind: TransportRabbitMQ})

	if b.rabbitmqHelpers == nil {
		b.rabbitmqHelpers = make(map[string]rabbitmqHelperBinding)
	}

	b.rabbitmqHelpers[name] = rabbitmqHelperBinding{publisher: publisher}

	// Idempotent: same name-keyed-resolution rationale as SQSTarget.
	if !b.hasRegisteredTransportFactory(TransportRabbitMQ) {
		b.RegisterTransport(TransportRabbitMQ, func(_ context.Context, opts TransportAdapterOptions) (TransportAdapter, error) {
			binding, ok := b.rabbitmqHelpers[opts.Name]
			if !ok {
				return nil, fmt.Errorf("%w: rabbitmq target %q has no registered helper binding", ErrMultiTransportRuntimeNotConfigured, opts.Name)
			}

			// Typed-nil + untyped nil guard. See SQSTarget for rationale.
			if transport.IsNilInterface(binding.publisher) {
				return nil, fmt.Errorf("%w: rabbitmq target %q requires a non-nil publisher", ErrNilProducer, opts.Name)
			}

			return rabbitmq.New(binding.publisher)
		})
	}

	return b
}

// EventBridgePutEventsClient is the caller-supplied client interface used
// by the built-in EventBridge adapter.
type EventBridgePutEventsClient = eventbridge.EventBridgePutEventsClient

// EventBridgePingClient is the optional health capability production
// EventBridge clients should implement. EventBridge Adapter.Healthy fails
// closed when the caller-supplied client does not expose this probe.
type EventBridgePingClient = eventbridge.EventBridgePingClient

// EventBridgePutEventsResultClient is an optional capability EventBridge
// clients can implement to let the adapter detect per-entry PutEvents failures
// when the provider call itself returns nil.
type EventBridgePutEventsResultClient = eventbridge.EventBridgePutEventsResultClient

// EventBridgeEntry is one PutEvents entry the adapter constructs from a
// CloudEvents-binary-mode message. See package documentation for the
// canonical Detail JSON shape.
type EventBridgeEntry = eventbridge.Entry

// EventBridgePutEventsResult is the SDK-neutral PutEvents result returned by
// EventBridgePutEventsResultClient.
type EventBridgePutEventsResult = eventbridge.PutEventsResult

// EventBridgePutEventsEntryResult is one per-entry EventBridge PutEvents result.
type EventBridgePutEventsEntryResult = eventbridge.PutEventsEntryResult

// EventBridgeAdapter constructs a built-in EventBridge TransportAdapter
// bound to the supplied client.
func EventBridgeAdapter(client EventBridgePutEventsClient) (TransportAdapter, error) {
	return eventbridge.New(client)
}

// EventBridgeTarget appends an EventBridge target plus a registration of
// the EventBridge transport factory so Builder.Build wires the adapter
// automatically. Multiple EventBridgeTarget calls on the same Builder are
// resolved per-target-name at construction time — see SQSTarget for the
// rationale.
func (b *Builder) EventBridgeTarget(name string, client EventBridgePutEventsClient) *Builder {
	if b == nil {
		return b
	}

	b.Target(TargetConfig{Name: name, Kind: TransportEventBridge})

	if b.eventbridgeHelpers == nil {
		b.eventbridgeHelpers = make(map[string]eventbridgeHelperBinding)
	}

	b.eventbridgeHelpers[name] = eventbridgeHelperBinding{client: client}

	// Idempotent: same name-keyed-resolution rationale as SQSTarget.
	if !b.hasRegisteredTransportFactory(TransportEventBridge) {
		b.RegisterTransport(TransportEventBridge, func(_ context.Context, opts TransportAdapterOptions) (TransportAdapter, error) {
			binding, ok := b.eventbridgeHelpers[opts.Name]
			if !ok {
				return nil, fmt.Errorf("%w: eventbridge target %q has no registered helper binding", ErrMultiTransportRuntimeNotConfigured, opts.Name)
			}

			// Typed-nil + untyped nil guard. See SQSTarget for rationale.
			if transport.IsNilInterface(binding.client) {
				return nil, fmt.Errorf("%w: eventbridge target %q requires a non-nil client", ErrNilProducer, opts.Name)
			}

			return eventbridge.New(binding.client)
		})
	}

	return b
}
