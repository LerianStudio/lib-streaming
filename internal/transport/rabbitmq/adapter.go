// Package rabbitmq ships a TransportAdapter that publishes lib-streaming
// CloudEvents-shaped messages to RabbitMQ via a caller-supplied publisher.
//
// IMPORTANT — events-only.
// lib-streaming is a producer for *business events* meant for third-party /
// SaaS subscribers. RabbitMQ remains the recommended primitive for *internal
// command queues*, which continue to live on
// `github.com/LerianStudio/lib-commons/v5/commons/rabbitmq`. The two are
// orthogonal — neither replaces the other.
//
// The adapter does NOT depend on streadway/amqp or amqp091-go. Callers
// fulfill the RabbitMQPublisher interface with whichever AMQP client their
// service already uses.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// DefaultContentType is the wire content-type the adapter sends when the
// CloudEvents envelope does not carry one. Matches the lib-streaming
// payload contract (always JSON).
const DefaultContentType = "application/json"

// RabbitMQPublisher is the minimal interface the adapter requires from a
// caller-owned AMQP client. Implementations are typically a small wrapper
// around amqp.Channel.PublishWithContext that pins delivery mode (durable
// queues) and confirms.
//
// Implementations MUST treat headers as a write-once map: the adapter
// builds a fresh map per call and does not retain a reference. body is a
// fresh slice the adapter does not retain.
type RabbitMQPublisher interface {
	Publish(ctx context.Context, exchange, routingKey, contentType string, body []byte, headers map[string]any) error
}

// RabbitMQPingClient is the capability Adapter.Healthy requires. It is kept
// separate from RabbitMQPublisher so caller-owned AMQP wrappers can expose the
// cheapest meaningful probe for their connection/channel lifecycle.
type RabbitMQPingClient interface {
	Ping(ctx context.Context) error
}

// Adapter publishes business events to a RabbitMQ exchange via the supplied
// RabbitMQPublisher.
type Adapter struct {
	publisher RabbitMQPublisher
}

// New constructs a RabbitMQ Adapter bound to the supplied publisher.
//
// New rejects both untyped nil AND typed-nil interface values (e.g.
// `var p *fakePublisher; New(p)`). Without the typed-nil guard, a
// statically-typed nil would survive the `publisher == nil` check and
// the adapter would NPE on the first Publish call.
func New(publisher RabbitMQPublisher) (*Adapter, error) {
	if transport.IsNilInterface(publisher) {
		return nil, fmt.Errorf("%w: rabbitmq: nil publisher", contract.ErrNilProducer)
	}

	return &Adapter{publisher: publisher}, nil
}

// Kind reports the transport kind implemented by this adapter.
func (*Adapter) Kind() contract.TransportKind { return contract.TransportRabbitMQ }

// Publish sends one CloudEvents-binary-mode message to a RabbitMQ exchange.
//
// Destination shape is exchange (Name) plus routing key (Address). Both are
// required. Full contract.Destination.Validate() runs once at construction
// time inside NewRouteDefinition; the per-Publish hot path drops the
// revalidation (and its embedded SSRF DNS lookup for URL-shaped Address
// values) and keeps only the cheap kind-mismatch + empty-field guards.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	if a == nil || transport.IsNilInterface(a.publisher) {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if message.Destination.Kind != contract.TransportRabbitMQ {
		return fmt.Errorf("%w: rabbitmq adapter destination kind=%q", contract.ErrInvalidDestination, message.Destination.Kind)
	}

	exchange := message.Destination.Name
	routingKey := message.Destination.Address

	if exchange == "" {
		return fmt.Errorf("%w: rabbitmq exchange name required", contract.ErrInvalidDestination)
	}

	if routingKey == "" {
		return fmt.Errorf("%w: rabbitmq routing key required", contract.ErrInvalidDestination)
	}

	headers := buildHeaders(message)
	contentType := pickContentType(message)

	return a.publisher.Publish(ctx, exchange, routingKey, contentType, message.Payload, headers)
}

// Healthy delegates to RabbitMQPingClient. Publishers without Ping fail closed
// so readiness cannot report healthy without a real probe.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil || transport.IsNilInterface(a.publisher) {
		return contract.ErrNilProducer
	}

	pinger, ok := a.publisher.(RabbitMQPingClient)
	if !ok || transport.IsNilInterface(pinger) {
		return errors.New("rabbitmq: health check requires publisher implementing RabbitMQPingClient")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return pinger.Ping(ctx)
}

// Flush is a no-op. Confirm-mode batching belongs to the publisher.
func (*Adapter) Flush(context.Context) error { return nil }

// Close is a no-op. The connection lifetime is owned by the caller.
func (*Adapter) Close(context.Context) error { return nil }

// Classify maps common RabbitMQ / AMQP errors to streaming error classes.
func (*Adapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ClassContextCanceled
	}

	if errors.Is(err, contract.ErrPayloadTooLarge) || errors.Is(err, contract.ErrInvalidDestination) {
		return contract.ClassValidation
	}

	if contract.IsCallerError(err) {
		return contract.ClassValidation
	}

	msg := strings.ToLower(err.Error())

	switch {
	case containsAny(msg, "no_route", "no route", "not_found", "not found", "no exchange", "no queue"):
		return contract.ClassTopicNotFound
	case containsAny(msg, "access_refused", "access refused", "auth", "unauthorized", "permission denied"):
		return contract.ClassAuth
	case containsAny(msg, "resource_locked", "precondition_failed", "precondition failed", "channel closed"):
		return contract.ClassBrokerOverloaded
	case containsAny(msg, "timeout", "timed out", "deadline exceeded"):
		return contract.ClassNetworkTimeout
	}

	return contract.ClassBrokerUnavailable
}

func containsAny(msg string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
}

// buildHeaders flattens caller attributes + CloudEvents headers into the
// AMQP headers table. CloudEvents headers (binary mode) carry the envelope;
// they always win on key collisions.
func buildHeaders(message transport.TransportMessage) map[string]any {
	if len(message.Attributes) == 0 && len(message.Headers) == 0 && message.TenantID == "" {
		return nil
	}

	headers := make(map[string]any, len(message.Attributes)+len(message.Headers)+1)

	for k, v := range message.Attributes {
		headers[k] = v
	}

	for _, h := range message.Headers {
		// AMQP headers are typed; we send byte slices verbatim so the
		// CloudEvents binary representation is preserved on the wire.
		headers[h.Key] = append([]byte(nil), h.Value...)
	}

	if message.TenantID != "" {
		headers["X-Tenant-ID"] = message.TenantID
	}

	return headers
}

// pickContentType prefers a CloudEvents-supplied content-type
// (ce-datacontenttype header) when present, otherwise falls back to the
// adapter default. The CloudEvents header is the source of truth for
// payload encoding; lib-streaming today only emits JSON, but we surface
// the header so future content types Just Work.
func pickContentType(message transport.TransportMessage) string {
	for _, h := range message.Headers {
		if strings.EqualFold(h.Key, "content-type") || strings.EqualFold(h.Key, "ce-datacontenttype") {
			if len(h.Value) > 0 {
				return string(h.Value)
			}
		}
	}

	return DefaultContentType
}
