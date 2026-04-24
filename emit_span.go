package streaming

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

// tracerName is the instrumentation-library name used when the caller did
// not supply a tracer via WithTracer. Matches the per-package convention
// (see github.com/LerianStudio/lib-commons/v5/commons/rabbitmq/rabbitmq.go: otel.Tracer("rabbitmq")) so operators
// can filter on this library in tracing backends.
const tracerName = "streaming"

// emitSpanName is the OTEL span name for each Emit invocation. Stable; a
// rename would break downstream trace filters and should be coordinated
// with TRD §7.2 + every dashboard that keys off this name.
const emitSpanName = "streaming.emit"

// setEmitSpanAttributes sets the TRD §7.2 attribute set on the streaming.emit
// span. Called at span creation before any Execute branch runs so even error
// paths carry the full diagnostic envelope.
//
// messaging.kafka.message.key is emitted only when the package logger has
// debug enabled — keeps partition keys out of high-volume trace stores
// unless explicitly opted in.
//
// tenant.id goes on the SPAN ONLY. Metrics never receive this label (DX-D02).
// This is load-bearing: the automated cardinality test asserts it.
//
// No ctx parameter: span attributes attach via the span's own context; the
// debug check consults the logger directly. A ctx argument here would be
// unused (flagged by linter).
//
// topic is threaded from Emit (already computed once per Emit) so we avoid
// recomputing event.Topic() per span attribute set.
func (p *Producer) setEmitSpanAttributes(span trace.Span, event Event, topic, definitionKey string, policy DeliveryPolicy) {
	if !span.IsRecording() {
		// Fast path for no-op spans: building the attribute slice is pure
		// waste when the backend will drop them all. IsRecording is the
		// sanctioned cheap-check entry point (TRD §7.2 note + otel docs).
		return
	}

	// event.policy encodes the three delivery modes into one attribute so
	// span cardinality stays bounded at 10 keys (down from 12 in v0.2.0 and
	// one fewer than v0.1.0). event.delivery_enabled was dropped entirely
	// because it is always true by the time we set span attributes — the
	// !hasDeliveryPath gate in emit.go short-circuits before the span is
	// ever created.
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", topic),
		// "messaging.operation.type" (modern semconv) — NOT the deprecated
		// "messaging.operation" key. If semconv v1.27 is imported later,
		// replace the literal with semconv.MessagingOperationTypeKey.
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.client.id", p.cfg.ClientID),
		attribute.String("event.resource_type", event.ResourceType),
		attribute.String("event.event_type", event.EventType),
		attribute.String("event.definition_key", definitionKey),
		attribute.String("tenant.id", event.TenantID),
		attribute.String("streaming.producer_id", p.producerID),
		attribute.String("event.policy", formatPolicyAttr(policy)),
	)

	if p.logger.Enabled(log.LevelDebug) {
		partKey := event.PartitionKey()
		if p.partFn != nil {
			partKey = p.partFn(event)
		}

		span.SetAttributes(attribute.String("messaging.kafka.message.key", partKey))
	}
}

// formatPolicyAttr renders the three delivery modes into a single
// "direct:<mode>,outbox:<mode>,dlq:<mode>" string so the span carries one
// attribute instead of three. Operators grep this string instead of joining
// three keys; trace backend cardinality is lower.
func formatPolicyAttr(policy DeliveryPolicy) string {
	return fmt.Sprintf("direct:%s,outbox:%s,dlq:%s", policy.Direct, policy.Outbox, policy.DLQ)
}
