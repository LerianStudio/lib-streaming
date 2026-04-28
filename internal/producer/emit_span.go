package producer

import (
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
// tenant.id goes on the SPAN ONLY (never on metrics — DX-D02; the automated
// cardinality test asserts that). The attribute is emitted ONLY when
// TenantID is non-empty, mirroring the wire-format discipline in
// buildCloudEventsHeaders (which omits ce-tenantid for system events).
// This keeps trace search/group-by clean: ops queries grouping by tenant
// see no phantom empty-string bucket from system events.
//
// streaming.system_event is emitted (true) for system-event emissions so
// operators can split tenant traffic from system traffic in one trace
// query without joining ce-tenantid presence.
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
	// span cardinality stays bounded. event.delivery_enabled was dropped
	// entirely because it is always true by the time we set span attributes
	// — the !HasDeliveryPath gate in emit.go short-circuits before the
	// span is ever created.
	//
	// Build the attribute slice with sensible upfront capacity (9 fixed
	// attributes + 1 conditional tenant + 1 conditional system_event +
	// up to 1 debug-only key). The compiler-friendly shape avoids slice
	// growth in the steady state.
	attrs := make([]attribute.KeyValue, 0, 12)
	attrs = append(attrs,
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
		attribute.String("streaming.producer_id", p.producerID),
		attribute.String("event.policy", formatPolicyAttr(policy)),
	)

	if event.TenantID != "" {
		attrs = append(attrs, attribute.String("tenant.id", event.TenantID))
	}

	if event.SystemEvent {
		attrs = append(attrs, attribute.Bool("streaming.system_event", true))
	}

	span.SetAttributes(attrs...)

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
//
// Uses string concat instead of fmt.Sprintf — same allocation count, ~2.4×
// faster (51.6 ns/op → 21.5 ns/op per benchmark). This site is on the hot
// path for every recorded Emit, so the perf delta compounds at high RPS.
func formatPolicyAttr(policy DeliveryPolicy) string {
	return "direct:" + string(policy.Direct) +
		",outbox:" + string(policy.Outbox) +
		",dlq:" + string(policy.DLQ)
}
