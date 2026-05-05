package producer

import (
	"context"
	"strconv"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

// publishRouteDLQ writes the original payload to the route's DLQ destination
// preserving every CloudEvents header verbatim and adding the six
// x-lerian-dlq-* headers (TRD §C8). Per-route DLQ destination resolution:
//
//  1. route.DLQ if explicitly set on the RouteDefinition;
//  2. Kafka-like convention "<source>.dlq" when the route transport is
//     TransportKafkaLike;
//  3. otherwise no-op (logged at INFO; the failure is still surfaced to the
//     caller via the originating outcome — DLQ delivery is best-effort).
//     INFO (not WARN) because route-level DLQ gaps are an expected
//     configuration shape for non-Kafka deployments where the operator chose
//     not to wire a DLQ destination; alerting noise on every failed emit
//     would be operationally wrong. Operators who DO want DLQ on every
//     failure must wire route.DLQ explicitly.
//
// DLQ writes are best-effort from the Emit caller's perspective: failures are
// logged + counted via streaming_dlq_publish_failed_total and returned to the
// route dispatcher only so it can distinguish delivered, skipped, and failed
// DLQ side effects in metrics/span state.
func (p *Producer) publishRouteDLQ(
	ctx context.Context,
	rt *targetRuntime,
	event Event,
	route contract.RouteDefinition,
	cause error,
	firstAttempt time.Time,
) (bool, error) {
	if p == nil || rt == nil || rt.adapter == nil {
		return false, nil
	}

	dlqDest, sourceLabel, ok := p.resolveRouteDLQDestination(route, rt)
	if !ok {
		// No DLQ configured and no derivable default — best-effort no-op.
		// Log once at INFO level so operators can spot route-level DLQ
		// gaps without alerting noise on every failed emit.
		p.logger.Log(ctx, log.LevelInfo,
			"streaming: route DLQ skipped — no destination resolvable for non-Kafka transport",
			log.String("producer_id", p.producerID),
			log.String("route_key", route.Key),
			log.String("target", route.Target),
			log.String("transport", string(route.Destination.Kind)),
		)

		return false, nil
	}

	if err := dlqDest.Validate(); err != nil {
		// Configuration bug: surface in logs + counter, do not propagate.
		p.metrics.recordDLQFailed(ctx, sourceLabel)
		p.logger.Log(ctx, log.LevelError, "streaming: route DLQ destination invalid",
			log.String("producer_id", p.producerID),
			log.String("route_key", route.Key),
			log.String("target", route.Target),
			log.String("error", err.Error()),
		)

		return false, err
	}

	cls := rt.adapter.Classify(cause)

	causeMessage := ""
	if cause != nil {
		causeMessage = sanitizeBrokerURL(cause.Error())
	}

	headers := buildCloudEventsTransportHeaders(event)
	headers = append(headers,
		transport.Header{Key: dlqHeaderSourceTopic, Value: []byte(sourceLabel)},
		transport.Header{Key: dlqHeaderErrorClass, Value: []byte(cls)},
		transport.Header{Key: dlqHeaderErrorMessage, Value: []byte(causeMessage)},
		transport.Header{Key: dlqHeaderRetryCount, Value: []byte(strconv.Itoa(extractRetryCount(cause)))},
		transport.Header{Key: dlqHeaderFirstFailureAt, Value: []byte(firstAttempt.UTC().Format(time.RFC3339Nano))},
		transport.Header{Key: dlqHeaderProducerID, Value: []byte(p.producerID)},
	)

	partKey := event.PartitionKey()
	if p.partFn != nil {
		partKey = p.partFn(event)
	}

	message := transport.TransportMessage{
		Destination: dlqDest,
		TenantID:    event.TenantID,
		Key:         partKey,
		Payload:     event.Payload,
		Headers:     headers,
	}

	if err := rt.adapter.Publish(ctx, message); err != nil {
		p.metrics.recordDLQFailed(ctx, sourceLabel)
		p.logger.Log(ctx, log.LevelError, "streaming: route DLQ publish failed",
			log.String("producer_id", p.producerID),
			log.String("route_key", route.Key),
			log.String("target", route.Target),
			log.String("error_class", string(cls)),
			log.String("dlq_destination", describeDestination(dlqDest)),
			log.String("error", sanitizeBrokerURL(err.Error())),
		)

		return false, err
	}

	return true, nil
}

// resolveRouteDLQDestination applies the three-step precedence rule documented
// on publishRouteDLQ. Returns the resolved Destination, a human-readable
// "source label" used for metrics + headers (the source topic name in
// Kafka's case, the route key in non-Kafka transports), and a boolean
// reporting whether a destination was successfully resolved.
//
// rt is currently unused but kept in the signature: future per-target DLQ
// fallback (e.g. Builder.Target(...).WithDefaultDLQ(...)) lands on this
// runtime, and a signature change later would ripple through every test
// fixture that constructs a fake targetRuntime.
func (p *Producer) resolveRouteDLQDestination(route contract.RouteDefinition, _ *targetRuntime) (contract.Destination, string, bool) {
	sourceLabel := dlqSourceLabelFor(route)

	if route.DLQ != nil {
		return *route.DLQ, sourceLabel, true
	}

	if route.Destination.Kind == contract.TransportKafkaLike && route.Destination.Name != "" {
		return contract.Destination{
			Kind: contract.TransportKafkaLike,
			Name: dlqTopic(route.Destination.Name),
		}, route.Destination.Name, true
	}

	return contract.Destination{}, sourceLabel, false
}

// dlqSourceLabelFor returns the "source" label written to the DLQ message
// header and used as the metric topic label. For Kafka-like routes this is
// the source topic; for other transports we use a stable identifier
// derived from the route to keep metric cardinality bounded.
func dlqSourceLabelFor(route contract.RouteDefinition) string {
	if route.Destination.Kind == contract.TransportKafkaLike && route.Destination.Name != "" {
		return route.Destination.Name
	}

	return route.Key
}
