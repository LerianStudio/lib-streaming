package producer

import (
	"context"
	"github.com/LerianStudio/lib-streaming/v4/obs"
	"slices"
	"strconv"
	"time"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
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
//
// SIZE: a DLQ copy carries the payload plus the CloudEvents headers plus the
// forensic set, so it is strictly LARGER than the record it quarantines and a
// near-cap payload can be refused where the original would have been accepted.
// When the transport refuses it on size, the write is retried ONCE with the
// payload omitted and marked (x-lerian-dlq-payload-omitted). Metadata-only
// evidence beats no evidence — but note the producer-side payload is genuinely
// gone, unlike a consumer quarantine, whose origin coordinates make it
// recoverable from the source topic.
func (p *Producer) publishRouteDLQ(
	ctx context.Context,
	rt *targetRuntime,
	event Event,
	route contract.RouteDefinition,
	cause error,
	firstAttempt time.Time,
) (bool, error) {
	if p == nil {
		// Receiver-nil DX guard: nil *Producer cannot fire its own
		// asserter usefully. Match the package contract.
		return false, nil
	}

	if rt == nil || rt.adapter == nil {
		// State-corruption invariant violation. dispatchRoute reaches
		// publishRouteDLQ AFTER asserting rt and rt.adapter are non-
		// nil (see dispatchRoute's rt-nil assertion in emit_multi.go).
		// Reaching here with either nil
		// means a corrupted runtime — and (false, nil) here is
		// indistinguishable from "no DLQ destination configured" by
		// the caller's outcome classifier, which is exactly the
		// silent-failure mode we want surfaced. Fire the trident,
		// then preserve the (false, nil) so the dispatch outcome
		// reporting stays unchanged.
		a := p.newAsserter("publish_dlq_route.guard")
		_ = a.NotNil(ctx, rt, "target runtime must be non-nil at publishRouteDLQ",
			"producer_id", p.producerID,
			"route_key", route.Key,
		)

		if rt != nil {
			_ = a.NotNil(ctx, rt.adapter, "target adapter must be non-nil at publishRouteDLQ",
				"producer_id", p.producerID,
				"route_key", route.Key,
				"target", rt.name,
			)
		}

		return false, nil
	}

	dlqDest, sourceLabel, ok := p.resolveRouteDLQDestination(route, rt)
	if !ok {
		// No DLQ configured and no derivable default — best-effort no-op.
		// Log once at INFO level so operators can spot route-level DLQ
		// gaps without alerting noise on every failed emit.
		p.logger.Log(ctx, obs.LevelInfo,
			"streaming: route DLQ skipped — no destination resolvable for non-Kafka transport",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
			"transport", string(route.Destination.Kind),
		)

		return false, nil
	}

	if err := dlqDest.Validate(); err != nil {
		// Configuration bug: surface in logs + counter, do not propagate.
		p.metrics.recordDLQFailed(ctx, sourceLabel)
		p.logger.Log(ctx, obs.LevelError, "streaming: route DLQ destination invalid",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
			"error", err.Error(),
		)

		return false, err
	}

	cls := rt.adapter.Classify(cause)

	causeMessage := ""
	if cause != nil {
		// Bounded: the cause string is the one unbounded value on a DLQ record,
		// and a DLQ copy is strictly larger than the payload it quarantines. An
		// unbounded one could be the thing that pushes a near-cap record past
		// the broker's limit. See dlqheader.MaxErrorMessageBytes.
		causeMessage = dlqheader.TruncateErrorMessage(sanitizeBrokerURL(cause.Error()))
	}

	headers := buildTransportHeaders(ctx, event)
	headers = append(headers,
		transport.Header{Key: dlqheader.SourceTopic, Value: []byte(sourceLabel)},
		transport.Header{Key: dlqheader.ErrorClass, Value: []byte(cls)},
		transport.Header{Key: dlqheader.ErrorMessage, Value: []byte(causeMessage)},
		transport.Header{Key: dlqheader.RetryCount, Value: []byte(strconv.Itoa(extractRetryCount(cause)))},
		transport.Header{Key: dlqheader.FirstFailureAt, Value: []byte(firstAttempt.UTC().Format(time.RFC3339Nano))},
		transport.Header{Key: dlqheader.ProducerID, Value: []byte(p.producerID)},
	)

	partKey := p.resolvePartitionKey(event)

	message := transport.TransportMessage{
		Destination: dlqDest,
		TenantID:    event.TenantID,
		Key:         partKey,
		Payload:     event.Payload,
		Headers:     headers,
		Attributes:  dlqDest.Attributes,
	}

	err := rt.adapter.Publish(ctx, transport.CloneMessage(message))
	if err != nil && dlqheader.IsSizeError(err) {
		// The copy does not fit. Quarantining the metadata WITHOUT the payload
		// beats losing the entry entirely: the event id, tenant, route, and
		// error class are what an operator triages on, and they are exactly
		// what a dropped DLQ write destroys. The payload is NOT recoverable on
		// this side — the original publish never landed anywhere — so the
		// marker headers say plainly that it is gone.
		message.Payload = nil
		message.Headers = append(slices.Clone(headers),
			transport.Header{Key: dlqheader.PayloadOmitted, Value: []byte("true")},
			transport.Header{Key: dlqheader.PayloadBytes, Value: []byte(strconv.Itoa(len(event.Payload)))},
		)

		err = rt.adapter.Publish(ctx, transport.CloneMessage(message))
	}

	if err != nil {
		p.metrics.recordDLQFailed(ctx, sourceLabel)
		p.logger.Log(ctx, obs.LevelError, "streaming: route DLQ publish failed",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
			"error_class", string(cls),
			"dlq_destination", describeDestination(dlqDest),
			"error", sanitizeBrokerURL(err.Error()),
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
