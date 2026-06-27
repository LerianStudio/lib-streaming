package consumer

import (
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/metrics"
	"go.opentelemetry.io/otel/trace"
)

// Option configures the consumer runtime at construction. Mirrors the producer's
// EmitterOption functional-options idiom. It operates on the concrete
// consumerRuntime (the public surface is the Runner interface).
type Option func(*consumerRuntime)

// WithLogger sets the structured logger.
func WithLogger(l log.Logger) Option {
	return func(c *consumerRuntime) { c.logger = l }
}

// WithMetricsFactory wires the metrics factory for consumer instruments
// (streaming_consumer_handled_total, _retry_total, _dlq_total, _commit_total,
// _seek_back_total, _fetch_error_total, _poll_duration_ms).
func WithMetricsFactory(f *metrics.MetricsFactory) Option {
	return func(c *consumerRuntime) { c.metrics = f }
}

// WithTracer overrides the tracer used for poll/handle spans.
func WithTracer(t trace.Tracer) Option {
	return func(c *consumerRuntime) { c.tracer = t }
}

// WithClassifier wires the optional handler-error reclassifier (transient flip
// off the fail-closed terminal default; see Classifier).
func WithClassifier(fn Classifier) Option {
	return func(c *consumerRuntime) { c.classifier = fn }
}

// WithDLQPublisher wires the DLQ republish seam. Production wires
// transportDLQPublisher (the internal transport.TransportAdapter seam — NOT the
// public Emitter). Tests inject a recording fake.
func WithDLQPublisher(p dlqPublisher) Option {
	return func(c *consumerRuntime) { c.dlq = p }
}

// WithCodec overrides the CloudEvents header decoder (tenant extraction seam).
// Defaults to cloudevents.ParseCloudEventsHeaders.
func WithCodec(fn codecFunc) Option {
	return func(c *consumerRuntime) { c.codec = fn }
}
