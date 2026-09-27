package consumer

import (
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/obs"
)

// Option configures the consumer runtime at construction. Mirrors the producer's
// EmitterOption functional-options idiom. It operates on the concrete
// consumerRuntime (the public surface is the Runner interface).
type Option func(*consumerRuntime)

// WithLogger sets the structured logger.
func WithLogger(l obs.Logger) Option {
	return func(c *consumerRuntime) { c.logger = l }
}

// WithMetricsRecorder wires the sink for the consumer instruments
// (streaming_consumer_handled_total, _retry_total, _dlq_total, _commit_total,
// _seek_back_total, _fetch_error_total, _poll_duration_ms).
func WithMetricsRecorder(f obs.MetricsRecorder) Option {
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

// WithDiscardDispatch installs the DLQ-reader seam. The root builder's
// DiscardHandler(...) is its only caller: keeping the installer here, on an
// option a service cannot construct (this package is internal, and the root
// re-exports only logger/metrics/tracer), is what makes "this consumer is a DLQ
// reader" a build-time fact rather than a method-set accident.
func WithDiscardDispatch(fn DiscardDispatch) Option {
	return func(c *consumerRuntime) { c.discard = fn }
}

// WithSignatureVerifier makes the runtime REQUIRE a valid envelope signature on
// every record, checked right after the codec and before the ce-source check,
// in both handler modes. A record that fails quarantines with a signature cause
// kind and never reaches a handler or the Classifier. A nil verifier leaves
// verification off. The root builder's RequireSignatures(...) is the intended
// caller; New refuses it on a DLQ reader.
func WithSignatureVerifier(v *envelopesig.Verifier) Option {
	return func(c *consumerRuntime) { c.verifier = v }
}

// WithCodec overrides the CloudEvents header decoder (tenant extraction seam).
// Defaults to cloudevents.ParseCloudEventsHeaders.
func WithCodec(fn codecFunc) Option {
	return func(c *consumerRuntime) { c.codec = fn }
}
