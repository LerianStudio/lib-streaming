package streaming

import (
	"crypto/tls"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/internal/producer"
)

// EmitterOption configures a Producer at construction time.
type EmitterOption = producer.EmitterOption

// WithLogger sets the structured logger used by the producer.
func WithLogger(l log.Logger) EmitterOption { return producer.WithLogger(l) }

// WithMetricsFactory wires the metrics factory used to register streaming instruments.
func WithMetricsFactory(f *metrics.MetricsFactory) EmitterOption {
	return producer.WithMetricsFactory(f)
}

// WithTracer overrides the tracer used for streaming emit spans.
func WithTracer(t trace.Tracer) EmitterOption { return producer.WithTracer(t) }

// WithCircuitBreakerManager lets the caller share a process-level breaker manager.
func WithCircuitBreakerManager(m circuitbreaker.Manager) EmitterOption {
	return producer.WithCircuitBreakerManager(m)
}

// WithPartitionKey overrides the default Event.PartitionKey behavior.
func WithPartitionKey(fn func(Event) string) EmitterOption { return producer.WithPartitionKey(fn) }

// WithCloseTimeout caps how long Close waits for flush and drain.
func WithCloseTimeout(d time.Duration) EmitterOption { return producer.WithCloseTimeout(d) }

// WithOutboxRepository adapts a lib-commons OutboxRepository for fallback writes.
func WithOutboxRepository(repo outbox.OutboxRepository) EmitterOption {
	return producer.WithOutboxRepository(repo)
}

// WithOutboxWriter wires a custom outbox writer boundary.
func WithOutboxWriter(writer OutboxWriter) EmitterOption { return producer.WithOutboxWriter(writer) }

// WithTLSConfig sets the TLS configuration for broker connections.
func WithTLSConfig(cfg *tls.Config) EmitterOption { return producer.WithTLSConfig(cfg) }

// WithSASL sets the SASL mechanism for broker authentication.
func WithSASL(mechanism sasl.Mechanism) EmitterOption { return producer.WithSASL(mechanism) }

// WithAllowSystemEvents opts the producer into accepting system events.
func WithAllowSystemEvents() EmitterOption { return producer.WithAllowSystemEvents() }

// WithCatalog wires the immutable event catalog required by NewProducer.
func WithCatalog(catalog Catalog) EmitterOption { return producer.WithCatalog(catalog) }
