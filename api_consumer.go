package streaming

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/metrics"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/internal/consumer"
)

// Handler is the only interface a consuming service implements. The library
// owns commit, retry, seek-back, DLQ, tenant scoping, and rebalance safety.
//
//	type myHandler struct{}
//	func (myHandler) Handle(ctx context.Context, ev streaming.Event, payload []byte) error {
//	    // ev.TenantID is set from ce-tenantid by the library — filter on it
//	    // before any tenant-scoped business logic (cross-tenant leak otherwise).
//	    return nil
//	}
type Handler = consumer.Handler

// Classifier optionally RECLASSIFIES a known HANDLER-return error as transient
// (retryable) instead of the fail-closed default. Handler-return errors default
// to TERMINAL -> DLQ when no Classifier recognizes them; a money-path consumer
// MUST supply one marking its known-transient downstream faults (Midaz/Postgres
// down, etc.) as retry, else a transient outage over-quarantines into the DLQ.
// Return true for an error that should be retried (a recoverable downstream
// blip), false to let it take the fail-closed terminal path. It governs ONLY
// handler-return errors — transport errors are classified by the transport seam
// and codec-decode errors are always terminal. See docs/design/consumer.md §7a.
type Classifier = consumer.Classifier

// Consumer is the hardened at-least-once group consumer surface (Run/Close/
// Healthy). Construct it with NewConsumer().…​.Build(ctx); drive it with
// Run(ctx); stop it with Close. It is an interface — mirroring the producer's
// public Emitter — so Build returns either the real runtime or the disabled-mode
// no-op from the Enabled kill switch.
type Consumer = consumer.Runner

// ConsumerOption is the functional-option type for advanced consumer wiring
// not covered by a dedicated builder method.
type ConsumerOption = consumer.Option

// WithConsumerLogger sets the structured logger.
func WithConsumerLogger(l log.Logger) ConsumerOption { return consumer.WithLogger(l) }

// WithConsumerMetricsFactory wires the metrics factory for consumer instruments.
func WithConsumerMetricsFactory(f *metrics.MetricsFactory) ConsumerOption {
	return consumer.WithMetricsFactory(f)
}

// WithConsumerTracer overrides the tracer used for poll/handle spans.
func WithConsumerTracer(t trace.Tracer) ConsumerOption { return consumer.WithTracer(t) }

// ConsumerBuilder assembles a Consumer programmatically, mirroring the
// producer's streaming.Builder idiom. Build constructs the franz-go group
// client (with BlockRebalanceOnPoll + DisableAutoCommit) and the at-least-once
// runtime.
//
//	c, err := streaming.NewConsumer().
//	    Brokers(cfg.Brokers...).
//	    Group("my-service").
//	    Topics("lerian.streaming.transaction.created").
//	    TLS(tlsCfg).
//	    SASL(mech).
//	    Handler(myHandler{}).
//	    DLQTopicSuffix(".dlq").  // optional; default ".dlq"
//	    RetryBudget(3).
//	    Classifier(isTerminal).
//	    Build(ctx)
//	if err != nil { return err }
//	go func() { _ = c.Run(ctx) }()  // SafeGo in production
//	defer c.Close()
//
// There is deliberately no DLQ(emitter) knob: the DLQ must not flow through the
// public Emitter (its catalog/payload/header gates reject the very poison it
// must quarantine). The consumer constructs its own DLQ publisher internally
// over the internal transport seam, reusing the same Brokers/TLS/SASL config it
// consumes with. See docs/design/consumer.md §1 and §6.
type ConsumerBuilder struct {
	cfg        consumer.ConsumerConfig
	handler    Handler
	classifier Classifier
	opts       []ConsumerOption
}

// NewConsumer returns a ConsumerBuilder defaulted to ENABLED — an explicitly
// fluent-built consumer is meant to run (mirrors the producer default at
// api.go:771). Config-driven callers that gate on a deployment flag use
// .Enabled(cfg.Flag); the env path (LoadConsumerConfig) keeps its own
// STREAMING_CONSUMER_ENABLED kill switch.
func NewConsumer() *ConsumerBuilder {
	// Seed the same defaults LoadConsumerConfig applies so a minimal fluent build
	// (Brokers/Group/Topics/Handler only) passes Validate and runs. Without these,
	// the zero-value backoff/dwell/timeout fields would fail validation and the
	// default-enabled builder would be unusable. The DLQ suffix default also
	// prevents <topic><""> == the source topic (a terminal record would loop
	// forever instead of quarantining).
	b := &ConsumerBuilder{cfg: consumer.DefaultBuilderConfig()}

	return b
}

// Enabled gates whether Build yields a real runtime (true) or the disabled-mode
// no-op (false). Defaults to true via NewConsumer; pass .Enabled(cfg.Flag) to
// drive it from config.
func (b *ConsumerBuilder) Enabled(v bool) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Enabled = v

	return b
}

// Brokers sets the bootstrap broker list.
func (b *ConsumerBuilder) Brokers(brokers ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Brokers = append([]string(nil), brokers...)

	return b
}

// Group sets the consumer group id.
func (b *ConsumerBuilder) Group(group string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Group = group

	return b
}

// Topics sets the subscription list.
func (b *ConsumerBuilder) Topics(topics ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Topics = append([]string(nil), topics...)

	return b
}

// TLS sets the TLS config for broker dials. Validated at Build (shared with the
// producer via the wave-2 internal/kafkasec extraction).
func (b *ConsumerBuilder) TLS(cfg *tls.Config) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithTLSConfig(cfg)

	return b
}

// SASL sets the SASL mechanism. SASL requires TLS unless AllowPlaintextSASL.
func (b *ConsumerBuilder) SASL(mechanism sasl.Mechanism) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithSASL(mechanism)

	return b
}

// AllowPlaintextSASL permits SASL without TLS for local/dev brokers only.
func (b *ConsumerBuilder) AllowPlaintextSASL() *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithAllowPlaintextSASL()

	return b
}

// Handler wires the service-supplied record handler (required).
func (b *ConsumerBuilder) Handler(h Handler) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.handler = h

	return b
}

// DLQTopicSuffix sets the suffix appended to the source topic to derive the DLQ
// topic (<topic><suffix>). Default ".dlq". This is the only public DLQ knob:
// the DLQ publisher itself is constructed internally over the transport seam,
// not from a caller-supplied emitter (see ConsumerBuilder docs).
func (b *ConsumerBuilder) DLQTopicSuffix(suffix string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.DLQTopicSuffix = suffix

	return b
}

// RetryBudget sets the IN-LOOP transient-failure retry count within a single
// poll cycle (the connection-blip absorber; typically resolves in 0-1 attempts).
// It is NOT "retries before DLQ": transients NEVER reach the DLQ. When the
// in-loop budget is exhausted (a SUSTAINED transient) the runtime seeks back and
// blocks its partition head-of-line — block beats lose. Only classified
// TERMINAL/poison reaches the DLQ; handler-return AND codec-decode errors
// default to terminal (fail-closed) unless the optional Classifier reclassifies
// a handler error as transient. See docs/design/consumer.md §2/§7a.
func (b *ConsumerBuilder) RetryBudget(n int) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.RetryBudget = n

	return b
}

// Classifier wires the optional handler-error reclassifier (transient flip off
// the fail-closed terminal default; see the Classifier type).
func (b *ConsumerBuilder) Classifier(fn Classifier) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.classifier = fn

	return b
}

// CloseTimeout bounds graceful drain on Close.
func (b *ConsumerBuilder) CloseTimeout(d time.Duration) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.CloseTimeout = d

	return b
}

// Options appends arbitrary ConsumerOptions (logger, metrics, tracer, custom
// codec). Parity escape hatch for options without a dedicated method.
func (b *ConsumerBuilder) Options(opts ...ConsumerOption) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.opts = append(b.opts, opts...)

	return b
}

// Build validates the builder, constructs the franz-go group client, constructs
// the internal DLQ publisher over the transport seam (same Brokers/TLS/SASL as
// the consume client), and returns the at-least-once Consumer runtime.
//
// When cfg.Enabled is false, Build returns a no-op Consumer (consumer.NewNoop)
// whose Run blocks until ctx-cancel and whose Close/Healthy are no-ops —
// mirroring the producer's NoopEmitter kill-switch.
func (b *ConsumerBuilder) Build(ctx context.Context) (Consumer, error) {
	if b == nil {
		return nil, consumer.ErrNilHandler
	}

	// Fold the builder-level classifier into the option list so the runtime
	// reclassifier seam stays single-sourced (consumer.WithClassifier).
	opts := b.opts
	if b.classifier != nil {
		opts = append(opts, consumer.WithClassifier(b.classifier))
	}

	// consumer.Build owns the full production wiring: the Enabled kill switch,
	// cfg.Validate, the franz-go group client (BlockRebalanceOnPoll +
	// DisableAutoCommit + TLS/SASL via kafkasec), the internal transport-seam DLQ
	// publisher over the same config (NOT the public Emitter), and the transport
	// error-source classifier. The handler typed-nil guard lives there too.
	return consumer.Build(ctx, b.cfg, b.handler, opts...)
}
