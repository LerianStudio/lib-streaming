package streaming

import (
	"context"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/emitter"
	"github.com/LerianStudio/lib-streaming/internal/producer"
)

// Emitter publishes cataloged domain events and exposes lifecycle/health hooks.
type Emitter = contract.Emitter

// NoopEmitter is the fail-safe no-op implementation returned when streaming is disabled.
type NoopEmitter = emitter.NoopEmitter

// Producer is the public franz-go-backed Emitter facade.
//
// The runtime implementation is intentionally hidden under internal/producer so
// applications depend on the root streaming API rather than implementation
// packages.
type Producer struct {
	inner *producer.Producer
}

// Compile-time assertions: the wrapper Producer must satisfy both the public
// Emitter contract (Emit/Close/Healthy) AND the lib-commons App lifecycle
// contract (Run). A missing or renamed method fails the build here, not at a
// distant call site in the consuming service's main.go.
var (
	_ Emitter     = (*Producer)(nil)
	_ commons.App = (*Producer)(nil)
)

// New constructs the appropriate Emitter for cfg.
//
// When cfg disables streaming or has no brokers, New returns a NoopEmitter.
// Otherwise it constructs a real Producer.
func New(ctx context.Context, cfg Config, opts ...EmitterOption) (Emitter, error) {
	if !cfg.Enabled || len(cfg.Brokers) == 0 {
		return NewNoopEmitter(), nil
	}

	return NewProducer(ctx, cfg, opts...)
}

// NewProducer constructs a real Producer and never substitutes a NoopEmitter.
func NewProducer(ctx context.Context, cfg Config, opts ...EmitterOption) (*Producer, error) {
	inner, err := producer.NewProducer(ctx, cfg, opts...)
	if err != nil {
		return nil, err
	}

	return &Producer{inner: inner}, nil
}

// NewNoopEmitter returns an Emitter whose methods are unconditional no-ops.
func NewNoopEmitter() Emitter {
	return emitter.NewNoopEmitter()
}

// Emit publishes a cataloged event request.
func (p *Producer) Emit(ctx context.Context, request EmitRequest) error {
	if p == nil || p.inner == nil {
		return ErrNilProducer
	}

	return p.inner.Emit(ctx, request)
}

// Close flushes and closes the producer. It is idempotent.
func (p *Producer) Close() error {
	if p == nil || p.inner == nil {
		return nil
	}

	return p.inner.Close()
}

// CloseContext flushes and closes the producer using ctx as the upper bound.
func (p *Producer) CloseContext(ctx context.Context) error {
	if p == nil || p.inner == nil {
		return nil
	}

	return p.inner.CloseContext(ctx)
}

// Healthy reports whether the producer can accept new events.
func (p *Producer) Healthy(ctx context.Context) error {
	if p == nil || p.inner == nil {
		return NewHealthError(Down, ErrNilProducer)
	}

	return p.inner.Healthy(ctx)
}

// Run blocks until the launcher or producer lifecycle shuts down.
func (p *Producer) Run(launcher *commons.Launcher) error {
	if p == nil || p.inner == nil {
		return ErrNilProducer
	}

	return p.inner.Run(launcher)
}

// RunContext blocks until ctx is canceled or the producer is closed.
func (p *Producer) RunContext(ctx context.Context, launcher *commons.Launcher) error {
	if p == nil || p.inner == nil {
		return ErrNilProducer
	}

	return p.inner.RunContext(ctx, launcher)
}

// RegisterOutboxRelay registers the stable streaming outbox relay handler.
func (p *Producer) RegisterOutboxRelay(registry *outbox.HandlerRegistry) error {
	if p == nil || p.inner == nil {
		return ErrNilProducer
	}

	return p.inner.RegisterOutboxRelay(registry)
}

// Descriptor returns a validated publisher descriptor with ProducerID populated.
func (p *Producer) Descriptor(base PublisherDescriptor) (PublisherDescriptor, error) {
	if p == nil || p.inner == nil {
		return PublisherDescriptor{}, ErrNilProducer
	}

	return p.inner.Descriptor(base)
}
