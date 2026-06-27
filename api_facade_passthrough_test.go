//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
)

// TestProducer_NilReceiverGuards proves every lifecycle method on the public
// Producer facade fails closed (never panics, never silently succeeds) when the
// inner producer is nil — the guard branch at the top of each method. A zero
// *Producer{} has a nil inner, which is the path under test. These are the
// guards a consuming service relies on when wiring is incomplete.
func TestProducer_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var p *streaming.Producer // nil receiver

	if err := p.Close(); err != nil {
		t.Errorf("Close() on nil Producer = %v; want nil (idempotent no-op)", err)
	}

	if err := p.CloseContext(context.Background()); err != nil {
		t.Errorf("CloseContext() on nil Producer = %v; want nil", err)
	}

	if err := p.Emit(context.Background(), streaming.EmitRequest{}); err == nil {
		t.Error("Emit() on nil Producer = nil; want ErrNilProducer")
	}

	if err := p.Healthy(context.Background()); err == nil {
		t.Error("Healthy() on nil Producer = nil; want a Down health error")
	}

	if err := p.Run(nil); err == nil {
		t.Error("Run() on nil Producer = nil; want ErrNilProducer")
	}

	if err := p.RunContext(context.Background(), nil); err == nil {
		t.Error("RunContext() on nil Producer = nil; want ErrNilProducer")
	}

	if err := p.RegisterOutboxRelay(nil); err == nil {
		t.Error("RegisterOutboxRelay() on nil Producer = nil; want ErrNilProducer")
	}

	if _, err := p.Descriptor(streaming.PublisherDescriptor{}); err == nil {
		t.Error("Descriptor() on nil Producer = nil; want ErrNilProducer")
	}
}

// TestProducerOption_Constructors proves every public EmitterOption constructor
// returns a non-nil option. They are thin aliases over internal/producer; the
// test pins the wiring so a future rename can't silently return nil.
func TestProducerOption_Constructors(t *testing.T) {
	t.Parallel()

	opts := map[string]streaming.EmitterOption{
		"WithMetricsFactory":        streaming.WithMetricsFactory(nil),
		"WithTracer":                streaming.WithTracer(nil),
		"WithCircuitBreakerManager": streaming.WithCircuitBreakerManager(nil),
		"WithOutboxWriter":          streaming.WithOutboxWriter(nil),
		"WithAllowSystemEvents":     streaming.WithAllowSystemEvents(),
	}

	for name, opt := range opts {
		if opt == nil {
			t.Errorf("%s() = nil; want a non-nil EmitterOption", name)
		}
	}
}

// TestContractReExports exercises the trivial contract/manifest re-export
// helpers (one-line delegations to internal packages) so a broken re-export
// surfaces here rather than at a consuming service's call site.
func TestContractReExports(t *testing.T) {
	t.Parallel()

	// DefaultDeliveryPolicy / ResolveDeliveryPolicy delegate to contract.
	def := streaming.DefaultDeliveryPolicy()
	if _, err := streaming.ResolveDeliveryPolicy(streaming.EventDefinition{}, streaming.DeliveryPolicyOverride{}, streaming.DeliveryPolicyOverride{}); err != nil {
		// ResolveDeliveryPolicy may legitimately error on an empty definition;
		// we only require it not to panic. Use def to avoid an unused warning.
		_ = def
	}

	// NewHealthError wraps a cause with a state.
	cause := errors.New("boom")
	if he := streaming.NewHealthError(streaming.Down, cause); he == nil {
		t.Error("NewHealthError() = nil; want a non-nil *HealthError")
	} else if !errors.Is(he, cause) {
		t.Error("NewHealthError() does not wrap its cause")
	}

	// NewEventDefinition / NewPublisherDescriptor validate-and-normalize; an empty
	// input is expected to error, but must not panic.
	if _, err := streaming.NewEventDefinition(streaming.EventDefinition{}); err == nil {
		t.Error("NewEventDefinition(empty) = nil error; want a validation error")
	}

	if _, err := streaming.NewPublisherDescriptor(streaming.PublisherDescriptor{}); err == nil {
		t.Error("NewPublisherDescriptor(empty) = nil error; want a validation error")
	}
}
