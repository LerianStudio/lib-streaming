//go:build unit

package streaming

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/metrics"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LerianStudio/lib-streaming/internal/consumer"
	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// noopHandler is a do-nothing Handler for white-box builder-construction tests
// (package streaming, so it cannot reuse the streaming_test one in
// api_consumer_test.go).
type noopHandler struct{}

func (noopHandler) Handle(context.Context, contract.Event, []byte) error { return nil }

// TestConsumerBuilder_SettersMapToConfig asserts every ConsumerBuilder setter
// writes the right ConsumerConfig field. White-box (package streaming) so it can
// read b.cfg directly — the producer's builder_assert_test.go uses the same
// idiom. TLS/SASL/AllowPlaintextSASL hold their state in unexported config
// fields, so those three are asserted via Build's validation branching below
// (TestConsumerBuilder_TLSSASL_*), not by reading the field here.
func TestConsumerBuilder_SettersMapToConfig(t *testing.T) {
	t.Parallel()

	b := NewConsumer().
		Enabled(false).
		Brokers("b1:9092", "b2:9092").
		Group("svc-group").
		Topics("topic.a", "topic.b").
		Handler(noopHandler{}).
		DLQTopicSuffix(".quarantine").
		RetryBudget(7).
		CloseTimeout(12 * time.Second)

	if b.cfg.Enabled {
		t.Error("Enabled(false) did not clear cfg.Enabled")
	}

	if len(b.cfg.Brokers) != 2 || b.cfg.Brokers[0] != "b1:9092" || b.cfg.Brokers[1] != "b2:9092" {
		t.Errorf("Brokers = %v; want [b1:9092 b2:9092]", b.cfg.Brokers)
	}

	if b.cfg.Group != "svc-group" {
		t.Errorf("Group = %q; want svc-group", b.cfg.Group)
	}

	if len(b.cfg.Topics) != 2 || b.cfg.Topics[0] != "topic.a" || b.cfg.Topics[1] != "topic.b" {
		t.Errorf("Topics = %v; want [topic.a topic.b]", b.cfg.Topics)
	}

	if b.handler == nil {
		t.Error("Handler did not set b.handler")
	}

	if b.cfg.DLQTopicSuffix != ".quarantine" {
		t.Errorf("DLQTopicSuffix = %q; want .quarantine", b.cfg.DLQTopicSuffix)
	}

	if b.cfg.RetryBudget != 7 {
		t.Errorf("RetryBudget = %d; want 7", b.cfg.RetryBudget)
	}

	if b.cfg.CloseTimeout != 12*time.Second {
		t.Errorf("CloseTimeout = %s; want 12s", b.cfg.CloseTimeout)
	}
}

// TestConsumerBuilder_ClassifierAndOptions asserts Classifier and Options wire
// into the builder state (b.classifier / b.opts), the two fields Build folds
// into the runtime option list.
func TestConsumerBuilder_ClassifierAndOptions(t *testing.T) {
	t.Parallel()

	b := NewConsumer().
		Classifier(func(error) bool { return true }).
		Options(WithConsumerLogger(log.NewNop()), WithConsumerTracer(noop.NewTracerProvider().Tracer("t")))

	if b.classifier == nil {
		t.Error("Classifier did not set b.classifier")
	}

	if len(b.opts) != 2 {
		t.Errorf("Options appended %d opts; want 2", len(b.opts))
	}
}

// TestConsumerBuilder_NilReceiverGuards proves every setter is nil-safe: calling
// each on a nil *ConsumerBuilder returns nil without panicking (the defensive
// guard at the top of each method). A chained nil call must not blow up.
func TestConsumerBuilder_NilReceiverGuards(t *testing.T) {
	t.Parallel()

	var b *ConsumerBuilder

	// Each call returns the (nil) receiver; none may panic.
	checks := []func() *ConsumerBuilder{
		func() *ConsumerBuilder { return b.Enabled(true) },
		func() *ConsumerBuilder { return b.Brokers("x") },
		func() *ConsumerBuilder { return b.Group("g") },
		func() *ConsumerBuilder { return b.Topics("t") },
		func() *ConsumerBuilder { return b.TLS(&tls.Config{MinVersion: tls.VersionTLS12}) },
		func() *ConsumerBuilder { return b.SASL(plain.Auth{User: "u", Pass: "p"}.AsMechanism()) },
		func() *ConsumerBuilder { return b.AllowPlaintextSASL() },
		func() *ConsumerBuilder { return b.Handler(noopHandler{}) },
		func() *ConsumerBuilder { return b.DLQTopicSuffix(".dlq") },
		func() *ConsumerBuilder { return b.RetryBudget(3) },
		func() *ConsumerBuilder { return b.Classifier(func(error) bool { return false }) },
		func() *ConsumerBuilder { return b.CloseTimeout(time.Second) },
		func() *ConsumerBuilder { return b.Options(WithConsumerLogger(log.NewNop())) },
	}

	for i, fn := range checks {
		if got := fn(); got != nil {
			t.Errorf("check %d: nil-receiver setter returned non-nil %v; want nil", i, got)
		}
	}

	// Build on a nil receiver fails closed (ErrNilHandler), never panics.
	if _, err := b.Build(context.Background()); err == nil {
		t.Error("Build() on nil receiver = nil error; want a fail-closed error")
	}
}

// TestConsumerBuilder_TLSSetterEnablesSASL proves TLS and SASL state actually
// reach the config: a SASL-without-TLS build is rejected by Validate, while the
// same SASL build WITH TLS succeeds — observable proof both setters wrote their
// (unexported) config fields. AllowPlaintextSASL is exercised the same way.
func TestConsumerBuilder_TLSSetterEnablesSASL(t *testing.T) {
	t.Parallel()

	mech := plain.Auth{User: "u", Pass: "p"}.AsMechanism()

	base := func() *ConsumerBuilder {
		return NewConsumer().
			Brokers("localhost:9092").
			Group("g").
			Topics("t").
			Handler(noopHandler{})
	}

	// SASL without TLS -> rejected at Build (SASL-requires-TLS gate).
	if _, err := base().SASL(mech).Build(context.Background()); !errors.Is(err, ErrPlaintextSASLNotAllowed) {
		t.Errorf("Build with SASL and no TLS; want ErrPlaintextSASLNotAllowed: %v", err)
	}

	// SASL with TLS -> accepted.
	c, err := base().SASL(mech).TLS(&tls.Config{MinVersion: tls.VersionTLS12}).Build(context.Background())
	if err != nil {
		t.Fatalf("Build with SASL+TLS = %v; want nil", err)
	}

	_ = c.Close()

	// SASL with explicit plaintext opt-in -> accepted.
	c2, err := base().SASL(mech).AllowPlaintextSASL().Build(context.Background())
	if err != nil {
		t.Fatalf("Build with SASL + AllowPlaintextSASL = %v; want nil", err)
	}

	_ = c2.Close()
}

// TestConsumerBuilder_BuildEnabledFalseIsNoop proves Build short-circuits to the
// disabled-mode no-op when Enabled(false): the no-op is always Healthy and needs
// no brokers (it never dials), distinguishing it from the real runtime.
func TestConsumerBuilder_BuildEnabledFalseIsNoop(t *testing.T) {
	t.Parallel()

	// No brokers/group/topics/handler at all — a real Build would fail Validate;
	// the disabled path skips validation entirely and returns the no-op.
	c, err := NewConsumer().Enabled(false).Build(context.Background())
	if err != nil {
		t.Fatalf("Build(Enabled=false) = %v; want nil (no-op, no validation)", err)
	}

	defer func() { _ = c.Close() }()

	if err := c.Healthy(context.Background()); err != nil {
		t.Errorf("no-op Healthy() = %v; want nil (always ready)", err)
	}
}

// TestConsumerOptionPassthroughs proves the WithConsumer* facade options forward
// to the internal consumer.With* options (they are thin aliases; the test pins
// the wiring so a future rename can't silently drop one). Construction-only: a
// non-nil ConsumerOption is the observable.
func TestConsumerOptionPassthroughs(t *testing.T) {
	t.Parallel()

	opts := []ConsumerOption{
		WithConsumerLogger(log.NewNop()),
		WithConsumerMetricsFactory(&metrics.MetricsFactory{}),
		WithConsumerTracer(noop.NewTracerProvider().Tracer("t")),
	}

	for i, opt := range opts {
		if opt == nil {
			t.Errorf("WithConsumer* option %d = nil; want a non-nil ConsumerOption", i)
		}
	}

	// Smoke: a built runtime accepts the passthrough options without error.
	c, err := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Topics("t").
		Handler(noopHandler{}).
		Options(opts...).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build with passthrough options = %v; want nil", err)
	}

	_ = c.Close()
}

// ensure the consumer alias types resolve (compile-time pin).
var _ consumer.Runner = (Consumer)(nil)
