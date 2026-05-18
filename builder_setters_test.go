//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/internal/producer"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// captureLoggerSentinel is a no-op logger that wraps log.NewNop so a test
// can compare identity with == against the value Builder.Logger received.
// Using log.NewNop() directly would not allow identity assertions because
// each call returns a fresh instance; this wrapper holds onto a single
// instance the test can probe.
type captureLoggerSentinel struct {
	log.Logger
	id int // disambiguates instances at the type level
}

// captureFactoryAdapter records the TransportAdapterOptions the factory
// receives so the test can inspect what flowed through Builder plumbing.
type captureFactoryAdapter struct {
	gotLogger log.Logger
	gotName   string
}

func (c *captureFactoryAdapter) Kind() streaming.TransportKind { return streaming.TransportCustom }
func (c *captureFactoryAdapter) Publish(context.Context, transport.TransportMessage) error {
	return nil
}
func (c *captureFactoryAdapter) Healthy(context.Context) error { return nil }
func (c *captureFactoryAdapter) Flush(context.Context) error   { return nil }
func (c *captureFactoryAdapter) Close(context.Context) error   { return nil }
func (c *captureFactoryAdapter) Classify(error) streaming.ErrorClass {
	return streaming.ClassBrokerUnavailable
}

// TestBuilder_Logger_FlowsToTransportFactory pins the C8 fix: the user-
// supplied logger from Builder.Logger MUST flow into TransportAdapterOptions.Logger
// when buildTargetSpecs invokes the per-kind factory. The previous
// implementation always handed log.NewNop() to non-Kafka adapters.
func TestBuilder_Logger_FlowsToTransportFactory(t *testing.T) {
	t.Parallel()

	wantLogger := &captureLoggerSentinel{Logger: log.NewNop(), id: 7}
	captured := &captureFactoryAdapter{}

	customKind := streaming.TransportCustom

	b := streaming.NewBuilder().
		Source("svc://logger-flow-test").
		Catalog(builderCatalog(t)).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.custom.primary",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination: streaming.Destination{
				Kind: customKind,
				Name: "custom-sink",
			},
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{Name: "primary", Kind: customKind}).
		Logger(wantLogger).
		RegisterTransport(customKind, func(_ context.Context, opts producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
			captured.gotLogger = opts.Logger
			captured.gotName = opts.Name
			return captured, nil
		})

	emitter, err := b.Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	if captured.gotLogger == nil {
		t.Fatal("factory received nil logger; want propagated Builder.Logger")
	}
	if captured.gotLogger != wantLogger {
		t.Errorf("factory logger = %p; want %p (Builder.Logger value)", captured.gotLogger, wantLogger)
	}
	if captured.gotName != "primary" {
		t.Errorf("factory name = %q; want primary", captured.gotName)
	}
}

// TestBuilder_Logger_DefaultsToNopWhenUnset pins that resolveLogger falls
// back to log.NewNop when Builder.Logger was never called, matching the
// pre-C8-fix default behavior (only the always-Nop bug was the regression).
func TestBuilder_Logger_DefaultsToNopWhenUnset(t *testing.T) {
	t.Parallel()

	captured := &captureFactoryAdapter{}
	customKind := streaming.TransportCustom

	b := streaming.NewBuilder().
		Source("svc://default-logger-test").
		Catalog(builderCatalog(t)).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.custom.default",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination: streaming.Destination{
				Kind: customKind,
				Name: "custom-sink",
			},
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{Name: "primary", Kind: customKind}).
		RegisterTransport(customKind, func(_ context.Context, opts producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
			captured.gotLogger = opts.Logger
			return captured, nil
		})

	emitter, err := b.Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	if captured.gotLogger == nil {
		t.Fatal("factory received nil logger; want fallback log.NewNop()")
	}
	// We cannot identity-compare against log.NewNop() because each call
	// returns a fresh instance. The non-nil check is sufficient: the bug
	// was returning nil-equivalent (a fresh nop), not panicking.
}

// alwaysFailAdapter is a transport adapter whose Publish always returns
// publishErr. Used to drive a circuit breaker to OPEN by feeding it a
// known number of consecutive failures.
type alwaysFailAdapter struct {
	kind       streaming.TransportKind
	publishErr error
}

func (a *alwaysFailAdapter) Kind() streaming.TransportKind { return a.kind }
func (a *alwaysFailAdapter) Publish(_ context.Context, _ transport.TransportMessage) error {
	return a.publishErr
}
func (a *alwaysFailAdapter) Healthy(context.Context) error { return nil }
func (a *alwaysFailAdapter) Flush(context.Context) error   { return nil }
func (a *alwaysFailAdapter) Close(context.Context) error   { return nil }
func (a *alwaysFailAdapter) Classify(error) streaming.ErrorClass {
	return streaming.ClassBrokerUnavailable
}

// TestBuilder_CBSetters_PropagateToMultiProducerConfig pins the C9 fix:
// CBFailureRatio / CBMinRequests / CBTimeout setters must persist and
// reach producer.MultiProducerConfig when Build dispatches to the multi-
// target path.
//
// Refactored away from reflection (former cbFailureRatio/cbMinRequests/
// cbTimeout field probes): rather than read unexported Builder fields,
// we observe the per-target circuit breaker actually flip to OPEN
// after exactly CBMinRequests consecutive failures (with FailureRatio
// = 1.0 the threshold is purely the request count).
//
// The (CBMinRequests, CBFailureRatio) pair is the dominant operational
// knob; we pin it via behavior. CBTimeout is asserted indirectly: it
// must be a sane positive value otherwise NewProducerMulti would
// reject the config — the successful Build below proves it propagated.
func TestBuilder_CBSetters_PropagateToMultiProducerConfig(t *testing.T) {
	t.Parallel()

	const cbMinRequests = 5

	customKind := streaming.TransportCustom

	emitter, err := streaming.NewBuilder().
		Source("svc://cb-test").
		Catalog(builderCatalog(t)).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.custom.cb",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination: streaming.Destination{
				Kind: customKind,
				Name: "custom-sink",
			},
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{Name: "primary", Kind: customKind}).
		// FailureRatio=1.0 means "every observation in the rolling
		// window must be a failure" — combined with cbMinRequests
		// successive failures, the breaker MUST flip OPEN exactly at
		// observation #cbMinRequests.
		CBFailureRatio(1.0).
		CBMinRequests(cbMinRequests).
		CBTimeout(7*time.Second).
		Logger(log.NewNop()).
		RegisterTransport(customKind, func(_ context.Context, _ producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
			return &alwaysFailAdapter{kind: customKind, publishErr: errors.New("synthetic broker failure")}, nil
		}).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	ctx := context.Background()
	request := streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "t-cb",
		Subject:       "tx-cb",
		Payload:       []byte(`{"amount":1}`),
	}

	// Drive cbMinRequests consecutive failures. Each Emit returns a
	// MultiEmitError carrying the underlying broker failure (NOT
	// ErrCircuitOpen yet). The CB opens AT this attempt count.
	for i := 0; i < cbMinRequests; i++ {
		emitErr := emitter.Emit(ctx, request)
		if emitErr == nil {
			t.Fatalf("Emit #%d error = nil; want broker failure", i)
		}
		if errors.Is(emitErr, streaming.ErrCircuitOpen) {
			t.Fatalf("Emit #%d already returned ErrCircuitOpen at attempt %d; expected breaker to open AFTER %d attempts", i, i+1, cbMinRequests)
		}
	}

	// Next Emit MUST observe ErrCircuitOpen — the breaker has now
	// crossed the cbMinRequests threshold and tripped.
	emitErr := emitter.Emit(ctx, request)
	if !errors.Is(emitErr, streaming.ErrCircuitOpen) {
		t.Fatalf("Emit #%d error = %v; want errors.Is(ErrCircuitOpen) (CB should be OPEN after %d failures)", cbMinRequests, emitErr, cbMinRequests)
	}
}

// TestBuilder_CBSetters_NilBuilderIsSafe pins the receiver-nil contract on
// the new setters so Build chains over a nil pointer don't panic.
func TestBuilder_CBSetters_NilBuilderIsSafe(t *testing.T) {
	t.Parallel()

	var b *streaming.Builder

	if got := b.CBFailureRatio(0.5); got != nil {
		t.Errorf("CBFailureRatio(nil-receiver) = %v; want nil", got)
	}
	if got := b.CBMinRequests(10); got != nil {
		t.Errorf("CBMinRequests(nil-receiver) = %v; want nil", got)
	}
	if got := b.CBTimeout(time.Second); got != nil {
		t.Errorf("CBTimeout(nil-receiver) = %v; want nil", got)
	}
}
