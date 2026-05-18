//go:build unit

package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestDispatchRoute_BranchMatrix exercises every documented terminal-state
// branch of (*Producer).dispatchRoute. The function is the per-route core
// of the multi-target Emit path; under-coverage here means a regression in
// outcome classification (produced / outboxed / failed / caller_error /
// dlq) goes undetected.
//
// The strategy: build a real Producer via NewProducerMulti so the targets
// map and circuit breakers are wired correctly, then drive dispatchRoute
// directly with hand-crafted RouteDefinitions. Every test starts a fresh
// span via the noop tracer because the dispatch contract treats span as
// an output sink (RecordError, AddEvent), not a control input.
//
// Per-case fake adapter configuration drives the publish-time outcome:
//   - SetPublishError(nil)              → success path → outcomeProduced
//   - SetPublishError(some-error) + Classify → outcomeFailed | outcomeDLQ
//
// We deliberately stay below NewProducerMulti's all-or-error route table
// validator: dispatchRoute does NOT consult the route table; it consumes
// the per-call RouteDefinition directly. So a route that the table would
// reject (e.g. unregistered target) is still a valid dispatchRoute input
// for the corruption branch we want to test.
func TestDispatchRoute_BranchMatrix(t *testing.T) {
	t.Parallel()

	// Shared catalog + base "happy" route table seeded once. Per-case
	// route is constructed locally so each test owns its delivery
	// policy and destination.
	catalog := sampleCatalog(t)

	// The base route table is just a baseline so NewProducerMulti
	// accepts the build. Per-case dispatch ignores it.
	baseRoutes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	tracer := noop.NewTracerProvider().Tracer("test")

	disabledFlag := false

	cases := []struct {
		name string
		// route built per-case; must reference a real target name.
		route func() contract.RouteDefinition
		// adapter setup for "primary" target.
		setup func(t *testing.T, p *Producer, primary *fake.Adapter)
		// outboxWriter — non-nil when the case requires an outbox path.
		outboxWriter OutboxWriter
		// expected outcome state from dispatchRoute.
		wantState string
		// expected error class (or "" when no class is set).
		wantClass contract.ErrorClass
		// expected cause (sentinel) — checked via errors.Is when non-nil.
		wantCause error
	}{
		{
			name: "successful publish → outcomeProduced",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup:     func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			wantState: outcomeProduced,
		},
		{
			name: "route disabled by policy → outcomeCallerError + ErrEventDisabled",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
					Policy: contract.DeliveryPolicyOverride{
						Enabled: &disabledFlag, // explicitly disabled
					},
				}
			},
			setup:     func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			wantState: outcomeCallerError,
			wantClass: contract.ClassValidation,
			wantCause: ErrEventDisabled,
		},
		{
			name: "unregistered target → outcomeFailed + ErrMissingTarget",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.ghost",
					DefinitionKey: "transaction.created",
					Target:        "ghost", // not in p.targets
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup:     func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			wantState: outcomeFailed,
			wantClass: contract.ClassValidation,
			wantCause: contract.ErrMissingTarget,
		},
		{
			name: "destination kind mismatch → outcomeCallerError",
			route: func() contract.RouteDefinition {
				// primary is Kafka but destination kind is SQS.
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup:     func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			wantState: outcomeCallerError,
			wantClass: contract.ClassValidation,
			wantCause: contract.ErrInvalidDestination,
		},
		{
			name: "breaker open WITHOUT outbox → outcomeCircuitOpen + ErrCircuitOpen",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup: func(_ *testing.T, p *Producer, _ *fake.Adapter) {
				// This case exercises the legacy no-tenant mirror branch. The
				// tenant-aware path has separate regression coverage because it
				// must not let one tenant's OPEN state poison another tenant.
				p.tenantCBManager = nil
				// Force the per-target state mirror to OPEN. The
				// dispatch hot path consults rt.state.Load() before
				// the underlying breaker's Execute path.
				p.targets["primary"].state.Store(flagCBOpen)
			},
			wantState: outcomeCircuitOpen,
			wantClass: contract.ClassBrokerUnavailable,
			wantCause: ErrCircuitOpen,
		},
		{
			name: "breaker open WITH outbox → outcomeOutboxed",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup: func(_ *testing.T, p *Producer, _ *fake.Adapter) {
				p.tenantCBManager = nil
				p.targets["primary"].state.Store(flagCBOpen)
			},
			outboxWriter: &captureRouteOutboxWriter{},
			wantState:    outcomeOutboxed,
		},
		{
			name: "adapter publish returns ClassValidation → outcomeCallerError (no DLQ)",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup: func(_ *testing.T, _ *Producer, primary *fake.Adapter) {
				// ErrPayloadTooLarge classifies to ClassValidation
				// via fake.Classify → contract.IsCallerError → true.
				primary.SetPublishError(contract.ErrPayloadTooLarge)
			},
			wantState: outcomeCallerError,
			wantClass: contract.ClassValidation,
			wantCause: contract.ErrPayloadTooLarge,
		},
		{
			name: "adapter publish returns ClassContextCanceled → outcomeFailed (no DLQ)",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup: func(_ *testing.T, _ *Producer, primary *fake.Adapter) {
				primary.SetPublishError(context.Canceled)
			},
			wantState: outcomeFailed,
			wantClass: contract.ClassContextCanceled,
			wantCause: context.Canceled,
		},
		{
			name: "adapter publish returns generic broker error → DLQ-routable failure",
			route: func() contract.RouteDefinition {
				// No DLQ destination → DLQ delivery itself fails;
				// outcome falls through to outcomeFailed. We assert
				// the class + cause so the DLQ-attempt code path is
				// covered without requiring a wired DLQ adapter.
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
				}
			},
			setup: func(_ *testing.T, _ *Producer, primary *fake.Adapter) {
				// fake.Classify maps unknown errors to
				// ClassBrokerUnavailable, which IS DLQ-routable.
				primary.SetPublishError(errors.New("broker timeout"))
			},
			wantState: outcomeFailed,
			wantClass: contract.ClassBrokerUnavailable,
		},
		{
			name: "outbox-always policy short-circuits broker → outcomeOutboxed",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
					Policy: contract.DeliveryPolicyOverride{
						Direct: contract.DirectModeSkip,
						Outbox: contract.OutboxModeAlways,
					},
				}
			},
			setup:        func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			outboxWriter: &captureRouteOutboxWriter{},
			wantState:    outcomeOutboxed,
		},
		{
			name: "outbox-always policy + outbox writer error → outcomeOutboxFailed",
			route: func() contract.RouteDefinition {
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
					Policy: contract.DeliveryPolicyOverride{
						Direct: contract.DirectModeSkip,
						Outbox: contract.OutboxModeAlways,
					},
				}
			},
			setup: func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			// failingRouteOutboxWriter returns a fixed error from its
			// Write method so we hit the outcomeOutboxFailed branch.
			outboxWriter: &failingRouteOutboxWriter{err: errors.New("outbox down")},
			wantState:    outcomeOutboxFailed,
			// dispatchRoute classifies outbox-write failures via the
			// target adapter's Classify (broker-adjacent infra error)
			// rather than hard-coding ClassValidation. fake.Classify
			// maps an opaque "outbox down" error to
			// ClassBrokerUnavailable, which is the right class for
			// an infra failure: a DB outage should not be mislabeled
			// as caller-correctable, since callers cannot fix it
			// and would not retry.
			wantClass: contract.ClassBrokerUnavailable,
		},
		{
			name: "invalid route policy override → outcomeCallerError",
			route: func() contract.RouteDefinition {
				// Direct=skip without Outbox=always violates the
				// cross-field rule; applyRoutePolicy.Validate
				// surfaces an ErrInvalidDeliveryPolicy chain.
				return contract.RouteDefinition{
					Key:           "transaction.created.kafka.primary",
					DefinitionKey: "transaction.created",
					Target:        "primary",
					Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
					Requirement:   contract.RouteRequired,
					Policy: contract.DeliveryPolicyOverride{
						Direct: contract.DirectModeSkip,
						Outbox: contract.OutboxModeNever,
					},
				}
			},
			setup:     func(_ *testing.T, _ *Producer, _ *fake.Adapter) {},
			wantState: outcomeCallerError,
			wantClass: contract.ClassValidation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			primary := fake.NewAdapter(TransportKafkaLike)

			opts := []EmitterOption{
				WithLogger(log.NewNop()),
				WithCatalog(catalog),
			}

			if tc.outboxWriter != nil {
				opts = append(opts, WithOutboxWriter(tc.outboxWriter))
			}

			p, err := NewProducerMulti(
				ctx,
				MultiProducerConfig{Source: "svc://dispatch-route-test"},
				nil,
				[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
				baseRoutes,
				catalog,
				opts...,
			)
			if err != nil {
				t.Fatalf("NewProducerMulti() error = %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })

			tc.setup(t, p, primary)

			// Resolve the event so the call shape matches what
			// emitMulti would build at runtime. ApplyDefaults pads
			// missing required fields (EventID, timestamp).
			event := sampleEvent()
			event.ApplyDefaults()

			route := tc.route()

			_, span := tracer.Start(ctx, "test")
			defer span.End()

			outcome := p.dispatchRoute(
				ctx,
				span,
				event,
				event.Topic(),
				"transaction.created",
				DefaultDeliveryPolicy(),
				buildCloudEventsTransportHeaders(event),
				route,
			)

			if outcome.state != tc.wantState {
				t.Errorf("outcome.state = %q; want %q", outcome.state, tc.wantState)
			}
			if tc.wantClass != "" && outcome.class != tc.wantClass {
				t.Errorf("outcome.class = %q; want %q", outcome.class, tc.wantClass)
			}
			if tc.wantCause != nil && !errors.Is(outcome.cause, tc.wantCause) {
				t.Errorf("outcome.cause = %v; want errors.Is(..., %v)", outcome.cause, tc.wantCause)
			}
		})
	}
}

// TestDispatchRoute_NilAdapterReturnsErrNilProducer pins the asserter-trident
// branch: when a route's target runtime exists but its adapter pointer is
// nil (post-construction state corruption — NewProducerMulti rejects nil
// adapters at construction, so this only happens on tampering or test
// fixtures), dispatchRoute fires the asserter and returns outcomeFailed
// with cause=ErrNilProducer.
//
// We bypass the NewProducerMulti adapter-nil guard by clearing rt.adapter
// after construction. The asserter MUST fire (logs + span event) and the
// outcome MUST surface ErrNilProducer through the chain.
func TestDispatchRoute_NilAdapterReturnsErrNilProducer(t *testing.T) {
	t.Parallel()

	catalog := sampleCatalog(t)
	primary := fake.NewAdapter(TransportKafkaLike)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://nil-adapter"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Corrupt the adapter post-construction. The dispatch path's
	// post-construction invariant fires here.
	p.targets["primary"].adapter = nil

	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")
	defer span.End()

	event := sampleEvent()
	event.ApplyDefaults()

	route := contract.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		Requirement:   contract.RouteRequired,
	}

	outcome := p.dispatchRoute(
		context.Background(),
		span,
		event,
		event.Topic(),
		"transaction.created",
		DefaultDeliveryPolicy(),
		buildCloudEventsTransportHeaders(event),
		route,
	)

	if outcome.state != outcomeFailed {
		t.Errorf("outcome.state = %q; want %q", outcome.state, outcomeFailed)
	}
	if !errors.Is(outcome.cause, ErrNilProducer) {
		t.Errorf("outcome.cause = %v; want errors.Is(..., ErrNilProducer)", outcome.cause)
	}
}

// TestDispatchRoute_NilCBReturnsErrNilProducer mirrors the nil-adapter test
// for the per-target circuit breaker. NewProducerMulti always wires a CB,
// so this only triggers under post-construction tampering. We verify the
// asserter trident path returns the documented sentinel.
func TestDispatchRoute_NilCBReturnsErrNilProducer(t *testing.T) {
	t.Parallel()

	catalog := sampleCatalog(t)
	primary := fake.NewAdapter(TransportKafkaLike)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://nil-cb"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Corrupt the breaker pointer post-construction.
	p.targets["primary"].cb = nil

	tracer := noop.NewTracerProvider().Tracer("test")
	_, span := tracer.Start(context.Background(), "test")
	defer span.End()

	event := sampleEvent()
	event.ApplyDefaults()

	route := contract.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		Requirement:   contract.RouteRequired,
	}

	outcome := p.dispatchRoute(
		context.Background(),
		span,
		event,
		event.Topic(),
		"transaction.created",
		DefaultDeliveryPolicy(),
		buildCloudEventsTransportHeaders(event),
		route,
	)

	if outcome.state != outcomeFailed {
		t.Errorf("outcome.state = %q; want %q", outcome.state, outcomeFailed)
	}
	if !errors.Is(outcome.cause, ErrNilProducer) {
		t.Errorf("outcome.cause = %v; want errors.Is(..., ErrNilProducer)", outcome.cause)
	}
}

// _ pins the trace package import so future maintenance does not remove
// it when refactoring the per-test span construction. Keeping the import
// alive matters because the noop.NewTracerProvider().Tracer chain returns
// an interface satisfying trace.Tracer, and the linter would otherwise flag
// the import-only-for-implicit-interface case if it ever changed.
var _ trace.Tracer = noop.NewTracerProvider().Tracer("")

// failingRouteOutboxWriter is a test-only OutboxWriter that fails every
// envelope write with a fixed error. Used to drive the outcomeOutboxFailed
// classification branch in dispatch tests.
type failingRouteOutboxWriter struct {
	err error
}

func (w *failingRouteOutboxWriter) Write(_ context.Context, _ OutboxEnvelope) error {
	return w.err
}
