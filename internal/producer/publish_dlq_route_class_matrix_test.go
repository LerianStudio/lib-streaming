//go:build unit

package producer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// classifyingFakeAdapter is a class-aware fake transport for the DLQ
// matrix test. Source-route Publish always fails with publishErr, classed
// via the configured pinClass. Destination Publish (= DLQ topic when in
// Kafka-fallback mode) succeeds and records the message for inspection.
type classifyingFakeAdapter struct {
	mu          sync.Mutex
	kind        contract.TransportKind
	publishErr  error
	pinClass    contract.ErrorClass
	sourceTopic string

	dlqMessages []transport.TransportMessage
}

func newClassifyingFakeAdapter(sourceTopic string, publishErr error, pinClass contract.ErrorClass) *classifyingFakeAdapter {
	return &classifyingFakeAdapter{
		kind:        contract.TransportKafkaLike,
		publishErr:  publishErr,
		pinClass:    pinClass,
		sourceTopic: sourceTopic,
	}
}

func (a *classifyingFakeAdapter) Kind() contract.TransportKind {
	return a.kind
}

func (a *classifyingFakeAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	if message.Destination.Name == a.sourceTopic {
		return a.publishErr
	}

	// DLQ destination — record for inspection and succeed.
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dlqMessages = append(a.dlqMessages, transport.CloneMessage(message))

	return nil
}

func (a *classifyingFakeAdapter) Healthy(context.Context) error { return nil }
func (a *classifyingFakeAdapter) Flush(context.Context) error   { return nil }
func (a *classifyingFakeAdapter) Close(context.Context) error   { return nil }

// Classify returns the pinned class for any non-nil error so test cases
// can drive each ErrorClass independently. nil falls through to a benign
// default (matches the fake.Adapter convention).
func (a *classifyingFakeAdapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ClassContextCanceled
	}

	return a.pinClass
}

func (a *classifyingFakeAdapter) DLQMessages() []transport.TransportMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]transport.TransportMessage, len(a.dlqMessages))
	copy(out, a.dlqMessages)
	return out
}

// TestPublishRouteDLQ_ClassMatrix pins the eight-class DLQ routing rules
// from TRD §C9 for the multi-target dispatch path. For each ErrorClass:
//
//   - ClassValidation, ClassContextCanceled → no DLQ publish, error
//     surfaces to caller; streaming_dlq_total NOT incremented.
//   - All other classes → DLQ publish to <source>.dlq (Kafka fallback);
//     DLQ failures do NOT surface to caller, streaming_dlq_publish_failed_total
//     would increment if the DLQ adapter rejected (covered by separate
//     test).
//
// The test exercises a single-target multi-target Producer. Required-route
// failures aggregate into MultiEmitError; we assert on the embedded
// per-route Class to confirm the classification flowed end-to-end.
func TestPublishRouteDLQ_ClassMatrix(t *testing.T) {
	t.Parallel()

	type wantOutcome struct {
		dlqDelivered bool
		errorClass   contract.ErrorClass
	}

	cases := []struct {
		name  string
		class contract.ErrorClass
		want  wantOutcome
	}{
		{
			name:  "validation does not route",
			class: contract.ClassValidation,
			want: wantOutcome{
				dlqDelivered: false,
				errorClass:   contract.ClassValidation,
			},
		},
		{
			name:  "context canceled does not route",
			class: contract.ClassContextCanceled,
			want: wantOutcome{
				dlqDelivered: false,
				errorClass:   contract.ClassContextCanceled,
			},
		},
		{
			name:  "serialization routes to DLQ",
			class: contract.ClassSerialization,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassSerialization,
			},
		},
		{
			name:  "auth routes to DLQ",
			class: contract.ClassAuth,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassAuth,
			},
		},
		{
			name:  "topic_not_found routes to DLQ",
			class: contract.ClassTopicNotFound,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassTopicNotFound,
			},
		},
		{
			name:  "broker_unavailable routes to DLQ",
			class: contract.ClassBrokerUnavailable,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassBrokerUnavailable,
			},
		},
		{
			name:  "network_timeout routes to DLQ",
			class: contract.ClassNetworkTimeout,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassNetworkTimeout,
			},
		},
		{
			name:  "broker_overloaded routes to DLQ",
			class: contract.ClassBrokerOverloaded,
			want: wantOutcome{
				dlqDelivered: true,
				errorClass:   contract.ClassBrokerOverloaded,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			catalog := sampleCatalog(t)
			sourceTopic := "lerian.streaming.transaction.created"

			adapter := newClassifyingFakeAdapter(sourceTopic, errors.New("simulated publish failure"), tc.class)

			routes := mustMultiRouteTable(t,
				multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", sourceTopic, contract.RouteRequired),
			)

			p, err := NewProducerMulti(
				ctx,
				MultiProducerConfig{Source: "svc://dlq-matrix"},
				nil,
				[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
				routes,
				catalog,
				WithLogger(log.NewNop()),
			)
			if err != nil {
				t.Fatalf("NewProducerMulti() error = %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })

			emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
			if emitErr == nil {
				t.Fatalf("Emit() error = nil; want non-nil for class %q", tc.class)
			}

			// Per-route class travels in MultiEmitError.Required[0].Class.
			var multi *contract.MultiEmitError
			if !errors.As(emitErr, &multi) {
				t.Fatalf("Emit() error = %v; want MultiEmitError", emitErr)
			}
			if got := len(multi.Required); got != 1 {
				t.Fatalf("MultiEmitError.Required len = %d; want 1", got)
			}
			if got := multi.Required[0].Class; got != tc.want.errorClass {
				t.Errorf("MultiEmitError.Required[0].Class = %q; want %q", got, tc.want.errorClass)
			}

			dlqMessages := adapter.DLQMessages()
			if tc.want.dlqDelivered {
				if len(dlqMessages) != 1 {
					t.Errorf("DLQ messages = %d; want 1 for class %q (DLQ-routable)", len(dlqMessages), tc.class)
				} else {
					if dest := dlqMessages[0].Destination; dest.Name != sourceTopic+".dlq" {
						t.Errorf("DLQ destination = %q; want %s.dlq", dest.Name, sourceTopic)
					}
				}
			} else {
				if len(dlqMessages) != 0 {
					t.Errorf("DLQ messages = %d; want 0 for class %q (non-DLQ-routable)", len(dlqMessages), tc.class)
				}
			}
		})
	}
}

func TestPublishRouteDLQ_DestinationAttributesReachAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := sampleCatalog(t)
	sourceTopic := "lerian.streaming.transaction.created"
	adapter := newClassifyingFakeAdapter(sourceTopic, errors.New("simulated publish failure"), contract.ClassBrokerUnavailable)
	dlq := contract.Destination{
		Kind:       TransportKafkaLike,
		Name:       sourceTopic + ".dlq",
		Attributes: map[string]string{"eventbridge.resources": "arn:aws:events:us-east-1:123:rule/dlq"},
	}
	route := multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", sourceTopic, contract.RouteRequired)
	route.DLQ = &dlq
	routes := mustMultiRouteTable(t, route)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://dlq-attributes"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err == nil {
		t.Fatal("Emit() error = nil; want source failure")
	}

	messages := adapter.DLQMessages()
	if got := len(messages); got != 1 {
		t.Fatalf("DLQ messages = %d; want 1", got)
	}
	if got := messages[0].Attributes["eventbridge.resources"]; got != "arn:aws:events:us-east-1:123:rule/dlq" {
		t.Fatalf("DLQ message attribute = %q; want DLQ destination attribute", got)
	}
}

// failPublishRouteDLQAdapter is a transport adapter where the source
// publish ALWAYS fails (driving DLQ routing) AND the DLQ destination
// publish ALSO ALWAYS fails (so we can pin the
// streaming_dlq_publish_failed_total counter increment).
//
// The contract this fixture exists to pin: when the producer attempts
// DLQ delivery and the DLQ publish itself fails, that failure must NOT
// surface to the Emit caller — it logs at ERROR level and increments
// streaming_dlq_publish_failed_total instead. The original Required
// failure surface is unchanged.
type failPublishRouteDLQAdapter struct {
	mu          sync.Mutex
	kind        contract.TransportKind
	sourceErr   error
	dlqErr      error
	sourceTopic string

	dlqAttempts int
}

func newFailPublishRouteDLQAdapter(sourceTopic string, sourceErr, dlqErr error) *failPublishRouteDLQAdapter {
	return &failPublishRouteDLQAdapter{
		kind:        contract.TransportKafkaLike,
		sourceErr:   sourceErr,
		dlqErr:      dlqErr,
		sourceTopic: sourceTopic,
	}
}

func (a *failPublishRouteDLQAdapter) Kind() contract.TransportKind { return a.kind }

func (a *failPublishRouteDLQAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	if message.Destination.Name == a.sourceTopic {
		return a.sourceErr
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.dlqAttempts++

	return a.dlqErr
}

func (a *failPublishRouteDLQAdapter) Healthy(context.Context) error { return nil }
func (a *failPublishRouteDLQAdapter) Flush(context.Context) error   { return nil }
func (a *failPublishRouteDLQAdapter) Close(context.Context) error   { return nil }

func (a *failPublishRouteDLQAdapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ClassContextCanceled
	}
	// Pin every error to ClassBrokerUnavailable so the source failure is
	// DLQ-routable and the DLQ-publish-failed counter can be exercised.
	return contract.ClassBrokerUnavailable
}

func (a *failPublishRouteDLQAdapter) DLQAttempts() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dlqAttempts
}

// TestPublishRouteDLQ_PublishFailIncrementsCounter pins the contract
// referenced by the comment block on TestPublishRouteDLQ_ClassMatrix
// (lines 88-92): "DLQ failures do NOT surface to caller,
// streaming_dlq_publish_failed_total would increment if the DLQ adapter
// rejected (covered by separate test)".
//
// Setup:
//
//   - Single Required Kafka route. Source publish fails with a
//     ClassBrokerUnavailable-classed error → DLQ routable.
//   - The same adapter handles BOTH source and DLQ publish (Kafka
//     fallback convention). DLQ publish ALSO fails.
//
// Assertions:
//
//  1. Emit returns the original Required failure (NOT the DLQ failure
//     — the DLQ rejection must be swallowed).
//  2. streaming_dlq_publish_failed_total{topic=<source-topic>}
//     increments exactly once.
//  3. streaming_dlq_total may also fire (recorded BEFORE the publish
//     attempt) but that is covered by the existing matrix test;
//     here we only pin the failure counter.
func TestPublishRouteDLQ_PublishFailIncrementsCounter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := sampleCatalog(t)
	sourceTopic := "lerian.streaming.transaction.created"

	adapter := newFailPublishRouteDLQAdapter(
		sourceTopic,
		errors.New("simulated source publish failure"),
		errors.New("simulated DLQ publish failure"),
	)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", sourceTopic, contract.RouteRequired),
	)

	factory, snapshot := newManualMeterSetup(t)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://dlq-fail"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithMetricsFactory(factory),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
	if emitErr == nil {
		t.Fatal("Emit() error = nil; want MultiEmitError carrying the source failure")
	}

	// (1) The original Required-failure must surface; DLQ rejection is
	// best-effort and must NOT propagate.
	var multi *contract.MultiEmitError
	if !errors.As(emitErr, &multi) {
		t.Fatalf("Emit() error = %T (%v); want *MultiEmitError", emitErr, emitErr)
	}
	if got := len(multi.Required); got != 1 {
		t.Fatalf("MultiEmitError.Required len = %d; want 1", got)
	}
	if !errors.Is(emitErr, adapter.sourceErr) {
		t.Errorf("errors.Is(emitErr, sourceErr) = false; want true (DLQ failure must not replace source failure in chain)")
	}
	if errors.Is(emitErr, adapter.dlqErr) {
		t.Errorf("errors.Is(emitErr, dlqErr) = true; want false (DLQ failure must NOT surface to caller)")
	}

	// Sanity-check the adapter actually saw a DLQ publish attempt
	// (otherwise the counter assertion below is meaningless).
	if got := adapter.DLQAttempts(); got != 1 {
		t.Fatalf("DLQ adapter attempts = %d; want 1 (DLQ routing must have fired)", got)
	}

	// (2) streaming_dlq_publish_failed_total increments exactly once.
	rm := snapshot()
	dlqFailed, ok := findMetric(rm, metricNameDLQFailed)
	if !ok {
		var seen []string
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				seen = append(seen, m.Name)
			}
		}
		t.Fatalf("metric %q not found in snapshot; metrics seen = %v", metricNameDLQFailed, seen)
	}

	total, attrSets := sumInt64DataPoints(t, dlqFailed)
	if total != 1 {
		t.Errorf("streaming_dlq_publish_failed_total = %d; want 1", total)
	}
	if len(attrSets) != 1 {
		t.Fatalf("attrSets len = %d; want 1", len(attrSets))
	}
	if got := attrSets[0]["topic"]; got != sourceTopic {
		t.Errorf("topic label = %q; want %q (DLQ counter is keyed on SOURCE topic)", got, sourceTopic)
	}
}
