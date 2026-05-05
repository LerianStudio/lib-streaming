//go:build unit

package producer

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestCBListener_PerTargetTransitionsAreIsolated exercises the multi-target
// CB state listener: forcing transitions on one target's breaker MUST NOT
// move another target's rt.state mirror. Tests three transitions per the
// open/half-open/closed lifecycle.
func TestCBListener_PerTargetTransitionsAreIsolated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	fakeMgr := newFakeCBManager()

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-cb-listener-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	rtPrimary := p.targets["primary"]
	rtSecondary := p.targets["secondary"]
	if rtPrimary == nil || rtSecondary == nil {
		t.Fatalf("targets not registered: primary=%v secondary=%v", rtPrimary, rtSecondary)
	}

	// Pre-conditions: both states start CLOSED.
	if got := rtPrimary.state.Load(); got != flagCBClosed {
		t.Fatalf("primary initial state = %d; want %d", got, flagCBClosed)
	}
	if got := rtSecondary.state.Load(); got != flagCBClosed {
		t.Fatalf("secondary initial state = %d; want %d", got, flagCBClosed)
	}

	// Drive secondary OPEN. Primary MUST stay CLOSED.
	fakeMgr.ForceTransition(rtSecondary.cbServiceName, circuitbreaker.StateOpen)

	if got := rtSecondary.state.Load(); got != flagCBOpen {
		t.Errorf("secondary state after OPEN = %d; want %d", got, flagCBOpen)
	}
	if got := rtPrimary.state.Load(); got != flagCBClosed {
		t.Errorf("primary state after secondary OPEN = %d; want %d (isolation broken)", got, flagCBClosed)
	}

	// Drive secondary HALF-OPEN. Primary still untouched.
	fakeMgr.ForceTransition(rtSecondary.cbServiceName, circuitbreaker.StateHalfOpen)

	if got := rtSecondary.state.Load(); got != flagCBHalfOpen {
		t.Errorf("secondary state after HALF-OPEN = %d; want %d", got, flagCBHalfOpen)
	}
	if got := rtPrimary.state.Load(); got != flagCBClosed {
		t.Errorf("primary state after secondary HALF-OPEN = %d; want %d (isolation broken)", got, flagCBClosed)
	}

	// Drive secondary back to CLOSED.
	fakeMgr.ForceTransition(rtSecondary.cbServiceName, circuitbreaker.StateClosed)

	if got := rtSecondary.state.Load(); got != flagCBClosed {
		t.Errorf("secondary state after CLOSED = %d; want %d", got, flagCBClosed)
	}
	if got := rtPrimary.state.Load(); got != flagCBClosed {
		t.Errorf("primary state remains = %d; want %d", got, flagCBClosed)
	}
}

// TestCBListener_TwoTargetsIndependentTransitions exercises independent
// transitions on both targets in interleaved order. Confirms each
// rt.state reflects only its own breaker's transitions.
func TestCBListener_TwoTargetsIndependentTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	fakeMgr := newFakeCBManager()

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-cb-independent-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	rtPrimary := p.targets["primary"]
	rtSecondary := p.targets["secondary"]
	if rtPrimary == nil || rtSecondary == nil {
		t.Fatalf("targets not registered")
	}

	// Each pair drives one breaker and checks both states.
	cases := []struct {
		name        string
		serviceName string
		to          circuitbreaker.State
		wantPrimary int32
		wantSecond  int32
	}{
		{
			name:        "primary OPEN",
			serviceName: rtPrimary.cbServiceName,
			to:          circuitbreaker.StateOpen,
			wantPrimary: flagCBOpen,
			wantSecond:  flagCBClosed,
		},
		{
			name:        "secondary OPEN",
			serviceName: rtSecondary.cbServiceName,
			to:          circuitbreaker.StateOpen,
			wantPrimary: flagCBOpen,
			wantSecond:  flagCBOpen,
		},
		{
			name:        "primary HALF-OPEN",
			serviceName: rtPrimary.cbServiceName,
			to:          circuitbreaker.StateHalfOpen,
			wantPrimary: flagCBHalfOpen,
			wantSecond:  flagCBOpen,
		},
		{
			name:        "primary CLOSED",
			serviceName: rtPrimary.cbServiceName,
			to:          circuitbreaker.StateClosed,
			wantPrimary: flagCBClosed,
			wantSecond:  flagCBOpen,
		},
		{
			name:        "secondary HALF-OPEN",
			serviceName: rtSecondary.cbServiceName,
			to:          circuitbreaker.StateHalfOpen,
			wantPrimary: flagCBClosed,
			wantSecond:  flagCBHalfOpen,
		},
		{
			name:        "secondary CLOSED",
			serviceName: rtSecondary.cbServiceName,
			to:          circuitbreaker.StateClosed,
			wantPrimary: flagCBClosed,
			wantSecond:  flagCBClosed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakeMgr.ForceTransition(tc.serviceName, tc.to)

			if got := rtPrimary.state.Load(); got != tc.wantPrimary {
				t.Errorf("primary state = %d; want %d", got, tc.wantPrimary)
			}
			if got := rtSecondary.state.Load(); got != tc.wantSecond {
				t.Errorf("secondary state = %d; want %d", got, tc.wantSecond)
			}
		})
	}
}

// TestCBListener_ForeignServiceNameIgnored asserts the listener drops
// notifications for service names that do not belong to this Producer.
// The Manager broadcasts every transition to every listener, so a noisy
// cohabiting Producer must not move our per-target rt.state mirror.
func TestCBListener_ForeignServiceNameIgnored(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	fakeMgr := newFakeCBManager()

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-cb-foreign-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	rtPrimary := p.targets["primary"]
	if rtPrimary == nil {
		t.Fatalf("primary target not registered")
	}

	// Snapshot the pre-notification state so we can assert non-mutation
	// regardless of whether the constructor or any prior transition moved
	// it. (In practice it starts at flagCBClosed via the zero value.)
	preState := rtPrimary.state.Load()

	// Find the registered listener and call directly with a foreign name.
	if fakeMgr.listenerCount() != 1 {
		t.Fatalf("listenerCount = %d; want 1", fakeMgr.listenerCount())
	}
	listener := fakeMgr.listeners[0]

	// Drive with a foreign service name — one that cannot match any of
	// our per-target runtimes. The listener MUST NOT move rtPrimary.state.
	listener.OnStateChange(ctx, "streaming.producer:other-instance:target:other",
		circuitbreaker.StateClosed, circuitbreaker.StateOpen)

	if got := rtPrimary.state.Load(); got != preState {
		t.Errorf("primary rt.state after foreign notification = %d; want %d (foreign filter broken)", got, preState)
	}

	// And p.targetState (the test-helper view onto the same atomic) must
	// agree with the direct rt.state.Load above.
	if got := p.targetState("primary"); got != preState {
		t.Errorf("p.targetState(\"primary\") after foreign notification = %d; want %d", got, preState)
	}
}
