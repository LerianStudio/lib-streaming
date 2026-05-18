//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

func TestProducer_TenantAwareCircuitBreakersIsolateTenants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{
			Source:         "svc://tenant-cb-test",
			CBFailureRatio: 1,
			CBMinRequests:  1,
			CBTimeout:      time.Hour,
		},
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

	if p.tenantCBManager == nil {
		t.Fatal("tenantCBManager = nil; want lib-commons TenantAwareManager")
	}

	primary.SetPublishError(errors.New("tenant-a upstream outage"))
	if err := p.Emit(ctx, tenantRequest("tenant_a")); err == nil {
		t.Fatal("tenant_a first Emit error = nil; want broker failure to trip tenant breaker")
	}

	serviceName := p.targets["primary"].cbServiceName
	if got := p.tenantCBManager.GetStateForTenant("tenant_a", serviceName); got != circuitbreaker.StateOpen {
		t.Fatalf("tenant_a breaker state = %s; want open", got)
	}

	primary.SetPublishError(nil)
	if err := p.Emit(ctx, tenantRequest("tenant_b")); err != nil {
		t.Fatalf("tenant_b Emit error = %v; want nil (tenant_a breaker must not poison tenant_b)", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("published messages after tenant_b = %d; want 1", got)
	}

	if err := p.Emit(ctx, tenantRequest("tenant_a")); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("tenant_a second Emit error = %v; want ErrCircuitOpen", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("published messages after tenant_a open retry = %d; want 1", got)
	}
}

func TestProducer_TenantAwareCircuitOpenOutboxFallbackWritesEnvelope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	writer := &captureRouteOutboxWriter{}
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{
			Source:         "svc://tenant-cb-outbox-test",
			CBFailureRatio: 1,
			CBMinRequests:  1,
			CBTimeout:      time.Hour,
		},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithOutboxWriter(writer),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	primary.SetPublishError(errors.New("tenant-a upstream outage"))
	if err := p.Emit(ctx, tenantRequest("tenant_a")); err == nil {
		t.Fatal("tenant_a first Emit error = nil; want broker failure to trip tenant breaker")
	}

	primary.SetPublishError(nil)
	if err := p.Emit(ctx, tenantRequest("tenant_a")); err != nil {
		t.Fatalf("tenant_a second Emit error = %v; want nil (tenant-scoped outbox fallback)", err)
	}

	if got := len(primary.Messages()); got != 0 {
		t.Fatalf("primary published after tenant_a open retry = %d; want 0", got)
	}
	if got := len(writer.envelopes); got != 1 {
		t.Fatalf("writer envelopes = %d; want 1", got)
	}
	if got := writer.envelopes[0]; got.Target != "primary" || got.RouteKey != "transaction.created.kafka.primary" || got.Event.TenantID != "tenant_a" {
		t.Fatalf("outbox envelope = %+v; want primary route for tenant_a", got)
	}

	if err := p.Emit(ctx, tenantRequest("tenant_b")); err != nil {
		t.Fatalf("tenant_b Emit error = %v; want nil (tenant_a outbox fallback must not poison tenant_b)", err)
	}
	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published after tenant_b = %d; want 1", got)
	}
}

func TestProducer_TenantAwareCircuitBreakerSkipsSystemEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	catalog, err := NewCatalog(EventDefinition{
		Key:             "ledger.rolled_over",
		ResourceType:    "ledger",
		EventType:       "rolled_over",
		SchemaVersion:   "1.0.0",
		DataContentType: defaultDataContentType,
		SystemEvent:     true,
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}
	routes, err := contract.NewRouteTable(
		multiTestRoute("ledger.rolled-over.kafka.primary", "ledger.rolled_over", "primary", "lerian.streaming.ledger.rolled_over", contract.RouteRequired),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{
			Source:         "svc://system-cb-test",
			CBFailureRatio: 1,
			CBMinRequests:  1,
			CBTimeout:      time.Hour,
		},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	req := EmitRequest{DefinitionKey: "ledger.rolled_over", TenantID: "tenant_a", Payload: []byte(`{}`)}
	primary.SetPublishError(errors.New("system upstream outage"))
	if err := p.Emit(ctx, req); err == nil {
		t.Fatal("system event first Emit error = nil; want broker failure to trip no-tenant breaker")
	}

	serviceName := p.targets["primary"].cbServiceName
	if got := p.tenantCBManager.GetStateForTenant("tenant_a", serviceName); got != circuitbreaker.StateUnknown {
		t.Fatalf("tenant-scoped system breaker state = %s; want unknown", got)
	}
	if got := p.cbManager.GetState(serviceName); got != circuitbreaker.StateOpen {
		t.Fatalf("no-tenant system breaker state = %s; want open", got)
	}

	primary.SetPublishError(nil)
	if err := p.Emit(ctx, req); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("system event second Emit error = %v; want ErrCircuitOpen from no-tenant breaker", err)
	}
}

func TestProducer_TenantAwareRecoveryPokesRegisteredTenantBreakers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{
			Source:         "svc://tenant-cb-recovery-test",
			CBFailureRatio: 1,
			CBMinRequests:  1,
			CBTimeout:      50 * time.Millisecond,
		},
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

	primary.SetPublishError(errors.New("tenant-a upstream outage"))
	if err := p.Emit(ctx, tenantRequest("tenant_a")); err == nil {
		t.Fatal("tenant_a first Emit error = nil; want broker failure to trip tenant breaker")
	}

	serviceName := p.targets["primary"].cbServiceName
	if got := p.tenantCBManager.GetStateForTenant("tenant_a", serviceName); got != circuitbreaker.StateOpen {
		t.Fatalf("tenant_a breaker state = %s; want open", got)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		p.pokeAllTargetCBs()
		if got := p.tenantCBManager.GetStateForTenant("tenant_a", serviceName); got == circuitbreaker.StateHalfOpen {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := p.tenantCBManager.GetStateForTenant("tenant_a", serviceName); got != circuitbreaker.StateHalfOpen {
		t.Fatalf("tenant_a breaker state after recovery pokes = %s; want half-open", got)
	}
}

func TestCircuitBreakerTenantIDScope(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		event    Event
		want     string
		wantOK   bool
		wantHash bool
	}{
		{name: "valid tenant passes through", event: Event{TenantID: "tenant_a"}, want: "tenant_a", wantOK: true},
		{name: "invalid tenant hashes", event: Event{TenantID: "urn:tenant:lerian:dev"}, wantOK: true, wantHash: true},
		{name: "empty tenant has no tenant scope", event: Event{}, wantOK: false},
		{name: "system event has no tenant scope", event: Event{TenantID: "tenant_a", SystemEvent: true}, wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := circuitBreakerTenantID(tc.event)
			if ok != tc.wantOK {
				t.Fatalf("circuitBreakerTenantID ok = %v; want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if tc.wantHash {
				if got == tc.event.TenantID {
					t.Fatal("circuitBreakerTenantID returned raw invalid tenant ID; want safe surrogate")
				}
				if !strings.HasPrefix(got, circuitBreakerTenantPrefix) {
					t.Fatalf("circuitBreakerTenantID = %q; want %q prefix", got, circuitBreakerTenantPrefix)
				}
			} else if got != tc.want {
				t.Fatalf("circuitBreakerTenantID = %q; want %q", got, tc.want)
			}
			if !tmcore.IsValidTenantID(got) {
				t.Fatalf("circuitBreakerTenantID = %q; want tenant-manager-valid ID", got)
			}

			gotAgain, ok := circuitBreakerTenantID(tc.event)
			if !ok || gotAgain != got {
				t.Fatalf("circuitBreakerTenantID second call = %q, %v; want %q, true", gotAgain, ok, got)
			}
		})
	}
}

func TestCBListener_TenantTransitionDoesNotPoisonNoTenantMirror(t *testing.T) {
	t.Parallel()

	p := &Producer{
		producerID:         "producer-tenant-listener",
		logger:             log.NewNop(),
		metrics:            newStreamingMetrics(nil, log.NewNop()),
		primaryTargetName:  "primary",
		targets:            map[string]*targetRuntime{},
		cloudEventsSource:  "svc://tenant-listener-test",
		cbRecoveryInterval: time.Hour,
	}
	rt := &targetRuntime{name: "primary", kind: TransportKafkaLike, cbServiceName: targetCBServiceName(p.producerID, "primary")}
	p.targets["primary"] = rt

	listener := &streamingStateListener{producer: p}
	listener.OnTenantStateChange(context.Background(), "tenant_a", rt.cbServiceName, circuitbreaker.StateClosed, circuitbreaker.StateOpen)

	if got := rt.state.Load(); got != flagCBClosed {
		t.Fatalf("rt.state after tenant transition = %d; want flagCBClosed (%d)", got, flagCBClosed)
	}
}

func tenantRequest(tenantID string) EmitRequest {
	req := sampleRequest()
	req.TenantID = tenantID

	return req
}
