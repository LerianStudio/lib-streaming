//go:build unit

package producer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

type prevalidatingAdapter struct {
	validateErr error
	publishN    atomic.Int64
	mutate      bool
	published   atomic.Value
}

func (*prevalidatingAdapter) Kind() contract.TransportKind { return contract.TransportCustom }

func (a *prevalidatingAdapter) ValidateMessage(message transport.TransportMessage) error {
	if a.mutate {
		if len(message.Payload) > 0 {
			message.Payload[0] = 'X'
		}
		for i := range message.Headers {
			if message.Headers[i].Key == "ce-id" && len(message.Headers[i].Value) > 0 {
				message.Headers[i].Value[0] = 'X'
			}
		}
	}

	return a.validateErr
}

func (a *prevalidatingAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	a.publishN.Add(1)
	a.published.Store(transport.CloneMessage(message))

	return nil
}

func (*prevalidatingAdapter) Healthy(context.Context) error { return nil }
func (*prevalidatingAdapter) Flush(context.Context) error   { return nil }
func (*prevalidatingAdapter) Close(context.Context) error   { return nil }

func (*prevalidatingAdapter) Classify(err error) contract.ErrorClass {
	if contract.IsCallerError(err) {
		return contract.ClassValidation
	}

	return contract.ClassBrokerUnavailable
}

func TestProducer_TransportValidationMutationDoesNotLeakToPublish(t *testing.T) {
	adapter := &prevalidatingAdapter{mutate: true}
	route, err := contract.NewRouteDefinition(contract.RouteDefinition{
		Key:           "transaction.created.custom.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   contract.Destination{Kind: contract.TransportCustom, Name: "custom-sink"},
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v", err)
	}
	routes, err := contract.NewRouteTable(route)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://test", CBTimeout: time.Second},
		nil,
		[]TargetSpec{{Name: "primary", Kind: contract.TransportCustom, Adapter: adapter}},
		routes,
		sampleCatalog(t),
		WithLogger(log.NewNop()),
		WithCatalog(singleEventCatalog(t)),
		WithCircuitBreakerManager(newFakeCBManager()),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(context.Background(), sampleRequest()); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	published, ok := adapter.published.Load().(transport.TransportMessage)
	if !ok {
		t.Fatal("published message not captured")
	}
	if got := string(published.Payload); got != string(sampleRequest().Payload) {
		t.Fatalf("published payload = %q; want original %q", got, string(sampleRequest().Payload))
	}
	for _, header := range published.Headers {
		if header.Key == "ce-id" && len(header.Value) > 0 && header.Value[0] == 'X' {
			t.Fatalf("published ce-id header was mutated: %q", string(header.Value))
		}
	}
}

func TestProducer_TransportValidationBypassesCircuitBreaker(t *testing.T) {
	fakeMgr := newFakeCBManager()
	adapter := &prevalidatingAdapter{validateErr: contract.ErrPayloadTooLarge}
	route, err := contract.NewRouteDefinition(contract.RouteDefinition{
		Key:           "transaction.created.custom.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   contract.Destination{Kind: contract.TransportCustom, Name: "custom-sink"},
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v", err)
	}
	routes, err := contract.NewRouteTable(route)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://test", CBTimeout: time.Second},
		nil,
		[]TargetSpec{{Name: "primary", Kind: contract.TransportCustom, Adapter: adapter}},
		routes,
		sampleCatalog(t),
		WithLogger(log.NewNop()),
		WithCatalog(singleEventCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	err = p.Emit(context.Background(), sampleRequest())
	if !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Fatalf("Emit() error = %v; want ErrPayloadTooLarge", err)
	}
	if !contract.IsCallerError(err) {
		t.Fatalf("Emit() error = %v; want caller-correctable", err)
	}
	if got := adapter.publishN.Load(); got != 0 {
		t.Fatalf("Publish calls = %d; want 0 after pre-validation failure", got)
	}

	fakeMgr.mu.Lock()
	breaker := fakeMgr.breakers[p.targets["primary"].cbServiceName]
	fakeMgr.mu.Unlock()
	if breaker == nil {
		t.Fatal("fake circuit breaker was not registered")
	}
	if got := breaker.executeCalls(); got != 0 {
		t.Fatalf("circuit breaker executions = %d; want 0 for caller validation failure", got)
	}
	if got := fakeMgr.GetState(p.targets["primary"].cbServiceName); got != circuitbreaker.StateClosed {
		t.Fatalf("circuit breaker state = %v; want closed", got)
	}
}

func singleEventCatalog(tb testing.TB) contract.Catalog {
	tb.Helper()

	catalog, err := contract.NewCatalog(contract.EventDefinition{
		Key:             "transaction.created",
		ResourceType:    "transaction",
		EventType:       "created",
		SchemaVersion:   "1.0.0",
		DataContentType: defaultDataContentType,
	})
	if err != nil {
		tb.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}
