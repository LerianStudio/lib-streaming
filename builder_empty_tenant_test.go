//go:build unit

package streaming_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/log"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/internal/producer"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// captureEmptyTenantAdapter records every published message so the test can
// assert that an empty-tenant non-system event reached the transport.
type captureEmptyTenantAdapter struct {
	mu       sync.Mutex
	messages []transport.TransportMessage
}

func (c *captureEmptyTenantAdapter) Kind() streaming.TransportKind { return streaming.TransportCustom }

func (c *captureEmptyTenantAdapter) Publish(_ context.Context, msg transport.TransportMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.messages = append(c.messages, msg)

	return nil
}

func (c *captureEmptyTenantAdapter) Healthy(context.Context) error { return nil }
func (c *captureEmptyTenantAdapter) Flush(context.Context) error   { return nil }
func (c *captureEmptyTenantAdapter) Close(context.Context) error   { return nil }
func (c *captureEmptyTenantAdapter) Classify(error) streaming.ErrorClass {
	return streaming.ClassBrokerUnavailable
}

func (c *captureEmptyTenantAdapter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.messages)
}

// buildEmptyTenantBuilder wires a custom-transport Builder for the
// "transaction.created" definition against the supplied capture adapter.
func buildEmptyTenantBuilder(t *testing.T, captured *captureEmptyTenantAdapter) *streaming.Builder {
	t.Helper()

	customKind := streaming.TransportCustom

	return streaming.NewBuilder().
		Source("svc://empty-tenant-test").
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
		Logger(log.NewNop()).
		RegisterTransport(customKind, func(_ context.Context, _ producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
			return captured, nil
		})
}

// TestBuilder_EmptyTenant_ReachesTransport pins issue #24 at the public
// Builder seam: an empty TenantID on a non-system event is a first-class
// single-tenant scope. With NO opt-in (the option no longer exists), the
// emit succeeds and dispatches to the transport instead of being rejected.
func TestBuilder_EmptyTenant_ReachesTransport(t *testing.T) {
	t.Parallel()

	captured := &captureEmptyTenantAdapter{}

	emitter, err := buildEmptyTenantBuilder(t, captured).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	err = emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "",
		Payload:       json.RawMessage(`{"amount":100}`),
	})
	if err != nil {
		t.Fatalf("Emit() with empty tenant error = %v; want nil", err)
	}

	if got := captured.count(); got != 1 {
		t.Fatalf("captured messages = %d; want 1 (event must reach transport)", got)
	}
}
