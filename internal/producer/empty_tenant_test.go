//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- Issue #24: empty tenant is a first-class single-tenant scope ------------

// TestProducer_EmitEmptyTenant_DirectPath pins issue #24 on the synchronous
// publish path: a non-system event with an empty TenantID is accepted end-to-
// end with no opt-in (the option no longer exists). Empty tenant is a valid
// single-tenant scope, not a caller fault.
func TestProducer_EmitEmptyTenant_DirectPath(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	req := sampleRequest()
	req.TenantID = ""

	if err := emitter.Emit(context.Background(), req); err != nil {
		t.Fatalf("Emit empty tenant err = %v; want nil", err)
	}

	consumer := newConsumer(t, cluster, "lerian.streaming.transaction.created")
	fetchCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	fetches := consumer.PollFetches(fetchCtx)

	var produced int
	fetches.EachRecord(func(_ *kgo.Record) { produced++ })

	if produced != 1 {
		t.Fatalf("produced records = %d; want 1 (empty-tenant event must reach the broker)", produced)
	}
}

// TestProducer_EmitEmptyTenant_PreFlight pins that the synchronous preflight
// gate accepts an empty-tenant non-system event with no opt-in.
func TestProducer_EmitEmptyTenant_PreFlight(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	e := sampleEvent()
	e.TenantID = ""
	e.SystemEvent = false
	(&e).ApplyDefaults()

	if err := p.preFlightWithPayload(context.Background(), e, true); err != nil {
		t.Errorf("preFlight empty tenant err = %v; want nil", err)
	}
}

// TestProducer_EmitEmptyTenant_OutboxPath pins that the outbox-fallback path
// persists a VALID envelope for an empty-tenant non-system event with no
// opt-in. The relay decodes the row with no producer-option context, so the
// full Validate must also pass on replay.
func TestProducer_EmitEmptyTenant_OutboxPath(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	repo := &fakeOutboxRepo{}

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(repo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	routes := p.routes.Routes("transaction.created")
	if len(routes) == 0 {
		t.Fatalf("no routes for transaction.created; cannot build envelope")
	}

	route := routes[0]

	e := sampleEvent()
	e.TenantID = ""
	e.SystemEvent = false
	(&e).ApplyDefaults()

	if err := p.publishRouteOutbox(context.Background(), e, "transaction.created", route, DefaultDeliveryPolicy()); err != nil {
		t.Fatalf("publishRouteOutbox empty-tenant err = %v; want nil", err)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("repo.Create was not called; envelope not persisted")
	}

	var persisted OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &persisted); err != nil {
		t.Fatalf("json.Unmarshal row.Payload err = %v", err)
	}

	if err := persisted.Validate(); err != nil {
		t.Fatalf("persisted.Validate() err = %v; want nil (relay must accept empty tenant)", err)
	}
}

// TestProducer_EmitEmptyTenant_PartitionKeyUnchanged is a regression guard:
// accepting an empty tenant must NOT alter partition-key derivation. A
// non-system event still partitions by TenantID (empty here), and a system
// event still uses the "system:" prefix.
func TestProducer_EmitEmptyTenant_PartitionKeyUnchanged(t *testing.T) {
	t.Parallel()

	nonSystem := sampleEvent()
	nonSystem.TenantID = ""

	nonSystem.SystemEvent = false
	if got := nonSystem.PartitionKey(); got != "" {
		t.Errorf("non-system empty-tenant PartitionKey() = %q; want \"\"", got)
	}

	system := sampleEvent()
	system.TenantID = ""

	system.SystemEvent = true
	if got := system.PartitionKey(); got != "system:"+system.EventType {
		t.Errorf("system PartitionKey() = %q; want %q", got, "system:"+system.EventType)
	}
}
