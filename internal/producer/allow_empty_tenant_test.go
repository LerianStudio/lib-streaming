//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
)

// --- Issue #24: WithAllowEmptyTenant opt-in -----------------------------------

// TestProducer_PreFlight_EmptyTenant_WithOpt_Accepted pins the synchronous
// gate relaxation: a non-system event with an empty TenantID passes preflight
// when the Producer was built WithAllowEmptyTenant. This is the single-tenant
// deployment opt-in and is distinct from SystemEvent.
func TestProducer_PreFlight_EmptyTenant_WithOpt_Accepted(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithAllowEmptyTenant(),
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
		t.Errorf("preFlight empty tenant with WithAllowEmptyTenant err = %v; want nil", err)
	}
}

// TestProducer_PreFlight_EmptyTenant_WithoutOpt_Rejected pins the default
// (no opt-in) behavior is unchanged: an empty-tenant non-system event is
// still rejected with ErrMissingTenantID at the synchronous gate.
func TestProducer_PreFlight_EmptyTenant_WithoutOpt_Rejected(t *testing.T) {
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

	err = p.preFlightWithPayload(context.Background(), e, true)
	if !errors.Is(err, ErrMissingTenantID) {
		t.Errorf("preFlight empty tenant without opt err = %v; want ErrMissingTenantID", err)
	}
}

// TestProducer_AllowEmptyTenant_DoesNotEnableSystemEvents pins that the
// new option is orthogonal to SystemEvent: a SystemEvent=true emission on a
// Producer that opted into empty tenant but NOT into system events is still
// rejected with ErrSystemEventsNotAllowed. AllowEmptyTenant must not become
// a backdoor into the privileged system:* partition space.
func TestProducer_AllowEmptyTenant_DoesNotEnableSystemEvents(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithAllowEmptyTenant(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	e := sampleEvent()
	e.TenantID = ""
	e.SystemEvent = true
	(&e).ApplyDefaults()

	err = p.preFlightWithPayload(context.Background(), e, true)
	if !errors.Is(err, ErrSystemEventsNotAllowed) {
		t.Errorf("preFlight system event with AllowEmptyTenant err = %v; want ErrSystemEventsNotAllowed", err)
	}
}

// TestProducer_AllowEmptyTenant_PersistsFlagOnEnvelope pins that a Producer
// built WithAllowEmptyTenant stamps AllowEmptyTenant=true onto every
// OutboxEnvelope it persists, so the relay — which decodes the row with no
// producer-option context — respects the opt-in on replay. The persisted
// row must also carry an empty-tenant event WITHOUT being rejected at the
// write-time ValidateShape gate.
func TestProducer_AllowEmptyTenant_PersistsFlagOnEnvelope(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	repo := &fakeOutboxRepo{}

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(repo),
		WithAllowEmptyTenant(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	if !p.allowEmptyTenant {
		t.Fatalf("p.allowEmptyTenant = false; want true (option not threaded onto Producer)")
	}

	// Resolve a real route for the "transaction.created" definition so the
	// envelope shape is valid for everything except the tenant rule.
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
		t.Fatalf("publishRouteOutbox empty-tenant with AllowEmptyTenant err = %v; want nil", err)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("repo.Create was not called; envelope not persisted")
	}

	var persisted OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &persisted); err != nil {
		t.Fatalf("json.Unmarshal row.Payload err = %v", err)
	}

	if !persisted.AllowEmptyTenant {
		t.Fatalf("persisted envelope AllowEmptyTenant = false; want true")
	}

	// Relay-time decode path must respect the opt-in: full Validate on the
	// persisted row passes despite the empty tenant.
	if err := persisted.Validate(); err != nil {
		t.Fatalf("persisted.Validate() err = %v; want nil (relay must respect AllowEmptyTenant)", err)
	}
}

// TestProducer_AllowEmptyTenant_PartitionKeyUnchanged is a regression guard:
// the opt-in must NOT alter partition-key derivation. A non-system event
// still partitions by TenantID (empty here), and a system event still uses
// the "system:" prefix. AllowEmptyTenant only relaxes the empty-tenant
// rejection — it has zero effect on routing/partitioning.
func TestProducer_AllowEmptyTenant_PartitionKeyUnchanged(t *testing.T) {
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
