//go:build unit

package producer

import (
	"testing"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
)

// The canonical merge-semantics table test lives in
// internal/contract/route_merge_test.go (the single source of truth,
// contract.MergeRouteOverrides). These tests cover the producer-side
// integration: the WithRouteOverrides option and the single-target
// autoGenerateKafkaRoutes wiring that applies the shared helper.

func billingOverrideRoute() contract.RouteDefinition {
	return contract.RouteDefinition{
		Key:           "billing-recorded.kafka.primary",
		DefinitionKey: "billing_recorded",
		Target:        "primary",
		Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.billing.recorded"},
		Requirement:   contract.RouteRequired,
	}
}

func TestWithRouteOverrides_SetsOptionDefensively(t *testing.T) {
	t.Parallel()

	input := []contract.RouteDefinition{billingOverrideRoute()}

	var o emitterOptions

	WithRouteOverrides(input...)(&o)

	if len(o.routeOverrides) != 1 {
		t.Fatalf("routeOverrides len = %d, want 1", len(o.routeOverrides))
	}

	if o.routeOverrides[0].DefinitionKey != "billing_recorded" {
		t.Errorf("routeOverrides[0].DefinitionKey = %q, want %q", o.routeOverrides[0].DefinitionKey, "billing_recorded")
	}

	// Defensive copy: mutating the caller slice must not affect the option.
	input[0] = contract.RouteDefinition{Key: "mutated"}

	if o.routeOverrides[0].Key != "billing-recorded.kafka.primary" {
		t.Errorf("routeOverrides[0].Key = %q after caller mutation; want defensive copy", o.routeOverrides[0].Key)
	}
}

// TestAutoGenerateKafkaRoutes_AppliesOverride proves the single-target path
// merges overrides via the shared helper: an override sharing a catalog
// definition's DefinitionKey REPLACES the auto-generated route (no
// double-publish), while other definitions keep their auto route.
func TestAutoGenerateKafkaRoutes_AppliesOverride(t *testing.T) {
	t.Parallel()

	catalog, err := contract.NewCatalog(
		contract.EventDefinition{Key: "transaction.created", ResourceType: "transaction", EventType: "created"},
		contract.EventDefinition{Key: "billing_recorded", ResourceType: "billing", EventType: "recorded"},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	table, err := autoGenerateKafkaRoutes(catalog, "//svc", []contract.RouteDefinition{billingOverrideRoute()})
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	billingRoutes := table.Routes("billing_recorded")
	if len(billingRoutes) != 1 {
		t.Fatalf("billing routes = %d, want exactly 1 (override replaces auto)", len(billingRoutes))
	}

	if billingRoutes[0].Destination.Name != "lerian.streaming.billing.recorded" {
		t.Errorf("billing route destination = %q, want the override topic %q",
			billingRoutes[0].Destination.Name, "lerian.streaming.billing.recorded")
	}

	if got := len(table.Routes("transaction.created")); got != 1 {
		t.Fatalf("transaction routes = %d, want 1 (auto-generated, unaffected)", got)
	}
}
