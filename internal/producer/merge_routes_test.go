//go:build unit

package producer

import (
	"testing"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// The canonical merge-semantics table test lives in
// internal/contract/route_merge_test.go (the single source of truth,
// contract.MergeRouteOverrides). These tests cover the producer-side
// integration: the WithRouteOverrides option and the single-target
// autoGenerateKafkaRoutes wiring that applies the shared helper.

func billingOverrideRoute() contract.RouteDefinition {
	return contract.RouteDefinition{
		Key:           "billing_recorded.kafka.replica",
		DefinitionKey: "billing_recorded",
		Target:        "replica",
		Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.billing-svc"},
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

	if o.routeOverrides[0].Key != "billing_recorded.kafka.replica" {
		t.Errorf("routeOverrides[0].Key = %q after caller mutation; want defensive copy", o.routeOverrides[0].Key)
	}
}

// TestAutoGenerateKafkaRoutes_SingleCatchAllRoute pins the v3 route collapse:
// the convenience constructor synthesizes ONE catch-all route to the
// application's topic, not one route per catalog definition. Every definition
// resolves through it.
func TestAutoGenerateKafkaRoutes_SingleCatchAllRoute(t *testing.T) {
	t.Parallel()

	table, err := autoGenerateKafkaRoutes("billing-svc", nil)
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	if got := table.Len(); got != 1 {
		t.Fatalf("route table len = %d; want exactly 1 catch-all route", got)
	}

	for _, definitionKey := range []string{"transaction.created", "billing_recorded", "anything"} {
		routes := table.Routes(definitionKey)
		if len(routes) != 1 {
			t.Fatalf("Routes(%q) = %d routes; want 1", definitionKey, len(routes))
		}

		if routes[0].Destination.Name != "lerian.streaming.billing-svc" {
			t.Errorf("Routes(%q) destination = %q; want the app topic", definitionKey, routes[0].Destination.Name)
		}
	}
}

// TestAutoGenerateKafkaRoutes_AppliesOverride proves the single-target path
// merges overrides via the shared helper: a definition-scoped override wins
// for its own definition (no double-publish alongside the catch-all), while
// every other definition still resolves through the catch-all route.
func TestAutoGenerateKafkaRoutes_AppliesOverride(t *testing.T) {
	t.Parallel()

	table, err := autoGenerateKafkaRoutes("billing-svc", []contract.RouteDefinition{billingOverrideRoute()})
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	billingRoutes := table.Routes("billing_recorded")
	if len(billingRoutes) != 1 {
		t.Fatalf("billing routes = %d, want exactly 1 (scoped override beats catch-all)", len(billingRoutes))
	}

	if billingRoutes[0].Target != "replica" {
		t.Errorf("billing route target = %q, want the override target %q", billingRoutes[0].Target, "replica")
	}

	transactionRoutes := table.Routes("transaction.created")
	if len(transactionRoutes) != 1 {
		t.Fatalf("transaction routes = %d, want 1 (catch-all, unaffected)", len(transactionRoutes))
	}

	if transactionRoutes[0].Target != "primary" {
		t.Errorf("transaction route target = %q, want the catch-all target %q", transactionRoutes[0].Target, "primary")
	}
}
