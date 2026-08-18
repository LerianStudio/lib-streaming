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
// merges overrides via the shared helper, under the ADDITIVE-per-target
// resolution rule: an override on a DIFFERENT target adds a destination for
// its definition and leaves the app-topic catch-all route in place, so the
// event still reaches the app stream. Every other definition is untouched.
func TestAutoGenerateKafkaRoutes_AppliesOverride(t *testing.T) {
	t.Parallel()

	table, err := autoGenerateKafkaRoutes("billing-svc", []contract.RouteDefinition{billingOverrideRoute()})
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	billingRoutes := table.Routes("billing_recorded")
	if len(billingRoutes) != 2 {
		t.Fatalf("billing routes = %d, want 2 (catch-all primary PLUS the replica override)", len(billingRoutes))
	}

	targets := map[string]bool{}
	for _, route := range billingRoutes {
		targets[route.Target] = true
	}

	if !targets["primary"] || !targets["replica"] {
		t.Errorf("billing route targets = %v; want both primary (app topic, never suppressed) and replica (override)", targets)
	}

	transactionRoutes := table.Routes("transaction.created")
	if len(transactionRoutes) != 1 {
		t.Fatalf("transaction routes = %d, want 1 (catch-all, unaffected)", len(transactionRoutes))
	}

	if transactionRoutes[0].Target != "primary" {
		t.Errorf("transaction route target = %q, want the catch-all target %q", transactionRoutes[0].Target, "primary")
	}
}

// TestAutoGenerateKafkaRoutes_SameTargetOverrideReplacesCatchAll is the other
// half of the additive rule: an override that names the SAME target as the
// catch-all replaces it for that definition, so re-pointing a handful of
// events at a different topic does not double-publish them.
func TestAutoGenerateKafkaRoutes_SameTargetOverrideReplacesCatchAll(t *testing.T) {
	t.Parallel()

	override := billingOverrideRoute()
	override.Key = "billing_recorded.kafka.primary"
	override.Target = "primary"
	override.Destination.Name = "lerian.streaming.billing-svc-audit"

	table, err := autoGenerateKafkaRoutes("billing-svc", []contract.RouteDefinition{override})
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	billingRoutes := table.Routes("billing_recorded")
	if len(billingRoutes) != 1 {
		t.Fatalf("billing routes = %d, want exactly 1 (same-target override replaces the catch-all)", len(billingRoutes))
	}

	if got := billingRoutes[0].Destination.Name; got != "lerian.streaming.billing-svc-audit" {
		t.Errorf("billing destination = %q; want the override topic", got)
	}
}
