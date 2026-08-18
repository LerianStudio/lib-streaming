//go:build unit

package contract

import (
	"errors"
	"testing"
)

func kafkaRoute(key, definitionKey, target, topic string) RouteDefinition {
	return RouteDefinition{
		Key:           key,
		DefinitionKey: definitionKey,
		Target:        target,
		Destination:   Destination{Kind: TransportKafkaLike, Name: topic},
		Requirement:   RouteRequired,
	}
}

// TestRouteKey_AllowsUnderscores pins the v3 route-key charset. v2 forbade
// underscores, which forced every consuming repo (midaz, matcher, lender,
// br-consignado-gw) to carry '_'->'-' translation machinery because
// ResourceTypes are snake_case — one repo already shipped a latent bug from
// the dual forms.
func TestRouteKey_AllowsUnderscores(t *testing.T) {
	t.Parallel()

	valid := []string{
		"loan_contract.disbursed",
		"primary.kafka",
		"a_b.c_d.e_f",
		"br_consignado_gw.primary",
		"mixed-and_underscored.key",
	}

	for _, key := range valid {
		if _, err := NewRouteDefinition(kafkaRoute(key, "", "primary", "lerian.streaming.lender")); err != nil {
			t.Errorf("NewRouteDefinition(key=%q) error = %v; want nil", key, err)
		}
	}

	invalid := []string{
		"_leading.underscore",
		"trailing.underscore_.",
		"no-dot-at-all",
		"UPPER.case",
		"double..dot",
	}

	for _, key := range invalid {
		if _, err := NewRouteDefinition(kafkaRoute(key, "", "primary", "lerian.streaming.lender")); !errors.Is(err, ErrInvalidRouteDefinition) {
			t.Errorf("NewRouteDefinition(key=%q) error = %v; want ErrInvalidRouteDefinition", key, err)
		}
	}
}

// TestRouteTable_CatchAllServesEveryDefinition pins the v3 route model: one
// route with an empty DefinitionKey serves the whole catalog. This is what
// replaced v2's one-route-per-catalog-entry fanout, which under topic
// collapse would have been N rows pointing at one identical destination.
func TestRouteTable_CatchAllServesEveryDefinition(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	for _, definitionKey := range []string{"loan.disbursed", "installment.settled", "anything_at_all"} {
		routes := table.Routes(definitionKey)
		if len(routes) != 1 {
			t.Fatalf("Routes(%q) returned %d routes; want 1 (catch-all)", definitionKey, len(routes))
		}

		if routes[0].Key != "primary.kafka" {
			t.Errorf("Routes(%q)[0].Key = %q; want primary.kafka", definitionKey, routes[0].Key)
		}
	}
}

// TestRouteTable_DefinitionScopedRouteWinsOverCatchAll pins the precedence
// that makes "shadow only THESE events to SQS" expressible: a definition with
// its own route does NOT also fire the catch-all, so the selected events are
// not double-published.
func TestRouteTable_DefinitionScopedRouteWinsOverCatchAll(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(
		kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"),
		kafkaRoute("loan_disbursed.replica", "loan.disbursed", "replica", "lerian.streaming.lender"),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	scoped := table.Routes("loan.disbursed")
	if len(scoped) != 1 || scoped[0].Target != "replica" {
		t.Fatalf("Routes(loan.disbursed) = %+v; want exactly the replica-scoped route", scoped)
	}

	fallback := table.Routes("installment.settled")
	if len(fallback) != 1 || fallback[0].Target != "primary" {
		t.Fatalf("Routes(installment.settled) = %+v; want the catch-all route", fallback)
	}
}

// TestRouteTable_NoCatchAllYieldsNoRoutes pins that an unmatched definition
// with no catch-all present still resolves to nothing, so the producer's
// ErrNoRoutesConfigured path stays reachable.
func TestRouteTable_NoCatchAllYieldsNoRoutes(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(kafkaRoute("loan_disbursed.primary", "loan.disbursed", "primary", "lerian.streaming.lender"))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	if got := table.Routes("installment.settled"); len(got) != 0 {
		t.Fatalf("Routes(installment.settled) = %+v; want none", got)
	}
}

// TestRouteTable_RoutesReturnsDefensiveCopyOfCatchAll pins that the catch-all
// bucket keeps the same immutability guarantee as the definition-scoped index —
// a caller mutating the returned slice must not corrupt the table for every
// other definition it serves.
func TestRouteTable_RoutesReturnsDefensiveCopyOfCatchAll(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	got := table.Routes("loan.disbursed")
	got[0].Target = "mutated"

	if again := table.Routes("loan.disbursed"); again[0].Target != "primary" {
		t.Fatalf("catch-all route was mutated through the returned slice: target = %q", again[0].Target)
	}
}
