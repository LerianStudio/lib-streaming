//go:build unit

package contract

import (
	"errors"
	"slices"
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

// TestRouteTable_DefinitionScopedRouteIsAdditiveAcrossTargets pins the v3
// resolution semantics, which are ADDITIVE per target, not winner-take-all.
//
// The old winner-take-all rule silently diverted an event OFF the app topic:
// scoping a definition to a SECOND target (the "shadow these events to SQS"
// shape) suppressed the catch-all Kafka route for that definition, so the
// event never reached the app stream at all while Emit still reported success.
// Durable loss with a green dashboard.
//
// The rule now: for a definition key, resolution returns the definition-scoped
// routes PLUS every catch-all route whose Target no scoped route already
// covers. Scoping the SAME target overrides the catch-all for that target
// only; scoping a DIFFERENT target adds to it and never suppresses it.
func TestRouteTable_DefinitionScopedRouteIsAdditiveAcrossTargets(t *testing.T) {
	t.Parallel()

	t.Run("different target adds to the catch-all", func(t *testing.T) {
		t.Parallel()

		table, err := NewRouteTable(
			kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"),
			kafkaRoute("loan_disbursed.replica", "loan.disbursed", "replica", "lerian.streaming.lender"),
		)
		if err != nil {
			t.Fatalf("NewRouteTable() error = %v", err)
		}

		targets := routeTargets(table.Routes("loan.disbursed"))
		if len(targets) != 2 || !slices.Contains(targets, "primary") || !slices.Contains(targets, "replica") {
			t.Fatalf("Routes(loan.disbursed) targets = %v; want both the catch-all primary and the scoped replica", targets)
		}
	})

	t.Run("same target overrides the catch-all for that target", func(t *testing.T) {
		t.Parallel()

		table, err := NewRouteTable(
			kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"),
			kafkaRoute("loan_disbursed.primary", "loan.disbursed", "primary", "lerian.streaming.lender_override"),
		)
		if err != nil {
			t.Fatalf("NewRouteTable() error = %v", err)
		}

		scoped := table.Routes("loan.disbursed")
		if len(scoped) != 1 {
			t.Fatalf("Routes(loan.disbursed) = %+v; want exactly one route (scoped overrides catch-all on the same target)", scoped)
		}

		if scoped[0].Key != "loan_disbursed.primary" {
			t.Errorf("Routes(loan.disbursed)[0].Key = %q; want the scoped override", scoped[0].Key)
		}
	})

	t.Run("unscoped definitions still resolve to the catch-all", func(t *testing.T) {
		t.Parallel()

		table, err := NewRouteTable(
			kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"),
			kafkaRoute("loan_disbursed.replica", "loan.disbursed", "replica", "lerian.streaming.lender"),
		)
		if err != nil {
			t.Fatalf("NewRouteTable() error = %v", err)
		}

		fallback := table.Routes("installment.settled")
		if len(fallback) != 1 || fallback[0].Target != "primary" {
			t.Fatalf("Routes(installment.settled) = %+v; want the catch-all route", fallback)
		}
	})
}

// TestRouteTable_SecondCatchAllMirrorsEveryDefinition NAMES the consequence of
// additive resolution that is easiest to reach for by accident: a second route
// with an empty DefinitionKey mirrors the app's ENTIRE stream to a second
// destination. That is app-wide mirroring, and it is intended — the only way
// to express "everything, twice" — but it is double-publish, so it is pinned
// here deliberately rather than discovered in production.
func TestRouteTable_SecondCatchAllMirrorsEveryDefinition(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(
		kafkaRoute("primary.kafka", "", "primary", "lerian.streaming.lender"),
		kafkaRoute("mirror.kafka", "", "mirror", "lerian.streaming.lender"),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	for _, definitionKey := range []string{"loan.disbursed", "installment.settled"} {
		targets := routeTargets(table.Routes(definitionKey))
		if len(targets) != 2 {
			t.Fatalf("Routes(%q) targets = %v; want app-wide mirroring to both catch-all targets", definitionKey, targets)
		}
	}
}

func routeTargets(routes []RouteDefinition) []string {
	targets := make([]string, 0, len(routes))
	for _, route := range routes {
		targets = append(targets, route.Target)
	}

	return targets
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
