//go:build unit

package contract

import (
	"testing"
)

func mergeTestRoute(defKey, key string) RouteDefinition {
	return mergeTestRouteTarget(defKey, key, "primary")
}

func mergeTestRouteTarget(defKey, key, target string) RouteDefinition {
	return RouteDefinition{
		Key:           key,
		DefinitionKey: defKey,
		Target:        target,
		Destination:   Destination{Kind: TransportKafkaLike, Name: "topic-" + defKey},
		Requirement:   RouteRequired,
	}
}

func keysByDefinitionKey(routes []RouteDefinition) map[string][]string {
	out := make(map[string][]string, len(routes))
	for _, r := range routes {
		out[r.DefinitionKey] = append(out[r.DefinitionKey], r.Key)
	}

	return out
}

func TestMergeRouteOverrides(t *testing.T) {
	t.Parallel()

	base := []RouteDefinition{
		mergeTestRoute("billing_recorded", "billing-recorded.kafka.primary"),
		mergeTestRoute("transaction_created", "transaction-created.kafka.primary"),
	}

	cases := []struct {
		name         string
		base         []RouteDefinition
		overrides    []RouteDefinition
		wantLen      int
		wantKeyByDef map[string]string
	}{
		{
			name:      "empty overrides returns base unchanged",
			base:      base,
			overrides: nil,
			wantLen:   2,
			wantKeyByDef: map[string]string{
				"billing_recorded":    "billing-recorded.kafka.primary",
				"transaction_created": "transaction-created.kafka.primary",
			},
		},
		{
			name:      "empty base returns overrides",
			base:      nil,
			overrides: []RouteDefinition{mergeTestRoute("billing_recorded", "billing-recorded.kafka.override")},
			wantLen:   1,
			wantKeyByDef: map[string]string{
				"billing_recorded": "billing-recorded.kafka.override",
			},
		},
		{
			name: "override replaces base route with same DefinitionKey",
			base: base,
			overrides: []RouteDefinition{
				mergeTestRoute("billing_recorded", "billing-recorded.kafka.override"),
			},
			wantLen: 2,
			wantKeyByDef: map[string]string{
				"billing_recorded":    "billing-recorded.kafka.override",
				"transaction_created": "transaction-created.kafka.primary",
			},
		},
		{
			name: "override with new DefinitionKey is appended",
			base: base,
			overrides: []RouteDefinition{
				mergeTestRoute("audit_logged", "audit-logged.kafka.override"),
			},
			wantLen: 3,
			wantKeyByDef: map[string]string{
				"billing_recorded":    "billing-recorded.kafka.primary",
				"transaction_created": "transaction-created.kafka.primary",
				"audit_logged":        "audit-logged.kafka.override",
			},
		},
		{
			name: "multiple overrides: one replaces, one appends",
			base: base,
			overrides: []RouteDefinition{
				mergeTestRoute("billing_recorded", "billing-recorded.kafka.override"),
				mergeTestRoute("audit_logged", "audit-logged.kafka.override"),
			},
			wantLen: 3,
			wantKeyByDef: map[string]string{
				"billing_recorded":    "billing-recorded.kafka.override",
				"transaction_created": "transaction-created.kafka.primary",
				"audit_logged":        "audit-logged.kafka.override",
			},
		},
		{
			// Finding A: two overrides sharing a (DefinitionKey, Target) must be
			// deduplicated before appending, LAST override winning — otherwise
			// both survive and the definition double-publishes to that target.
			name: "duplicate overrides same DefinitionKey+Target: last wins",
			base: base,
			overrides: []RouteDefinition{
				mergeTestRoute("billing_recorded", "billing-recorded.kafka.first"),
				mergeTestRoute("billing_recorded", "billing-recorded.kafka.last"),
			},
			wantLen: 2,
			wantKeyByDef: map[string]string{
				"billing_recorded":    "billing-recorded.kafka.last",
				"transaction_created": "transaction-created.kafka.primary",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := MergeRouteOverrides(tc.base, tc.overrides)

			if len(got) != tc.wantLen {
				t.Fatalf("merged len = %d, want %d (got %+v)", len(got), tc.wantLen, got)
			}

			idx := keysByDefinitionKey(got)
			for defKey, wantKey := range tc.wantKeyByDef {
				keys := idx[defKey]

				if len(keys) != 1 {
					t.Fatalf("DefinitionKey %q appears %d times, want exactly 1 (keys=%v)", defKey, len(keys), keys)
				}

				if keys[0] != wantKey {
					t.Errorf("DefinitionKey %q -> Key %q, want %q", defKey, keys[0], wantKey)
				}
			}

			// Shared base baseline must never be mutated by the merge.
			if base[0].Key != "billing-recorded.kafka.primary" {
				t.Errorf("MergeRouteOverrides mutated its base input: base[0].Key = %q", base[0].Key)
			}
		})
	}
}

// TestMergeRouteOverrides_PreservesSiblingTargetRoutes pins Finding B: an
// override for one Target must REPLACE only the base route sharing its
// (DefinitionKey, Target) and leave the definition's sibling-target base routes
// intact. A DefinitionKey-only removal would wrongly drop the secondary route.
func TestMergeRouteOverrides_PreservesSiblingTargetRoutes(t *testing.T) {
	t.Parallel()

	base := []RouteDefinition{
		mergeTestRouteTarget("transaction_created", "transaction-created.kafka.primary", "primary"),
		mergeTestRouteTarget("transaction_created", "transaction-created.kafka.secondary", "secondary"),
	}

	overrides := []RouteDefinition{
		mergeTestRouteTarget("transaction_created", "transaction-created.kafka.primary-override", "primary"),
	}

	got := MergeRouteOverrides(base, overrides)

	// Expect exactly two routes for transaction_created: the overridden primary
	// route plus the preserved secondary route.
	if len(got) != 2 {
		t.Fatalf("merged len = %d, want 2 (got %+v)", len(got), got)
	}

	keysByTarget := make(map[string]string, len(got))
	for _, r := range got {
		if r.DefinitionKey != "transaction_created" {
			t.Fatalf("unexpected DefinitionKey %q in result", r.DefinitionKey)
		}

		if _, dup := keysByTarget[r.Target]; dup {
			t.Fatalf("target %q appears more than once (double-publish): %+v", r.Target, got)
		}

		keysByTarget[r.Target] = r.Key
	}

	if keysByTarget["primary"] != "transaction-created.kafka.primary-override" {
		t.Errorf("primary target Key = %q, want override to win", keysByTarget["primary"])
	}

	if keysByTarget["secondary"] != "transaction-created.kafka.secondary" {
		t.Errorf("secondary sibling-target route not preserved: Key = %q", keysByTarget["secondary"])
	}
}
