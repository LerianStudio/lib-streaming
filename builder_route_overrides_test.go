//go:build unit

// White-box (package streaming) so it can read the unexported routeOverrides
// field and call the unexported mergedRoutes helper directly: the setter→field
// mapping, defensive-copy invariant, and merge semantics are asserted here,
// mirroring the white-box approach in api_consumer_builder_whitebox_test.go.
// The nil-receiver contract mirrors TestBuilder_CBSetters_NilBuilderIsSafe.
package streaming

import (
	"testing"
)

func TestBuilder_RouteOverrides_StoresDefensiveCopy(t *testing.T) {
	t.Parallel()

	r1 := RouteDefinition{Key: "billing-recorded.kafka.primary", DefinitionKey: "billing_recorded", Target: "primary"}
	r2 := RouteDefinition{Key: "billing-recorded.kafka.secondary", DefinitionKey: "billing_recorded", Target: "secondary"}

	input := []RouteDefinition{r1, r2}
	b := NewBuilder().RouteOverrides(input...)

	if b == nil {
		t.Fatal("RouteOverrides returned nil *Builder; want chainable builder")
	}

	if len(b.routeOverrides) != 2 {
		t.Fatalf("routeOverrides len = %d, want 2", len(b.routeOverrides))
	}

	if b.routeOverrides[0].Key != r1.Key || b.routeOverrides[1].Key != r2.Key {
		t.Errorf("routeOverrides keys = [%q %q], want [%q %q]",
			b.routeOverrides[0].Key, b.routeOverrides[1].Key, r1.Key, r2.Key)
	}

	// Defensive copy: mutating the caller's backing array after the call must
	// not change the builder's stored slice.
	input[0] = RouteDefinition{Key: "mutated"}

	if b.routeOverrides[0].Key != r1.Key {
		t.Errorf("routeOverrides[0].Key = %q after caller mutation; want %q (defensive copy)",
			b.routeOverrides[0].Key, r1.Key)
	}
}

func TestBuilder_RouteOverrides_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()

	var b *Builder

	if got := b.RouteOverrides(RouteDefinition{Key: "x"}); got != nil {
		t.Errorf("RouteOverrides(nil-receiver) = %v; want nil", got)
	}
}

// TestBuilder_mergedRoutes locks the Builder-path merge semantics (REPLACE by
// DefinitionKey via the shared contract.MergeRouteOverrides helper): base-only,
// overrides-only, disjoint union, and same-DefinitionKey replacement.
func TestBuilder_mergedRoutes(t *testing.T) {
	t.Parallel()

	baseTx := RouteDefinition{Key: "transaction-created.kafka.primary", DefinitionKey: "transaction.created", Target: "primary"}
	overrideTxSameKey := RouteDefinition{Key: "transaction-created.kafka.override", DefinitionKey: "transaction.created", Target: "primary"}
	billingOverride := RouteDefinition{Key: "billing-recorded.kafka.primary", DefinitionKey: "billing_recorded", Target: "primary"}

	cases := []struct {
		name         string
		routes       []RouteDefinition
		overrides    []RouteDefinition
		wantLen      int
		wantKeyByDef map[string]string
	}{
		{
			name:         "base only (no overrides)",
			routes:       []RouteDefinition{baseTx},
			overrides:    nil,
			wantLen:      1,
			wantKeyByDef: map[string]string{"transaction.created": "transaction-created.kafka.primary"},
		},
		{
			name:         "overrides only (no base routes)",
			routes:       nil,
			overrides:    []RouteDefinition{billingOverride},
			wantLen:      1,
			wantKeyByDef: map[string]string{"billing_recorded": "billing-recorded.kafka.primary"},
		},
		{
			name:      "disjoint DefinitionKeys are unioned",
			routes:    []RouteDefinition{baseTx},
			overrides: []RouteDefinition{billingOverride},
			wantLen:   2,
			wantKeyByDef: map[string]string{
				"transaction.created": "transaction-created.kafka.primary",
				"billing_recorded":    "billing-recorded.kafka.primary",
			},
		},
		{
			name:         "same DefinitionKey: override replaces base (one route)",
			routes:       []RouteDefinition{baseTx},
			overrides:    []RouteDefinition{overrideTxSameKey},
			wantLen:      1,
			wantKeyByDef: map[string]string{"transaction.created": "transaction-created.kafka.override"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := NewBuilder().Routes(tc.routes...).RouteOverrides(tc.overrides...)
			got := b.mergedRoutes()

			if len(got) != tc.wantLen {
				t.Fatalf("mergedRoutes() len = %d, want %d (got %+v)", len(got), tc.wantLen, got)
			}

			byDef := make(map[string][]string, len(got))
			for _, r := range got {
				byDef[r.DefinitionKey] = append(byDef[r.DefinitionKey], r.Key)
			}

			for defKey, wantKey := range tc.wantKeyByDef {
				keys := byDef[defKey]

				if len(keys) != 1 {
					t.Fatalf("DefinitionKey %q appears %d times, want exactly 1 (keys=%v)", defKey, len(keys), keys)
				}

				if keys[0] != wantKey {
					t.Errorf("DefinitionKey %q -> Key %q, want %q", defKey, keys[0], wantKey)
				}
			}
		})
	}
}
