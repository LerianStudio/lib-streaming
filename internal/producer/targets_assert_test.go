//go:build unit

package producer

import (
	"context"
	"testing"
)

// TestTargets_LookupByServiceName_NilEntry_FiresAssertion pins T4 site
// targets.go:80. A nil entry in p.targets — never reachable under
// NewProducerMulti's contract — must fire the asserter trident under
// operation="targets.lookup_by_service_name" and still allow the lookup
// to skip the corrupted slot without panicking.
func TestTargets_LookupByServiceName_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	got := p.targetRuntimeByServiceName("non-matching")
	if got != nil {
		t.Fatalf("targetRuntimeByServiceName() = %+v; want nil for nil entry", got)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil targets-map entry during lookup")
	}
}

// TestTargets_CloseTargets_NilEntry_FiresAssertion pins T4 site
// targets.go:133. A nil entry at Close MUST fire the trident — silently
// skipping leaks the adapter socket forever.
func TestTargets_CloseTargets_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	if err := p.closeTargets(context.Background()); err != nil {
		t.Fatalf("closeTargets() error = %v; want nil (silent skip preserved)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil targets-map entry during Close")
	}
}

// TestTargets_HealthyTargets_EmptyMap_FiresAssertion pins T4 site
// targets.go:173. healthyTargets is gated upstream by Healthy on
// len(p.targets) > 0. Reaching it with an empty map is a defense-in-
// depth state-corruption signal that MUST fire the trident.
func TestTargets_HealthyTargets_EmptyMap_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		targets:    map[string]*targetRuntime{}, // empty map by force
	}

	err := p.healthyTargets(context.Background())
	if err == nil {
		t.Fatal("healthyTargets() = nil; want *HealthError(Down)")
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on empty targets map at healthyTargets")
	}
}

// TestTargets_HealthyTargets_NilEntry_FiresAssertion pins T4 site
// targets.go:186. A nil entry during health iteration MUST fire the
// trident — the health string surfaces the corruption to the caller
// but without the asserter the metric stays at zero.
func TestTargets_HealthyTargets_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	err := p.healthyTargets(context.Background())
	if err == nil {
		t.Fatal("healthyTargets() = nil; want error reflecting corrupted slot")
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil entry during health iteration")
	}
}
