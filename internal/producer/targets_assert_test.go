//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestTargets_LookupByServiceName_NilEntry_FiresAssertion pins T4 site
// targets.go:80. A nil entry in p.targets — never reachable under
// NewProducerMulti's contract — must fire the asserter trident under
// operation="targets.lookup_by_service_name" and still allow the lookup
// to skip the corrupted slot without panicking.
func TestTargets_LookupByServiceName_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	logger := newCaptureLogger()
	p := &Producer{
		logger:     logger,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	got := p.targetRuntimeByServiceName("non-matching")
	if got != nil {
		t.Fatalf("targetRuntimeByServiceName() = %+v; want nil for nil entry", got)
	}

	if !logger.containsAssertionFailure() {
		t.Fatal("expected asserter trident to fire on nil targets-map entry during lookup")
	}
}

// TestTargets_CloseTargets_NilEntry_FiresAssertion pins T4 site
// targets.go:133. A nil entry at Close MUST fire the trident — silently
// skipping leaks the adapter socket forever.
func TestTargets_CloseTargets_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	logger := newCaptureLogger()
	p := &Producer{
		logger:     logger,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	if err := p.closeTargets(context.Background()); err != nil {
		t.Fatalf("closeTargets() error = %v; want nil (silent skip preserved)", err)
	}

	if !logger.containsAssertionFailure() {
		t.Fatal("expected asserter trident to fire on nil targets-map entry during Close")
	}
}

// TestTargets_HealthyTargets_EmptyMap_FiresAssertion pins T4 site
// targets.go:173. healthyTargets is gated upstream by Healthy on
// len(p.targets) > 0. Reaching it with an empty map is a defense-in-
// depth state-corruption signal that MUST fire the trident.
func TestTargets_HealthyTargets_EmptyMap_FiresAssertion(t *testing.T) {
	t.Parallel()

	logger := newCaptureLogger()
	p := &Producer{
		logger:     logger,
		producerID: "pid-test",
		targets:    map[string]*targetRuntime{}, // empty map by force
	}

	err := p.healthyTargets(context.Background())
	if err == nil {
		t.Fatal("healthyTargets() = nil; want *HealthError(Down)")
	}

	if !logger.containsAssertionFailure() {
		t.Fatal("expected asserter trident to fire on empty targets map at healthyTargets")
	}
}

// TestTargets_HealthyTargets_NilEntry_FiresAssertion pins T4 site
// targets.go:186. A nil entry during health iteration MUST fire the
// trident — the health string surfaces the corruption to the caller
// but without the asserter the metric stays at zero.
func TestTargets_HealthyTargets_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	logger := newCaptureLogger()
	p := &Producer{
		logger:     logger,
		producerID: "pid-test",
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	err := p.healthyTargets(context.Background())
	if err == nil {
		t.Fatal("healthyTargets() = nil; want error reflecting corrupted slot")
	}

	if !logger.containsAssertionFailure() {
		t.Fatal("expected asserter trident to fire on nil entry during health iteration")
	}
}

func TestTargets_HealthyTargets_CBRecoveryLoopNotRunningDegradesHealth(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	p := &Producer{
		logger:             newCaptureLogger(),
		producerID:         "pid-test",
		metrics:            newStreamingMetrics(nil, nil),
		cbRecoveryInterval: time.Second,
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", kind: contract.TransportKafkaLike, adapter: adapter},
		},
	}

	err := p.healthyTargets(context.Background())
	if err == nil || !strings.Contains(err.Error(), "recovery loop") {
		t.Fatalf("healthyTargets() err = %v; want recovery-loop liveness error", err)
	}

	var healthErr interface{ State() contract.HealthState }
	if !errors.As(err, &healthErr) {
		t.Fatalf("healthyTargets() err = %T; want HealthError", err)
	}
	if healthErr.State() != contract.Degraded {
		t.Fatalf("HealthError.State() = %s; want degraded while target is otherwise healthy", healthErr.State())
	}
}

func TestTargets_HealthyTargets_CBRecoveryLoopFreshIsHealthy(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	p := &Producer{
		logger:             newCaptureLogger(),
		producerID:         "pid-test",
		metrics:            newStreamingMetrics(nil, nil),
		cbRecoveryInterval: time.Second,
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", kind: contract.TransportKafkaLike, adapter: adapter},
		},
	}
	p.cbRecoveryRunning.Store(true)
	p.cbRecoveryLastPokeUnix.Store(time.Now().UTC().UnixNano())

	if err := p.healthyTargets(context.Background()); err != nil {
		t.Fatalf("healthyTargets() err = %v; want nil for fresh recovery liveness", err)
	}
}

func TestTargets_HealthyTargets_CBRecoveryLoopStaleDegradesHealth(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	p := &Producer{
		logger:             newCaptureLogger(),
		producerID:         "pid-test",
		metrics:            newStreamingMetrics(nil, nil),
		cbRecoveryInterval: time.Second,
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", kind: contract.TransportKafkaLike, adapter: adapter},
		},
	}
	p.cbRecoveryRunning.Store(true)
	p.cbRecoveryLastPokeUnix.Store(time.Now().Add(-3 * time.Second).UnixNano())

	err := p.healthyTargets(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("healthyTargets() err = %v; want stale recovery-loop liveness error", err)
	}
}
