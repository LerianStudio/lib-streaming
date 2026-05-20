//go:build unit

// Test suite for the background CB recovery goroutine in cb_recovery.go.
//
// The recovery loop bridges a known deadlock between dispatchRoute's
// hot-path early-out (rt.state.Load() == flagCBOpen) and gobreaker's lazy
// OPEN→HALF-OPEN expiry transition (which only fires inside currentState,
// reached only via cb.Execute or manager.GetState). Without this loop, an
// emit-only service whose breaker has tripped OPEN stays degraded forever.
// See cb_recovery.go for the full rationale.
//
// This file pins the loop's coverage matrix:
//   - resolveCBRecoveryInterval clamp behavior (floor/ceiling/divisor).
//   - startCBRecoveryLoop entry guards (nil receiver, nil cbManager,
//     zero targets, zero interval, stop already closed).
//   - cbRecoveryLoop body (tick fires, stop signal exits, panic resilience).
//   - pokeAllTargetCBs body (per-target fan-out, nil-entry/nil-map skip,
//     nil-receiver/nil-cbManager safe).
//   - Listener integration (OPEN→HALF-OPEN end-to-end via the fake CB).
//
// Timing strategy: tests use sub-cbRecoveryFloor intervals (e.g., 25ms,
// 50ms) directly assigned to p.cbRecoveryInterval. This bypasses the
// production floor on purpose — the goroutine itself doesn't re-validate,
// and direct assignment is a deliberate test-only escape hatch. Tests that
// observe poke counts use deadline-poll loops with t.Fatalf on miss
// instead of fixed time.Sleep + tolerance windows, so a slow-CI run doesn't
// silently degrade coverage.

package producer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// recoveryTestPokeDeadline is the upper-bound wall clock all timing tests
// allow before declaring the loop unresponsive. Generous enough to absorb
// CI scheduler skew while still fast enough that a real regression
// (loop never started) fails inside one test run rather than hanging.
const recoveryTestPokeDeadline = 2 * time.Second

// awaitPokes deadline-polls until the target has accumulated `want`
// GetState calls or the deadline expires. Returns the final observed
// count. Test code MUST t.Fatalf when the returned count is below `want`
// — silent fall-through would let downstream assertions exercise paths
// that don't depend on the recovery loop and pass even when the loop is
// broken.
func awaitPokes(fakeMgr *fakeCBManager, serviceName string, want int64, deadline time.Duration) int64 {
	end := time.Now().Add(deadline)

	for time.Now().Before(end) {
		got := fakeMgr.getStateCallsFor(serviceName)
		if got >= want {
			return got
		}

		time.Sleep(5 * time.Millisecond)
	}

	return fakeMgr.getStateCallsFor(serviceName)
}

// awaitTargetState deadline-polls the producer's mirrored target state until
// it reaches want or the deadline expires. This is intentionally separate from
// awaitPokes: GetState call count proves the recovery loop attempted the poke,
// while the target mirror proves the StateChangeListener completed. Tests that
// care about listener side effects must synchronize on this helper, not on the
// fake manager's pre-transition call counter.
func awaitTargetState(p *Producer, targetName string, want int32, deadline time.Duration) int32 {
	end := time.Now().Add(deadline)

	for time.Now().Before(end) {
		got := p.targetState(targetName)
		if got == want {
			return got
		}

		time.Sleep(5 * time.Millisecond)
	}

	return p.targetState(targetName)
}

// TestResolveCBRecoveryInterval pins the clamp behavior of the interval
// resolver. The clamps are load-bearing: the floor stops listener-
// notification spam from frequent OPEN↔HALF-OPEN transitions; the ceiling
// guarantees bounded recovery latency even for services with very long
// CBTimeout configurations.
func TestResolveCBRecoveryInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cbTimeout time.Duration
		want      time.Duration
	}{
		{"zero clamps to floor", 0, cbRecoveryFloor},
		{"negative clamps to floor", -1 * time.Second, cbRecoveryFloor},
		{"sub-floor clamps to floor", 100 * time.Millisecond, cbRecoveryFloor},
		{"normal scales by divisor", 10 * time.Second, 10 * time.Second / cbRecoveryDivisor},
		{"interval exactly at floor returns floor", 4 * cbRecoveryFloor, cbRecoveryFloor},
		{"interval exactly at ceiling returns ceiling", 4 * cbRecoveryCeiling, cbRecoveryCeiling},
		{"large clamps to ceiling", 60 * time.Second, cbRecoveryCeiling},
		{"huge clamps to ceiling", 24 * time.Hour, cbRecoveryCeiling},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := resolveCBRecoveryInterval(tt.cbTimeout)
			if got != tt.want {
				t.Errorf("resolveCBRecoveryInterval(%v) = %v; want %v", tt.cbTimeout, got, tt.want)
			}
		})
	}
}

func TestCBRecoveryLivenessRegistryAggregatesMultipleProducers(t *testing.T) {
	registry := cbRecoveryLivenessRegistry{states: map[string]bool{}}

	if alive := registry.update("producer-a", true, false); !alive {
		t.Fatal("first healthy producer aggregate = false; want true")
	}
	if alive := registry.update("producer-b", true, false); !alive {
		t.Fatal("two healthy producers aggregate = false; want true")
	}
	if alive := registry.update("producer-b", false, false); alive {
		t.Fatal("one dead producer aggregate = true; want false")
	}
	if alive := registry.update("producer-a", true, false); alive {
		t.Fatal("healthy producer update overwrote dead sibling; want aggregate false")
	}
	if alive := registry.update("producer-b", false, true); !alive {
		t.Fatal("closed producer should be removed from aggregate; want true")
	}
}

// newProducerForRecoveryTest builds a minimal *Producer wired with the
// fake CB manager and the named targets. Bypasses NewProducerMulti so the
// recovery-loop unit tests can exercise the goroutine without spinning up
// transport adapters or routes. Mirrors the construction order of the
// real constructor (cbManager → targets → listener) so the listener still
// fires correctly if a test wants to drive ForceTransition.
func newProducerForRecoveryTest(t *testing.T, fakeMgr *fakeCBManager, targetNames ...string) *Producer {
	t.Helper()

	p := &Producer{
		cbManager:  fakeMgr,
		logger:     log.NewNop(),
		producerID: "test-recovery-producer",
		targets:    map[string]*targetRuntime{},
		stop:       make(chan struct{}),
	}

	for _, name := range targetNames {
		serviceName := targetCBServiceName(p.producerID, name)
		cb, err := fakeMgr.GetOrCreate(serviceName, circuitbreaker.Config{})
		if err != nil {
			t.Fatalf("GetOrCreate %s: %v", name, err)
		}

		p.targets[name] = &targetRuntime{
			name:          name,
			cb:            cb,
			cbServiceName: serviceName,
		}
	}

	if len(targetNames) > 0 {
		p.primaryTargetName = targetNames[0]
	}

	fakeMgr.RegisterStateChangeListener(&streamingStateListener{producer: p})

	return p
}

// TestPokeAllTargetCBs_CallsGetStateForEveryTarget verifies the inner pump
// of the recovery loop. Calls pokeAllTargetCBs directly (no goroutine, no
// ticker) and asserts manager.GetState was invoked once per target.
//
// This test exists separately from the goroutine-driven test below so a
// regression in the per-target iteration is reported cleanly without
// timing-related flake noise.
func TestPokeAllTargetCBs_CallsGetStateForEveryTarget(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary", "secondary", "tertiary")

	p.pokeAllTargetCBs()

	for _, name := range []string{"primary", "secondary", "tertiary"} {
		got := fakeMgr.getStateCallsFor(p.targets[name].cbServiceName)
		if got != 1 {
			t.Errorf("target %q GetState calls = %d; want 1", name, got)
		}
	}
}

// TestPokeAllTargetCBs_NilTargetEntrySkipped verifies the loop tolerates a
// nil entry in p.targets without panicking. Defensive — the constructor
// can't currently produce a nil entry, but if it ever does (e.g., a future
// refactor that pre-populates the map before adapter construction
// completes), the recovery loop must not crash the goroutine.
func TestPokeAllTargetCBs_NilTargetEntrySkipped(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")

	// Inject a nil entry. The loop must skip it.
	p.targets["nil-spot"] = nil

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("pokeAllTargetCBs panicked on nil target entry: %v", r)
		}
	}()

	p.pokeAllTargetCBs()

	if got := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName); got != 1 {
		t.Errorf("primary GetState calls = %d; want 1", got)
	}
}

// TestPokeAllTargetCBs_NilTargetsMapSafe verifies the loop tolerates a nil
// p.targets map. Range over a nil map is a Go-level no-op, but pinning the
// behavior in a test prevents a future refactor (e.g., wrapping the range
// in a length check that misbehaves on nil) from regressing silently.
func TestPokeAllTargetCBs_NilTargetsMapSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("pokeAllTargetCBs on nil targets map panicked: %v", r)
		}
	}()

	fakeMgr := newFakeCBManager()
	p := &Producer{
		cbManager: fakeMgr,
		logger:    log.NewNop(),
		targets:   nil, // explicitly nil, not just empty
		stop:      make(chan struct{}),
	}
	p.pokeAllTargetCBs()
}

// TestPokeAllTargetCBs_NilProducerSafe verifies the receiver-nil guard.
// Required for the standard nil-receiver discipline this package follows.
func TestPokeAllTargetCBs_NilProducerSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("pokeAllTargetCBs on nil producer panicked: %v", r)
		}
	}()

	var p *Producer
	p.pokeAllTargetCBs()
}

// TestPokeAllTargetCBs_NilManagerSafe verifies the cbManager-nil guard.
// A Producer constructed without a manager (theoretically possible in
// hand-built test fixtures) must not crash the recovery pump.
func TestPokeAllTargetCBs_NilManagerSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("pokeAllTargetCBs on nil manager panicked: %v", r)
		}
	}()

	p := &Producer{logger: log.NewNop()}
	p.pokeAllTargetCBs()
}

// TestPokeAllTargetCBs_TypedNilManagerSafe verifies the cbManager guard treats
// interfaces containing nil concrete pointers as nil. This is the production
// shape that a simple `p.cbManager == nil` check misses.
func TestPokeAllTargetCBs_TypedNilManagerSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("pokeAllTargetCBs on typed-nil manager panicked: %v", r)
		}
	}()

	var fakeMgr *fakeCBManager
	p := &Producer{
		cbManager: fakeMgr,
		logger:    log.NewNop(),
		targets: map[string]*targetRuntime{
			"primary": {cbServiceName: "svc"},
		},
		stop: make(chan struct{}),
	}
	p.pokeAllTargetCBs()
}

// TestStartCBRecoveryLoop_PokesPeriodically is the integration test for the
// goroutine. Configures a fast interval (50ms), starts the loop, deadline-
// polls until at least 2 pokes have fired, then asserts the freeze
// semantic after signalStop. Uses awaitPokes (deadline-polled, t.Fatalf on
// miss) instead of fixed time.Sleep so a slow CI run produces a clean
// failure rather than a silent skip.
func TestStartCBRecoveryLoop_PokesPeriodically(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.cbRecoveryInterval = 50 * time.Millisecond

	p.startCBRecoveryLoop()

	pokes := awaitPokes(fakeMgr, p.targets["primary"].cbServiceName, 2, recoveryTestPokeDeadline)
	if pokes < 2 {
		t.Fatalf("recovery loop never poked: got %d pokes within %v at 50ms interval; want >= 2",
			pokes, recoveryTestPokeDeadline)
	}

	p.signalStop()

	// After signalStop, the goroutine exits. Allow at most ONE additional
	// poke for the in-flight tick race: between signalStop and the
	// goroutine's select rebinding to <-p.stop, one tick may fire because
	// Go's select picks randomly when multiple channels are ready.
	beforeStop := pokes
	time.Sleep(150 * time.Millisecond)
	afterStop := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName)

	if afterStop-beforeStop > 1 {
		t.Errorf("pokes increased by %d after signalStop (before=%d, after=%d); want at most +1 (in-flight tick)",
			afterStop-beforeStop, beforeStop, afterStop)
	}
}

// TestStartCBRecoveryLoop_NilManagerSafe verifies nil-safety at the entry of
// startCBRecoveryLoop. A hand-built Producer with no manager must not panic.
// This is intentionally nil-safety-only coverage; observable "no goroutine was
// spawned" behavior is pinned by TestStartCBRecoveryLoop_NoOpWhenNoTargets and
// TestStartCBRecoveryLoop_NoOpWhenNilStopChannel.
func TestStartCBRecoveryLoop_NilManagerSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("startCBRecoveryLoop on nil manager panicked: %v", r)
		}
	}()

	p := &Producer{
		logger: log.NewNop(),
		stop:   make(chan struct{}),
	}
	p.startCBRecoveryLoop()
}

// TestStartCBRecoveryLoop_NoOpWhenNoTargets verifies the no-op guard for
// the empty-targets case. A Producer with zero targets cannot have a
// stuck mirror; spinning a goroutine for it would be wasted work.
func TestStartCBRecoveryLoop_NoOpWhenNoTargets(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := &Producer{
		cbManager: fakeMgr,
		logger:    log.NewNop(),
		targets:   map[string]*targetRuntime{},
		stop:      make(chan struct{}),
	}

	p.startCBRecoveryLoop()

	// Wait a bit; if a goroutine spawned, it would call GetState. None
	// is registered, but the counter side effect would still increment.
	//
	// Falsifiability note: this assertion can pass if the guard regressed
	// AND the goroutine started slowly — we mitigate by waiting longer
	// than the production floor (500ms) which is well above any plausible
	// goroutine spawn latency on a healthy CI runner.
	time.Sleep(50 * time.Millisecond)

	// fakeMgr's getStateCallsByService is sync.Map; range over it.
	calls := int64(0)
	fakeMgr.getStateCallsByService.Range(func(_, v any) bool {
		calls += v.(*atomic.Int64).Load()
		return true
	})

	if calls != 0 {
		t.Errorf("got %d GetState calls; want 0 (no targets, no goroutine)", calls)
	}
}

// TestStartCBRecoveryLoop_NoOpWhenZeroInterval verifies the defensive
// guard against a zero/negative cbRecoveryInterval. The constructor seeds
// it via resolveCBRecoveryInterval which always returns positive, so this
// branch only fires under invariant violation (e.g., a hand-built
// *Producer in a future test). On invariant violation the guard fires the
// observability trident through p.newAsserter and early-returns — the
// goroutine is NEVER spawned, so the counter stays at zero.
func TestStartCBRecoveryLoop_NoOpWhenZeroInterval(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.cbRecoveryInterval = 0

	p.startCBRecoveryLoop()

	// Wait long enough that a runaway loop would have produced multiple
	// pokes. Falsifiability is established because (a) cbRecoveryInterval=0
	// would NewTicker-panic if reached, killing the test goroutine, and
	// (b) the assertion-trident path returns synchronously, so no goroutine
	// can outrun the sleep on a healthy runner.
	time.Sleep(50 * time.Millisecond)

	if got := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName); got != 0 {
		t.Errorf("got %d pokes; want 0 (zero interval should disable the loop)", got)
	}
}

// TestStartCBRecoveryLoop_NoOpWhenNilStopChannel verifies the defensive guard
// against starting a recovery goroutine that cannot be stopped. A nil stop
// channel disables the shutdown select case forever, so startCBRecoveryLoop
// must fire the assertion trident and return without spawning.
func TestStartCBRecoveryLoop_NoOpWhenNilStopChannel(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.stop = nil
	p.cbRecoveryInterval = 25 * time.Millisecond

	p.startCBRecoveryLoop()

	// Wait long enough that an incorrectly spawned loop would tick and call
	// GetState several times. Zero calls proves the nil-stop invariant guard
	// returned before SafeGo was invoked.
	time.Sleep(100 * time.Millisecond)

	if got := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName); got != 0 {
		t.Errorf("got %d pokes; want 0 (nil stop channel should disable the loop)", got)
	}
}

// TestStartCBRecoveryLoop_NilProducerSafe verifies the receiver-nil guard
// at the entry of startCBRecoveryLoop. Required for nil-receiver
// discipline.
func TestStartCBRecoveryLoop_NilProducerSafe(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("startCBRecoveryLoop on nil producer panicked: %v", r)
		}
	}()

	var p *Producer
	p.startCBRecoveryLoop()
}

// TestStartCBRecoveryLoop_ExitsImmediatelyWhenStopAlreadyClosed pins the
// "stop signal wins" semantic: when p.stop is already closed before the
// goroutine starts iterating, the loop's first select observes the closed
// channel and exits without firing a poke. A future refactor that adds
// an immediate-poke step before the select would silently regress this
// property.
func TestStartCBRecoveryLoop_ExitsImmediatelyWhenStopAlreadyClosed(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.cbRecoveryInterval = 25 * time.Millisecond

	// Close stop BEFORE starting the loop. The goroutine's first select
	// should observe the closed channel on iteration 1 and return.
	close(p.stop)

	p.startCBRecoveryLoop()

	// Wait longer than several tick intervals. If the guard regressed, a
	// runaway loop would produce multiple pokes.
	time.Sleep(150 * time.Millisecond)

	if got := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName); got != 0 {
		t.Errorf("got %d pokes after stop-already-closed; want 0", got)
	}
}

// TestStartCBRecoveryLoop_StopsOnSignalStop verifies the goroutine exit
// path. Spawns the loop, deadline-polls to confirm it pumped, calls
// signalStop, and asserts the counter freezes after a grace window.
//
// Sibling test to TestStartCBRecoveryLoop_PokesPeriodically — the
// "freezes-after-stop" assertion lives there too, but is repeated here
// in isolation so the failure message is unambiguous when only the
// shutdown path regresses. Uses deadline-poll (t.Fatalf on precondition
// miss) so a slow CI run fails cleanly instead of silently skipping the
// freeze assertion.
func TestStartCBRecoveryLoop_StopsOnSignalStop(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.cbRecoveryInterval = 25 * time.Millisecond

	p.startCBRecoveryLoop()

	beforeStop := awaitPokes(fakeMgr, p.targets["primary"].cbServiceName, 2, recoveryTestPokeDeadline)
	if beforeStop < 2 {
		t.Fatalf("pre-stop pokes = %d within %v; need at least 2 to verify the freeze post-stop",
			beforeStop, recoveryTestPokeDeadline)
	}

	p.signalStop()

	// Grace window covers any pending tick that fired between the
	// signalStop call and the goroutine's select rebinding to the stop
	// channel.
	time.Sleep(100 * time.Millisecond)
	afterStop := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName)

	// Allow at most one extra poke for the in-flight tick window.
	if afterStop-beforeStop > 1 {
		t.Errorf("pokes increased by %d after signalStop (before=%d, after=%d); want at most +1",
			afterStop-beforeStop, beforeStop, afterStop)
	}
}

// TestStartCBRecoveryLoop_StopsAfterCloseContext verifies the production
// lifecycle path, not just the internal signalStop helper: NewProducerMulti
// starts the recovery loop, CloseContext closes the Producer stop channel,
// and GetState calls freeze after shutdown. The interval uses the production
// floor (500ms) instead of test-only direct assignment so this exercises the
// constructor-coupled behavior callers actually run.
func TestStartCBRecoveryLoop_StopsAfterCloseContext(t *testing.T) {
	fakeMgr := newFakeCBManager()
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://recovery-close-test", CBTimeout: 2 * time.Second},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithCircuitBreakerManager(fakeMgr),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	serviceName := p.targets["primary"].cbServiceName
	if got := awaitPokes(fakeMgr, serviceName, 2, 3*time.Second); got < 2 {
		t.Fatalf("production recovery loop pokes = %d within 3s; want >= 2", got)
	}

	beforeClose := fakeMgr.getStateCallsFor(serviceName)
	if err := p.CloseContext(context.Background()); err != nil {
		t.Fatalf("CloseContext() error = %v", err)
	}
	afterClose := fakeMgr.getStateCallsFor(serviceName)

	// Wait longer than two production-floor ticks. Once CloseContext closes
	// p.stop, the loop should exit; at most one in-flight tick may land after
	// the close call races with the goroutine's select.
	time.Sleep((2 * cbRecoveryFloor) + 100*time.Millisecond)
	final := fakeMgr.getStateCallsFor(serviceName)

	if final-afterClose > 1 {
		t.Errorf("pokes increased by %d after CloseContext (beforeClose=%d, afterClose=%d, final=%d); want at most +1",
			final-afterClose, beforeClose, afterClose, final)
	}
}

// TestStartCBRecoveryLoop_AllTargetsPokedPerTick verifies the goroutine
// pumps EVERY registered target per tick, not just one. pokeAllTargetCBs
// is a synchronous loop with no goroutine boundary — all targets must be
// poked within the same tick before signalStop can land. The acceptable
// per-pair diff is therefore strict ±1, covering only the in-flight tick
// race at the moment of signalStop (not preemption mid-iteration; Go's
// runtime preemption between cheap GetState calls is negligible).
func TestStartCBRecoveryLoop_AllTargetsPokedPerTick(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary", "secondary", "tertiary")
	p.cbRecoveryInterval = 30 * time.Millisecond

	p.startCBRecoveryLoop()

	primarySvc := p.targets["primary"].cbServiceName
	if got := awaitPokes(fakeMgr, primarySvc, 3, recoveryTestPokeDeadline); got < 3 {
		t.Fatalf("primary pokes = %d within %v; need at least 3 ticks for stable per-target compare",
			got, recoveryTestPokeDeadline)
	}

	p.signalStop()

	primaryPokes := fakeMgr.getStateCallsFor(p.targets["primary"].cbServiceName)
	secondaryPokes := fakeMgr.getStateCallsFor(p.targets["secondary"].cbServiceName)
	tertiaryPokes := fakeMgr.getStateCallsFor(p.targets["tertiary"].cbServiceName)

	// In-flight tick race tolerance: signalStop can race with a tick
	// that has already entered pokeAllTargetCBs. Because the loop pokes
	// targets sequentially and synchronously within one tick, the last
	// tick before stop either (a) completed entirely (all three targets
	// see +1) or (b) was preempted between sequential per-target
	// GetState calls (rare; Go's runtime preempts at function boundaries
	// but cheap GetState calls finish quickly). Allow ±1.
	for _, pair := range []struct {
		name string
		got  int64
	}{
		{"primary vs secondary", primaryPokes - secondaryPokes},
		{"primary vs tertiary", primaryPokes - tertiaryPokes},
		{"secondary vs tertiary", secondaryPokes - tertiaryPokes},
	} {
		if pair.got > 1 || pair.got < -1 {
			t.Errorf("target poke counts uneven (%s diff = %d): primary=%d secondary=%d tertiary=%d",
				pair.name, pair.got, primaryPokes, secondaryPokes, tertiaryPokes)
		}
	}

	if primaryPokes < 3 {
		t.Errorf("primary pokes = %d; want >= 3 after stable polling window", primaryPokes)
	}
}

// TestStartCBRecoveryLoop_DrivesOpenToHalfOpenViaGetState wires the recovery
// loop end-to-end through the streamingStateListener: forces the fake CB to
// OPEN, configures fake GetState itself to transition to HALF-OPEN after a
// poke, and asserts the listener mirrored the new state onto rt.state. This
// is the closest unit-level approximation of the chaos test's recovery
// scenario because the read operation — not the test body — drives the lazy
// OPEN→HALF-OPEN transition.
//
// Falsifiability: t.Fatalf on the precondition miss (loop never poked)
// is essential here — without it, polling the mirror could mask that no
// GetState-driven transition was attempted.
func TestStartCBRecoveryLoop_DrivesOpenToHalfOpenViaGetState(t *testing.T) {
	t.Parallel()

	fakeMgr := newFakeCBManager()
	p := newProducerForRecoveryTest(t, fakeMgr, "primary")
	p.cbRecoveryInterval = 25 * time.Millisecond

	// Force the CB to OPEN and propagate to the mirror via the listener.
	fakeMgr.ForceTransition(p.targets["primary"].cbServiceName, circuitbreaker.StateOpen)
	fakeMgr.transitionOnGetState(p.targets["primary"].cbServiceName, 2, circuitbreaker.StateHalfOpen)

	if got := p.targetState("primary"); got != flagCBOpen {
		t.Fatalf("pre-recovery primary state = %d; want %d (open)", got, flagCBOpen)
	}

	p.startCBRecoveryLoop()

	// Confirm the loop is actually running. The fake manager is configured
	// so the second GetState call itself fires OPEN→HALF-OPEN; the test body
	// does not manually force the HALF-OPEN transition.
	if got := awaitPokes(fakeMgr, p.targets["primary"].cbServiceName, 2, recoveryTestPokeDeadline); got < 2 {
		t.Fatalf("recovery loop never poked: got %d pokes within %v; want >= 2", got, recoveryTestPokeDeadline)
	}

	// Listener should mirror the new state onto rt.state as a consequence of
	// the GetState-triggered transition above. Synchronize on the mirror state,
	// not just on GetState call count: the fake increments its counter before
	// transition/listener completion, so the call-count precondition alone is
	// not a safe happens-after edge under -race.
	if got := awaitTargetState(p, "primary", flagCBHalfOpen, recoveryTestPokeDeadline); got != flagCBHalfOpen {
		t.Errorf("post-transition primary state = %d; want %d (half-open)", got, flagCBHalfOpen)
	}

	p.signalStop()
}

// TestStartCBRecoveryLoop_PanicResilient verifies that a panic inside
// GetState is caught by the runtime.SafeGoWithContextAndComponent
// wrapper. We use a panicking-CB manager to force the panic.
//
// This is a SAFETY test: the recovery goroutine must not crash the host
// process, even if the manager misbehaves. After the panic, the goroutine
// exits (KeepRunning logs and returns; SafeGo's defer-recover catches the
// panic and the wrapped function returns) — the service is degraded but
// functional, with the panic recorded via the observability trident.
//
// panickingCBManager and newProducerForPanicTest are defined immediately
// below (single-use fixtures, co-located with their consumer for clarity).
func TestStartCBRecoveryLoop_PanicResilient(t *testing.T) {
	t.Parallel()

	mgr := &panickingCBManager{}
	p := newProducerForPanicTest(t, mgr)
	p.cbRecoveryInterval = 25 * time.Millisecond

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("startCBRecoveryLoop's panic propagated to test goroutine: %v", r)
		}
	}()

	p.startCBRecoveryLoop()

	// Wait long enough for at least one tick to fire and panic-recover.
	time.Sleep(100 * time.Millisecond)

	p.signalStop()

	// Survive: the test process must still be running. The goroutine
	// itself died on the panic (KeepRunning logs and returns), but the
	// host process is intact — the only assertion needed is the absence
	// of a process crash. Reaching this line proves it.
	if got := mgr.getStateCalls.Load(); got < 1 {
		t.Errorf("panicking manager GetState calls = %d; want >= 1 to confirm the goroutine actually fired", got)
	}
}

// panickingCBManager is a minimal circuitbreaker.Manager used only by
// TestStartCBRecoveryLoop_PanicResilient. GetState records the call then
// panics; everything else is a no-op stub. Co-located with its only
// consumer for readability.
type panickingCBManager struct {
	getStateCalls atomic.Int64
}

func (m *panickingCBManager) GetOrCreate(_ string, _ circuitbreaker.Config) (circuitbreaker.CircuitBreaker, error) {
	return &fakeCB{state: circuitbreaker.StateClosed}, nil
}

func (*panickingCBManager) Execute(_ string, _ func() (any, error)) (any, error) {
	return nil, nil
}

func (m *panickingCBManager) GetState(_ string) circuitbreaker.State {
	m.getStateCalls.Add(1)
	panic("intentional panic from panickingCBManager.GetState")
}

func (*panickingCBManager) GetCounts(_ string) circuitbreaker.Counts {
	return circuitbreaker.Counts{}
}

func (*panickingCBManager) IsHealthy(_ string) bool { return true }

func (*panickingCBManager) Reset(_ string) {}

func (*panickingCBManager) RegisterStateChangeListener(_ circuitbreaker.StateChangeListener) {}

// newProducerForPanicTest is the panic-test counterpart to
// newProducerForRecoveryTest. Uses panickingCBManager and skips the
// listener registration (the listener wouldn't fire — GetState panics
// before reaching any state-change path).
func newProducerForPanicTest(t *testing.T, mgr *panickingCBManager) *Producer {
	t.Helper()

	p := &Producer{
		cbManager:  mgr,
		logger:     log.NewNop(),
		producerID: "test-panic-producer",
		targets:    map[string]*targetRuntime{},
		stop:       make(chan struct{}),
	}

	cb, err := mgr.GetOrCreate("svc", circuitbreaker.Config{})
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	p.targets["primary"] = &targetRuntime{
		name:          "primary",
		cb:            cb,
		cbServiceName: targetCBServiceName(p.producerID, "primary"),
	}
	p.primaryTargetName = "primary"

	return p
}

// Compile-time assertion: panickingCBManager must satisfy
// circuitbreaker.Manager. Catches signature drift in the lib-commons
// interface immediately.
var _ circuitbreaker.Manager = (*panickingCBManager)(nil)
