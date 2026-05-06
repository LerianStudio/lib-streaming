//go:build unit

package producer

import (
	"testing"
	"time"
)

// TestCBRecovery_PokeAllTargets_NilEntry_FiresAssertion pins T4 site
// cb_recovery.go:209. A nil entry in p.targets during the poke loop MUST
// fire the asserter trident — without it, a stuck-OPEN breaker for that
// slot would never recover (silent permanent stall).
func TestCBRecovery_PokeAllTargets_NilEntry_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	mgr := newFakeCBManager()

	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		cbManager:  mgr,
		targets: map[string]*targetRuntime{
			"corrupted": nil,
		},
	}

	p.pokeAllTargetCBs()

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil entry during CB poke")
	}
}

// TestCBRecovery_StartLoop_NilCBManager_FiresAssertion pins T4 site
// cb_recovery.go:118. NewProducerMulti always initializes p.cbManager.
// A hand-built *Producer that skipped that step and reached
// startCBRecoveryLoop with non-empty targets and a nil manager triggers
// the post-construction invariant — fire the trident.
func TestCBRecovery_StartLoop_NilCBManager_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()

	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		// cbManager intentionally left nil to simulate a hand-built
		// *Producer that bypassed NewProducerMulti.
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", cbServiceName: "svc"},
		},
		// Setting a valid (non-zero) interval here exercises the cbManager-nil
		// branch, which appears before the interval-validity branch in
		// startCBRecoveryLoop. If those branches reorder, this test stops
		// covering the cbManager-nil path.
		cbRecoveryInterval: time.Millisecond,
	}

	p.startCBRecoveryLoop()

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil cbManager at startCBRecoveryLoop")
	}
}

// TestCBRecovery_StartLoop_InvalidInterval_FiresAssertion pins T4 site:
// the interval-validity branch in startCBRecoveryLoop must fire the
// trident under operation="cb_recovery.start" when a hand-built *Producer
// reaches the loop with a zero or negative cbRecoveryInterval. The
// constructor seeds this via resolveCBRecoveryInterval which always
// returns a positive duration; reaching the loop with <=0 means
// NewProducerMulti was bypassed.
func TestCBRecovery_StartLoop_InvalidInterval_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	mgr := newFakeCBManager()

	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		cbManager:  mgr,
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", cbServiceName: "svc"},
		},
		// Zero interval — hand-built Producer skipped resolveCBRecoveryInterval.
		cbRecoveryInterval: 0,
		stop:               make(chan struct{}),
	}

	p.startCBRecoveryLoop()

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on zero cbRecoveryInterval at startCBRecoveryLoop")
	}
}

// TestCBRecovery_StartLoop_NilStopChannel_FiresAssertion pins T4 site:
// the stop-channel-nil branch in startCBRecoveryLoop must fire the
// trident when a hand-built *Producer reaches the loop with a nil stop
// channel. The constructor seeds p.stop so CloseContext can always signal
// the recovery goroutine; a nil stop would leak the goroutine.
func TestCBRecovery_StartLoop_NilStopChannel_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	mgr := newFakeCBManager()

	p := &Producer{
		logger:     cap,
		producerID: "pid-test",
		cbManager:  mgr,
		targets: map[string]*targetRuntime{
			"primary": {name: "primary", cbServiceName: "svc"},
		},
		cbRecoveryInterval: time.Millisecond,
		// stop intentionally nil — hand-built Producer skipped channel init.
		stop: nil,
	}

	p.startCBRecoveryLoop()

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on nil stop channel at startCBRecoveryLoop")
	}
}
