package streaming

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
)

// TestCBListener_NilProducer_FiresAssertion pins T-002: OnStateChange must
// NOT silently return when producer is nil — the post-registration invariant
// (listener always holds a non-nil producer) is an invariant violation that
// MUST fire the observability trident.
//
// Before T-002: `if l == nil || l.producer == nil { return }` — silent, no log,
// no metric, so cbStateFlag stops mirroring under a detached listener and
// outbox fallback never triggers on broker outage. Most dangerous finding in
// the sweep.
//
// After T-002: the listener constructs a fallback asserter backed by a
// capture logger (the nil-producer case cannot use p.newAsserter), fires
// asserter.NotNil, the log layer emits "ASSERTION FAILED", then the function
// returns to preserve the existing no-panic contract. The l == nil
// receiver-nil check remains as a DX guard — receiver-nil is distinct from
// the listener's post-registration invariant.
func TestCBListener_NilProducer_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	l := &streamingStateListener{producer: nil, fallbackLogger: cap}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("listener with nil producer panicked: %v", r)
		}
	}()

	l.OnStateChange(context.Background(), "svc", circuitbreaker.StateClosed, circuitbreaker.StateOpen)

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter to fire an ASSERTION FAILED log line on nil producer")
	}
}
