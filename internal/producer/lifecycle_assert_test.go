package streaming

import (
	"context"
	"testing"
)

// TestCloseContext_NilClient_FiresAssertion pins T-005 site 1: a hand-built
// *Producer with nil client reaching CloseContext must fire the asserter
// (instead of silently returning nil) so the fixture-antipattern is visible
// on dashboards. Close is still idempotent via the existing atomic.Bool CAS,
// and still returns nil to the caller after the assertion fires — the
// public "Close always returns nil on the post-guard fast path" contract
// stays intact.
func TestCloseContext_NilClient_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:     cap,
		producerID: "test-producer-close-nil-client",
		stop:       make(chan struct{}),
	}

	err := p.CloseContext(context.Background())
	if err != nil {
		t.Fatalf("CloseContext on nil-client Producer should still return nil; got %v", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil client at Close time")
	}
}

// TestSignalStop_NilStopChannel_FiresAssertion pins T-005 site 2: a
// hand-built *Producer with a nil stop channel must fire the asserter when
// signalStop is invoked. Before T-005, signalStop silently skipped the
// close, which would hang RunContext forever on shutdown. After T-005, the
// asserter fires and close(p.stop) is still guarded by sync.Once so no
// panic on re-entry.
func TestSignalStop_NilStopChannel_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	// Pre-flip closed so CloseContext takes the "already closed" branch and
	// still invokes signalStop, which is the site under test.
	p := &Producer{
		logger:     cap,
		producerID: "test-producer-nil-stop",
		stop:       nil,
	}
	p.closed.Store(true)
	// sanity: confirm the atomic flag actually flipped (atomic.Bool has no
	// exported getter aside from Load, but this guards against someone
	// renaming the field out from under us).
	if !p.closed.Load() {
		t.Fatalf("test setup: closed.Load() = false, want true")
	}

	// signalStop is not exported; reach it via CloseContext's "already
	// closed" branch which calls signalStop before returning.
	err := p.CloseContext(context.Background())
	if err != nil {
		t.Fatalf("CloseContext on nil-stop Producer should still return nil; got %v", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil stop channel at signalStop")
	}
}
