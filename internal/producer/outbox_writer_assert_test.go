package producer

import (
	"context"
	"errors"
	"testing"
)

// TestLibCommonsOutboxWriter_Write_NilRepo_FiresAssertion pins T-006 site 1:
// constructing libCommonsOutboxWriter with a nil repo directly (bypassing
// WithOutboxRepository's own nil guard) and calling Write must fire the
// asserter AND still return ErrOutboxNotConfigured to the caller so
// upstream outcome classification stays unchanged.
//
// Logger injection happens via the writer's own logger field — no
// package-level mutable state, no swap helper, no parallel-safety constraint.
func TestLibCommonsOutboxWriter_Write_NilRepo_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	w := &libCommonsOutboxWriter{repo: nil, logger: cap}
	err := w.Write(context.Background(), OutboxEnvelope{})
	if !errors.Is(err, ErrOutboxNotConfigured) {
		t.Errorf("Write err = %v, want ErrOutboxNotConfigured", err)
	}
	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil repo at Write")
	}
}

// TestLibCommonsOutboxWriter_WriteWithTx_NilRepo_FiresAssertion pins T-006
// site 2: same pattern for WriteWithTx. The ambient-transaction path must
// fire the asserter and still return ErrOutboxNotConfigured.
//
// Per-instance logger injection — no shared state with sibling tests.
func TestLibCommonsOutboxWriter_WriteWithTx_NilRepo_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	w := &libCommonsOutboxWriter{repo: nil, logger: cap}
	err := w.WriteWithTx(context.Background(), nil, OutboxEnvelope{})
	if !errors.Is(err, ErrOutboxNotConfigured) {
		t.Errorf("WriteWithTx err = %v, want ErrOutboxNotConfigured", err)
	}
	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil repo at WriteWithTx")
	}
}
