//go:build unit

package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// errMyBusinessRule stands in for the one error a service's classifier actually
// recognizes. The shape below — "retry everything that is NOT my business rule"
// — is the common one, and it is what turns a library verdict into a wedge.
var errMyBusinessRule = errors.New("test: my business rule rejected this")

// retryEverythingElse is the classifier shape under test: it says "transient"
// for every error it does not own. Perfectly reasonable for a service to write,
// and fatal if a structural, never-satisfiable library verdict is routed
// through it.
func retryEverythingElse(err error) bool { return !errors.Is(err, errMyBusinessRule) }

// TestLibraryVerdicts_BypassTheServiceClassifier proves the two sentinels the
// LIBRARY synthesizes are quarantined outright, not offered to the service's
// Classifier.
//
// ErrUnhandledEvent and ErrUnexpectedSource are structural: no handler exists
// for this key, or this record came from a source the consumer refuses. Neither
// can ever become satisfiable by waiting. Routed through a classifier that
// retries what it does not recognize, each became "transient" — retried to
// exhaustion, seeked back, partition halted, redelivered, forever. Under one
// topic per app that is the producing application's whole stream stuck behind
// one record, with nothing ever reaching the DLQ where an operator would see it.
func TestLibraryVerdicts_BypassTheServiceClassifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantKind string
	}{
		{"no handler registered for the key", fmt.Errorf("%w: %q", ErrUnhandledEvent, "loan.disbursed"), dlqCauseUnhandledKey},
		{"record came from an unexpected producer", fmt.Errorf("%w: got %q", ErrUnexpectedSource, "stranger"), dlqCauseSourceMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error { return tt.err }}
			client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 9, ceHeaders("tenantA", false))))
			dlq := &fakeDLQ{}

			r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryEverythingElse))
			runUntilClosed(t, r)

			if dlq.count() != 1 {
				t.Fatalf("DLQ count = %d; want 1 — a structural library verdict must quarantine, never retry", dlq.count())
			}

			if _, kind := dlq.lastCause(); kind != tt.wantKind {
				t.Errorf("cause kind = %q; want %q", kind, tt.wantKind)
			}

			if len(client.seeks) != 0 {
				t.Errorf("seek-backs = %d; want 0 — a retried structural verdict wedges the partition forever", len(client.seeks))
			}

			// One handler call: no in-loop retry budget was spent on a verdict
			// that can never be satisfied.
			if got := handler.callCount(); got != 1 {
				t.Errorf("handler called %d times; want 1 (no retries on a structural verdict)", got)
			}

			if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 10 {
				t.Errorf("committed watermark = %d; want 10 (commit strictly after the quarantine copy is durable)", wm)
			}
		})
	}
}

// TestServiceVerdicts_StillReachTheClassifier is the other half of the
// short-circuit: narrowing it to the library's own sentinels must not take the
// Classifier out of the loop for the errors it exists to reclassify.
func TestServiceVerdicts_StillReachTheClassifier(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
		return errors.New("postgres: connection refused")
	}}

	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 9, ceHeaders("tenantA", false))))
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryEverythingElse))
	runUntilClosed(t, r)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 — a downstream blip the Classifier owns must retry, not quarantine", dlq.count())
	}

	if len(client.seeks) == 0 {
		t.Error("expected a seek-back for the sustained reclassified transient; got none")
	}
}
