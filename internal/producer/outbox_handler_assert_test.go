package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// TestHandleOutboxRow_NilRow_FiresAssertion pins T-007: the lib-commons
// outbox Dispatcher contract guarantees row is non-nil when invoking a
// registered handler. A nil row therefore means the Dispatcher broke its
// contract — distinct from 'handler rejected a valid row'. Before T-007,
// operators saw both failure modes as an indistinguishable
// outbox.ErrOutboxEventRequired return.
//
// After T-007: asserter.NotNil fires the trident AND the handler still
// returns outbox.ErrOutboxEventRequired so the Dispatcher's retry/fail
// semantics remain intact.
func TestHandleOutboxRow_NilRow_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{logger: cap, producerID: "test-producer-nil-row"}

	err := p.handleOutboxRow(context.Background(), nil)

	if !errors.Is(err, outbox.ErrOutboxEventRequired) {
		t.Errorf("handleOutboxRow(nil) err = %v, want outbox.ErrOutboxEventRequired", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil row")
	}
}
