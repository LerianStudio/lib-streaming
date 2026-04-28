package producer

import (
	"context"
	"errors"
	"testing"
)

// TestPublishToOutbox_NilOutboxWriter_FiresAssertion pins T-008: every call
// site of publishToOutbox has already gated on outbox availability (the
// circuit-open branch checks p.outboxWriter != nil; the outbox-always
// branch runs only after ResolveDeliveryPolicy rejects direct=skip +
// outbox!=always). Reaching publishToOutbox with nil p.outboxWriter means
// a caller invariant was broken.
//
// Before T-008: silent ErrOutboxNotConfigured returned, indistinguishable
// at the outcome layer from a legitimate outbox write failure. After T-008:
// asserter.NotNil fires; ErrOutboxNotConfigured still returned so upstream
// outcome classification is unchanged.
func TestPublishToOutbox_NilOutboxWriter_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{
		logger:       cap,
		producerID:   "test-producer-nil-outbox-writer",
		outboxWriter: nil,
	}

	topic := "lerian.streaming.transaction.created"
	err := p.publishToOutbox(context.Background(), Event{}, topic, DefaultDeliveryPolicy(), "some.key")

	if !errors.Is(err, ErrOutboxNotConfigured) {
		t.Errorf("publishToOutbox err = %v, want ErrOutboxNotConfigured", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil outboxWriter")
	}
}
