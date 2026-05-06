//go:build unit

package contract

import (
	"errors"
	"testing"
)

// TestOutboxEnvelope_ValidateShape_VersionMismatch_WrapsSentinel pins T7
// site outbox_envelope.go — the schema-evolution canary. The previous
// bare fmt.Errorf is replaced with the canonical ErrInvalidOutboxEnvelope
// wrap so callers can errors.Is consistently with every other envelope
// failure. Trident fires under operation="outbox_envelope.validate_shape"
// with violation="version_mismatch".
func TestOutboxEnvelope_ValidateShape_VersionMismatch_WrapsSentinel(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	envelope.Version = 99 // future / corrupted version

	err := envelope.ValidateShape()
	if !errors.Is(err, ErrInvalidOutboxEnvelope) {
		t.Fatalf("ValidateShape() error = %v; want errors.Is(ErrInvalidOutboxEnvelope)", err)
	}
}

// TestOutboxEnvelope_ValidateShape_KindTransportMismatch_WrapsSentinel pins
// T7 site outbox_envelope.go — last gate before outbox replay
// dispatches to a target adapter. Trident fires under
// operation="outbox_envelope.validate_shape" with
// violation="kind_transport_mismatch".
func TestOutboxEnvelope_ValidateShape_KindTransportMismatch_WrapsSentinel(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	// Envelope.Transport stays Kafka (default) but Destination.Kind moves
	// to SQS — mismatch must be rejected.
	envelope.Destination = Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"}

	err := envelope.ValidateShape()
	if !errors.Is(err, ErrInvalidOutboxEnvelope) {
		t.Fatalf("ValidateShape() error = %v; want errors.Is(ErrInvalidOutboxEnvelope)", err)
	}
}

// TestDeliveryPolicy_Validate_DirectSkipRequiresOutboxAlways pins T7 site
// delivery_policy.go — sole production guard against silent-no-op
// deployments. A policy with Direct=skip and Outbox != always has no
// delivery path; the trident fires with
// violation="direct_skip_requires_outbox_always" alongside the wrapped
// ErrInvalidDeliveryPolicy sentinel.
//
// We do NOT call t.Parallel() because the subtests swap the package-default
// asserter logger via setContractAsserterLogger; the swap is a global
// pointer flip and concurrent tests would observe whichever logger is
// current. Mirror event_topic_assert_test.go's discipline.
func TestDeliveryPolicy_Validate_DirectSkipRequiresOutboxAlways(t *testing.T) {
	cases := []struct {
		name   string
		outbox OutboxMode
	}{
		{"outbox=never", OutboxModeNever},
		{"outbox=fallback_on_circuit_open", OutboxModeFallbackOnCircuitOpen},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cap := newCaptureContractLogger()
			prev := setContractAsserterLogger(cap)
			t.Cleanup(func() { setContractAsserterLogger(prev) })

			p := DeliveryPolicy{
				Enabled: true,
				Direct:  DirectModeSkip,
				Outbox:  tc.outbox,
				DLQ:     DLQModeOnRoutableFailure,
			}

			err := p.Validate()
			if !errors.Is(err, ErrInvalidDeliveryPolicy) {
				t.Fatalf("Validate() error = %v; want errors.Is(ErrInvalidDeliveryPolicy)", err)
			}

			if !cap.containsMessage("ASSERTION FAILED") {
				t.Fatal("expected asserter trident to fire on Direct=skip with Outbox != always")
			}
		})
	}
}
