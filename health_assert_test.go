package streaming

import (
	"context"
	"testing"
)

// TestHealthy_NilClient_FiresAssertion pins T-004: when a caller constructs
// a *Producer that bypasses NewProducer and ends up with a nil client,
// Healthy(ctx) must (a) still return a *HealthError with State()==Down so
// the public contract of /readyz is unchanged, AND (b) fire the asserter
// so operators see the real root cause (construction antipattern) instead
// of a generic "client not initialized" string that does not match any
// errors.Is sentinel.
func TestHealthy_NilClient_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{logger: cap, producerID: "test-producer-client-nil"}

	err := p.Healthy(context.Background())

	if err == nil {
		t.Fatal("expected *HealthError when client is nil, got nil")
	}

	healthErr, ok := err.(*HealthError)
	if !ok {
		t.Fatalf("expected *HealthError, got %T: %v", err, err)
	}
	if healthErr.State() != Down {
		t.Errorf("health state = %v, want Down", healthErr.State())
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NotNil to fire on nil client")
	}
}
