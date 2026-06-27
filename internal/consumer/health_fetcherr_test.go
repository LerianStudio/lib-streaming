//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"
)

// TestHealthy_FalseAfterFetchErrorCycle proves a non-shutdown fetch-error cycle
// leaves Healthy() reporting NOT-ok (finding #3): the group is not cleanly
// fetching, so lastPollOK must be stored as !fetchErr, not unconditionally true.
func TestHealthy_FalseAfterFetchErrorCycle(t *testing.T) {
	t.Parallel()

	authErr := errors.New("SASL authentication failed")
	client := newFakeGroupClient(errorFetch("t", 0, authErr))
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq, WithLogger(newSpyLogger()))

	// One poll cycle with a non-shutdown fetch error.
	stop, _ := r.pollCycle(context.Background())
	if stop {
		t.Fatal("pollCycle returned stop=true for a non-shutdown fetch error; want stop=false")
	}

	if err := r.Healthy(context.Background()); err == nil {
		t.Error("Healthy() = nil after a fetch-error cycle; want NOT-ready (group not cleanly fetching)")
	}
}

// TestHealthy_TrueAfterCleanCycle is the control: a clean poll cycle (no fetch
// error) marks the consumer healthy.
func TestHealthy_TrueAfterCleanCycle(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 1, ceHeaders("tenantA", false))))
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	stop, _ := r.pollCycle(context.Background())
	if stop {
		t.Fatal("pollCycle returned stop=true for a clean cycle; want stop=false")
	}

	if err := r.Healthy(context.Background()); err != nil {
		t.Errorf("Healthy() = %v after a clean cycle; want nil", err)
	}
}
