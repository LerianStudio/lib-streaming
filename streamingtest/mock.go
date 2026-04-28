package streaming

import (
	"context"
	"sync"
	"testing"
	"time"
)

// MockEmitter is a concurrency-safe, zero-dependency test double for Emitter.
// It captures every emitted EmitRequest (deep-copied) and lets tests inspect,
// count, or wait on them via the Assert* helpers and WaitForEvent.
//
// Safe for concurrent use from any number of goroutines (DX-A03). All
// internal state is guarded by an unexported mutex; captured events are
// deep-copied on Emit so post-hoc caller mutation does not change the
// captured slice.
type MockEmitter struct {
	mu       sync.Mutex
	requests []EmitRequest
	err      error
	closed   bool
}

// NewMockEmitter returns a fresh MockEmitter with an empty event buffer and
// no injected error.
func NewMockEmitter() *MockEmitter {
	return &MockEmitter{
		requests: make([]EmitRequest, 0),
	}
}

// Emit captures a deep copy of the request. When SetError has set an error,
// that error is returned and the event is NOT captured — simulating a
// publish failure in the caller's code path.
func (m *MockEmitter) Emit(_ context.Context, request EmitRequest) error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return m.err
	}

	m.requests = append(m.requests, deepCopyEmitRequest(request))

	return nil
}

// Requests returns a snapshot of captured requests in emission order. The
// returned slice is a deep copy — callers may mutate it without affecting
// the mock's internal state.
func (m *MockEmitter) Requests() []EmitRequest {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]EmitRequest, len(m.requests))
	for i, request := range m.requests {
		out[i] = deepCopyEmitRequest(request)
	}

	return out
}

// SetError makes subsequent Emit calls return err without capturing the
// event. Pass nil to restore the happy path.
func (m *MockEmitter) SetError(err error) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.err = err
}

// Reset clears the captured event buffer and the injected error. Leaves
// the closed flag intact — use a fresh NewMockEmitter for full reset.
func (m *MockEmitter) Reset() {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.requests = m.requests[:0]
	m.err = nil
}

// Close is idempotent; always returns nil. The MockEmitter does not reject
// Emit after Close — tests that care about the post-Close contract should
// check on a real Producer.
func (m *MockEmitter) Close() error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	return nil
}

// Healthy always returns nil — the mock is always "healthy" unless tests
// explicitly override via SetError (which affects Emit only, by design).
func (m *MockEmitter) Healthy(_ context.Context) error {
	return nil
}

// deepCopyEmitRequest returns a fully independent copy of the EmitRequest. The Payload
// slice is copied so caller-side mutation after Emit does not change the
// captured bytes.
func deepCopyEmitRequest(request EmitRequest) EmitRequest {
	var payload []byte
	if len(request.Payload) > 0 {
		payload = make([]byte, len(request.Payload))
		copy(payload, request.Payload)
	}

	out := request
	out.Payload = payload

	if request.PolicyOverride.Enabled != nil {
		enabled := *request.PolicyOverride.Enabled
		out.PolicyOverride.Enabled = &enabled
	}

	return out
}

// AssertEventEmitted fails t when no captured request has the given definition
// key. Uses testing.TB so helpers work in benchmarks and
// fuzz tests. Calls t.Helper() for clean stack traces.
func AssertEventEmitted(t testing.TB, m *MockEmitter, definitionKey string) {
	t.Helper()

	for _, request := range m.Requests() {
		if request.DefinitionKey == definitionKey {
			return
		}
	}

	t.Errorf("expected event %s to be emitted; none found in %d captured requests", definitionKey, len(m.Requests()))
}

// AssertEventCount fails t when the count of captured events matching the
// definition key does not equal n.
func AssertEventCount(t testing.TB, m *MockEmitter, definitionKey string, n int) {
	t.Helper()

	count := 0

	for _, request := range m.Requests() {
		if request.DefinitionKey == definitionKey {
			count++
		}
	}

	if count != n {
		t.Errorf("expected %d events of %s; got %d", n, definitionKey, count)
	}
}

// AssertTenantID fails t when no captured event carries the given tenant ID.
func AssertTenantID(t testing.TB, m *MockEmitter, tenantID string) {
	t.Helper()

	for _, request := range m.Requests() {
		if request.TenantID == tenantID {
			return
		}
	}

	t.Errorf("expected at least one event with TenantID=%q; none found in %d captured requests", tenantID, len(m.Requests()))
}

// AssertNoEvents fails t when any event was captured.
func AssertNoEvents(t testing.TB, m *MockEmitter) {
	t.Helper()

	if got := len(m.Requests()); got != 0 {
		t.Errorf("expected no events to be emitted; got %d", got)
	}
}

// WaitForEvent blocks until the matcher returns true on a newly-observed
// request, or timeout elapses. Calls t.Fatalf on timeout. Returns the matching
// request on success.
//
// The poll interval is fixed at 1ms — intentionally small so wall-clock
// tests see fast convergence. Under testing/synctest the polling loop is
// fully deterministic because time advances only when every goroutine in
// the bubble is blocked, so the 1ms granularity does NOT add real wait time.
//
// Nil-ctx safe: passing a nil ctx falls back to context.Background.
// Nil-matcher is a test programming bug — calls t.Fatalf instead of panicking
// mid-loop with a nil-deref.
func WaitForEvent(t testing.TB, ctx context.Context, m *MockEmitter, matcher func(EmitRequest) bool, timeout time.Duration) EmitRequest {
	t.Helper()

	if matcher == nil {
		t.Fatalf("WaitForEvent: matcher must not be nil")
		return EmitRequest{}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	const pollInterval = 1 * time.Millisecond

	deadline := time.Now().Add(timeout)

	for {
		for _, request := range m.Requests() {
			if matcher(request) {
				return request
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("WaitForEvent timed out after %v", timeout)
			return EmitRequest{}
		}

		// Respect context cancellation; unusual in unit tests but prevents
		// the loop from running forever under a canceled ctx.
		select {
		case <-ctx.Done():
			t.Fatalf("WaitForEvent canceled: %v", ctx.Err())
			return EmitRequest{}
		case <-time.After(pollInterval):
		}
	}
}
