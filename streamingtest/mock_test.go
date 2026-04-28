//go:build unit

package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// TestMockEmitter_CaptureAndEvents verifies the simplest contract: Emit
// records the request, and Requests() returns the slice in FIFO order.
func TestMockEmitter_CaptureAndEvents(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	ctx := context.Background()

	requests := []EmitRequest{
		{DefinitionKey: "transaction.created", TenantID: "t-1"},
		{DefinitionKey: "account.updated", TenantID: "t-2"},
		{DefinitionKey: "ledger.closed", TenantID: "t-3"},
	}

	for _, request := range requests {
		if err := m.Emit(ctx, request); err != nil {
			t.Fatalf("Emit returned unexpected error: %v", err)
		}
	}

	got := m.Requests()
	if len(got) != len(requests) {
		t.Fatalf("len(Requests()) = %d; want %d", len(got), len(requests))
	}
	for i, request := range requests {
		if got[i].DefinitionKey != request.DefinitionKey || got[i].TenantID != request.TenantID {
			t.Errorf("Requests()[%d] = %+v; want %+v", i, got[i], request)
		}
	}
}

// TestMockEmitter_DeepCopy ensures captured events are independent of the
// caller's struct — mutating the original Payload after Emit does not change
// the captured value.
func TestMockEmitter_DeepCopy(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	ctx := context.Background()

	payload := json.RawMessage(`{"amount":100}`)
	request := EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "t-1",
		Payload:       payload,
	}

	if err := m.Emit(ctx, request); err != nil {
		t.Fatalf("Emit returned unexpected error: %v", err)
	}

	// Mutate the payload bytes underneath the original (simulate caller reuse).
	for i := range payload {
		payload[i] = 'X'
	}

	captured := m.Requests()[0]
	if string(captured.Payload) != `{"amount":100}` {
		t.Errorf("captured payload mutated by caller: got %q; want %q", string(captured.Payload), `{"amount":100}`)
	}
}

// TestMockEmitter_SetError forces Emit to return a preset error.
func TestMockEmitter_SetError(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	ctx := context.Background()

	m.SetError(ErrPayloadTooLarge)

	err := m.Emit(ctx, EmitRequest{DefinitionKey: "transaction.created", TenantID: "t-1"})
	if err != ErrPayloadTooLarge {
		t.Errorf("Emit() err = %v; want ErrPayloadTooLarge", err)
	}

	// Clearing restores the happy path.
	m.SetError(nil)
	if err := m.Emit(ctx, EmitRequest{DefinitionKey: "transaction.created", TenantID: "t-1"}); err != nil {
		t.Errorf("Emit after SetError(nil) returned error: %v", err)
	}
}

// TestMockEmitter_Reset clears all captured events.
func TestMockEmitter_Reset(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	ctx := context.Background()

	_ = m.Emit(ctx, EmitRequest{DefinitionKey: "transaction.created", TenantID: "t-1"})
	_ = m.Emit(ctx, EmitRequest{DefinitionKey: "account.updated", TenantID: "t-1"})

	if len(m.Requests()) != 2 {
		t.Fatalf("pre-reset len = %d; want 2", len(m.Requests()))
	}

	m.Reset()

	if len(m.Requests()) != 0 {
		t.Errorf("post-reset len = %d; want 0", len(m.Requests()))
	}
}

// TestMockEmitter_ConcurrentEmits spawns 1000 goroutines emitting once each
// and asserts every emission is captured. Run with -race to detect any
// unguarded shared state (DX-A03, DX-C01).
func TestMockEmitter_ConcurrentEmits(t *testing.T) {
	t.Parallel()

	const n = 1000

	m := NewMockEmitter()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = m.Emit(ctx, EmitRequest{
				DefinitionKey: "transaction.created",
				TenantID:      fmt.Sprintf("t-%d", i),
			})
		}(i)
	}
	wg.Wait()

	if got := len(m.Requests()); got != n {
		t.Errorf("captured %d events; want %d", got, n)
	}
}

// TestMockEmitter_CloseHealthy exercises the lifecycle surface.
func TestMockEmitter_CloseHealthy(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	ctx := context.Background()

	if err := m.Healthy(ctx); err != nil {
		t.Errorf("Healthy() = %v; want nil", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close() = %v; want nil", err)
	}
	// Idempotent close.
	if err := m.Close(); err != nil {
		t.Errorf("second Close() = %v; want nil", err)
	}
}

// TestMockEmitter_NilReceiver verifies every method is nil-safe. A nil
// *MockEmitter is a common shape when a test helper forgets to initialize.
func TestMockEmitter_NilReceiver(t *testing.T) {
	t.Parallel()

	var m *MockEmitter

	if err := m.Emit(context.Background(), EmitRequest{}); err != nil {
		t.Errorf("nil.Emit = %v; want nil", err)
	}
	if got := m.Requests(); got != nil {
		t.Errorf("nil.Requests = %v; want nil", got)
	}
	// SetError and Reset should be no-ops (no panic).
	m.SetError(ErrNotJSON)
	m.Reset()

	if err := m.Close(); err != nil {
		t.Errorf("nil.Close = %v; want nil", err)
	}
	if err := m.Healthy(context.Background()); err != nil {
		t.Errorf("nil.Healthy = %v; want nil", err)
	}
}

// TestAssertEventEmitted_Pass passes when a matching event exists.
func TestAssertEventEmitted_Pass(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()
	_ = m.Emit(context.Background(), EmitRequest{DefinitionKey: "transaction.created", TenantID: "t-1"})

	// All four exported assertion helpers must succeed silently on a
	// matching mock. Any failure bubbles up through the real *testing.T.
	AssertEventEmitted(t, m, "transaction.created")
	AssertEventCount(t, m, "transaction.created", 1)
	AssertTenantID(t, m, "t-1")
}

// TestAssertNoEvents_Pass passes on a fresh mock.
func TestAssertNoEvents_Pass(t *testing.T) {
	t.Parallel()

	m := NewMockEmitter()

	AssertNoEvents(t, m)
}

// TestWaitForEvent_DeterministicMatch uses testing/synctest so polling timing
// is virtual. The goroutine emits after 10ms virtual; the wait timeout is 1s.
// Under synctest, the wait resolves after the emit with no real-wall-clock
// sleep (DX-C04).
func TestWaitForEvent_DeterministicMatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		m := NewMockEmitter()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			time.Sleep(10 * time.Millisecond)
			_ = m.Emit(ctx, EmitRequest{DefinitionKey: "transaction.created", TenantID: "t-1"})
		}()

		got := WaitForEvent(t, ctx, m, func(request EmitRequest) bool {
			return request.DefinitionKey == "transaction.created"
		}, 1*time.Second)

		if got.TenantID != "t-1" {
			t.Errorf("WaitForEvent returned event %+v; want TenantID=t-1", got)
		}
	})
}

// TestWaitForEvent_NilContext verifies that passing a nil context to
// WaitForEvent falls back to context.Background rather than panicking on a
// nil-deref. Mirrors the nil-ctx defense in Producer.Healthy / CloseContext.
func TestWaitForEvent_NilContext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		m := NewMockEmitter()

		go func() {
			time.Sleep(5 * time.Millisecond)
			_ = m.Emit(context.Background(), EmitRequest{
				DefinitionKey: "transaction.created",
				TenantID:      "t-1",
			})
		}()

		//nolint:staticcheck // intentional nil ctx to verify fallback
		got := WaitForEvent(t, nil, m, func(request EmitRequest) bool {
			return request.DefinitionKey == "transaction.created"
		}, 1*time.Second)

		if got.TenantID != "t-1" {
			t.Errorf("WaitForEvent(nil ctx) returned %+v; want TenantID=t-1", got)
		}
	})
}
