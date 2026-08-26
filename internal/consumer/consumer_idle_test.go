//go:build unit

package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// idleGroupClient models a broker serving an EMPTY, quiet topic — the shape the
// scripted fakeGroupClient cannot express, because it answers every poll from a
// script and falls off the end into a shutdown fetch.
//
// It mirrors franz-go exactly (kgo/consumer.go PollRecords): with no records
// buffered and no source ready to drain, PollFetches BLOCKS. It returns only
// when the caller's ctx is done — as kgo.NewErrFetch(ctx.Err()), the synthetic
// partition -1 fetch reaching the runtime through Errors()/EachError only — or
// when the client is closed, as the ErrClientClosed flavour of the same fetch.
//
// Records: never. That is the entire point.
type idleGroupClient struct {
	mu     sync.Mutex
	polls  int
	allows int

	closeOnce sync.Once
	stop      chan struct{}
}

func newIdleGroupClient() *idleGroupClient {
	return &idleGroupClient{stop: make(chan struct{})}
}

func (f *idleGroupClient) PollFetches(ctx context.Context) kgo.Fetches {
	f.mu.Lock()
	f.polls++
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		return kgo.NewErrFetch(ctx.Err())
	case <-f.stop:
		return kgo.NewErrFetch(kgo.ErrClientClosed)
	}
}

func (f *idleGroupClient) CommitRecords(_ context.Context, _ ...*kgo.Record) error { return nil }

func (f *idleGroupClient) SetOffsets(_ map[string]map[int32]kgo.EpochOffset) {}

func (f *idleGroupClient) AllowRebalance() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.allows++
}

func (f *idleGroupClient) Close() {
	f.closeOnce.Do(func() { close(f.stop) })
}

func (f *idleGroupClient) counts() (polls, allows int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.polls, f.allows
}

// runIdleConsumer starts a runtime against an idle broker and returns it with
// the client, the log spy, and a wait func that blocks until Run has returned.
func runIdleConsumer(t *testing.T, ctx context.Context, pollTimeout time.Duration) (*consumerRuntime, *idleGroupClient, *spyLogger, func() error) {
	t.Helper()

	client := newIdleGroupClient()
	spy := newSpyLogger()

	c := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
		cfg.PollTimeout = pollTimeout
	}, client, &fakeHandler{}, &fakeDLQ{}, WithLogger(spy))

	errCh := make(chan error, 1)

	go func() { errCh <- c.Run(ctx) }()

	return c, client, spy, func() error { return <-errCh }
}

// TestIdleTopicBecomesHealthy is the readiness deadlock, reproduced.
//
// A consumer joined to a group on a topic with ZERO traffic must report ready.
// PollFetches blocks until records arrive, so before the per-cycle deadline
// existed no poll cycle ever COMPLETED, lastPollOK was never stored, and
// Healthy returned ErrNotReady forever. A service gating /readyz on it (lender's
// consignado_exclusion check) stayed 0/1 until somebody hand-produced an event.
//
// An idle poll window with a joined group and no fetch errors IS healthy.
func TestIdleTopicBecomesHealthy(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, _, _, wait := runIdleConsumer(t, ctx, 50*time.Millisecond)

	deadline := time.After(2 * time.Second)

	for {
		if err := c.Healthy(ctx); err == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("Healthy never became nil on an idle topic: %v", c.Healthy(ctx))
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()

	if err := wait(); err != nil {
		t.Fatalf("Run after ctx cancel: got %v, want nil", err)
	}
}

// TestIdleCycleIsNotAFetchError pins the disambiguation on the OTHER side: an
// expired per-cycle deadline must not be metered or logged as a broker fetch
// error, and must not drive the fetch-error backoff. Quiet is not broken.
func TestIdleCycleIsNotAFetchError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, client, spy, wait := runIdleConsumer(t, ctx, 25*time.Millisecond)

	// Let several idle cycles elapse.
	time.Sleep(200 * time.Millisecond)

	cancel()

	if err := wait(); err != nil {
		t.Fatalf("Run after ctx cancel: got %v, want nil", err)
	}

	if spy.contains("fetch error") {
		t.Fatalf("idle poll window logged a fetch error: %v", spy.lines())
	}

	if spy.contains("DATA LOSS") {
		t.Fatalf("idle poll window logged data loss: %v", spy.lines())
	}

	polls, _ := client.counts()
	if polls < 2 {
		t.Fatalf("expected repeated idle poll cycles, got %d polls", polls)
	}
}

// TestIdleCyclePairsAllowRebalance guards Req 3 across the new return path.
// franz-go adds a poller before returning its synthetic ctx fetch, so the
// deadline path owes an AllowRebalance exactly like a record-bearing poll. An
// unpaired freeze wedges the group's next rebalance and hangs Close/LeaveGroup.
func TestIdleCyclePairsAllowRebalance(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, client, _, wait := runIdleConsumer(t, ctx, 20*time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	cancel()

	if err := wait(); err != nil {
		t.Fatalf("Run after ctx cancel: got %v, want nil", err)
	}

	polls, allows := client.counts()
	if polls != allows {
		t.Fatalf("AllowRebalance not paired 1:1 with PollFetches: %d polls, %d allows", polls, allows)
	}

	if polls < 2 {
		t.Fatalf("expected repeated idle poll cycles, got %d polls", polls)
	}
}

// TestIdleConsumerCloseReturnsNil keeps the shutdown semantics that the cycle
// deadline must NOT borrow: Close is a clean stop, Run returns nil, and Healthy
// flips to closed rather than staying green off the last idle cycle.
func TestIdleConsumerCloseReturnsNil(t *testing.T) {
	t.Parallel()

	// Close alone drives shutdown here — no ctx-cancel — so the test ctx is
	// enough and the deadline must not be mistaken for either signal.
	ctx := t.Context()

	c, _, _, wait := runIdleConsumer(t, ctx, 50*time.Millisecond)

	time.Sleep(120 * time.Millisecond)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := wait(); err != nil {
		t.Fatalf("Run after Close: got %v, want nil", err)
	}

	if err := c.Healthy(ctx); err == nil {
		t.Fatal("Healthy returned nil after Close")
	}
}

// TestPollWaitResolvesZeroToDefault covers the config half of the fix. A fluent
// NewConsumer() build never touches PollTimeout, so zero is the value the
// deadlocked deployments actually carried; it must resolve to the bounded
// default rather than to "block forever".
func TestPollWaitResolvesZeroToDefault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		set  time.Duration
		want time.Duration
	}{
		{"unset resolves to default", 0, defaultPollTimeout},
		{"explicit value is honoured", 3 * time.Second, 3 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := &consumerRuntime{cfg: ConsumerConfig{PollTimeout: tc.set}}
			if got := c.pollWait(); got != tc.want {
				t.Fatalf("pollWait() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestDrainFetchErrorsIdleWindow pins the classification directly: the runtime
// must read a deadline-expiry fetch as an empty CLEAN cycle — neither the
// shutdown path (which would make Run return) nor the fetch-error path (which
// would flip Healthy red and back off).
func TestDrainFetchErrorsIdleWindow(t *testing.T) {
	t.Parallel()

	c := newTestRuntime(t, newIdleGroupClient(), &fakeHandler{}, &fakeDLQ{})

	stop, fetchErr := c.drainFetchErrors(context.Background(), kgo.NewErrFetch(context.DeadlineExceeded))
	if stop {
		t.Fatal("deadline expiry took the shutdown path; it is an idle window, not a Close")
	}

	if fetchErr {
		t.Fatal("deadline expiry counted as a fetch error; an empty poll window is clean")
	}

	// The parent-cancel path is unchanged and still wins.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	stop, fetchErr = c.drainFetchErrors(canceled, kgo.NewErrFetch(context.Canceled))
	if !stop || fetchErr {
		t.Fatalf("parent cancel: stop=%v fetchErr=%v, want true/false", stop, fetchErr)
	}

	// A real broker fetch error still backs off and fails readiness.
	stop, fetchErr = c.drainFetchErrors(context.Background(), kgo.NewErrFetch(errors.New("SASL authentication failed")))
	if stop || !fetchErr {
		t.Fatalf("broker error: stop=%v fetchErr=%v, want false/true", stop, fetchErr)
	}
}
