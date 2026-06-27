//go:build integration

package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// This file is the docs/design/consumer.md §9 mandated kfake-backed smoke test.
//
// The §9 split: the scripted-fake unit suite (consumer_fake_test.go) owns the 5
// correctness requirements because kfake group rejoins are timing-flaky. This
// ONE integration test owns the part the fake can NOT prove: the REAL franz-go
// poll-wait / rebalance / LeaveGroup interaction. The fake's AllowRebalance()
// and Close() are inert counters, so the deadlock that lived in the real
// BlockRebalanceOnPoll <-> LeaveGroup handshake was UNEXERCISED by Wave 2.
//
// The deadlock: kgo.BlockRebalanceOnPoll freezes the group rebalance for the
// life of each polled batch. Close() calls client.Close() -> LeaveGroup, which
// must drive a rebalance — and BLOCKS on the frozen one until AllowRebalance is
// called. consumer.go pairs every PollFetches with a deferred AllowRebalance
// (pollCycle, the fix). Without that pairing, Close() hangs forever. The
// bounded-timeout assertion below is the teeth: a deadlock surfaces as a test
// FAILURE, never a hung CI.

const (
	kfakeSourceTopic = "lerian.streaming.loan.created"
	kfakeGroup       = "consumer-kfake-smoke"
	// closeBudget bounds Close(): a clean LeaveGroup is sub-second; a frozen
	// rebalance never returns. 10s is generous headroom over the franz-go
	// LeaveGroup round-trip while still failing fast on a deadlock.
	closeBudget = 10 * time.Second
	// runBudget bounds Run's clean return after the shutdown signal.
	runBudget = 10 * time.Second
)

// TestMain runs goleak at PACKAGE scope (goleak.VerifyTestMain = the TestMain
// form of VerifyNone the spec §9 allows). Package scope is deliberate, not
// per-test: franz-go's Client.Close() waits synchronously for the group-left
// handshake (consumer_group.go <-c.g.left) but its heartbeat / loopFetch /
// session-close goroutines observe ctx-cancel and exit a few scheduler ticks
// LATER — a per-test VerifyNone races that async teardown and flakes. At
// TestMain scope the detector runs after every t.Cleanup (cluster.Close +
// client.Close) and after the scheduler has drained, so the only survivors are
// the kfake/kgo stragglers explicitly ignored below.
//
// THIS DOES NOT BLUNT THE TEETH: the deadlock the test proves surfaces as the
// closeWithinBudget t.Fatalf timeout (a frozen rebalance hangs Close FOREVER),
// not as a goleak finding — goleak is the secondary leak-free check for the
// CLEAN path. The ignore set mirrors internal/producer/main_test.go; the kfake
// conn workers (clientConn.read/write, group.manage) are added because the
// consumer joins a real group (the producer suite only produces).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("testing.(*T).Run"),
		goleak.IgnoreTopFunction("runtime.goexit"),
		// kfake cluster + per-connection workers, torn down by cluster.Close,
		// which t.Cleanup runs AFTER the suite's last test.
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).run"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).listen"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*broker).handleConn"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*broker).serve"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*clientConn).read"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*clientConn).write"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*group).manage"),
		// kfake listen socket parks here until cluster.Close.
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		// kgo background workers (consumer client + DLQ adapter client). They
		// exit only after their own Close cancels the client ctx, which may park
		// a few ticks past the last test — racing-but-correct stragglers.
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).pushMetrics"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
	)
}

// kfakeCluster spins up a single-partition kfake cluster seeding the source
// topic and its .dlq sibling (the DLQ adapter Build constructs targets it). One
// partition keeps the poll deterministic and fast. Registered for t.Cleanup.
func kfakeCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(1),
		kfake.SeedTopics(1, kfakeSourceTopic, kfakeSourceTopic+".dlq"),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// kfakeConsumerConfig is a real, validatable ConsumerConfig pointed at the kfake
// cluster with short timeouts so the smoke test stays fast and deterministic.
func kfakeConsumerConfig(cluster *kfake.Cluster) ConsumerConfig {
	return ConsumerConfig{
		Enabled:             true,
		Brokers:             cluster.ListenAddrs(),
		Group:               kfakeGroup,
		Topics:              []string{kfakeSourceTopic},
		ClientID:            "consumer-kfake-smoke",
		RetryBudget:         1,
		RetryBackoffInitial: time.Millisecond,
		RetryBackoffMax:     2 * time.Millisecond,
		RetryInLoopMaxDwell: 10 * time.Millisecond,
		HaltBackoff:         time.Millisecond,
		PollTimeout:         200 * time.Millisecond,
		CloseTimeout:        closeBudget,
		DLQTopicSuffix:      ".dlq",
	}
}

// countingHandler counts Handle calls; always succeeds. The smoke test cares
// about the poll/rebalance/close lifecycle, not disposition — so the handler is
// trivial and never produces poison.
type countingHandler struct {
	mu    sync.Mutex
	calls int
}

func (h *countingHandler) Handle(_ context.Context, _ contract.Event, _ []byte) error {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()

	return nil
}

func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.calls
}

// keepProducing publishes a valid CloudEvents record to the source topic every
// ~tick until stop is closed, via a throwaway kgo client. A NON-EMPTY poll is
// what makes franz-go HOLD the BlockRebalanceOnPoll freeze past PollFetches
// (consumer.go fill(): an empty fetch immediately unaddPoller()s, only a
// non-empty fetch keeps the rebalance frozen) — so a record is mandatory to
// create the deadlock condition the test proves.
//
// It produces REPEATEDLY rather than once to remove the only flake source in
// this smoke test: kfake's fresh-group join/first-fetch latency is
// non-deterministic (the spec §9 calls kfake group timing flaky and keeps it
// out of the correctness suite for exactly this reason). A steady trickle
// guarantees a record is always waiting at/after the consumer's cursor whenever
// the group finally stabilizes, so the consumer reliably polls a non-empty
// batch. The goroutine exits on stop (joined via wg by the caller), so it does
// not leak under goleak.
func keepProducing(t *testing.T, cluster *kfake.Cluster, stop <-chan struct{}, wg *sync.WaitGroup) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("producer client init err = %v", err)
	}

	wg.Go(func() {
		defer cl.Close()

		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			rec := &kgo.Record{
				Topic:   kfakeSourceTopic,
				Headers: ceHeadersForKfake("tenant-smoke"),
				Value:   []byte(`{"ok":true}`),
			}

			// Fire-and-forget produce; a transient error during teardown is fine.
			cl.Produce(context.Background(), rec, nil)

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})
}

// ceHeadersForKfake builds a minimal valid CloudEvents binary-mode header set so
// the real codec parses the record and dispatches it to the handler (no DLQ).
func ceHeadersForKfake(tenant string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("evt-kfake-1")},
		{Key: "ce-source", Value: []byte("//test/source")},
		{Key: "ce-type", Value: []byte("studio.lerian.loan.created")},
		{Key: "ce-time", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		{Key: "ce-resourcetype", Value: []byte("loan")},
		{Key: "ce-eventtype", Value: []byte("created")},
		{Key: "ce-schemaversion", Value: []byte("1.0.0")},
		{Key: "ce-tenantid", Value: []byte(tenant)},
	}
}

// runConsumer launches the consumer's Run in a goroutine and returns a channel
// that receives Run's error when it returns. The caller drives shutdown.
func runConsumer(ctx context.Context, c Runner) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- c.Run(ctx)
	}()

	return done
}

// closeWithinBudget runs Close in a goroutine and asserts it returns within
// closeBudget. THIS IS THE TEETH: a frozen-rebalance deadlock (missing
// AllowRebalance) makes Close block forever, which surfaces here as a FAILURE
// instead of a hung process / hung CI.
func closeWithinBudget(t *testing.T, c Runner) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close() err = %v; want nil", err)
		}
	case <-time.After(closeBudget):
		t.Fatalf("Close() did not return within %s — shutdown DEADLOCK "+
			"(BlockRebalanceOnPoll frozen, AllowRebalance never called)", closeBudget)
	}
}

// assertRunReturnedNil waits for Run to return and asserts it returned nil (a
// CLEAN shutdown), not the ctx error — the lib-commons Launcher contract.
func assertRunReturnedNil(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() err = %v; want nil on clean shutdown", err)
		}
	case <-time.After(runBudget):
		t.Fatalf("Run() did not return within %s after shutdown signal", runBudget)
	}
}

// TestIntegration_Consumer_CtxCancelThenClose covers ordering (1): the
// lib-commons Launcher shape — cancel the Run ctx, wait for Run to return nil,
// THEN Close(). Close must still return within budget (the LeaveGroup must not
// deadlock on a rebalance frozen by the consumer's last poll). goleak.VerifyNone
// proves the poll goroutine is gone.
func TestIntegration_Consumer_CtxCancelThenClose(t *testing.T) {
	cluster := kfakeCluster(t)

	// Trickle records for the whole test so every poll is non-empty (a frozen
	// rebalance) and the fresh-group join latency cannot starve the consumer.
	stop, wg := make(chan struct{}), &sync.WaitGroup{}

	keepProducing(t, cluster, stop, wg)
	defer func() { close(stop); wg.Wait() }()

	handler := &countingHandler{}

	c, err := Build(context.Background(), kfakeConsumerConfig(cluster), handler,
		WithLogger(log.NewNop()))
	if err != nil {
		t.Fatalf("Build err = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := runConsumer(ctx, c)

	// Let the consumer join the group and poll at least one record, so a
	// rebalance is genuinely frozen at the moment of shutdown.
	waitForHandle(t, handler)

	// Ordering (1): ctx-cancel FIRST, wait for Run to drain cleanly...
	cancel()
	assertRunReturnedNil(t, done)

	// ...THEN Close. The deferred AllowRebalance from the last poll must have
	// released the freeze so LeaveGroup completes within budget.
	closeWithinBudget(t, c)
}

// TestIntegration_Consumer_CloseOnlyMidPoll covers ordering (2): call Close()
// while Run is mid-poll (no prior ctx-cancel). Close injects the synthetic
// ErrClientClosed fetch that breaks the in-flight PollFetches; Run must return
// nil and Close must return within budget. This is the ordering that most
// directly exercises the BlockRebalanceOnPoll <-> LeaveGroup deadlock: Close
// drives LeaveGroup against a rebalance the in-flight poll froze.
func TestIntegration_Consumer_CloseOnlyMidPoll(t *testing.T) {
	cluster := kfakeCluster(t)

	// Trickle records through the WHOLE test, including across the Close call, so
	// the in-flight poll at Close time is reliably NON-EMPTY (rebalance frozen) —
	// that is precisely the state LeaveGroup must not deadlock on.
	stop, wg := make(chan struct{}), &sync.WaitGroup{}

	keepProducing(t, cluster, stop, wg)
	defer func() { close(stop); wg.Wait() }()

	handler := &countingHandler{}

	c, err := Build(context.Background(), kfakeConsumerConfig(cluster), handler,
		WithLogger(log.NewNop()))
	if err != nil {
		t.Fatalf("Build err = %v", err)
	}

	// Background ctx: ONLY Close drives shutdown here (no ctx-cancel).
	done := runConsumer(context.Background(), c)

	// Let the consumer poll a record so a rebalance is frozen, then Close
	// mid-loop (a poll is in flight / a frozen batch is held).
	waitForHandle(t, handler)

	// Close-only: must unblock the mid-poll Run AND return within budget.
	closeWithinBudget(t, c)
	assertRunReturnedNil(t, done)
}

// waitForHandle blocks until the handler has processed at least one record or a
// short deadline expires, guaranteeing the consumer has polled a real batch (so
// BlockRebalanceOnPoll froze a rebalance) before the test triggers shutdown.
// Polling the handler counter avoids a fixed sleep — deterministic, not flaky.
func waitForHandle(t *testing.T, h *countingHandler) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if h.count() > 0 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("consumer did not handle the seeded record within deadline "+
		"(handled=%d) — group join/poll did not complete", h.count())
}
