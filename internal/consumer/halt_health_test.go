//go:build unit

package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// healthyRuntime returns a runtime already past its first poll, so Healthy is
// governed by halt state alone rather than by the not-ready gate.
func healthyRuntime(t *testing.T) *consumerRuntime {
	t.Helper()

	r := newTestRuntime(t, newFakeGroupClient(), &fakeHandler{}, &fakeDLQ{})
	r.lastPollOK.Store(true)

	return r
}

// TestHealthy_ReportsAPartitionHaltedAcrossConsecutiveCycles closes the gap
// between "wedged" and "green".
//
// Readiness was !closed && lastPollOK, and BOTH stay true through a wedge: a
// poison record whose DLQ publish keeps failing, or a downstream outage holding
// a partition back, polls cleanly forever while processing nothing. The
// consumer reported healthy, the pod stayed in the load balancer, and the only
// evidence was a log line nobody was alerting on.
func TestHealthy_ReportsAPartitionHaltedAcrossConsecutiveCycles(t *testing.T) {
	t.Parallel()

	r := healthyRuntime(t)
	tp := topicPartition{topic: "lerian.streaming.lender", partition: 4}
	seen := map[topicPartition]struct{}{tp: {}}
	halted := map[topicPartition]string{tp: haltReasonDLQPublishFailed}

	// Below the threshold: one bad cycle is a blip, not a wedge. Reporting it
	// would flap readiness on every transient downstream hiccup.
	for cycle := 1; cycle < haltedCyclesUnhealthy; cycle++ {
		r.trackHalts(seen, halted)

		if err := r.Healthy(context.Background()); err != nil {
			t.Fatalf("Healthy() after %d halted cycle(s) = %v; want nil below the %d-cycle threshold",
				cycle, err, haltedCyclesUnhealthy)
		}
	}

	r.trackHalts(seen, halted)

	err := r.Healthy(context.Background())
	if !errors.Is(err, ErrPartitionHalted) {
		t.Fatalf("Healthy() after %d halted cycles = %v; want ErrPartitionHalted", haltedCyclesUnhealthy, err)
	}

	// The error has to say WHICH partition and WHY, or it sends an operator to
	// read logs to learn what the health check already knew.
	for _, want := range []string{"lerian.streaming.lender", "4", haltReasonDLQPublishFailed} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Healthy() = %q; want it to name %q", err, want)
		}
	}
}

// TestHealthy_RecoveryClearsTheHaltStreak pins the other direction: a partition
// that starts making progress again must restore readiness, and must reset the
// count rather than carrying a partial streak into the next incident.
func TestHealthy_RecoveryClearsTheHaltStreak(t *testing.T) {
	t.Parallel()

	r := healthyRuntime(t)
	tp := topicPartition{topic: "t", partition: 0}
	seen := map[topicPartition]struct{}{tp: {}}
	halted := map[topicPartition]string{tp: haltReasonSustainedTransient}

	for range haltedCyclesUnhealthy {
		r.trackHalts(seen, halted)
	}

	if err := r.Healthy(context.Background()); !errors.Is(err, ErrPartitionHalted) {
		t.Fatalf("Healthy() = %v; want ErrPartitionHalted before recovery", err)
	}

	// A clean cycle: the poll delivered the partition's records AND it did not
	// halt on them — real progress, not merely an absence from the halt set.
	r.trackHalts(seen, nil)

	if err := r.Healthy(context.Background()); err != nil {
		t.Fatalf("Healthy() after recovery = %v; want nil", err)
	}

	// And the streak restarted from zero: one bad cycle must not immediately
	// re-trip a counter that was still sitting at the threshold.
	r.trackHalts(seen, halted)

	if err := r.Healthy(context.Background()); err != nil {
		t.Fatalf("Healthy() one cycle after recovery = %v; want nil (the streak reset)", err)
	}
}

// TestHealthy_IntermittentHaltsDoNotTrip pins that the threshold counts
// CONSECUTIVE cycles. A partition that halts, then is DELIVERED AGAIN and
// processed clean, then halts again is making progress; treating that as a
// wedge would fail readiness on ordinary downstream jitter.
func TestHealthy_IntermittentHaltsDoNotTrip(t *testing.T) {
	t.Parallel()

	r := healthyRuntime(t)
	tp := topicPartition{topic: "t", partition: 0}
	seen := map[topicPartition]struct{}{tp: {}}
	halted := map[topicPartition]string{tp: haltReasonSustainedTransient}

	for range haltedCyclesUnhealthy * 3 {
		r.trackHalts(seen, halted)
		r.trackHalts(seen, nil)
	}

	if err := r.Healthy(context.Background()); err != nil {
		t.Fatalf("Healthy() = %v; want nil for alternating halt/recover cycles", err)
	}
}

// TestHealthy_ReportsAWedgeDrivenByARealPollLoop is the wiring test: the halt
// state the poll loop actually produces has to reach Healthy.
//
// It drives the exact production wedge — a terminal record whose DLQ publish
// keeps failing, so the runtime is fail-closed, seeks back, and re-attempts the
// same record every cycle without ever committing.
func TestHealthy_ReportsAWedgeDrivenByARealPollLoop(t *testing.T) {
	t.Parallel()

	poison := func() kgo.Fetches {
		return fetchOf("t", 0, rec("t", 0, 9, ceHeaders("tenantA", false)))
	}

	script := make([]kgo.Fetches, 0, haltedCyclesUnhealthy)
	for range haltedCyclesUnhealthy {
		script = append(script, poison())
	}

	handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
		return errors.New("terminal: loan already settled")
	}}

	client := newFakeGroupClient(script...)
	dlq := &fakeDLQ{failNext: true}

	r := newTestRuntime(t, client, handler, dlq)
	runUntilClosed(t, r)

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 0 {
		t.Fatalf("committed watermark = %d; want 0 — the wedge means nothing commits", wm)
	}

	err := r.Healthy(context.Background())
	if !errors.Is(err, ErrPartitionHalted) {
		t.Fatalf("Healthy() = %v; want ErrPartitionHalted after %d wedged cycles", err, haltedCyclesUnhealthy)
	}

	if !strings.Contains(err.Error(), haltReasonDLQPublishFailed) {
		t.Errorf("Healthy() = %q; want the halt cause named", err)
	}
}

// TestHealthy_AbsenceFromOnePollBatchIsNotProgress pins the difference between
// "the partition made progress" and "the partition was not in this poll's
// batch".
//
// A seek-back discards the partition's buffered records, so franz-go needs a
// fresh fetch round-trip before it re-delivers them, and a busy sibling
// partition can win that race — producing a poll batch the wedged partition is
// simply absent from (HaltBackoff=0 makes it likely, and that is documented as
// legal). Counting absence as recovery made the streak oscillate 1 -> 0 -> 1
// forever, so a permanently wedged partition never reached the threshold and
// readiness stayed green — the exact failure this check exists to catch.
func TestHealthy_AbsenceFromOnePollBatchIsNotProgress(t *testing.T) {
	t.Parallel()

	poison := func() kgo.Fetches {
		return fetchOf("t", 0, rec("t", 0, 9, ceHeaders("tenantA", false)))
	}

	// A batch carrying no partitions at all, dropped in after the first halt:
	// the wedged partition is absent without having made any progress.
	script := []kgo.Fetches{poison(), {}}
	for range haltedCyclesUnhealthy - 1 {
		script = append(script, poison())
	}

	handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
		return errors.New("terminal: loan already settled")
	}}

	r := newTestRuntime(t, newFakeGroupClient(script...), handler, &fakeDLQ{failNext: true})
	runUntilClosed(t, r)

	err := r.Healthy(context.Background())
	if !errors.Is(err, ErrPartitionHalted) {
		t.Fatalf("Healthy() = %v; want ErrPartitionHalted — an absent poll batch is not progress", err)
	}
}
