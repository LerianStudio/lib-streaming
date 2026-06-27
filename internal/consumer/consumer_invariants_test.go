//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
)

// runUntilClosed drives Run to completion. The fake serves the script then a
// synthetic ErrClientClosed fetch, so Run returns cleanly without a timer. A
// safety deadline guards against a regression that hot-spins the loop.
func runUntilClosed(t *testing.T, r *consumerRuntime) {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return — loop likely hot-spinning or blocked")
	}
}

// transientErr is a handler error the test Classifier reclassifies as transient.
var transientErr = errors.New("downstream temporarily unavailable")

// retryClassifier marks transientErr (and only it) as a known downstream blip.
func retryClassifier(err error) bool { return errors.Is(err, transientErr) }

// Invariant (a) — cross-poll masking (Req 1, cross-poll layer). Drives
// processFetches PER POLL (deterministic, no loop timing) on the same partition-0
// batch [A@5, B@7]. Poll 1: A@5 fails as a sustained transient -> seek-back to
// BARE offset 5, partition halted, B@7 (trailing VALID record) SKIPPED, NOTHING
// committed for the partition — the trailing valid record cannot leapfrog the
// uncommitted failure. Poll 2 (A@5 now succeeds): A@5 commits, then B@7 commits.
// The masking trap is committing watermark 8 in poll 1; we assert poll 1 commits
// zero for the partition.
func TestInvariant_A_CrossPollNoMasking(t *testing.T) {
	t.Parallel()

	attempts := map[string]int{}

	handler := &fakeHandler{fn: func(_ context.Context, ev cloudevents.Event, _ []byte) error {
		attempts[ev.Subject]++
		// A@5 fails through poll 1 (sustained); succeeds once re-delivered (poll 2).
		if ev.Subject == "off5" && attempts["off5"] <= 3 {
			return transientErr
		}

		return nil
	}}

	batch := fetchOf("t", 0,
		rec("t", 0, 5, withSubject(ceHeaders("tenantA", false), "off5")),
		rec("t", 0, 7, withSubject(ceHeaders("tenantA", false), "off7")),
	)

	client := newFakeGroupClient()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryClassifier))

	// --- Poll 1 ---
	halted := r.processFetches(context.Background(), batch)

	if _, ok := halted[topicPartition{"t", 0}]; !ok {
		t.Fatal("poll 1: partition 0 must be halted on the sustained transient")
	}

	if len(client.seeks) == 0 || client.seeks[0]["t"][0].Offset != 5 || client.seeks[0]["t"][0].Epoch != 7 {
		t.Fatalf("poll 1: seek-back = %+v; want bare offset 5, epoch 7", client.seeks)
	}

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 0 {
		t.Fatalf("poll 1: committed watermark = %d; want 0 (B@7 must NOT leapfrog the failed A@5)", wm)
	}

	if dlq.count() != 0 {
		t.Fatalf("poll 1: DLQ count = %d; want 0 (a transient NEVER goes to DLQ)", dlq.count())
	}

	// --- Poll 2 (A@5 re-delivered, now succeeds, then B@7) ---
	r.processFetches(context.Background(), batch)

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 8 {
		t.Errorf("poll 2: committed watermark = %d; want 8 (A@5 succeeded first, then B@7)", wm)
	}
}

// withSubject appends a ce-subject header so the test handler can key on it.
func withSubject(h []kgo.RecordHeader, subject string) []kgo.RecordHeader {
	return append(h, kgo.RecordHeader{Key: "ce-subject", Value: []byte(subject)})
}

// Invariant (b) — within-batch stop (Req 1, within-batch layer). In ONE poll,
// partition 0 has offsets 5 (transient fail), 6 (would succeed), 7 (would
// succeed). The loop must break at 5: neither 6 nor 7 may stage a watermark past
// 5, and the partition is seeked back to 5.
func TestInvariant_B_WithinBatchStop(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}
	handler := &fakeHandler{fn: func(_ context.Context, ev cloudevents.Event, _ []byte) error {
		seen[ev.Subject] = true
		if ev.Subject == "off5" {
			return transientErr
		}

		return nil
	}}

	r5 := rec("t", 0, 5, withSubject(ceHeaders("tenantA", false), "off5"))
	r6 := rec("t", 0, 6, withSubject(ceHeaders("tenantA", false), "off6"))
	r7 := rec("t", 0, 7, withSubject(ceHeaders("tenantA", false), "off7"))

	client := newFakeGroupClient()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryClassifier))

	r.processFetches(context.Background(), fetchOf("t", 0, r5, r6, r7))

	// Offset 5 is retried in-loop (RetryBudget+1 times) — that is correct. The
	// within-batch STOP means offsets 6 and 7 are never reached at all.
	if !seen["off5"] {
		t.Error("offset 5 must have been handled")
	}

	if seen["off6"] || seen["off7"] {
		t.Errorf("offsets after the failure were handled (%v); within-batch stop must break at offset 5", seen)
	}

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 0 {
		t.Errorf("committed watermark for t/0 = %d; want 0 (nothing may commit past the failure at offset 5)", wm)
	}

	if len(client.seeks) == 0 || client.seeks[0]["t"][0].Offset != 5 {
		t.Errorf("expected seek-back to offset 5; got %+v", client.seeks)
	}
}

// Invariant (c) — multi-visit halt set (Req 4). EachPartition visits partition 0
// TWICE in one poll. The first visit's record fails transiently (halts the
// partition); the second visit's records must be SKIPPED entirely.
func TestInvariant_C_MultiVisitHalt(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}
	handler := &fakeHandler{fn: func(_ context.Context, ev cloudevents.Event, _ []byte) error {
		seen[ev.Subject] = true
		if ev.Subject == "off5" {
			return transientErr
		}

		return nil
	}}

	first := []*kgo.Record{rec("t", 0, 5, withSubject(ceHeaders("tenantA", false), "off5"))}
	second := []*kgo.Record{
		rec("t", 0, 6, withSubject(ceHeaders("tenantA", false), "off6")),
		rec("t", 0, 7, withSubject(ceHeaders("tenantA", false), "off7")),
	}

	client := newFakeGroupClient()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryClassifier))

	r.processFetches(context.Background(), multiVisitFetch("t", 0, first, second))

	// First visit halts the partition on offset 5; the SECOND visit's records
	// (6 and 7) must be skipped entirely (Req 4 per-cycle halt set).
	if !seen["off5"] {
		t.Error("offset 5 (first visit) must have been handled")
	}

	if seen["off6"] || seen["off7"] {
		t.Errorf("second visit to a halted partition was processed (%v); must be skipped", seen)
	}

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 0 {
		t.Errorf("committed watermark for t/0 = %d; want 0 (halted partition stages nothing)", wm)
	}
}

// Invariant (d) — fail-closed classification (Req 2). Three records:
//   - an UNKNOWN handler error -> DLQ (fail-closed default, no Classifier match)
//   - a codec-decode fault (missing ce-id) -> DLQ (always terminal)
//   - a Classifier-reclassified transient -> retry, NEVER DLQ
func TestInvariant_D_FailClosed(t *testing.T) {
	t.Parallel()

	unknownErr := errors.New("account not found (domain-terminal)")

	handler := &fakeHandler{fn: func(_ context.Context, ev cloudevents.Event, _ []byte) error {
		switch ev.Subject {
		case "unknown":
			return unknownErr
		case "transient":
			return transientErr
		default:
			return nil
		}
	}}

	// Unknown handler error on partition 0; codec poison on partition 1;
	// reclassified transient on partition 2 — separate partitions so one halt
	// doesn't mask another's processing.
	rUnknown := rec("t", 0, 1, withSubject(ceHeaders("tenantA", false), "unknown"))

	badHeaders := ceHeaders("tenantA", false)
	badHeaders = dropHeader(badHeaders, "ce-id") // missing required header -> codec fault
	rCodec := rec("t", 1, 1, badHeaders)

	rTransient := rec("t", 2, 1, withSubject(ceHeaders("tenantA", false), "transient"))

	client := newFakeGroupClient(
		kgo.Fetches{{Topics: []kgo.FetchTopic{
			{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{rUnknown}}}},
			{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 1, Records: []*kgo.Record{rCodec}}}},
			{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 2, Records: []*kgo.Record{rTransient}}}},
		}}},
	)
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryClassifier))
	runUntilClosed(t, r)

	// Exactly two DLQ publishes: the unknown handler error and the codec poison.
	if dlq.count() != 2 {
		t.Errorf("DLQ count = %d; want 2 (unknown handler error + codec poison; transient must NOT DLQ)", dlq.count())
	}

	// The reclassified transient seeked back (partition 2), never DLQ'd.
	foundP2Seek := false
	for _, s := range client.seeks {
		if _, ok := s["t"][2]; ok {
			foundP2Seek = true
		}
	}

	if !foundP2Seek {
		t.Error("expected a seek-back on partition 2 (reclassified transient), found none")
	}
}

// dropHeader returns headers without the named key (to forge a codec fault).
func dropHeader(h []kgo.RecordHeader, key string) []kgo.RecordHeader {
	out := h[:0:0]
	for _, e := range h {
		if e.Key != key {
			out = append(out, e)
		}
	}

	return out
}

// Invariant (e) — transient never DLQ + sustained -> seek-back + halt (Req 1/2).
// A handler error with NO Classifier supplied is fail-closed (DLQ); to test the
// transient path we supply the Classifier and assert: zero DLQ, one seek-back,
// partition halted (no commit).
func TestInvariant_E_TransientNeverDLQ(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{fn: func(_ context.Context, _ cloudevents.Event, _ []byte) error {
		return transientErr
	}}

	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 9, ceHeaders("tenantA", false))))
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq, WithClassifier(retryClassifier))
	runUntilClosed(t, r)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 (sustained transient must seek back, NEVER DLQ)", dlq.count())
	}

	if len(client.seeks) == 0 || client.seeks[0]["t"][0].Offset != 9 {
		t.Errorf("expected seek-back to offset 9; got %+v", client.seeks)
	}

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 0 {
		t.Errorf("committed watermark = %d; want 0 (halted partition commits nothing)", wm)
	}

	// In-loop retries actually happened (budget 2 -> 1 initial + 2 retries = 3).
	if handler.callCount() != 3 {
		t.Errorf("handler called %d times; want 3 (1 initial + RetryBudget=2 in-loop retries)", handler.callCount())
	}
}

// Invariant (f) — empty TenantID is a valid single-tenant scope, NOT a DLQ
// reason (mirrors producer v1.6.2: "remove the WithAllowEmptyTenant opt-in —
// empty tenant is always valid"). A system event and a non-system empty-tenant
// business event are handled IDENTICALLY: both DISPATCH to the handler with an
// empty TenantID and COMMIT. Neither reaches the DLQ. This is the protected
// invariant that PROVES empty-tenant dispatch — the inversion of the old
// empty-tenant -> DLQ rule.
func TestInvariant_F_SystemEventAndEmptyTenant(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{} // succeeds on every dispatch

	// p0: a system event (no ce-tenantid, ce-systemevent=true) -> dispatched, committed.
	rSystem := rec("t", 0, 1, ceHeaders("", true))
	// p1: a NON-system event with empty ce-tenantid -> dispatched (empty tenant), committed.
	rNoTenant := rec("t", 1, 1, ceHeaders("", false))

	client := newFakeGroupClient(
		kgo.Fetches{{Topics: []kgo.FetchTopic{
			{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{rSystem}}}},
			{Topic: "t", Partitions: []kgo.FetchPartition{{Partition: 1, Records: []*kgo.Record{rNoTenant}}}},
		}}},
	)
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq)
	runUntilClosed(t, r)

	// BOTH dispatched (handler called twice), each with an EMPTY tenant — system
	// event and empty-tenant business event are indistinguishable at dispatch.
	if handler.callCount() != 2 {
		t.Errorf("handler called %d times; want 2 (system event AND empty-tenant business event BOTH dispatch)", handler.callCount())
	}

	if len(handler.tenant) != 2 || handler.tenant[0] != "" || handler.tenant[1] != "" {
		t.Errorf("dispatched tenants = %v; want two empty-tenant dispatches", handler.tenant)
	}

	// Neither reaches the DLQ — empty tenant is no longer a quarantine reason.
	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 (empty tenant is a valid single-tenant scope, NEVER DLQ)", dlq.count())
	}

	// Both committed (watermark = offset 1 + 1) — the success path advances.
	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 2 {
		t.Errorf("system-event committed watermark = %d; want 2 (offset 1 + 1)", wm)
	}

	if wm := client.committedWatermarks()[topicPartition{"t", 1}]; wm != 2 {
		t.Errorf("empty-tenant business-event committed watermark = %d; want 2 (offset 1 + 1)", wm)
	}
}

// Invariant (g) — fetch-error drain (Req 6). ErrClientClosed -> clean Run stop;
// *ErrDataLoss -> observed (not silent), loop continues then stops.
func TestInvariant_G_FetchErrorDrain(t *testing.T) {
	t.Parallel()

	t.Run("ErrClientClosed stops cleanly", func(t *testing.T) {
		t.Parallel()

		client := newFakeGroupClient(clientClosedFetch())
		dlq := &fakeDLQ{}
		handler := &fakeHandler{}

		r := newTestRuntime(t, client, handler, dlq)

		done := make(chan error, 1)
		go func() { done <- r.Run(context.Background()) }()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v; want nil clean stop on ErrClientClosed", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not stop on ErrClientClosed")
		}

		// No records dispatched on a pure shutdown poll. AllowRebalance still
		// fires (deferred per cycle, before the stop check — the Wave-3 deadlock
		// fix asserted by TestRegression_AllowRebalancePairsWithStopPathPoll).
		if handler.callCount() != 0 {
			t.Errorf("handler called %d times; want 0", handler.callCount())
		}
	})

	t.Run("ErrDataLoss observed not silent", func(t *testing.T) {
		t.Parallel()

		dataLoss := &kgo.ErrDataLoss{Topic: "t", Partition: 0, ConsumedTo: 5, ResetTo: 9}
		spy := newSpyLogger()

		client := newFakeGroupClient(errorFetch("t", 0, dataLoss))
		dlq := &fakeDLQ{}
		handler := &fakeHandler{}

		r := newTestRuntime(t, client, handler, dlq, WithLogger(spy))
		runUntilClosed(t, r)

		if !spy.contains("DATA LOSS") {
			t.Errorf("data-loss fetch error was not logged; spy lines = %v", spy.lines())
		}
	})
}

// Invariant — happy path success commits the watermark (dispositionCommit wired,
// not dead). Also asserts the tenant id was seeded onto the handler ctx.
func TestInvariant_SuccessCommitsAndSeedsTenant(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}

	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 41, ceHeaders("tenantZ", false))))
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq)
	runUntilClosed(t, r)

	if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 42 {
		t.Errorf("committed watermark = %d; want 42 (offset 41 + 1)", wm)
	}

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 (success path)", dlq.count())
	}

	if len(handler.tenant) != 1 || handler.tenant[0] != "tenantZ" {
		t.Errorf("seeded tenant = %v; want [tenantZ] (from ce-tenantid, never payload)", handler.tenant)
	}

	if client.allows == 0 {
		t.Error("AllowRebalance was never called; Req 3 requires exactly one per cycle")
	}
}

// Regression — shutdown-deadlock fix. PollFetches freezes the rebalance
// (BlockRebalanceOnPoll); AllowRebalance MUST pair with EVERY poll, including the
// drainFetchErrors stop path (the COMMON shutdown shape). Before the fix Run
// returned on the stop path BEFORE AllowRebalance, leaving the rebalance frozen
// so a subsequent Close() -> LeaveGroup hung forever. This drives a pure
// shutdown poll (script exhausted -> synthetic ErrClientClosed) and asserts the
// poll's rebalance freeze was released, then that Close completes (idempotent,
// non-blocking).
func TestRegression_AllowRebalancePairsWithStopPathPoll(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient() // empty script -> first poll is the ErrClientClosed stop
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	runUntilClosed(t, r)

	client.mu.Lock()
	polls, allows := client.pollCalls, client.allows
	client.mu.Unlock()

	if polls != 1 {
		t.Fatalf("pollCalls = %d; want 1 (one shutdown poll)", polls)
	}

	if allows != polls {
		t.Errorf("AllowRebalance called %d times for %d poll(s); the stop-path poll left the rebalance FROZEN (deadlock regression)", allows, polls)
	}

	// Close must complete without blocking on the (now-released) rebalance.
	done := make(chan error, 1)
	go func() { done <- r.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned %v; want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return — rebalance still frozen (deadlock)")
	}
}

// classify-unit coverage: the three sources map to the right disposition.
func TestClassify_BySource(t *testing.T) {
	t.Parallel()

	c := &consumerRuntime{}

	if got := c.classify(nil, sourceHandler); got != dispositionCommit {
		t.Errorf("classify(nil) = %v; want dispositionCommit", got)
	}

	if got := c.classify(cloudevents.ErrMissingRequiredHeader, sourceCodec); got != dispositionDLQ {
		t.Errorf("classify(codec) = %v; want dispositionDLQ", got)
	}

	if got := c.classify(errors.New("unknown"), sourceHandler); got != dispositionDLQ {
		t.Errorf("classify(unknown handler) = %v; want dispositionDLQ (fail-closed)", got)
	}

	c.classifier = retryClassifier
	if got := c.classify(transientErr, sourceHandler); got != dispositionRetry {
		t.Errorf("classify(reclassified handler transient) = %v; want dispositionRetry", got)
	}
}
