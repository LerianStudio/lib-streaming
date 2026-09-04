//go:build unit

package consumer

import (
	"context"
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/LerianStudio/lib-observability/v4/log"
	libMetrics "github.com/LerianStudio/lib-observability/v4/metrics"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// newUnmatchedMeterSetup builds a real SDK ManualReader-backed MetricsFactory
// plus a snapshot helper. A real meter (not a spy) keeps the assertion honest:
// the label set under test is the one the SDK actually observes, which is the
// thing that reaches a metrics backend and spends its cardinality budget.
func newUnmatchedMeterSetup(t *testing.T) (*libMetrics.MetricsFactory, func() metricdata.ResourceMetrics) {
	t.Helper()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	factory, err := libMetrics.NewMetricsFactory(provider.Meter("streaming-consumer-test"), log.NewNop())
	if err != nil {
		t.Fatalf("NewMetricsFactory err = %v", err)
	}

	snapshot := func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("Collect err = %v", err)
		}

		return rm
	}

	return factory, snapshot
}

// unmatchedEventKeyLabels returns the observed event_key label values on
// streaming_consumer_unmatched_total, mapped to their summed counts.
func unmatchedEventKeyLabels(t *testing.T, rm metricdata.ResourceMetrics) map[string]int64 {
	t.Helper()

	labels := make(map[string]int64)

	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != metricUnmatchedTotal {
				continue
			}

			agg, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s: data type = %T; want metricdata.Sum[int64]", m.Name, m.Data)
			}

			for _, dp := range agg.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if string(kv.Key) == "event_key" {
						labels[kv.Value.Emit()] += dp.Value
					}
				}
			}
		}
	}

	return labels
}

// countLines returns how many recorded log lines equal msg exactly.
func countLines(spy *spyLogger, msg string) int {
	n := 0

	for _, line := range spy.lines() {
		if line == msg {
			n++
		}
	}

	return n
}

// TestRecordUnmatched_CapsTheEventKeyLabelCardinality pins the bound on the
// event_key label of streaming_consumer_unmatched_total.
//
// The label is attacker-influenced: under one-topic-per-app the topic is
// writable by anything the ce-source allowlist admits, and the key is read off
// the record's own ce-resourcetype/ce-eventtype headers. Unbounded, it is a
// metrics-backend cardinality bomb dressed as observability — and the backend,
// not this process, is what falls over, so nothing here would ever report it.
//
// Two properties: the first maxUnmatchedEventKeyLabels distinct keys are
// metered verbatim, and everything past the cap folds into one "other" bucket.
func TestRecordUnmatched_CapsTheEventKeyLabelCardinality(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	factory, snapshot := newUnmatchedMeterSetup(t)
	spy := newSpyLogger()

	r := newTestRuntime(t, newFakeGroupClient(), &fakeHandler{}, &fakeDLQ{},
		WithMetricsRecorder(factory), WithLogger(spy))

	// One key past the cap: the first `cap` keep their own label, the last one
	// must not mint a label of its own.
	for i := range maxUnmatchedEventKeyLabels + 1 {
		r.recordUnmatched(ctx, "resource.evt_"+strconv.Itoa(i))
	}

	labels := unmatchedEventKeyLabels(t, snapshot())

	tests := []struct {
		name  string
		label string
		want  int64
	}{
		{"first key keeps its own label", "resource.evt_0", 1},
		{"last key inside the cap keeps its own label", "resource.evt_" + strconv.Itoa(maxUnmatchedEventKeyLabels-1), 1},
		{"the key past the cap folds into other", unmatchedEventKeyOverflow, 1},
		{"the key past the cap mints no label of its own", "resource.evt_" + strconv.Itoa(maxUnmatchedEventKeyLabels), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := labels[tt.label]; got != tt.want {
				t.Errorf("event_key=%q count = %d; want %d", tt.label, got, tt.want)
			}
		})
	}

	if got, want := len(labels), maxUnmatchedEventKeyLabels+1; got != want {
		t.Errorf("distinct event_key labels = %d; want %d (%d keys + %q)",
			got, want, maxUnmatchedEventKeyLabels, unmatchedEventKeyOverflow)
	}
}

// TestRecordUnmatched_WarnsOnceWhenTheLabelCapIsReached pins the boundary
// warning. Without it the metric quietly stops naming keys and the "other"
// bucket swallows every later drift with nothing anywhere saying why.
//
// It is a one-shot: firing per record would flood the log in exactly the
// high-volume case that made the cap necessary in the first place.
func TestRecordUnmatched_WarnsOnceWhenTheLabelCapIsReached(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	spy := newSpyLogger()

	r := newTestRuntime(t, newFakeGroupClient(), &fakeHandler{}, &fakeDLQ{}, WithLogger(spy))

	for i := range maxUnmatchedEventKeyLabels {
		r.recordUnmatched(ctx, "resource.evt_"+strconv.Itoa(i))
	}

	if got := countLines(spy, unmatchedLabelOverflowMessage); got != 0 {
		t.Fatalf("overflow warnings = %d before the cap was exceeded; want 0", got)
	}

	for i := range 10 {
		r.recordUnmatched(ctx, "resource.overflow_"+strconv.Itoa(i))
	}

	if got := countLines(spy, unmatchedLabelOverflowMessage); got != 1 {
		t.Errorf("overflow warnings = %d; want exactly 1", got)
	}
}

// TestRecordUnmatched_LogsOncePerKeyWhileMeteringPerRecord pins the split
// between the two signals.
//
// The metric counts every skipped record — that is the volume an operator sizes
// the problem by. The log fires once per KEY — the news is "this key has no
// handler", and repeating it per record would drown the log in exactly the
// live-stream case where it matters.
func TestRecordUnmatched_LogsOncePerKeyWhileMeteringPerRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	factory, snapshot := newUnmatchedMeterSetup(t)
	spy := newSpyLogger()

	dispatcher := NewDispatcher().On("loan.settled", func(context.Context, contract.Event, []byte) error { return nil })

	r := newTestRuntime(t, newFakeGroupClient(), dispatcher, &fakeDLQ{},
		WithMetricsRecorder(factory), WithLogger(spy))

	unread := contract.Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}

	for range 2 {
		if err := r.handler.Handle(ctx, unread, nil); err != nil {
			t.Fatalf("Handle() = %v; want nil under the UnmatchedIgnore default", err)
		}
	}

	if got := countLines(spy, unmatchedNoHandlerMessage); got != 1 {
		t.Errorf("no-handler warnings = %d; want 1 (once per key, not once per record)", got)
	}

	if got := unmatchedEventKeyLabels(t, snapshot())["loan.disbursed"]; got != 2 {
		t.Errorf("streaming_consumer_unmatched_total{event_key=loan.disbursed} = %d; want 2 (once per record)", got)
	}
}

// TestRecordUnmatched_NamesKeysPastTheLabelCap closes the blind spot the cap
// created.
//
// The per-key WARN lived INSIDE the below-cap branch, so once 64 distinct keys
// had been seen every new one was metered as "other" and named nowhere at all.
// The real fleet has 143 event keys across the four launch producers; any
// two-app consumer burns 64 in minutes, and a key that first appears on day 30
// — a producer shipping a new event this consumer was supposed to handle — was
// then invisible in both signals.
//
// The two signals are decoupled instead: the metric label stays capped (the
// backend's cardinality budget is the thing being protected), and the log keeps
// naming keys, globally rate-limited so it cannot flood in exactly the
// high-volume case that made the cap necessary.
func TestRecordUnmatched_NamesKeysPastTheLabelCap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	factory, snapshot := newUnmatchedMeterSetup(t)
	spy := newSpyLogger()

	r := newTestRuntime(t, newFakeGroupClient(), &fakeHandler{}, &fakeDLQ{},
		WithMetricsRecorder(factory), WithLogger(spy))

	for i := range maxUnmatchedEventKeyLabels {
		r.recordUnmatched(ctx, "resource.evt_"+strconv.Itoa(i))
	}

	if got := len(spy.fieldValues(unmatchedOverflowKeyMessage, "event_key")); got != 0 {
		t.Fatalf("overflow key lines = %d before the cap was exceeded; want 0", got)
	}

	// Past the cap: the key must still be named somewhere.
	r.recordUnmatched(ctx, "resource.day_thirty_drift")

	named := spy.fieldValues(unmatchedOverflowKeyMessage, "event_key")
	if len(named) != 1 {
		t.Fatalf("overflow key lines = %d; want exactly 1", len(named))
	}

	if named[0] != "resource.day_thirty_drift" {
		t.Errorf("named key = %v; want the key itself, not %q", named[0], unmatchedEventKeyOverflow)
	}

	// The metric label stays capped regardless — the log is what names keys.
	if got := unmatchedEventKeyLabels(t, snapshot())["resource.day_thirty_drift"]; got != 0 {
		t.Errorf("event_key=resource.day_thirty_drift count = %d; want 0 (the label cap is unchanged)", got)
	}
}

// TestRecordUnmatched_RateLimitsTheOverflowKeyLog pins the bound on the new log
// line. Naming every overflow key per record would flood the log in exactly the
// live-stream case the cap exists for; the limiter keeps it to one line per
// window, with no unbounded key set retained to decide "newly seen".
func TestRecordUnmatched_RateLimitsTheOverflowKeyLog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	spy := newSpyLogger()

	r := newTestRuntime(t, newFakeGroupClient(), &fakeHandler{}, &fakeDLQ{}, WithLogger(spy))

	for i := range maxUnmatchedEventKeyLabels {
		r.recordUnmatched(ctx, "resource.evt_"+strconv.Itoa(i))
	}

	for i := range 500 {
		r.recordUnmatched(ctx, "resource.flood_"+strconv.Itoa(i))
	}

	if got := len(spy.fieldValues(unmatchedOverflowKeyMessage, "event_key")); got != 1 {
		t.Errorf("overflow key lines = %d for 500 distinct keys in one window; want 1", got)
	}
}
