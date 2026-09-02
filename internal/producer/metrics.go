package producer

import (
	"context"
	"sync"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v3/obs"
)

// Outcome label values. The "outcome" label on streaming metrics is a closed
// enum per TRD §7.1. NEVER introduce a new value without a TRD amendment:
// downstream dashboards and SLO alerts key off this set.
//
//   - outcomeProduced:     direct publish succeeded (CLOSED circuit, broker healthy).
//   - outcomeOutboxed:     circuit OPEN but outbox fallback wrote the event durably.
//   - outcomeCircuitOpen:  circuit OPEN and no outbox → caller got ErrCircuitOpen.
//   - outcomeCallerError:  preflight rejection or caller-class EmitError.
//   - outcomeDLQ:          publish failed with an infra class; payload went to {topic}.dlq.
//   - outcomeFailed:       direct publish failed and no DLQ route was taken.
//   - outcomeOutboxFailed: outbox fallback attempted but the outbox write
//     itself failed — distinct from outcomeCallerError because the root
//     cause is outbox infrastructure, not caller input.
const (
	outcomeProduced     = "produced"
	outcomeOutboxed     = "outboxed"
	outcomeCircuitOpen  = "circuit_open"
	outcomeCallerError  = "caller_error"
	outcomeDLQ          = "dlq"
	outcomeFailed       = "failed"
	outcomeOutboxFailed = "outbox_failed"
)

// Metric names. Kept colocated with the recorder so a TRD rename is a one-file
// edit. Names match TRD §7.1 verbatim — do not reword casually; they are the
// public contract for Grafana dashboards and alert rules.
const (
	metricNameEmitted                   = "streaming_emitted_total"
	metricNameEmitDurationMS            = "streaming_emit_duration_ms"
	metricNameDLQTotal                  = "streaming_dlq_total"
	metricNameDLQFailed                 = "streaming_dlq_publish_failed_total"
	metricNameOutboxRouted              = "streaming_outbox_routed_total"
	metricNameOutboxReplayTargetUnknown = "streaming_outbox_replay_target_unknown_total"
	metricNameCircuitState              = "streaming_circuit_state"
	metricNameCBRecoveryLiveness        = "streaming_cb_recovery_liveness"
	metricTopicUnresolved               = "__unresolved__"
)

const labelTopic = "topic"

// streamingMetrics records the streaming instrument set through the
// caller-supplied obs.MetricsRecorder.
//
// The recorder flattens instrument creation into the record call, so this
// type holds no builders, no per-instrument sync.Once and no label-set cache:
// caching instruments is the recorder implementation's job, and
// lib-observability's *metrics.MetricsFactory already does it.
//
// Nil-safety rules, enforced by the three helpers below:
//   - nil receiver   -> no-op, no panic
//   - nil recorder   -> one WARN, then no-op forever after
//   - record failure -> WARN naming the metric; never propagated to the
//     caller, because a metrics outage must not fail an emit
type streamingMetrics struct {
	// recorder is the caller-supplied metrics sink. MAY be nil.
	recorder obs.MetricsRecorder

	// logger is never nil - newStreamingMetrics substitutes a no-op logger
	// when the caller passes nil, mirroring the broader package convention.
	logger obs.Logger

	// warnOnce guards the single "metrics disabled" WARN emitted when
	// recorder is nil at first-record time. Subsequent calls are silent.
	warnOnce sync.Once
}

// newStreamingMetrics constructs a streamingMetrics whose behaviour is
// determined by recorder:
//
//   - recorder != nil -> real recording.
//   - recorder == nil -> all record* methods are no-ops after a single WARN.
//
// logger is substituted with a no-op when nil so record* methods can call
// logger.Log unconditionally.
func newStreamingMetrics(recorder obs.MetricsRecorder, logger obs.Logger) *streamingMetrics {
	if isNilInterface(logger) {
		logger = log.NewNop()
	}

	if isNilInterface(recorder) {
		recorder = nil
	}

	return &streamingMetrics{recorder: recorder, logger: logger}
}

// warnNilRecorderOnce emits a single WARN when the metrics recorder is nil.
func (m *streamingMetrics) warnNilRecorderOnce(ctx context.Context) {
	m.warnOnce.Do(func() {
		m.logger.Log(ctx, obs.LevelWarn,
			"streaming: metrics recorder is nil; metrics are disabled")
	})
}

// ready reports whether m can record, warning once when it cannot.
func (m *streamingMetrics) ready(ctx context.Context) bool {
	if m == nil {
		return false
	}

	if m.recorder == nil {
		m.warnNilRecorderOnce(ctx)

		return false
	}

	return true
}

// addOne increments the named counter by 1.
func (m *streamingMetrics) addOne(ctx context.Context, name, description string, attrs map[string]string) {
	if !m.ready(ctx) {
		return
	}

	if err := m.recorder.AddCounter(ctx, name, description, "1", attrs, 1); err != nil {
		m.logger.Log(ctx, obs.LevelWarn, "streaming: metrics: record counter", "metric", name, "error", err)
	}
}

// setGauge sets the named gauge to value.
func (m *streamingMetrics) setGauge(ctx context.Context, name, description string, value int64) {
	if !m.ready(ctx) {
		return
	}

	if err := m.recorder.SetGauge(ctx, name, description, "1", nil, value); err != nil {
		m.logger.Log(ctx, obs.LevelWarn, "streaming: metrics: record gauge", "metric", name, "error", err)
	}
}

// recordMS records a millisecond duration on the named histogram.
func (m *streamingMetrics) recordMS(ctx context.Context, name, description string, attrs map[string]string, ms int64) {
	if !m.ready(ctx) {
		return
	}

	if err := m.recorder.RecordHistogram(ctx, name, description, "ms", attrs, float64(ms), nil); err != nil {
		m.logger.Log(ctx, obs.LevelWarn, "streaming: metrics: record histogram", "metric", name, "error", err)
	}
}
