package producer

import "context"

// This file holds the eight per-instrument record methods. They are thin
// wrappers over the addOne / setGauge / recordMS helpers in metrics.go, whose
// job is to pin each instrument's name, description and label set in exactly
// one place.
//
// The label sets are built per call. That is deliberate: the previous
// implementation cached a *metrics.CounterBuilder per label tuple to dodge
// the allocation, and paid for it by naming lib-observability builder types
// throughout the package. Instrument caching now lives inside the recorder,
// which is where it belongs; what remains per call is one small map.

// recordEmitted increments streaming_emitted_total by 1 with the given
// topic/outcome label set. The operation label is always "send".
func (m *streamingMetrics) recordEmitted(ctx context.Context, topic, outcome string) {
	m.addOne(ctx, metricNameEmitted,
		"Total streaming emits by topic, operation, and outcome.",
		map[string]string{labelTopic: topic, "operation": "send", "outcome": outcome})
}

// recordEmitDuration adds a sample to streaming_emit_duration_ms. The unit is
// milliseconds to match the TRD name; callers pass
// time.Since(start).Milliseconds().
func (m *streamingMetrics) recordEmitDuration(ctx context.Context, topic, outcome string, durationMs int64) {
	m.recordMS(ctx, metricNameEmitDurationMS,
		"Streaming emit duration in milliseconds by topic and outcome.",
		map[string]string{labelTopic: topic, "outcome": outcome}, durationMs)
}

// recordDLQ increments streaming_dlq_total by 1. Called after a successful DLQ
// publish; the error_class label encodes which of the 8 ErrorClass values
// caused the quarantine.
func (m *streamingMetrics) recordDLQ(ctx context.Context, topic, errorClass string) {
	m.addOne(ctx, metricNameDLQTotal,
		"Total events quarantined to the per-topic DLQ.",
		map[string]string{labelTopic: topic, "error_class": errorClass})
}

// recordDLQFailed increments streaming_dlq_publish_failed_total by 1. Called
// when the DLQ publish itself fails — the alerting signal operators watch,
// because a failing DLQ means correlated broker failure across both the source
// and DLQ topics.
func (m *streamingMetrics) recordDLQFailed(ctx context.Context, topic string) {
	m.addOne(ctx, metricNameDLQFailed,
		"Total DLQ publish attempts that failed themselves.",
		map[string]string{labelTopic: topic})
}

// recordOutboxRouted increments streaming_outbox_routed_total by 1. Called
// when a publish falls back to the outbox. reason is a closed set:
// "circuit_open" (the only T6 caller) or "broker_error" (reserved for v1.1).
func (m *streamingMetrics) recordOutboxRouted(ctx context.Context, topic, reason string) {
	m.addOne(ctx, metricNameOutboxRouted,
		"Total events routed to the outbox fallback by topic and reason.",
		map[string]string{labelTopic: topic, "reason": reason})
}

// recordOutboxReplayTargetUnknown increments
// streaming_outbox_replay_target_unknown_total by 1. Called from
// handleOutboxRow when an outbox replay row references a target name that is
// no longer registered (typically a config rename between the original
// failure and the replay attempt). The row returns an error so the dispatcher
// preserves retry/failure semantics; this counter is the operator's signal
// that replay is blocked on target configuration drift.
//
// Cardinality: target name is operator-controlled and bounded (typically
// single-digit count per service). Same discipline as the cb service-name
// dimension. No tenant_id label (PROJECT_RULES §13).
func (m *streamingMetrics) recordOutboxReplayTargetUnknown(ctx context.Context, target string) {
	m.addOne(ctx, metricNameOutboxReplayTargetUnknown,
		"Total outbox replay rows blocked because their target was not registered.",
		map[string]string{labelTarget: target})
}

// recordOutboxRelayRejected increments streaming_outbox_relay_rejected_total
// by 1. Called from handleOutboxRow whenever a decoded row is refused, with
// reason drawn from the closed set in outbox_handler.go:
//
//   - "version_unsupported": the row's envelope version is neither the written
//     version 2 nor the read-only legacy version 1. Undispatchable. The row is
//     bound for INVALID.
//   - "legacy_unroutable": a version-1 row that cannot be re-derived under the
//     current topology. Kept retryable so an operator can rewrite it.
//
// This metric exists because the INVALID transition itself is UNOBSERVABLE
// from here and very nearly unobservable anywhere. lib-commons flips
// FAILED -> INVALID inside a SQL CASE expression that emits nothing, its
// "outbox event is non-retryable; marking invalid" ERROR line is unreachable
// unless the service wired a retry classifier, and outbox.events.failed counts
// every failure identically with no reason dimension. Emitting at the point of
// REFUSAL — with the reason the storage layer will never record — is the only
// place lib-streaming can make the row's fate visible, and it fires
// independently of any WithOnInvalid callback the service may or may not have
// registered.
//
// Cardinality: reason is a closed two-value set; target is operator-controlled
// and bounded (single digits per service), matching
// recordOutboxReplayTargetUnknown. The offending ce-source and row id are
// deliberately NOT labels — they are unbounded, and they go in the paired
// ERROR log instead. No tenant_id label (PROJECT_RULES §13).
func (m *streamingMetrics) recordOutboxRelayRejected(ctx context.Context, target, reason string) {
	m.addOne(ctx, metricNameOutboxRelayRejected,
		"Total outbox relay rows refused, by reason.",
		map[string]string{labelTarget: target, "reason": reason})
}

// recordCircuitState sets the streaming_circuit_state gauge. state is one of
// flagCBClosed / flagCBHalfOpen / flagCBOpen (0/1/2). The instrument has no
// labels — a single gauge per process is sufficient.
//
// TRD §7.1 labels this "Int64UpDownCounter (gauge-like)"; the recorder only
// exposes a gauge, which is semantically equivalent here (both emit the
// current value, not a delta).
func (m *streamingMetrics) recordCircuitState(ctx context.Context, state int32) {
	m.setGauge(ctx, metricNameCircuitState,
		"Circuit breaker state: 0=closed, 1=half-open, 2=open.", int64(state))
}

// recordCBRecoveryLiveness sets streaming_cb_recovery_liveness to 1 when the
// per-Producer recovery goroutine is alive/fresh and 0 when it died or became
// stale. No labels: this is intentionally one bounded process-local signal.
//
// Unlike every other record method this one stays SILENT when no recorder is
// wired: it is called from the recovery goroutine's tick loop and from
// producer construction, so routing it through the warn-once path would turn
// "metrics are off" into a log line on a hot timer — and, worse, would make
// producer construction depend on the logger not failing.
func (m *streamingMetrics) recordCBRecoveryLiveness(ctx context.Context, alive bool) {
	if m == nil || m.recorder == nil {
		return
	}

	value := int64(0)
	if alive {
		value = 1
	}

	m.setGauge(ctx, metricNameCBRecoveryLiveness,
		"Circuit-breaker recovery goroutine liveness: 1=alive, 0=dead or stale.", value)
}
