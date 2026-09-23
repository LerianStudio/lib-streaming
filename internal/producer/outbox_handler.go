package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/LerianStudio/lib-streaming/v4/obs"

	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/tracing"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// RegisterOutboxRelay registers the stable streaming outbox relay handler.
func (p *Producer) RegisterOutboxRelay(registry *outbox.HandlerRegistry) error {
	if p == nil {
		return ErrNilProducer
	}

	if registry == nil {
		return ErrNilOutboxRegistry
	}

	if err := registry.Register(StreamingOutboxEventType, p.handleOutboxRow); err != nil {
		return fmt.Errorf("streaming: register outbox relay for %q: %w", StreamingOutboxEventType, err)
	}

	return nil
}

// handleOutboxRow is the outbox-Dispatcher-facing handler. It receives a
// previously-persisted OutboxEvent, decodes the OutboxEnvelope (which
// carries route metadata: target name, transport kind, destination), and
// dispatches the event directly to the originating target's adapter.
//
// Loop prevention (TRD §C7): this intentionally bypasses Emit and the
// per-target circuit breaker. Emit's circuit-open branch would itself
// write to the outbox — so if we called Emit here and the broker happened
// to still be down, the row we're trying to drain would re-enqueue a
// clone of itself, producing a monotonically-growing outbox backlog
// under sustained broker outage. Direct adapter dispatch has no such
// fallback; it returns the error and the Dispatcher honors its normal
// retry/backoff policy.
//
// The EventType exact-match check is defensive: the HandlerRegistry is
// exact-match, so handleOutboxRow should only ever be invoked for event
// types the caller registered. But if a bootstrap misconfiguration
// registers it for a non-streaming event, surfacing a silent publish of
// garbage would be worse than a no-op with a warning log would have been.
//
// When the original target is no longer registered (config change between
// failure and replay), the handler returns an error. The row must not be
// marked processed because no broker delivery happened.
func (p *Producer) handleOutboxRow(ctx context.Context, row *outbox.OutboxEvent) error {
	if p == nil {
		return ErrNilProducer
	}

	if row == nil {
		// Invariant violation: the lib-commons outbox Dispatcher contract
		// guarantees row is non-nil when invoking a registered handler.
		// Reaching here means the Dispatcher itself broke its contract —
		// distinct from 'handler rejected a valid row'. Fire the trident
		// so ops dashboards can tell the two apart, then preserve the
		// public sentinel so Dispatcher retry/fail semantics are unchanged.
		a := p.newAsserter("outbox_handler.handle_outbox_row")
		_ = a.NotNil(ctx, row, "outbox Dispatcher must not invoke handler with nil row")

		return outbox.ErrOutboxEventRequired
	}

	if row.EventType != StreamingOutboxEventType {
		// Not a streaming row. The Dispatcher shouldn't be calling us —
		// this means someone registered the handler for an unrelated
		// event type. Returning nil (not an error) is the conservative
		// choice: an error would cause the Dispatcher to mark the row
		// FAILED, which is destructive for a row that wasn't ours in
		// the first place.
		p.logger.Log(ctx, obs.LevelWarn, "streaming: outbox row routed to streaming handler but EventType is not the stable relay type",
			"row_id", row.ID.String(),
			"event_type", row.EventType,
			"expected_event_type", StreamingOutboxEventType,
		)

		return nil
	}

	var envelope contract.OutboxEnvelope
	// OutboxEnvelope carries Event, whose wire shape intentionally uses
	// Go-default field names for CloudEvents attributes and whose own
	// UnmarshalJSON restores an opaque payload from its base64 field.
	//
	// A row that does not decode into an envelope can never be published, so
	// the failure wraps ErrInvalidOutboxEnvelope (a caller-error sentinel) and
	// a wired classifier sends it to INVALID instead of retrying it.
	if err := json.Unmarshal(row.Payload, &envelope); err != nil {
		return fmt.Errorf("streaming: unmarshal outbox envelope row %s: %w: %w", row.ID, contract.ErrInvalidOutboxEnvelope, err)
	}

	if err := envelope.Validate(); err != nil {
		// An unsupported version is the one envelope failure whose row is
		// bound for INVALID with nothing anywhere recording WHY: lib-commons
		// performs the FAILED -> INVALID flip inside a SQL CASE that emits no
		// log and no reason-bearing metric, and the contract-package trident
		// that fires on this branch writes to a no-op logger because the
		// contract package has no access to the producer's. Surface it here,
		// where the producer's real logger and recorder are in hand.
		//
		// The returned error deliberately still wraps ErrInvalidOutboxEnvelope
		// (a caller-error sentinel), so a service that wired
		// WithRetryClassifier(streaming.IsCallerError) still gets the
		// immediate, correct INVALID for a row nothing can ever publish.
		if !contract.IsSupportedOutboxEnvelopeVersion(envelope.Version) {
			p.recordOutboxRelayRejection(ctx, row, envelope, relayRejectVersionUnsupported, err)
		}

		return fmt.Errorf("streaming: invalid outbox envelope row %s: %w", row.ID, err)
	}

	ctx = tracing.ExtractQueueTraceContext(ctx, map[string]string(envelope.TraceCarrier))

	// Resolve the destination BEFORE preflight. For a version-2 row this is
	// the persisted destination unchanged; for a version-1 row the persisted
	// Kafka topic is the stale per-event name and gets re-derived under the
	// current one-topic-per-app contract. See OutboxEnvelope.ResolveDestination.
	destination, err := envelope.ResolveDestination()
	if err != nil {
		return p.keepLegacyOutboxRow(ctx, row, envelope, err)
	}

	if err := p.preFlightWithPayload(ctx, envelope.Event, true); err != nil {
		// A version-1 row whose ce-source v4 rejects is failing because v4
		// demands something v2 permitted: v2 folded the source through a
		// lossy sanitizer, v4 refuses it outright. That is the only preflight
		// check that changed between the majors, so it is the only refusal
		// re-cast to keep the row. Every other preflight failure (system-event
		// gate, empty/oversized/non-JSON payload, header safety) was enforced
		// identically by v2, so the row is as unpublishable as a version-2 row
		// would be and stays a caller error bound for INVALID.
		if isLegacySourceRejection(envelope, err) {
			return p.keepLegacyOutboxRow(ctx, row, envelope, err)
		}

		return fmt.Errorf("streaming: outbox replay preflight rejected row %s: %w", row.ID, err)
	}

	rt, ok := p.targets[envelope.Target]
	if !ok || rt == nil || rt.adapter == nil {
		// Target was removed/renamed between failure and replay. Return
		// an error so the dispatcher preserves retry/failure semantics;
		// also increment streaming_outbox_replay_target_unknown_total so
		// ops dashboards can alert. Cardinality is bounded by
		// operator-controlled target names (typically <10 in real
		// deployments).
		p.metrics.recordOutboxReplayTargetUnknown(ctx, envelope.Target)
		p.logger.Log(ctx, obs.LevelWarn, "streaming: outbox replay skipped — target not registered",
			"row_id", row.ID.String(),
			"route_key", envelope.RouteKey,
			"target", envelope.Target,
			"transport", string(envelope.Transport),
		)

		return fmt.Errorf("streaming: replay outbox row %s: target %q is not registered: %w", row.ID, envelope.Target, contract.ErrMissingTarget)
	}

	if rt.kind != envelope.Transport || rt.kind != envelope.Destination.Kind {
		return fmt.Errorf("streaming: outbox replay row %s: target %q transport %q does not match envelope %q/%q",
			row.ID, envelope.Target, rt.kind, envelope.Transport, envelope.Destination.Kind)
	}

	partKey := p.resolvePartitionKey(envelope.Event)

	message := transport.TransportMessage{
		Destination: destination,
		TenantID:    envelope.Event.TenantID,
		Key:         partKey,
		Payload:     envelope.Event.Payload,
		Headers:     buildTransportHeaders(ctx, envelope.Event),
		Attributes:  destination.Attributes,
	}

	// Bypass the per-target breaker on replay — see godoc on this function
	// for the rationale (no re-enqueue loops, original failure already
	// counted).
	if err := rt.adapter.Publish(ctx, transport.CloneMessage(message)); err != nil {
		return fmt.Errorf("streaming: replay outbox row %s: %w", row.ID, err)
	}

	return nil
}

// isLegacySourceRejection reports whether a preflight refusal of envelope is a
// version-1 row failing ValidateSource — the same set ResolveDestination
// treats as unroutable for a version-1 Kafka row.
func isLegacySourceRejection(envelope contract.OutboxEnvelope, err error) bool {
	return envelope.Version == contract.OutboxEnvelopeVersionLegacy &&
		(errors.Is(err, contract.ErrMissingSource) || errors.Is(err, contract.ErrInvalidSource))
}

// Closed set of reasons for streaming_outbox_relay_rejected_total. Keep these
// in sync with the metric's godoc in metrics_recorders.go.
const (
	// relayRejectVersionUnsupported: envelope version is neither 2 (written)
	// nor 1 (read-only legacy). Undispatchable; the row is bound for INVALID.
	relayRejectVersionUnsupported = "version_unsupported"

	// relayRejectLegacyUnroutable: a version-1 row that cannot be re-derived
	// under the current topology. Kept retryable for operator action.
	relayRejectLegacyUnroutable = "legacy_unroutable"
)

// relayTargetUnknownLabel is the placeholder used in place of a target name
// that is not a registered target of this producer.
const relayTargetUnknownLabel = "unknown"

// boundedTargetLabel keeps the metric's target dimension bounded by the
// operator-controlled set of REGISTERED target names, substituting "unknown"
// for anything else.
//
// The version_unsupported path needs this and the ordering is the reason:
// ValidateShape checks the envelope version FIRST and returns immediately, so
// a row rejected for its version has had no other field validated. Target is
// then whatever bytes the row happened to hold — a corrupt row, a row from a
// future major with a different shape, an operator-edited row — and passing it
// straight to a counter label makes cardinality attacker- or accident-driven
// on a metric whose godoc promises it is bounded. The row id and the raw
// target still reach the paired ERROR log, which is not a label space.
func (p *Producer) boundedTargetLabel(target string) string {
	if target == "" {
		return relayTargetUnknownLabel
	}

	if rt, ok := p.targets[target]; ok && rt != nil {
		return target
	}

	return relayTargetUnknownLabel
}

// recordOutboxRelayRejection emits the ERROR log and the reason-labelled
// counter for a refused relay row. It is the ONLY observability an operator
// gets for a row heading to INVALID: lib-commons records no reason, and its
// own non-retryable ERROR line never fires unless the service wired a retry
// classifier. It runs regardless of whether a WithOnInvalid callback exists.
func (p *Producer) recordOutboxRelayRejection(
	ctx context.Context,
	row *outbox.OutboxEvent,
	envelope contract.OutboxEnvelope,
	reason string,
	cause error,
) {
	p.metrics.recordOutboxRelayRejected(ctx, p.boundedTargetLabel(envelope.Target), reason)

	p.logger.Log(ctx, obs.LevelError, "streaming: outbox relay refused a row",
		"row_id", row.ID.String(),
		"reason", reason,
		"envelope_version", envelope.Version,
		"route_key", envelope.RouteKey,
		"target", envelope.Target,
		"source", envelope.Event.Source,
		"resource_type", envelope.Event.ResourceType,
		"event_type", envelope.Event.EventType,
		"error", sanitizeBrokerURL(cause.Error()),
	)
}

// keepLegacyOutboxRow handles a version-1 row that decoded cleanly but cannot
// be dispatched under the current topology. It is the ONLY path that converts
// a caller-correctable rejection into a retryable one, and it exists so a
// durable row written by lib-streaming v2 survives an operator-visible window
// instead of being invalidated on its first attempt.
//
// The guarantee is bounded to the two refusals this function sees: destination
// RESOLUTION and a preflight SOURCE rejection (ErrMissingSource /
// ErrInvalidSource, the one preflight check v4 tightened over v2). Every other
// preflight refusal stays a caller error and is invalidated on attempt one, as
// it would be for a version-2 row. A caller-class failure raised deeper — inside a
// transport adapter, e.g. a v1 SQS or EventBridge row between the 256 KiB
// adapter cap and the 1 MiB preflight cap failing with ErrPayloadTooLarge —
// never reaches here, so the dispatcher invalidates it on attempt one with no
// legacy metric and no legacy log. That behaviour is unchanged from v2.1.0 and
// is not introduced by version-1 read support; it is recorded here so the
// godoc does not promise coverage the code does not have.
//
// What the caller gets back wraps ErrLegacyOutboxRowUnroutable, which is
// deliberately absent from callerErrorSentinels. With the documented
// WithRetryClassifier(streaming.IsCallerError) wiring the dispatcher therefore
// takes its RETRYABLE branch: MarkFailed rather than MarkInvalid, so the row
// stays claimable and is picked up again by ResetForRetry.
//
// It is not immortal, and the docs must not pretend otherwise. lib-commons
// promotes FAILED to INVALID once attempts reach MaxDispatchAttempts (default
// 10) — no handler can refuse that, and returning nil to dodge it would mark
// the row PUBLISHED, which is the silent loss this whole change removes. What
// the retryable path buys is the ENTIRE retry budget and backoff window with
// an ERROR log and a metric increment on every single attempt, instead of one
// INVALID transition on attempt one. The row is never deleted at any point:
// INVALID is a terminal STATUS on a retained row, so even an exhausted row is
// still there to be rewritten and replayed.
func (p *Producer) keepLegacyOutboxRow(
	ctx context.Context,
	row *outbox.OutboxEvent,
	envelope contract.OutboxEnvelope,
	cause error,
) error {
	p.recordOutboxRelayRejection(ctx, row, envelope, relayRejectLegacyUnroutable, cause)

	// ResolveDestination already returns the sentinel; preflight returns a bare
	// caller error. Wrap only in the second case so the stored last_error does
	// not repeat the same sentence twice inside its 512-char column.
	//
	// %v on the cause, never %w — see ErrLegacyOutboxRowUnroutable. The
	// preflight path reaches here holding ErrInvalidSource, a caller-error
	// sentinel; wrapping it would restore the immediate-INVALID behaviour.
	if errors.Is(cause, contract.ErrLegacyOutboxRowUnroutable) {
		return fmt.Errorf("streaming: outbox row %s: %w", row.ID, cause)
	}

	//nolint:errorlint // %v is REQUIRED here, not an oversight. cause on this
	// branch is a preflight rejection holding ErrInvalidSource, a caller-error
	// sentinel; %w would make IsCallerError true and the dispatcher would
	// destroy this durable row on its first attempt.
	// TestOutboxRelay_LegacyRowWithInvalidSourceFailingPreflightStaysRetryable fails if this is "fixed".
	return fmt.Errorf("streaming: outbox row %s: %w: %v", row.ID, contract.ErrLegacyOutboxRowUnroutable, cause)
}
