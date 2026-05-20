package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
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
		p.logger.Log(ctx, log.LevelWarn, "streaming: outbox row routed to streaming handler but EventType is not the stable relay type",
			log.String("row_id", row.ID.String()),
			log.String("event_type", row.EventType),
			log.String("expected_event_type", StreamingOutboxEventType),
		)

		return nil
	}

	var envelope contract.OutboxEnvelope
	// OutboxEnvelope embeds Event, whose wire shape intentionally uses
	// Go-default field names for CloudEvents attributes.
	if err := json.Unmarshal(row.Payload, &envelope); err != nil { //nolint:musttag // see comment above
		return fmt.Errorf("streaming: unmarshal outbox envelope row %s: %w", row.ID, err)
	}

	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("streaming: invalid outbox envelope row %s: %w", row.ID, err)
	}

	if err := p.preFlightWithPayload(ctx, envelope.Event, true); err != nil {
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
		p.logger.Log(ctx, log.LevelWarn, "streaming: outbox replay skipped — target not registered",
			log.String("row_id", row.ID.String()),
			log.String("route_key", envelope.RouteKey),
			log.String("target", envelope.Target),
			log.String("transport", string(envelope.Transport)),
		)

		return fmt.Errorf("streaming: replay outbox row %s: target %q is not registered: %w", row.ID, envelope.Target, contract.ErrMissingTarget)
	}

	if rt.kind != envelope.Transport || rt.kind != envelope.Destination.Kind {
		return fmt.Errorf("streaming: outbox replay row %s: target %q transport %q does not match envelope %q/%q",
			row.ID, envelope.Target, rt.kind, envelope.Transport, envelope.Destination.Kind)
	}

	partKey := envelope.Event.PartitionKey()
	if p.partFn != nil {
		partKey = p.partFn(envelope.Event)
	}

	message := transport.TransportMessage{
		Destination: envelope.Destination,
		TenantID:    envelope.Event.TenantID,
		Key:         partKey,
		Payload:     envelope.Event.Payload,
		Headers:     buildTransportHeaders(ctx, envelope.Event),
		Attributes:  envelope.Destination.Attributes,
	}

	// Bypass the per-target breaker on replay — see godoc on this function
	// for the rationale (no re-enqueue loops, original failure already
	// counted).
	if err := rt.adapter.Publish(ctx, transport.CloneMessage(message)); err != nil {
		return fmt.Errorf("streaming: replay outbox row %s: %w", row.ID, err)
	}

	return nil
}
