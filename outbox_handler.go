package streaming

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
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
// previously-persisted OutboxEvent, reconstructs the original
// streaming.Event, and relays it through p.publishDirect.
//
// Loop prevention (TRD §C7): this intentionally calls publishDirect, NOT
// Emit. Emit's circuit-open branch would itself write to the outbox — so
// if we called Emit here and the broker happened to still be down, the
// row we're trying to drain would re-enqueue a clone of itself, producing
// a monotonically-growing outbox backlog under sustained broker outage.
// publishDirect has no such fallback; it returns the error and the
// Dispatcher honors its normal retry/backoff policy.
//
// The EventType exact-match check is defensive: the HandlerRegistry is
// exact-match, so handleOutboxRow should only ever be invoked for event
// types the caller registered. But if a bootstrap misconfiguration
// registers it for a non-streaming event, surfacing a silent publish of
// garbage through publishDirect would be worse than a no-op with a
// warning log would have been. The equality guard short-circuits that.
func (p *Producer) handleOutboxRow(ctx context.Context, row *outbox.OutboxEvent) error {
	if p == nil {
		return ErrNilProducer
	}

	if row == nil {
		return outbox.ErrOutboxEventRequired
	}

	if row.EventType != StreamingOutboxEventType {
		// Not a streaming row. The Dispatcher shouldn't be calling us —
		// this means someone registered the handler for an unrelated
		// event type. Returning nil (not an error) is the conservative
		// choice: an error would cause the Dispatcher to mark the row
		// FAILED, which is destructive for a row that wasn't ours in
		// the first place.
		//
		// Log at WARN so the operator sees the misconfiguration instead
		// of silently dropping every mis-registered row. A quiet no-op
		// here previously masked registration bugs in production.
		p.logger.Log(ctx, log.LevelWarn, "streaming: outbox row routed to streaming handler but EventType is not the stable relay type",
			log.String("row_id", row.ID.String()),
			log.String("event_type", row.EventType),
			log.String("expected_event_type", StreamingOutboxEventType),
		)

		return nil
	}

	envelope, err := p.decodeOutboxRow(ctx, row)
	if err != nil {
		return err
	}

	// Re-run preFlight on the deserialized Event. An outbox row is just
	// persisted bytes — it may have been corrupted in transit, tampered
	// with at rest, or authored before we tightened header-sanitization
	// rules. Re-validation here guarantees publishDirect never sees a
	// structurally-invalid Event, and surfaces the violation as a FAILED
	// row (not a silent publish of garbage).
	//
	// Same safety property as unmarshal failure: returning an error marks
	// the row FAILED so the Dispatcher stops retrying a hopeless row.
	if err := p.preFlightWithPayload(envelope.Event, true); err != nil {
		return fmt.Errorf("streaming: outbox replay preflight rejected row %s: %w", row.ID, err)
	}

	// publishDirect (NOT Emit) — the whole point of this handler.
	//
	// Use the persisted envelope topic rather than recomputing here. The
	// envelope validator already proves it matches Event.Topic(), so replay
	// cannot be redirected to an arbitrary Kafka topic by tampering with the row.
	policy := envelope.Policy.Normalize()
	if err := policy.Validate(); err != nil {
		return err
	}

	if err := p.publishDirect(ctx, envelope.Event, envelope.Topic, policy); err != nil {
		return fmt.Errorf("streaming: replay outbox row %s: %w", row.ID, err)
	}

	return nil
}

func (p *Producer) decodeOutboxRow(ctx context.Context, row *outbox.OutboxEvent) (OutboxEnvelope, error) {
	var envelope OutboxEnvelope
	// musttag nolint: OutboxEnvelope embeds Event, whose wire shape intentionally
	// uses Go-default field names for CloudEvents fields.
	unmarshalErr := json.Unmarshal(row.Payload, &envelope) //nolint:musttag
	if unmarshalErr != nil {
		return OutboxEnvelope{}, fmt.Errorf("streaming: unmarshal outbox envelope row %s: %w", row.ID, unmarshalErr)
	}

	// Happy path: v0.2.0+ envelope, versioned and validatable.
	if envelope.Version == outboxEnvelopeVersion {
		if err := envelope.Validate(); err != nil {
			return OutboxEnvelope{}, fmt.Errorf("streaming: invalid outbox envelope row %s: %w", row.ID, err)
		}

		return envelope, nil
	}

	// Backward-compat shim: v0.1.0 rows persisted json.Marshal(Event) directly,
	// with no envelope wrapper. The JSON unmarshal above succeeds against the
	// envelope struct but leaves Version=0 because the legacy payload has no
	// "version" field. Attempt a second unmarshal into a bare Event and, if it
	// produces a non-empty event, synthesize a valid envelope around it so the
	// Dispatcher can drain the row. Operators must still drain the outbox to
	// retire this code path — we log at WARN on every hit so the residue is
	// visible.
	if envelope.Version == 0 {
		var legacyEvent Event
		// Event has no `json:` tags — encoding uses Go field names verbatim.
		// The musttag linter flags this; suppress with the same justification
		// as outbox_writer.go's marshal site (single source of truth on the
		// CloudEvents wire shape).
		//nolint:musttag
		if err := json.Unmarshal(row.Payload, &legacyEvent); err == nil &&
			(legacyEvent.TenantID != "" || legacyEvent.ResourceType != "" || legacyEvent.EventType != "") {
			synthesized := OutboxEnvelope{
				Version:       outboxEnvelopeVersion,
				Topic:         legacyEvent.Topic(),
				DefinitionKey: "",
				AggregateID:   p.deriveOutboxAggregateID(legacyEvent),
				Policy:        DefaultDeliveryPolicy(),
				Event:         legacyEvent,
			}

			if err := synthesized.Validate(); err == nil {
				p.logger.Log(ctx, log.LevelWarn, "legacy v0.1.0 outbox row migrated inline; drain outbox to silence",
					log.String("row_id", row.ID.String()),
				)

				return synthesized, nil
			}
		}
	}

	// Neither a v0.2.0 envelope nor a recoverable v0.1.0 row — surface the
	// original validation failure so the Dispatcher marks the row FAILED.
	return OutboxEnvelope{}, fmt.Errorf("streaming: invalid outbox envelope row %s: %w", row.ID, envelope.Validate())
}
