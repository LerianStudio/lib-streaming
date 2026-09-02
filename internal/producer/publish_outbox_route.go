package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v6/commons"
	"github.com/LerianStudio/lib-commons/v6/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/tracing"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// txContextKey is the context key that holds an ambient *sql.Tx for the
// outbox fallback to join via WriteWithTx.
type txContextKey struct{}

// WithOutboxTx returns a child context that asks publishRouteOutbox to join
// tx when the configured writer supports TransactionalOutboxWriter.
func WithOutboxTx(ctx context.Context, tx *sql.Tx) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, txContextKey{}, tx)
}

// publishRouteOutbox writes an OutboxEnvelope for the given route through
// the configured OutboxWriter. Used by the multi-target Emit path when a
// route's policy elects the outbox fallback (OutboxAlways or
// OutboxFallbackOnCircuitOpen on a circuit-open target).
//
// The envelope carries Target, Transport, Destination, RouteKey, and
// Requirement so the relay can dispatch the row back to the originating
// target adapter without going through the multi-target Emit path (which
// would re-enter the circuit breaker and risk re-enqueue loops).
//
// When an ambient *sql.Tx is present on ctx (WithOutboxTx), and the writer
// implements TransactionalOutboxWriter, the row joins the caller's SQL
// unit of work.
func (p *Producer) publishRouteOutbox(
	ctx context.Context,
	event Event,
	definitionKey string,
	route contract.RouteDefinition,
	policy contract.DeliveryPolicy,
) error {
	if p == nil {
		return ErrNilProducer
	}

	if p.outboxWriter == nil {
		// Caller path is responsible for gating on outbox availability;
		// reaching here without a writer is an invariant violation. Fire
		// the trident, return the documented sentinel.
		a := p.newAsserter("publish_outbox.publish_route_outbox")
		_ = a.NotNil(ctx, p.outboxWriter, "publishRouteOutbox must not be reached with nil outboxWriter",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
		)

		return ErrOutboxNotConfigured
	}

	envelope, err := p.newOutboxEnvelope(ctx, event, definitionKey, route, policy)
	if err != nil {
		return err
	}

	// ValidateShape skips Destination.Validate (which performs DNS lookups
	// and SSRF guards) on the synchronous persist path. The full Validate
	// runs in handleOutboxRow when decoding untrusted persisted bytes,
	// where the cost is acceptable and the input is not yet trusted.
	if err := envelope.ValidateShape(); err != nil {
		return err
	}

	// Ambient SQL transaction path. MongoDB transactions ride on ctx the
	// same way they do for the writer's Write path — the underlying
	// repository's Create picks up the v1 SessionContext when present.
	//
	// Strict type assertion: a non-*sql.Tx value on txContextKey is a
	// caller-side wiring bug. Surface a precise ErrOutboxTxUnsupported
	// instead of silently falling through to a non-tx write that would
	// break atomicity.
	if ctx != nil {
		if raw := ctx.Value(txContextKey{}); raw != nil {
			tx, ok := raw.(*sql.Tx)
			if !ok {
				return fmt.Errorf("%w: txContextKey value is %T, expected *sql.Tx", ErrOutboxTxUnsupported, raw)
			}

			if tx != nil {
				writer, ok := p.outboxWriter.(TransactionalOutboxWriter)
				if !ok {
					return ErrOutboxTxUnsupported
				}

				return writer.WriteWithTx(ctx, tx, envelope)
			}
			// tx == nil falls through to non-tx path below.
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if err := p.outboxWriter.Write(ctx, envelope); err != nil {
		return fmt.Errorf("streaming: outbox write: %w", err)
	}

	return nil
}

func (p *Producer) newOutboxEnvelope(
	ctx context.Context,
	event Event,
	definitionKey string,
	route contract.RouteDefinition,
	policy contract.DeliveryPolicy,
) (contract.OutboxEnvelope, error) {
	aggregateID, err := p.deriveOutboxAggregateID(event)
	if err != nil {
		return contract.OutboxEnvelope{}, err
	}

	return contract.OutboxEnvelope{
		Version:       contract.OutboxEnvelopeVersion,
		RouteKey:      route.Key,
		DefinitionKey: definitionKey,
		Target:        route.Target,
		Transport:     route.Destination.Kind,
		Destination:   route.Destination,
		AggregateID:   aggregateID,
		Requirement:   route.Requirement,
		Policy:        policy,
		TraceCarrier:  captureTraceCarrier(ctx),
		Event:         event,
	}, nil
}

func captureTraceCarrier(ctx context.Context) contract.TraceCarrier {
	if ctx == nil {
		return nil
	}

	injected := tracing.InjectQueueTraceContext(ctx)
	if len(injected) == 0 {
		return nil
	}

	carrier := make(contract.TraceCarrier, contract.MaxTraceCarrierEntries)

	for key, value := range injected {
		switch strings.ToLower(key) {
		case contract.TraceParentHeader:
			carrier[contract.TraceParentHeader] = value
		case contract.TraceStateHeader:
			carrier[contract.TraceStateHeader] = value
		}
	}

	if len(carrier) == 0 {
		return nil
	}

	return carrier
}

// deriveOutboxAggregateID produces a deterministic UUID from the event's
// partition key. Same tenant+aggregate → same AggregateID, which keeps the
// outbox row stream aligned with the broker partition stream and lets
// operators correlate the two by hash.
//
// SystemEvent=true events use a fresh unique UUID because their "partition
// key" ("system:<eventtype>") would otherwise collapse every system event
// into the same aggregate. UUIDv7 is preferred (the AggregateID persists on
// an outbox row, and time-ordered IDs keep B-tree locality); a generator
// error falls back to a random v4 via uuid.NewRandom, and a failure of both
// generators surfaces as an error instead of a panic.
//
// It resolves the partition key exactly the way Emit dispatch does: the
// WithPartitionKey override when one is wired and it yields a non-empty key,
// otherwise Event.PartitionKey(). An empty override key falls back to
// Event.PartitionKey(), which prevents aggregate-ID collapse across tenants.
func (p *Producer) deriveOutboxAggregateID(event Event) (uuid.UUID, error) {
	if event.SystemEvent {
		if id, err := commons.GenerateUUIDv7(); err == nil {
			return id, nil
		}

		id, err := uuid.NewRandom()
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("streaming: mint outbox aggregate id: %w", err)
		}

		return id, nil
	}

	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(p.resolvePartitionKey(event))), nil
}

// outboxRowFromEnvelope serializes an OutboxEnvelope into the lib-commons
// OutboxEvent shape with the stable StreamingOutboxEventType. Used by
// outbox_writer.go's libCommonsOutboxWriter on the production persist path.
func outboxRowFromEnvelope(envelope contract.OutboxEnvelope) (*outbox.OutboxEvent, error) {
	if err := envelope.ValidateShape(); err != nil {
		return nil, err
	}

	// OutboxEnvelope embeds Event, whose wire shape uses Go-default field
	// names for CloudEvents attributes.
	payload, err := json.Marshal(envelope) //nolint:musttag // see comment above
	if err != nil {
		return nil, fmt.Errorf("streaming: marshal outbox envelope: %w", err)
	}

	if len(payload) > maxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}

	rowID, err := commons.GenerateUUIDv7()
	if err != nil {
		rowID, err = uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("streaming: mint outbox row id: %w", err)
		}
	}

	return &outbox.OutboxEvent{
		ID:          rowID,
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}, nil
}
