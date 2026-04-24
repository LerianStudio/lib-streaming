package streaming

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// txContextKey is the context key that holds an ambient *sql.Tx for the
// outbox fallback to join via CreateWithTx.
//
// A package-local unexported type avoids collisions with any other
// package's context values (the standard idiom for context keys).
type txContextKey struct{}

// WithOutboxTx returns a child context that asks publishToOutbox to join tx
// when the configured writer supports TransactionalOutboxWriter.
func WithOutboxTx(ctx context.Context, tx *sql.Tx) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, txContextKey{}, tx)
}

// publishToOutbox serializes an already-pre-flighted Event to a versioned
// OutboxEnvelope and writes it via the configured OutboxWriter.
//
// When an ambient *sql.Tx is present on ctx under txContextKey, CreateWithTx
// is used so the write joins the caller's unit of work. Otherwise Create is
// called and the outbox repo opens its own transaction.
//
// Contract:
//   - On success, the caller path (Emit → cb.Execute closure) MUST return
//     (nil, nil) so the circuit breaker remains untouched — the outbox
//     absorbs the hit and the already-open breaker gets no additional
//     failure attributed to it.
//   - On OutboxRepository failure, the original error is returned so the
//     caller sees the failure. Silent drops are never acceptable — the
//     event becomes unrecoverable.
//   - Status/Attempts are NOT pre-set on the OutboxEvent. The outbox repo
//     normalizes them to PENDING/0 on insert (see
//     github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres/repository.go:normalizedCreateValues). Pre-
//     setting would either get overwritten or conflict with that logic.
//
// Nil-receiver safe. Returns ErrNilProducer / ErrOutboxNotConfigured so
// callers get deterministic sentinels instead of panics.
//
// topic is passed through from Emit so this function does not recompute
// event.Topic() — matches the threading that publishDirect and
// setEmitSpanAttributes already use.
func (p *Producer) publishToOutbox(ctx context.Context, event Event, topic string, policy DeliveryPolicy, definitionKey string) error {
	if p == nil {
		return ErrNilProducer
	}

	if p.outboxWriter == nil {
		// Invariant violation: every call site of publishToOutbox has
		// already gated on outbox availability (emit.go's circuit-open
		// branch checks p.outboxWriter != nil; the outbox-always branch
		// only runs when ResolveDeliveryPolicy permits direct=skip +
		// outbox=always). Reaching here with nil outboxWriter means a
		// caller-side invariant broke. Fire the trident so the contract
		// break is visible; still return ErrOutboxNotConfigured so upstream
		// outcome classification is unchanged.
		a := p.newAsserter("publish_outbox.publish_to_outbox")
		_ = a.NotNil(ctx, p.outboxWriter, "publishToOutbox must not be reached with nil outboxWriter",
			"producer_id", p.producerID,
			"topic", topic,
		)
		return ErrOutboxNotConfigured
	}

	// policy is already normalized — emit.go and outbox_handler.go both pass
	// policies returned from ResolveDeliveryPolicy / envelope.Policy.Normalize.
	// Re-normalizing here was a redundant hot-path allocation.
	envelope := OutboxEnvelope{
		Version:       outboxEnvelopeVersion,
		Topic:         topic,
		DefinitionKey: definitionKey,
		AggregateID:   p.deriveOutboxAggregateID(event),
		Policy:        policy,
		Event:         event,
	}

	// Ambient transaction path: CreateWithTx when the caller installed a
	// *sql.Tx on the package-local context key; otherwise fall through to Create.
	if ctx != nil {
		if tx, ok := ctx.Value(txContextKey{}).(*sql.Tx); ok && tx != nil {
			writer, ok := p.outboxWriter.(TransactionalOutboxWriter)
			if !ok {
				return ErrOutboxTxUnsupported
			}

			return writer.WriteWithTx(ctx, tx, envelope)
		}
	}

	if err := p.outboxWriter.Write(ctx, envelope); err != nil {
		return fmt.Errorf("streaming: outbox write: %w", err)
	}

	return nil
}

// deriveAggregateID produces a deterministic UUID from the event's
// partition key. Same tenant+aggregate → same AggregateID, which keeps the
// outbox row stream aligned with the Kafka partition stream and lets
// operators correlate the two by hash.
//
// For non-system events this hashes only the PartitionKey (= TenantID),
// giving tenant-level correlation. AggregateID is NOT per-event identity
// — use event.Subject for that.
//
// SystemEvent=true events use a random UUID because their "partition key"
// ("system:<eventtype>") would otherwise collapse every system event into
// the same aggregate — not what we want for audit/correlation.
func deriveAggregateID(event Event) uuid.UUID {
	if event.SystemEvent {
		return uuid.New()
	}

	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(event.PartitionKey()))
}

// deriveOutboxAggregateID produces the AggregateID using the same
// partition-key resolution as publishDirect: when a custom partition
// function is configured via WithPartitionKey, it takes precedence over
// the event's default PartitionKey(). This keeps outbox ordering aligned
// with the Kafka partition stream. Delegates to deriveAggregateID for the
// common case where no custom partition function is set.
func (p *Producer) deriveOutboxAggregateID(event Event) uuid.UUID {
	if p.partFn == nil {
		return deriveAggregateID(event)
	}

	if event.SystemEvent {
		return uuid.New()
	}

	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte(p.partFn(event)))
}
