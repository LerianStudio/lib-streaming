package streaming

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// OutboxWriter is the minimal durable-write boundary lib-streaming needs.
// Services may adapt their own outbox implementations to this interface; the
// built-in WithOutboxRepository adapter covers lib-commons/outbox.
type OutboxWriter interface {
	Write(ctx context.Context, envelope OutboxEnvelope) error
}

// TransactionalOutboxWriter is implemented by writers that can join a caller's
// ambient database transaction.
type TransactionalOutboxWriter interface {
	OutboxWriter
	WriteWithTx(ctx context.Context, tx *sql.Tx, envelope OutboxEnvelope) error
}

type libCommonsOutboxWriter struct {
	repo outbox.OutboxRepository
}

func (w *libCommonsOutboxWriter) Write(ctx context.Context, envelope OutboxEnvelope) error {
	if w == nil || isNilInterface(w.repo) {
		return ErrOutboxNotConfigured
	}

	row, err := outboxRowFromEnvelope(envelope)
	if err != nil {
		return err
	}

	if _, err := w.repo.Create(ctx, row); err != nil {
		return fmt.Errorf("streaming: outbox create: %w", err)
	}

	return nil
}

func (w *libCommonsOutboxWriter) WriteWithTx(ctx context.Context, tx *sql.Tx, envelope OutboxEnvelope) error {
	if w == nil || isNilInterface(w.repo) {
		return ErrOutboxNotConfigured
	}

	if tx == nil {
		return fmt.Errorf("%w: nil transaction", ErrOutboxTxUnsupported)
	}

	row, err := outboxRowFromEnvelope(envelope)
	if err != nil {
		return err
	}

	if _, err := w.repo.CreateWithTx(ctx, tx, row); err != nil {
		return fmt.Errorf("streaming: outbox create (tx): %w", err)
	}

	return nil
}

func outboxRowFromEnvelope(envelope OutboxEnvelope) (*outbox.OutboxEvent, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	// musttag nolint: OutboxEnvelope embeds Event, whose wire shape intentionally
	// uses Go-default field names for CloudEvents fields.
	payload, err := json.Marshal(envelope) //nolint:musttag
	if err != nil {
		return nil, fmt.Errorf("streaming: marshal outbox envelope: %w", err)
	}

	if len(payload) > maxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}

	return &outbox.OutboxEvent{
		ID:          uuid.New(),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}, nil
}

func isNilInterface(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
