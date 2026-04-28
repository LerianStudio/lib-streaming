package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// writerAsserterLogger backs the asserter used by libCommonsOutboxWriter
// when invariant guards fire. The writer has no *Producer reference (it is
// adapted from a caller-supplied outbox.OutboxRepository), so the asserter
// cannot source a logger via p.newAsserter.
//
// Default log.NewNop keeps hand-built writers silent until Producer bootstrap
// injects its resolved logger. Tests replace this variable in _test.go via
// swapWriterAsserterLogger to verify the trident emits when the nil-repo guard
// trips.
var writerAsserterLogger log.Logger = log.NewNop()

// OutboxWriter is the minimal durable-write boundary lib-streaming needs.
// Services may adapt their own outbox implementations to this interface; the
// built-in WithOutboxRepository adapter covers lib-commons/outbox.
type OutboxWriter interface {
	Write(ctx context.Context, envelope OutboxEnvelope) error
}

// TransactionalOutboxWriter is implemented by writers that can join a caller's
// ambient SQL transaction.
//
// MongoDB transactions do not flow through this interface. With the
// lib-commons/outbox/mongo repository, callers must invoke Emit with a
// go.mongodb.org/mongo-driver/mongo.SessionContext from the v1 driver; the
// regular Write(ctx, ...) path carries that session-bound context into the
// repository and joins the MongoDB transaction there. Driver v2 session
// contexts are a different type and are not joined by the v1 repository path.
type TransactionalOutboxWriter interface {
	OutboxWriter
	WriteWithTx(ctx context.Context, tx *sql.Tx, envelope OutboxEnvelope) error
}

type libCommonsOutboxWriter struct {
	repo   outbox.OutboxRepository
	logger log.Logger
}

func (w *libCommonsOutboxWriter) asserterLogger() log.Logger {
	if w != nil && w.logger != nil {
		return w.logger
	}

	return writerAsserterLogger
}

func (w *libCommonsOutboxWriter) Write(ctx context.Context, envelope OutboxEnvelope) error {
	// Receiver-nil is a DX guard, not an invariant — keep it as a silent
	// early-return so hand-nil callers see the expected sentinel.
	if w == nil {
		return ErrOutboxNotConfigured
	}

	// Invariant violation: libCommonsOutboxWriter has one construction path
	// (WithOutboxRepository), which already rejects nil repos via its own
	// nil-interface guard. Reaching here means a caller hand-built the
	// writer with a nil repo — a configuration bug indistinguishable from
	// "no outbox wired" at the caller's outcome-classification layer.
	// Fire the observability trident; still return ErrOutboxNotConfigured
	// so callers see the documented sentinel.
	a := assert.New(ctx, w.asserterLogger(), asserterComponent, "outbox_writer.write")
	if err := a.NotNil(ctx, w.repo, "libCommonsOutboxWriter repo must be non-nil post-construction"); err != nil {
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
	// Receiver-nil DX guard — see Write for rationale.
	if w == nil {
		return ErrOutboxNotConfigured
	}

	// Same invariant as Write: nil repo here means the writer was hand-
	// built bypassing WithOutboxRepository's nil guard. Fire the trident,
	// return ErrOutboxNotConfigured.
	a := assert.New(ctx, w.asserterLogger(), asserterComponent, "outbox_writer.write_with_tx")
	if err := a.NotNil(ctx, w.repo, "libCommonsOutboxWriter repo must be non-nil for WriteWithTx"); err != nil {
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
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("streaming: marshal outbox envelope: %w", err)
	}

	if len(payload) > maxPayloadBytes {
		return nil, ErrPayloadTooLarge
	}

	// UUIDv7 is time-ordered; outbox rows scanned by ID get natural
	// insertion-order playback. Fall back to v4 if v7 ever fails.
	rowID, err := commons.GenerateUUIDv7()
	if err != nil {
		rowID = uuid.New()
	}

	return &outbox.OutboxEvent{
		ID:          rowID,
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
