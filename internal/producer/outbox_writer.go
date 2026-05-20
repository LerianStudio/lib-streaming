package producer

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/LerianStudio/lib-observability/assert"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

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
// lib-commons/outbox/mongo repository, callers must invoke Emit with the
// context passed to mongo.Session.WithTransaction or mongo.NewSessionContext;
// the regular Write(ctx, ...) path carries that session-bound context into the
// repository and joins the MongoDB transaction there.
type TransactionalOutboxWriter interface {
	OutboxWriter
	WriteWithTx(ctx context.Context, tx *sql.Tx, envelope OutboxEnvelope) error
}

type libCommonsOutboxWriter struct {
	repo   outbox.OutboxRepository
	logger log.Logger
}

// asserterLogger returns the logger bound to this writer instance, falling
// back to log.NewNop when the writer was hand-built without a logger. The
// nop fallback keeps the asserter usable on hand-built fixtures while the
// production path always carries the Producer's logger (wired in NewProducer
// via the WithOutboxRepository adapter).
func (w *libCommonsOutboxWriter) asserterLogger() log.Logger {
	if w != nil && w.logger != nil {
		return w.logger
	}

	return log.NewNop()
}

// Write persists an OutboxEnvelope through the wrapped repository using
// the stable streaming outbox event type.
func (w *libCommonsOutboxWriter) Write(ctx context.Context, envelope OutboxEnvelope) error {
	if w == nil {
		return ErrOutboxNotConfigured
	}

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

// WriteWithTx persists an OutboxEnvelope under an ambient SQL transaction.
func (w *libCommonsOutboxWriter) WriteWithTx(ctx context.Context, tx *sql.Tx, envelope OutboxEnvelope) error {
	if w == nil {
		return ErrOutboxNotConfigured
	}

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

// isNilInterface is the producer-package alias for transport.IsNilInterface.
// Kept as a one-line wrapper to preserve the dozens of internal call sites
// that already use the lower-case name and to keep diffs minimal. The
// canonical implementation lives in internal/transport.
func isNilInterface(v any) bool {
	return transport.IsNilInterface(v)
}

// _ pins contract import as a real dependency: outboxRowFromEnvelope below
// works exclusively against contract.OutboxEnvelope (aliased here as
// OutboxEnvelope via aliases.go).
var _ = contract.OutboxEnvelopeVersion
