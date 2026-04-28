package streaming

import (
	"context"
	"database/sql"

	"github.com/LerianStudio/lib-streaming/internal/producer"
)

type (
	// OutboxWriter is the durable-write boundary used for outbox fallback.
	OutboxWriter = producer.OutboxWriter
	// TransactionalOutboxWriter can write streaming envelopes inside an ambient SQL transaction.
	TransactionalOutboxWriter = producer.TransactionalOutboxWriter
)

// WithOutboxTx stores an ambient SQL transaction on ctx for transactional outbox writes.
func WithOutboxTx(ctx context.Context, tx *sql.Tx) context.Context {
	return producer.WithOutboxTx(ctx, tx)
}
