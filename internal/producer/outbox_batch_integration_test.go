//go:build integration

package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/LerianStudio/lib-commons/v6/commons"
	"github.com/LerianStudio/lib-commons/v6/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v6/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v6/commons/postgres"
	"github.com/LerianStudio/lib-observability/v2/log"
)

func TestIntegration_Producer_EmitBatchCommitsAndRollsBackAtomically(t *testing.T) {
	t.Setenv(commons.EnvAllowInsecureTLS, "true")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	db, repo := newBatchPostgresOutbox(t, ctx)
	p, err := NewProducer(ctx, Config{
		Enabled:               true,
		Brokers:               []string{"127.0.0.1:1"},
		BatchLingerMs:         0,
		BatchMaxBytes:         defaultBatchMaxBytes,
		MaxBufferedRecords:    defaultMaxBufferedRecords,
		Compression:           defaultCompression,
		RecordRetries:         1,
		RecordDeliveryTimeout: time.Second,
		RequiredAcks:          defaultRequiredAcks,
		CBFailureRatio:        defaultCBFailureRatio,
		CBMinRequests:         defaultCBMinRequests,
		CBTimeout:             defaultCBTimeout,
		CloseTimeout:          50 * time.Millisecond,
		CloudEventsSource:     integrationSource,
	}, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)), WithOutboxRepository(repo))
	require.NoError(t, err, "NewProducer")
	t.Cleanup(func() { require.NoError(t, p.Close(), "Producer.Close") })

	tenantCtx := outbox.ContextWithTenantID(ctx, "tenant-batch-it")
	requests := []EmitRequest{
		{DefinitionKey: "transaction.created", TenantID: "tenant-batch-it", Subject: "tx-1", Payload: json.RawMessage(`{"sequence":1}`)},
		{DefinitionKey: "order.submitted", TenantID: "tenant-batch-it", Subject: "order-1", Payload: json.RawMessage(`{"sequence":2}`)},
	}

	rollbackTx, err := db.BeginTx(tenantCtx, nil)
	require.NoError(t, err, "BeginTx rollback")
	require.NoError(t, p.EmitBatch(WithOutboxTx(tenantCtx, rollbackTx), requests), "EmitBatch rollback")
	assertBatchRowCount(t, tenantCtx, rollbackTx, 2)
	require.NoError(t, rollbackTx.Rollback(), "Rollback")
	assertCommittedBatchRows(t, tenantCtx, db, nil)

	commitTx, err := db.BeginTx(tenantCtx, nil)
	require.NoError(t, err, "BeginTx commit")
	require.NoError(t, p.EmitBatch(WithOutboxTx(tenantCtx, commitTx), requests), "EmitBatch commit")
	require.NoError(t, commitTx.Commit(), "Commit")
	assertCommittedBatchRows(t, tenantCtx, db, []string{"transaction.created", "order.submitted"})
}

func newBatchPostgresOutbox(
	t *testing.T,
	ctx context.Context,
) (*sql.DB, *outboxpg.Repository) {
	t.Helper()

	container, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("streaming_batch_it"),
		tcpostgres.WithUsername("streaming"),
		tcpostgres.WithPassword("streaming"),
		tcpostgres.BasicWaitStrategies(),
	)
	if skipIfNoDocker(t, err) {
		return nil, nil
	}
	require.NoError(t, err, "postgres container start")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, container.Terminate(cleanupCtx), "postgres terminate")
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "postgres connection string")
	client, err := libPostgres.New(libPostgres.Config{PrimaryDSN: dsn, ReplicaDSN: dsn})
	require.NoError(t, err, "libPostgres.New")
	require.NoError(t, client.Connect(ctx), "postgres connect")
	t.Cleanup(func() { require.NoError(t, client.Close(), "postgres close") })

	db, err := client.Primary()
	require.NoError(t, err, "postgres primary")
	_, err = db.ExecContext(ctx, `
CREATE TYPE outbox_event_status AS ENUM ('PENDING','PROCESSING','PUBLISHED','FAILED','INVALID');
CREATE TABLE outbox_events (
    id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status outbox_event_status NOT NULL DEFAULT 'PENDING',
    attempts INT NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ,
    last_error VARCHAR(512),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    tenant_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id)
);`)
	require.NoError(t, err, "create outbox schema")

	resolver, err := outboxpg.NewColumnResolver(
		client,
		outboxpg.WithColumnResolverTableName("outbox_events"),
		outboxpg.WithColumnResolverTenantColumn("tenant_id"),
	)
	require.NoError(t, err, "NewColumnResolver")
	repo, err := outboxpg.NewRepository(
		client,
		resolver,
		resolver,
		outboxpg.WithTableName("outbox_events"),
		outboxpg.WithTenantColumn("tenant_id"),
	)
	require.NoError(t, err, "NewRepository")

	return db, repo
}

type batchRowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func assertBatchRowCount(t *testing.T, ctx context.Context, queryer batchRowQuerier, want int) {
	t.Helper()

	var got int
	require.NoError(t, queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = $1",
		"tenant-batch-it",
	).Scan(&got))
	assert.Equal(t, want, got)
}

func assertCommittedBatchRows(t *testing.T, ctx context.Context, db *sql.DB, wantDefinitions []string) {
	t.Helper()

	rows, err := db.QueryContext(ctx, `
SELECT payload
FROM outbox_events
WHERE tenant_id = $1
ORDER BY id`, "tenant-batch-it")
	require.NoError(t, err, "query committed outbox rows")
	t.Cleanup(func() { require.NoError(t, rows.Close(), "rows close") })

	definitions := make([]string, 0, len(wantDefinitions))
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))

		var envelope OutboxEnvelope
		require.NoError(t, json.Unmarshal(payload, &envelope))
		definitions = append(definitions, envelope.DefinitionKey)
	}
	require.NoError(t, rows.Err())
	if len(wantDefinitions) == 0 {
		assert.Empty(t, definitions)

		return
	}
	assert.Equal(t, wantDefinitions, definitions)
}
