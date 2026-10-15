//go:build integration

package streaming_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LerianStudio/lib-commons/v7/commons"
	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	outboxpg "github.com/LerianStudio/lib-commons/v7/commons/outbox/postgres"
	libPostgres "github.com/LerianStudio/lib-commons/v7/commons/postgres"
	"github.com/LerianStudio/lib-observability/v4/log"
	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// This file pins the topology the README documents for several binaries of
// one service that sign under their own source: each binary keeps its own
// outbox table in the shared database and runs an ordinary, unscoped
// lib-commons dispatcher over it. Against a real Postgres outbox and the real
// dispatcher, each relay publishes only its own facts, retries a row whose
// publish failed, and recovers a row a crash left in PROCESSING, and never
// touches the other binary's rows.

const (
	perBinaryTenant     = "tenant-per-binary"
	perBinaryDefinition = "loan_contract.disbursed"
)

type perBinary struct {
	source     string
	table      string
	topic      string
	ring       *streaming.Keyring
	producer   *streaming.Producer
	dispatcher *outbox.Dispatcher
}

func perBinaryDatabase(t *testing.T, ctx context.Context, tables ...string) (*libPostgres.Client, *sql.DB) {
	t.Helper()

	// The testcontainer is a plaintext localhost instance; lib-commons fails
	// closed on it without the documented bypass.
	t.Setenv(commons.EnvAllowInsecureTLS, "true")

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("streaming_per_binary_it"),
		tcpostgres.WithUsername("streaming"),
		tcpostgres.WithPassword("streaming"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = container.Terminate(cleanupCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	client, err := libPostgres.New(libPostgres.Config{PrimaryDSN: dsn, ReplicaDSN: dsn})
	require.NoError(t, err)
	require.NoError(t, client.Connect(ctx))
	t.Cleanup(func() { _ = client.Close() })

	db, err := client.Primary()
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `CREATE TYPE outbox_event_status AS ENUM ('PENDING','PROCESSING','PUBLISHED','FAILED','INVALID')`)
	require.NoError(t, err)

	// The lib-commons outbox migration, once per binary under its own name.
	for _, table := range tables {
		_, err = db.ExecContext(ctx, `CREATE TABLE `+table+` (
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
)`)
		require.NoError(t, err, "create %s", table)
	}

	return client, db
}

func newPerBinary(t *testing.T, ctx context.Context, client *libPostgres.Client, cluster *kfake.Cluster, source, table string, seed byte) *perBinary {
	t.Helper()

	resolver, err := outboxpg.NewColumnResolver(client,
		outboxpg.WithColumnResolverTableName(table),
		outboxpg.WithColumnResolverTenantColumn("tenant_id"),
	)
	require.NoError(t, err)

	repo, err := outboxpg.NewRepository(client, resolver, resolver,
		outboxpg.WithTableName(table),
		outboxpg.WithTenantColumn("tenant_id"),
	)
	require.NoError(t, err)

	ring, err := streaming.NewKeyring(streaming.SigningKey{
		ID:     "k1",
		Source: source,
		Secret: bytes.Repeat([]byte{seed}, streaming.MinSigningSecretBytes),
	})
	require.NoError(t, err)

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:           perBinaryDefinition,
		ResourceType:  "loan_contract",
		EventType:     "disbursed",
		SchemaVersion: "1.0.0",
	})
	require.NoError(t, err)

	topic := "lerian.streaming." + source

	emitter, err := streaming.NewBuilder().
		Source(source).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:         "primary.all",
			Target:      "primary",
			Destination: streaming.KafkaTopic(topic),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  cluster.ListenAddrs(),
			ClientID: "per-binary-" + source,
		}).
		OutboxRepository(repo).
		SignEnvelopes(ring, "k1").
		Logger(log.NewNop()).
		Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	producer, ok := emitter.(*streaming.Producer)
	require.True(t, ok, "Build returned %T", emitter)

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, producer.RegisterOutboxRelay(registry))

	dispatcher, err := outbox.NewDispatcher(repo, registry, nil, noop.NewTracerProvider().Tracer("test"),
		outbox.WithPublishMaxAttempts(1),
	)
	require.NoError(t, err)

	return &perBinary{source: source, table: table, topic: topic, ring: ring, producer: producer, dispatcher: dispatcher}
}

// enqueue writes one fact to the binary's outbox and returns nothing to the
// broker: the row is the only copy until the relay publishes it.
func (b *perBinary) enqueue(t *testing.T, ctx context.Context, payload string) {
	t.Helper()

	require.NoError(t, b.producer.Emit(outbox.ContextWithTenantID(ctx, perBinaryTenant), streaming.EmitRequest{
		DefinitionKey:  perBinaryDefinition,
		TenantID:       perBinaryTenant,
		Payload:        []byte(payload),
		PolicyOverride: streaming.DeliveryPolicyOverride{Direct: streaming.DirectModeSkip, Outbox: streaming.OutboxModeAlways},
	}))
}

// relay runs one dispatch cycle of the binary's own dispatcher for the tenant,
// as its Run loop does for every tenant it discovers in the binary's table.
func (b *perBinary) relay(ctx context.Context) {
	b.dispatcher.DispatchOnce(outbox.ContextWithTenantID(ctx, perBinaryTenant))
}

func perBinaryStatuses(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()

	rows, err := db.QueryContext(ctx, `SELECT status FROM `+table+` ORDER BY created_at, id`) //nolint:gosec // table is a test constant
	require.NoError(t, err)

	defer rows.Close()

	var statuses []string

	for rows.Next() {
		var status string
		require.NoError(t, rows.Scan(&status))
		statuses = append(statuses, status)
	}

	require.NoError(t, rows.Err())

	return statuses
}

// failNextProduceTo answers the next Produce request for topic with a
// non-retriable broker error, then steps aside: one failed publish, as a
// broker blip gives the relay.
func failNextProduceTo(cluster *kfake.Cluster, topic string) {
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		produce, ok := req.(*kmsg.ProduceRequest)
		if !ok || len(produce.Topics) != 1 || produceTopicName(cluster, produce.Topics[0]) != topic {
			cluster.KeepControl()

			return nil, nil, false //nolint:nilnil // kfake's "not handled" answer
		}

		resp, ok := produce.ResponseKind().(*kmsg.ProduceResponse)
		if !ok {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" answer
		}

		resp.Version = produce.Version

		for _, reqTopic := range produce.Topics {
			respTopic := kmsg.NewProduceResponseTopic()
			respTopic.Topic = reqTopic.Topic
			respTopic.TopicID = reqTopic.TopicID

			for _, reqPartition := range reqTopic.Partitions {
				respPartition := kmsg.NewProduceResponseTopicPartition()
				respPartition.Partition = reqPartition.Partition
				respPartition.ErrorCode = kerr.UnknownServerError.Code
				respTopic.Partitions = append(respTopic.Partitions, respPartition)
			}

			resp.Topics = append(resp.Topics, respTopic)
		}

		return resp, nil, true
	})
}

// produceTopicName names a Produce request topic, which v13+ carries by id.
func produceTopicName(cluster *kfake.Cluster, topic kmsg.ProduceRequestTopic) string {
	if topic.Topic != "" {
		return topic.Topic
	}

	if info := cluster.TopicIDInfo(topic.TopicID); info != nil {
		return info.Topic
	}

	return ""
}

// consumeVerified reads want records from topic and checks each verifies with
// ring, the binary's own source-bound key.
func consumeVerified(t *testing.T, cluster *kfake.Cluster, topic string, ring *streaming.Keyring, want int) []string {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)

	defer client.Close()

	verifier, err := streaming.NewVerifier(ring, 0)
	require.NoError(t, err)

	var payloads []string

	deadline, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for len(payloads) < want {
		fetches := client.PollFetches(deadline)
		require.NoError(t, deadline.Err(), "read %d of %d records from %s", len(payloads), want, topic)

		fetches.EachRecord(func(record *kgo.Record) {
			headers := make(map[string]any, len(record.Headers))
			for _, header := range record.Headers {
				headers[header.Key] = header.Value
			}

			require.NoError(t, verifier.Verify(headers, record.Value), "record on %s", topic)

			payloads = append(payloads, string(record.Value))
		})
	}

	return payloads
}

func TestIntegration_PerBinaryOutboxTable_EachRelayRecoversOnlyItsOwnRows(t *testing.T) {
	ctx := context.Background()
	client, db := perBinaryDatabase(t, ctx, "outbox_events_ledger_writer", "outbox_events_settlement_worker")

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.DefaultNumPartitions(1),
		kfake.SeedTopics(1, "lerian.streaming.ledger-writer", "lerian.streaming.settlement-worker"),
	)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	writer := newPerBinary(t, ctx, client, cluster, "ledger-writer", "outbox_events_ledger_writer", 'w')
	worker := newPerBinary(t, ctx, client, cluster, "settlement-worker", "outbox_events_settlement_worker", 's')

	writer.enqueue(t, ctx, `{"fact": "writer-1"}`)
	worker.enqueue(t, ctx, `{"fact": "worker-1"}`)

	// The writer's first publish meets a broker error: its row goes FAILED.
	// The worker's relay runs the same cycle and publishes only its own row.
	failNextProduceTo(cluster, writer.topic)
	writer.relay(ctx)
	worker.relay(ctx)

	assert.Equal(t, []string{outbox.OutboxStatusFailed}, perBinaryStatuses(t, ctx, db, writer.table))
	assert.Equal(t, []string{outbox.OutboxStatusPublished}, perBinaryStatuses(t, ctx, db, worker.table))

	// Past the retry window the writer's own relay publishes the failed row;
	// the worker's relay finds nothing of the writer's to claim.
	_, err = db.ExecContext(ctx, `UPDATE `+writer.table+` SET updated_at = now() - interval '1 hour'`) //nolint:gosec // table is a test constant
	require.NoError(t, err)
	worker.relay(ctx)
	assert.Equal(t, []string{outbox.OutboxStatusFailed}, perBinaryStatuses(t, ctx, db, writer.table))

	writer.relay(ctx)
	assert.Equal(t, []string{outbox.OutboxStatusPublished}, perBinaryStatuses(t, ctx, db, writer.table))

	// A row a crashed relay left in PROCESSING is reclaimed by its own relay.
	writer.enqueue(t, ctx, `{"fact": "writer-2"}`)
	_, err = db.ExecContext(ctx, `UPDATE `+writer.table+` SET status = 'PROCESSING', updated_at = now() - interval '1 hour' WHERE status = 'PENDING'`) //nolint:gosec // table is a test constant
	require.NoError(t, err)
	worker.relay(ctx)
	writer.relay(ctx)

	assert.Equal(t, []string{outbox.OutboxStatusPublished, outbox.OutboxStatusPublished}, perBinaryStatuses(t, ctx, db, writer.table))
	assert.Equal(t, []string{outbox.OutboxStatusPublished}, perBinaryStatuses(t, ctx, db, worker.table))

	// Every fact reached the wire once, signed under its own source. Payloads
	// are written in the form a JSONB column hands back.
	assert.ElementsMatch(t, []string{`{"fact": "writer-1"}`, `{"fact": "writer-2"}`}, consumeVerified(t, cluster, writer.topic, writer.ring, 2))
	assert.Equal(t, []string{`{"fact": "worker-1"}`}, consumeVerified(t, cluster, worker.topic, worker.ring, 1))
}
