//go:build integration

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	libMongo "github.com/LerianStudio/lib-commons/v5/commons/mongo"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	outboxmongo "github.com/LerianStudio/lib-commons/v5/commons/outbox/mongo"
)

// mongoImage pins the MongoDB container image used by the Mongo-backed outbox
// integration tests.
const mongoImage = "mongo:7"

func newMongoOutboxRepo(t *testing.T) (*libMongo.Client, *outboxmongo.Repository, string) {
	t.Helper()

	ctx := context.Background()
	container, err := tcmongo.Run(ctx, mongoImage, tcmongo.WithReplicaSet("rs0"))
	if skipIfNoDocker(t, err) {
		return nil, nil, ""
	}

	require.NoError(t, err, "mongo container start")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("mongo terminate: %v", err)
		}
	})

	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err, "mongo connection string")
	uri = mongoReplicaURI(uri)
	waitForMongoPrimary(t, uri)

	client, err := libMongo.NewClient(ctx, libMongo.Config{
		URI:      uri,
		Database: "streaming_it",
		Logger:   log.NewNop(),
	})
	require.NoError(t, err, "libMongo.NewClient")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Close(cleanupCtx); err != nil {
			t.Logf("mongo client close: %v", err)
		}
	})

	collectionName := "outbox_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	repo, err := outboxmongo.NewRepository(
		client,
		outboxmongo.WithLogger(log.NewNop()),
		outboxmongo.WithCollectionName(collectionName),
	)
	require.NoError(t, err, "outboxmongo.NewRepository")

	return client, repo, collectionName
}

func waitForMongoPrimary(t *testing.T, uri string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := mongodriver.Connect(ctx, mongooptions.Client().ApplyURI(uri))
		if err == nil {
			var hello struct {
				IsWritablePrimary bool `bson:"isWritablePrimary"`
				IsMaster          bool `bson:"ismaster"`
			}
			err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
			if err == nil && !hello.IsWritablePrimary && !hello.IsMaster {
				err = errors.New("mongo primary is not writable yet")
			}
		}
		if client != nil {
			disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if disconnectErr := client.Disconnect(disconnectCtx); disconnectErr != nil {
				t.Logf("mongo readiness disconnect: %v", disconnectErr)
			}
			disconnectCancel()
		}
		cancel()

		if err == nil {
			return
		}

		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	require.NoError(t, lastErr, "MongoDB replica set never became primary-ready")
}

func mongoReplicaURI(uri string) string {
	if strings.Contains(uri, "directConnection=") {
		return uri
	}

	if strings.Contains(uri, "?") {
		return uri + "&directConnection=true"
	}

	return uri + "?directConnection=true"
}

func mongoIntegrationConfig(brokers []string) Config {
	return Config{
		Enabled:               true,
		Brokers:               brokers,
		BatchLingerMs:         0,
		BatchMaxBytes:         defaultBatchMaxBytes,
		MaxBufferedRecords:    defaultMaxBufferedRecords,
		Compression:           defaultCompression,
		RecordRetries:         defaultRecordRetries,
		RecordDeliveryTimeout: defaultRecordDeliveryTimeout,
		RequiredAcks:          defaultRequiredAcks,
		CBFailureRatio:        defaultCBFailureRatio,
		CBMinRequests:         defaultCBMinRequests,
		CBTimeout:             defaultCBTimeout,
		CloseTimeout:          5 * time.Second,
		CloudEventsSource:     integrationSource,
	}
}

func assertMongoOutboxEnvelope(t *testing.T, doc bson.M, tenantID, topic, definitionKey string) {
	t.Helper()

	require.Equal(t, StreamingOutboxEventType, doc["event_type"])
	require.Equal(t, tenantID, doc["tenant_id"])

	payload, ok := doc["payload"].(string)
	require.Truef(t, ok, "mongo outbox payload type = %T; want string", doc["payload"])

	var envelope OutboxEnvelope
	require.NoError(t, json.Unmarshal([]byte(payload), &envelope), "decode persisted outbox envelope")
	require.Equal(t, topic, envelope.Topic)
	require.Equal(t, definitionKey, envelope.DefinitionKey)
	require.Equal(t, tenantID, envelope.Event.TenantID)
	require.Equal(t, "payment", envelope.Event.ResourceType)
	require.Equal(t, "authorized", envelope.Event.EventType)
}

// TestIntegration_MongoOutboxReplay exercises the Mongo-backed outbox path
// end-to-end: force the producer down the outbox branch, persist a row in
// MongoDB, then drain it back to Redpanda through the real Dispatcher and the
// registered streaming outbox relay handler.
func TestIntegration_MongoOutboxReplay(t *testing.T) {
	seed, c := startRedpanda(t)
	if c == nil {
		return
	}

	mongoClient, repo, collectionName := newMongoOutboxRepo(t)
	if repo == nil {
		return
	}

	brokers := []string{seed}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := topicPrefix + "payment.authorized"
	ensureTopics(t, brokers[0], 1, topic, dlqTopic(topic))
	consumer := newConsumerClient(t, brokers, topic)

	p, err := NewProducer(ctx, mongoIntegrationConfig(brokers),
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(repo),
	)
	require.NoError(t, err, "NewProducer")
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Logf("producer close: %v", err)
		}
	})

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, p.RegisterOutboxRelay(registry), "RegisterOutboxRelay")

	dispatcher, err := outbox.NewDispatcher(
		repo,
		registry,
		nil,
		noop.NewTracerProvider().Tracer("test"),
		outbox.WithBatchSize(10),
		outbox.WithPublishMaxAttempts(1),
	)
	require.NoError(t, err, "outbox.NewDispatcher")

	const tenantID = "tenant-outbox-mongo-it"
	tenantCtx := outbox.ContextWithTenantID(ctx, tenantID)
	p.cbStateFlag.Store(flagCBOpen)

	payload := json.RawMessage(`{"post":"mongo-replay"}`)
	require.NoError(t, p.Emit(tenantCtx, eventToRequest(Event{
		TenantID:     tenantID,
		ResourceType: "payment",
		EventType:    "authorized",
		Source:       integrationSource,
		Payload:      payload,
	})), "Emit to mongo outbox")

	database, err := mongoClient.Database(ctx)
	require.NoError(t, err, "mongoClient.Database")
	collection := database.Collection(collectionName)

	filter := bson.M{"event_type": StreamingOutboxEventType, "tenant_id": tenantID}
	count, err := collection.CountDocuments(ctx, filter)
	require.NoError(t, err, "CountDocuments")
	require.Equal(t, int64(1), count, "expected one mongo outbox row")

	var beforeDispatch bson.M
	require.NoError(t, collection.FindOne(ctx, filter).Decode(&beforeDispatch), "FindOne before dispatch")
	require.Equal(t, outbox.OutboxStatusPending, beforeDispatch["status"])
	assertMongoOutboxEnvelope(t, beforeDispatch, tenantID, topic, "payment.authorized")

	result := dispatcher.DispatchOnceResult(tenantCtx)
	require.Equal(t, 1, result.Processed)
	require.Equal(t, 1, result.Published)
	require.Equal(t, 0, result.Failed)
	require.Equal(t, 0, result.StateUpdateFailed)

	records := pollRecords(t, consumer, 1, 20*time.Second)
	require.Len(t, records, 1, "expected replayed broker record")
	hdrs := headerMap(records[0].Headers)
	assert.Equal(t, integrationSource, hdrs["ce-source"])
	assert.Equal(t, tenantID, hdrs["ce-tenantid"])
	assert.Equal(t, "payment", hdrs["ce-resourcetype"])
	assert.Equal(t, "authorized", hdrs["ce-eventtype"])
	assert.Equal(t, []byte(payload), records[0].Value)

	var afterDispatch bson.M
	require.NoError(t, collection.FindOne(ctx, filter).Decode(&afterDispatch), "FindOne after dispatch")
	require.Equal(t, outbox.OutboxStatusPublished, afterDispatch["status"])
	_, hasPublishedAt := afterDispatch["published_at"]
	assert.True(t, hasPublishedAt, "expected published_at to be persisted after replay")
}

// TestIntegration_MongoOutboxTransactionAtomicity proves that MongoDB-backed
// outbox writes join the caller's v1 mongo.SessionContext transaction through
// the normal Write(ctx, ...) path. Commit persists both the business write and
// the outbox row; rollback persists neither.
func TestIntegration_MongoOutboxTransactionAtomicity(t *testing.T) {
	mongoClient, repo, collectionName := newMongoOutboxRepo(t)
	if repo == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const brokerUnused = "127.0.0.1:1"
	topic := topicPrefix + "payment.authorized"
	p, err := NewProducer(ctx, mongoIntegrationConfig([]string{brokerUnused}),
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(repo),
	)
	require.NoError(t, err, "NewProducer")
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Logf("producer close: %v", err)
		}
	})

	p.cbStateFlag.Store(flagCBOpen)
	database, err := mongoClient.Database(ctx)
	require.NoError(t, err, "mongoClient.Database")
	businessCollection := database.Collection("business_atomicity")
	outboxCollection := database.Collection(collectionName)
	driverClient, err := mongoClient.Client(ctx)
	require.NoError(t, err, "mongoClient.Client")

	const tenantID = "tenant-outbox-mongo-tx"
	tenantCtx := outbox.ContextWithTenantID(ctx, tenantID)
	rollbackSentinel := errors.New("rollback sentinel")

	rollbackSession, err := driverClient.StartSession()
	require.NoError(t, err)
	t.Cleanup(func() { rollbackSession.EndSession(context.Background()) })

	_, err = rollbackSession.WithTransaction(tenantCtx, func(sessionCtx mongodriver.SessionContext) (any, error) {
		_, insertErr := businessCollection.InsertOne(sessionCtx, bson.M{"_id": "rollback", "tenant_id": tenantID})
		if insertErr != nil {
			return nil, insertErr
		}

		emitErr := p.Emit(sessionCtx, eventToRequest(Event{
			TenantID:     tenantID,
			ResourceType: "payment",
			EventType:    "authorized",
			Source:       integrationSource,
			Payload:      json.RawMessage(`{"tx":"rollback"}`),
		}))
		if emitErr != nil {
			return nil, emitErr
		}

		return nil, rollbackSentinel
	})
	require.ErrorIs(t, err, rollbackSentinel)

	rollbackBusinessCount, err := businessCollection.CountDocuments(ctx, bson.M{"_id": "rollback"})
	require.NoError(t, err)
	require.Equal(t, int64(0), rollbackBusinessCount)

	rollbackOutboxCount, err := outboxCollection.CountDocuments(ctx, bson.M{"event_type": StreamingOutboxEventType, "tenant_id": tenantID})
	require.NoError(t, err)
	require.Equal(t, int64(0), rollbackOutboxCount)

	commitSession, err := driverClient.StartSession()
	require.NoError(t, err)
	t.Cleanup(func() { commitSession.EndSession(context.Background()) })

	_, err = commitSession.WithTransaction(tenantCtx, func(sessionCtx mongodriver.SessionContext) (any, error) {
		_, insertErr := businessCollection.InsertOne(sessionCtx, bson.M{"_id": "commit", "tenant_id": tenantID})
		if insertErr != nil {
			return nil, insertErr
		}

		emitErr := p.Emit(sessionCtx, eventToRequest(Event{
			TenantID:     tenantID,
			ResourceType: "payment",
			EventType:    "authorized",
			Source:       integrationSource,
			Payload:      json.RawMessage(`{"tx":"commit"}`),
		}))
		if emitErr != nil {
			return nil, emitErr
		}

		return nil, nil
	})
	require.NoError(t, err)

	commitBusinessCount, err := businessCollection.CountDocuments(ctx, bson.M{"_id": "commit"})
	require.NoError(t, err)
	require.Equal(t, int64(1), commitBusinessCount)

	filter := bson.M{"event_type": StreamingOutboxEventType, "tenant_id": tenantID}
	commitOutboxCount, err := outboxCollection.CountDocuments(ctx, filter)
	require.NoError(t, err)
	require.Equal(t, int64(1), commitOutboxCount)

	var committedOutbox bson.M
	require.NoError(t, outboxCollection.FindOne(ctx, filter).Decode(&committedOutbox), "FindOne committed outbox row")
	require.Equal(t, outbox.OutboxStatusPending, committedOutbox["status"])
	assertMongoOutboxEnvelope(t, committedOutbox, tenantID, topic, "payment.authorized")
}
