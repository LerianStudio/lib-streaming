//go:build integration

package streaming_test

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/log"
	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// This file drives an OPAQUE payload through the transactional OUTBOX route
// end to end against the real Kafka protocol: persist, relay, publish,
// consume.
//
// The case is the br-slc rail handing an SFN XML document to br-siloc. The hop
// must be transactional with the command that produced it, which means the
// outbox route rather than a direct emit — and that route could not carry the
// document at all. The persisted envelope is JSON (lib-commons stores the
// outbox payload in a JSONB column), Event.Payload was json.RawMessage, and
// the encoder validates a RawMessage on its way out, so the write failed with
// "invalid character '<' looking for beginning of value" before any broker was
// involved. The same document emitted DIRECTLY published fine, which is why
// the field's doc comment ("sent unchanged as the Kafka value") read as true.
//
// Byte identity is the whole assertion. The document is ISO-8859-1, so it is
// not valid UTF-8 either; a JSON string would have carried it home with the
// offending byte replaced by U+FFFD and no error raised anywhere.

const (
	opaqueApp      = "slc"
	opaqueTopic    = "lerian.streaming.slc"
	opaqueDLQ      = "lerian.streaming.slc.dlq"
	opaqueConsumer = "siloc"
	opaqueConsDLQ  = "lerian.streaming.siloc.dlq"

	opaqueDefinitionKey = "documento.enviado"
	opaqueContentType   = "text/xml; charset=ISO-8859-1"
	opaqueTenant        = "tenant-slc"
	opaqueSubject       = "doc-0001"
	opaqueWaitBudget    = 20 * time.Second
)

// opaqueSFNDocument is the payload under test: XML, declared ISO-8859-1, and
// carrying byte 0xE7. Not valid JSON, and not valid UTF-8.
var opaqueSFNDocument = []byte("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><DOC><Nome>A\xe7\xe3o</Nome><Vlr>1200.00</Vlr></DOC>")

// captureRepo is a minimal outbox.OutboxRepository that records the rows the
// producer persists. It is deliberately the REPOSITORY seam and not a hand-
// rolled OutboxWriter: the repository path runs the library's real envelope
// marshal, which is the code that used to fail. A fake writer holding the
// envelope in memory would never marshal anything and would prove nothing.
type captureRepo struct {
	mu   sync.Mutex
	rows []*outbox.OutboxEvent
}

func (r *captureRepo) Create(_ context.Context, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rows = append(r.rows, event)

	return event, nil
}

func (r *captureRepo) CreateWithTx(_ context.Context, _ outbox.Tx, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	return r.Create(context.Background(), event)
}

func (r *captureRepo) CreateManyWithTx(_ context.Context, _ outbox.Tx, events []*outbox.OutboxEvent) ([]*outbox.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rows = append(r.rows, events...)

	return events, nil
}

func (r *captureRepo) only(t *testing.T) *outbox.OutboxEvent {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.rows) != 1 {
		t.Fatalf("outbox holds %d rows; want exactly 1", len(r.rows))
	}

	return r.rows[0]
}

func (*captureRepo) ListPending(context.Context, int) ([]*outbox.OutboxEvent, error) { return nil, nil }
func (*captureRepo) ListPendingByType(context.Context, string, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}
func (*captureRepo) ListTenants(context.Context) ([]string, error) { return nil, nil }
func (*captureRepo) GetByID(context.Context, uuid.UUID) (*outbox.OutboxEvent, error) {
	return nil, nil
}
func (*captureRepo) MarkPublished(context.Context, uuid.UUID, time.Time) error { return nil }
func (*captureRepo) MarkFailed(context.Context, uuid.UUID, string, int) error  { return nil }
func (*captureRepo) MarkInvalid(context.Context, uuid.UUID, string) error      { return nil }
func (*captureRepo) ListFailedForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*captureRepo) ResetForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*captureRepo) ResetStuckProcessing(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

// opaqueCluster seeds the producer's topic and DLQ plus the consumer's own DLQ.
func opaqueCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(1),
		kfake.SeedTopics(1, opaqueTopic, opaqueDLQ, opaqueConsDLQ),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// opaqueProducer builds a producer whose single definition declares a non-JSON
// content type. Nothing else about the wiring is special — no option, no
// constructor, no flag. Declaring the content type IS the API for opaque bytes.
func opaqueProducer(t *testing.T, cluster *kfake.Cluster, repo outbox.OutboxRepository) *streaming.Producer {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:             opaqueDefinitionKey,
		ResourceType:    "documento",
		EventType:       "enviado",
		SchemaVersion:   "1.0.0",
		DataContentType: opaqueContentType,
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	emitter, err := streaming.NewBuilder().
		Source(opaqueApp).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:         "primary.all",
			Target:      "primary",
			Destination: streaming.KafkaTopic(opaqueTopic),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  cluster.ListenAddrs(),
			ClientID: "opaque-kfake",
		}).
		OutboxRepository(repo).
		Logger(log.NewNop()).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	producer, ok := emitter.(*streaming.Producer)
	if !ok {
		t.Fatalf("Build() returned %T; want *streaming.Producer", emitter)
	}

	return producer
}

// opaqueHeader returns the first value for key, or "" when absent.
func opaqueHeader(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}

// opaqueAwaitRecord polls topic until one record lands or the budget expires.
func opaqueAwaitRecord(t *testing.T, cluster *kfake.Cluster, topic string, budget time.Duration) *kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("opaque-reader-"+topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("reader init err = %v", err)
	}

	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	for ctx.Err() == nil {
		var got *kgo.Record

		client.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if got == nil {
				got = r
			}
		})

		if got != nil {
			return got
		}
	}

	t.Fatalf("no record on %s within %s", topic, budget)

	return nil
}

// TestIntegration_OpaquePayloadRoundTripsThroughOutbox is the G4 gate: an XML
// document persisted through the outbox route and relayed onto Kafka arrives
// with its bytes unchanged, its CloudEvents attributes in headers exactly as
// the JSON path writes them, and its partition key intact.
func TestIntegration_OpaquePayloadRoundTripsThroughOutbox(t *testing.T) {
	cluster := opaqueCluster(t)
	repo := &captureRepo{}
	producer := opaqueProducer(t, cluster, repo)

	// Direct=skip + Outbox=always forces the transactional route, which is the
	// hop under test. The circuit-open fallback would reach the same code, but
	// only after an induced broker failure that the assertions would then have
	// to see past.
	err := producer.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey:  opaqueDefinitionKey,
		TenantID:       opaqueTenant,
		Subject:        opaqueSubject,
		Payload:        opaqueSFNDocument,
		PolicyOverride: streaming.DeliveryPolicyOverride{Direct: streaming.DirectModeSkip, Outbox: streaming.OutboxModeAlways},
	})
	if err != nil {
		t.Fatalf("Emit() error = %v; want nil (opaque payload must persist to the outbox)", err)
	}

	row := repo.only(t)

	// The row is what lands in a JSONB column. If it is not valid JSON the
	// insert fails in production even though the marshal succeeded here.
	if !jsonValid(row.Payload) {
		t.Fatalf("persisted outbox row is not valid JSON: %q", row.Payload)
	}

	registry := outbox.NewHandlerRegistry()
	if err := producer.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay() error = %v", err)
	}

	if err := registry.Handle(context.Background(), row); err != nil {
		t.Fatalf("relay Handle() error = %v; want nil", err)
	}

	rec := opaqueAwaitRecord(t, cluster, opaqueTopic, opaqueWaitBudget)

	if !bytes.Equal(rec.Value, opaqueSFNDocument) {
		t.Fatalf("record value is not byte-identical:\n got %q\nwant %q", rec.Value, opaqueSFNDocument)
	}

	if got := string(rec.Key); got != opaqueTenant {
		t.Errorf("partition key = %q; want the tenant id %q", got, opaqueTenant)
	}

	// The CloudEvents attribute set is the same one the JSON path writes —
	// nothing about an opaque payload changes the envelope on the wire.
	wantHeaders := map[string]string{
		"ce-specversion":     "1.0",
		"ce-source":          opaqueApp,
		"ce-type":            streaming.CloudEventsType(opaqueApp, "documento", "enviado"),
		"ce-subject":         opaqueSubject,
		"ce-datacontenttype": opaqueContentType,
		"ce-tenantid":        opaqueTenant,
		"ce-resourcetype":    "documento",
		"ce-eventtype":       "enviado",
		"ce-schemaversion":   "1.0.0",
	}

	for key, want := range wantHeaders {
		if got := opaqueHeader(rec, key); got != want {
			t.Errorf("header %s = %q; want %q", key, got, want)
		}
	}

	if got := opaqueHeader(rec, "ce-id"); got == "" {
		t.Error("header ce-id is empty; want the event id")
	}

	// A base64 wrapper leaking onto the wire is the failure mode the byte
	// comparison above would catch, but this names it explicitly.
	if bytes.Contains(rec.Value, []byte("PayloadOpaque")) {
		t.Errorf("record value carries the persistence-only opaque wrapper: %q", rec.Value)
	}
}

// TestIntegration_OpaquePayloadReachesConsumerUntouched closes the loop on the
// read side: a consumer built with this library hands its handler the same
// bytes and the declared content type, with no JSON decode attempted anywhere.
func TestIntegration_OpaquePayloadReachesConsumerUntouched(t *testing.T) {
	cluster := opaqueCluster(t)
	repo := &captureRepo{}
	producer := opaqueProducer(t, cluster, repo)

	type received struct {
		event   streaming.Event
		payload []byte
	}

	got := make(chan received, 1)

	consumer, err := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group("opaque-consumer").
		Source(opaqueConsumer).
		Apps(opaqueApp).
		OnFrom(opaqueApp, "documento.enviado", func(_ context.Context, ev streaming.Event, payload []byte) error {
			select {
			case got <- received{event: ev, payload: append([]byte(nil), payload...)}:
			default:
			}

			return nil
		}).
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("consumer Build() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- consumer.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	emitErr := producer.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey:  opaqueDefinitionKey,
		TenantID:       opaqueTenant,
		Subject:        opaqueSubject,
		Payload:        opaqueSFNDocument,
		PolicyOverride: streaming.DeliveryPolicyOverride{Direct: streaming.DirectModeSkip, Outbox: streaming.OutboxModeAlways},
	})
	if emitErr != nil {
		t.Fatalf("Emit() error = %v", emitErr)
	}

	registry := outbox.NewHandlerRegistry()
	if err := producer.RegisterOutboxRelay(registry); err != nil {
		t.Fatalf("RegisterOutboxRelay() error = %v", err)
	}

	if err := registry.Handle(context.Background(), repo.only(t)); err != nil {
		t.Fatalf("relay Handle() error = %v", err)
	}

	select {
	case r := <-got:
		if !bytes.Equal(r.payload, opaqueSFNDocument) {
			t.Fatalf("handler payload is not byte-identical:\n got %q\nwant %q", r.payload, opaqueSFNDocument)
		}

		if r.event.DataContentType != opaqueContentType {
			t.Errorf("handler DataContentType = %q; want %q", r.event.DataContentType, opaqueContentType)
		}

		if r.event.TenantID != opaqueTenant {
			t.Errorf("handler TenantID = %q; want %q", r.event.TenantID, opaqueTenant)
		}
	case <-time.After(opaqueWaitBudget):
		t.Fatalf("handler saw no record within %s", opaqueWaitBudget)
	}
}

// jsonValid is json.Valid, wrapped so the import list of this file stays about
// the streaming API rather than about encoding.
func jsonValid(b []byte) bool { return json.Valid(b) }
