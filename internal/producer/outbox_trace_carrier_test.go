//go:build unit

package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport/fake"
)

func TestProducer_EmitBatch_CapturesBoundedTraceCarrierAndRelayContinuesOriginTrace(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	origin := mustSpanContext(t,
		"4bf92f3577b34da6a3ce929d0e0e4736",
		"00f067aa0ba902b7",
		"vendor=value",
	)
	dispatcher := mustSpanContext(t,
		"11111111111111111111111111111111",
		"2222222222222222",
		"dispatcher=value",
	)

	member, err := baggage.NewMember("customer.email", "secret@example.com")
	if err != nil {
		t.Fatalf("baggage.NewMember() error = %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage.New() error = %v", err)
	}

	originCtx := baggage.ContextWithBaggage(trace.ContextWithSpanContext(context.Background(), origin), bag)
	repo := &fakeOutboxRepo{}
	adapter := fake.NewAdapter(TransportKafkaLike)
	catalog, err := NewCatalog(EventDefinition{
		Key: "transaction.created", ResourceType: "transaction", EventType: "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}
	routes, err := contract.NewRouteTable(multiTestRoute(
		"transaction.created.kafka.primary",
		"transaction.created",
		"primary",
		"topic.transactions",
		contract.RouteRequired,
	))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc-trace-batch"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithOutboxRepository(repo),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	err = p.EmitBatch(WithOutboxTx(originCtx, &sql.Tx{}), []EmitRequest{{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       json.RawMessage(`{"trace":"origin"}`),
	}})
	if err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	_, rows := repo.batchSnapshot()
	if len(rows) != 1 {
		t.Fatalf("persisted rows = %d; want 1", len(rows))
	}

	var envelope OutboxEnvelope
	if err := json.Unmarshal(rows[0].Payload, &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if got := len(envelope.TraceCarrier); got != 2 {
		t.Fatalf("TraceCarrier entries = %d; want 2: %#v", got, envelope.TraceCarrier)
	}
	if got, want := envelope.TraceCarrier[TraceParentHeader], "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"; got != want {
		t.Fatalf("traceparent = %q; want %q", got, want)
	}
	if got, want := envelope.TraceCarrier[TraceStateHeader], "vendor=value"; got != want {
		t.Fatalf("tracestate = %q; want %q", got, want)
	}
	if strings.Contains(string(rows[0].Payload), "baggage") || strings.Contains(string(rows[0].Payload), "secret@example.com") {
		t.Fatalf("persisted wire contains baggage/PII: %s", rows[0].Payload)
	}

	dispatcherCtx := trace.ContextWithSpanContext(context.Background(), dispatcher)
	adapter.SetPublishError(errors.New("broker unavailable"))
	if err := p.handleOutboxRow(dispatcherCtx, rows[0]); err == nil {
		t.Fatal("first handleOutboxRow() error = nil; want redelivery-triggering failure")
	}
	if got := len(adapter.Messages()); got != 0 {
		t.Fatalf("messages after failed delivery = %d; want 0", got)
	}

	adapter.SetPublishError(nil)
	if err := p.handleOutboxRow(dispatcherCtx, rows[0]); err != nil {
		t.Fatalf("redelivered handleOutboxRow() error = %v", err)
	}

	messages := adapter.Messages()
	if len(messages) != 1 {
		t.Fatalf("relayed messages = %d; want 1", len(messages))
	}
	headerValues := make(map[string]string, len(messages[0].Headers))
	for _, header := range messages[0].Headers {
		headerValues[header.Key] = string(header.Value)
	}
	if got, want := headerValues[TraceParentHeader], envelope.TraceCarrier[TraceParentHeader]; got != want {
		t.Fatalf("relayed traceparent = %q; want origin %q", got, want)
	}
	if got := headerValues[TraceParentHeader]; strings.Contains(got, dispatcher.SpanID().String()) {
		t.Fatalf("relayed traceparent continued dispatcher span: %q", got)
	}
}

func TestCaptureTraceCarrier_EmptyContextsDoNotPersistMetadata(t *testing.T) {
	t.Parallel()

	if carrier := captureTraceCarrier(nil); carrier != nil {
		t.Fatalf("captureTraceCarrier(nil) = %#v; want nil", carrier)
	}

	if carrier := captureTraceCarrier(context.Background()); carrier != nil {
		t.Fatalf("captureTraceCarrier(background) = %#v; want nil", carrier)
	}
}

func TestProducer_DeriveOutboxAggregateIDHonorsCustomPartitionAndSystemEvents(t *testing.T) {
	t.Parallel()

	p := &Producer{partFn: func(Event) string { return "custom-partition" }}
	event := Event{TenantID: "tenant-1", Subject: "subject-1"}

	got := p.deriveOutboxAggregateID(event)

	want := uuid.NewSHA1(uuid.NameSpaceDNS, []byte("custom-partition"))
	if got != want {
		t.Fatalf("aggregate id = %s; want the SHA1 of the custom partition key %s", got, want)
	}

	if got == deriveAggregateID(event) {
		t.Fatal("custom partition function was ignored — aggregate id matched the default derivation")
	}

	if got != p.deriveOutboxAggregateID(event) {
		t.Fatal("custom partition aggregate ID is not deterministic")
	}

	system := Event{SystemEvent: true}
	if p.deriveOutboxAggregateID(system) == p.deriveOutboxAggregateID(system) {
		t.Fatal("system event aggregate IDs must be random")
	}
}

func TestOutboxWriter_PersistsExactTraceCarrierWire(t *testing.T) {
	t.Parallel()

	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}
	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), newTestUUIDv7(t))
	envelope.TraceCarrier = TraceCarrier{
		TraceParentHeader: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceStateHeader:  "vendor=value",
	}

	want, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := writer.Write(context.Background(), envelope); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("repo.Create() was not called")
	}
	if string(row.Payload) != string(want) {
		t.Fatalf("persisted payload = %s; want exact wire %s", row.Payload, want)
	}
}

func mustSpanContext(t *testing.T, traceID, spanID, state string) trace.SpanContext {
	t.Helper()

	parsedTraceID, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex() error = %v", err)
	}
	parsedSpanID, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex() error = %v", err)
	}
	parsedState, err := trace.ParseTraceState(state)
	if err != nil {
		t.Fatalf("ParseTraceState() error = %v", err)
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    parsedTraceID,
		SpanID:     parsedSpanID,
		TraceFlags: trace.FlagsSampled,
		TraceState: parsedState,
	})
}
