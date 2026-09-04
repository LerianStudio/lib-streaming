//go:build unit

package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

type singleEnvelopeTransactionalWriter struct {
	writes   int
	txWrites int
}

func (w *singleEnvelopeTransactionalWriter) Write(context.Context, OutboxEnvelope) error {
	w.writes++

	return nil
}

func (w *singleEnvelopeTransactionalWriter) WriteWithTx(context.Context, *sql.Tx, OutboxEnvelope) error {
	w.txWrites++

	return nil
}

func TestProducer_EmitBatch_PersistsAllRoutesInDeterministicOrder(t *testing.T) {
	t.Parallel()

	catalog, routes := batchCatalogAndRoutes(t)
	repo := &fakeOutboxRepo{}
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc-batch-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
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

	ctx := WithOutboxTx(context.Background(), &sql.Tx{})
	err = p.EmitBatch(ctx, []EmitRequest{
		{DefinitionKey: "transaction.created", TenantID: "tenant-1", Subject: "tx-1", Payload: json.RawMessage(`{"sequence":1}`)},
		{DefinitionKey: "order.submitted", TenantID: "tenant-1", Subject: "order-1", Payload: json.RawMessage(`{"sequence":2}`)},
	})
	if err != nil {
		t.Fatalf("EmitBatch() error = %v", err)
	}

	calls, rows := repo.batchSnapshot()
	if calls != 1 {
		t.Fatalf("CreateManyWithTx calls = %d; want 1", calls)
	}
	if got := len(rows); got != 3 {
		t.Fatalf("persisted rows = %d; want 3", got)
	}
	if got := repo.createdWithTxCount(); got != 0 {
		t.Fatalf("per-event CreateWithTx calls = %d; want 0", got)
	}
	if got := repo.createdCount(); got != 0 {
		t.Fatalf("per-event Create calls = %d; want 0", got)
	}

	want := []struct {
		route       string
		target      string
		destination string
		definition  string
	}{
		{route: "transaction.created.kafka.primary", target: "primary", destination: "topic.transactions", definition: "transaction.created"},
		{route: "transaction.created.kafka.secondary", target: "secondary", destination: "topic.transactions.replica", definition: "transaction.created"},
		{route: "order.submitted.kafka.primary", target: "primary", destination: "topic.orders", definition: "order.submitted"},
	}

	for i := range rows {
		var envelope OutboxEnvelope
		if unmarshalErr := json.Unmarshal(rows[i].Payload, &envelope); unmarshalErr != nil {
			t.Fatalf("json.Unmarshal(rows[%d]) error = %v", i, unmarshalErr)
		}

		if envelope.RouteKey != want[i].route ||
			envelope.Target != want[i].target ||
			envelope.Destination.Name != want[i].destination ||
			envelope.DefinitionKey != want[i].definition {
			t.Fatalf("rows[%d] route tuple = (%q, %q, %q, %q); want (%q, %q, %q, %q)",
				i,
				envelope.RouteKey,
				envelope.Target,
				envelope.Destination.Name,
				envelope.DefinitionKey,
				want[i].route,
				want[i].target,
				want[i].destination,
				want[i].definition,
			)
		}
	}

	if got := len(primary.Messages()) + len(secondary.Messages()); got != 0 {
		t.Fatalf("direct publishes = %d; want 0", got)
	}
}

func TestProducer_EmitBatch_RejectsWholeBatchBeforePersistence(t *testing.T) {
	t.Parallel()

	catalog, routes := batchCatalogAndRoutes(t)
	repo := &fakeOutboxRepo{}
	p := newBatchTestProducer(t, catalog, routes, repo)

	err := p.EmitBatch(WithOutboxTx(context.Background(), &sql.Tx{}), []EmitRequest{
		{DefinitionKey: "transaction.created", TenantID: "tenant-1", Payload: json.RawMessage(`{"valid":true}`)},
		{DefinitionKey: "order.submitted", TenantID: "tenant-1", Payload: json.RawMessage(`not-json`)},
	})
	if !errors.Is(err, ErrNotJSON) {
		t.Fatalf("EmitBatch() error = %v; want ErrNotJSON", err)
	}

	calls, rows := repo.batchSnapshot()
	if calls != 0 || len(rows) != 0 {
		t.Fatalf("batch repository observed calls=%d rows=%d; want 0/0", calls, len(rows))
	}
	if repo.createdWithTxCount() != 0 || repo.createdCount() != 0 {
		t.Fatal("invalid batch fell back to per-event persistence")
	}
}

func TestProducer_EmitBatch_RejectsMissingTransactionalOutboxWiring(t *testing.T) {
	t.Parallel()

	catalog, routes := batchCatalogAndRoutes(t)
	request := []EmitRequest{{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       json.RawMessage(`{"valid":true}`),
	}}
	singleWriter := &singleEnvelopeTransactionalWriter{}

	tests := []struct {
		name    string
		options []EmitterOption
		ctx     context.Context
		want    error
		writer  *singleEnvelopeTransactionalWriter
	}{
		{
			name: "outbox not configured",
			ctx:  WithOutboxTx(context.Background(), &sql.Tx{}),
			want: ErrOutboxNotConfigured,
		},
		{
			name:    "ambient transaction missing",
			options: []EmitterOption{WithOutboxRepository(&fakeOutboxRepo{})},
			ctx:     context.Background(),
			want:    ErrOutboxTxUnsupported,
		},
		{
			name:    "writer lacks set-wise batch support",
			options: []EmitterOption{WithOutboxWriter(singleWriter)},
			ctx:     WithOutboxTx(context.Background(), &sql.Tx{}),
			want:    ErrOutboxTxUnsupported,
			writer:  singleWriter,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newBatchTestProducer(t, catalog, routes, nil, tt.options...)
			err := p.EmitBatch(tt.ctx, request)
			if !errors.Is(err, tt.want) {
				t.Fatalf("EmitBatch() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
			if tt.writer != nil && (tt.writer.writes != 0 || tt.writer.txWrites != 0) {
				t.Fatalf("unsupported batch writer received per-event fallback writes=%d txWrites=%d; want 0/0", tt.writer.writes, tt.writer.txWrites)
			}
		})
	}
}

func TestProducer_EmitBatch_RejectsDisabledEventWithoutPersistence(t *testing.T) {
	t.Parallel()

	disabled := DefaultDeliveryPolicy()
	disabled.Enabled = false
	catalog, err := NewCatalog(EventDefinition{
		Key: "transaction.created", ResourceType: "transaction", EventType: "created", DefaultPolicy: disabled,
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
	repo := &fakeOutboxRepo{}
	p := newBatchTestProducer(t, catalog, routes, repo)

	err = p.EmitBatch(WithOutboxTx(context.Background(), &sql.Tx{}), []EmitRequest{{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       json.RawMessage(`{"valid":true}`),
	}})
	if !errors.Is(err, ErrEventDisabled) {
		t.Fatalf("EmitBatch() error = %v; want ErrEventDisabled", err)
	}

	calls, rows := repo.batchSnapshot()
	if calls != 0 || len(rows) != 0 {
		t.Fatalf("disabled event persisted calls=%d rows=%d; want 0/0", calls, len(rows))
	}
}

func TestProducer_EmitBatch_EmptyBatchIsNoOp(t *testing.T) {
	t.Parallel()

	catalog, routes := batchCatalogAndRoutes(t)
	p := newBatchTestProducer(t, catalog, routes, nil)

	if err := p.EmitBatch(context.Background(), nil); err != nil {
		t.Fatalf("EmitBatch(nil) error = %v; want nil", err)
	}
}

func TestProducer_EmitBatch_LifecycleAndContextGuards(t *testing.T) {
	t.Parallel()

	request := []EmitRequest{{DefinitionKey: "transaction.created", Payload: json.RawMessage(`{"valid":true}`)}}

	var nilProducer *Producer
	if err := nilProducer.EmitBatch(context.Background(), request); !errors.Is(err, ErrNilProducer) {
		t.Fatalf("nil Producer.EmitBatch() error = %v; want ErrNilProducer", err)
	}

	catalog, routes := batchCatalogAndRoutes(t)
	p := newBatchTestProducer(t, catalog, routes, &fakeOutboxRepo{})
	if err := p.EmitBatch(nil, request); !errors.Is(err, ErrOutboxTxUnsupported) {
		t.Fatalf("EmitBatch(nil context) error = %v; want ErrOutboxTxUnsupported", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := p.EmitBatch(context.Background(), nil); !errors.Is(err, ErrEmitterClosed) {
		t.Fatalf("closed Producer.EmitBatch(empty) error = %v; want ErrEmitterClosed", err)
	}
}

func TestProducer_EmitBatch_RejectsRoutePayloadOverTransportCap(t *testing.T) {
	t.Parallel()

	event := Event{Payload: make(json.RawMessage, contract.MaxPayloadBytesForKind(contract.TransportSQS)+1)}
	err := enforceBatchRoutePayloadCap(event, []contract.RouteDefinition{{
		Destination: contract.Destination{Kind: contract.TransportSQS},
	}})
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("enforceBatchRoutePayloadCap() error = %v; want ErrPayloadTooLarge", err)
	}
}

func TestProducer_EmitBatch_RejectsUnknownDefinitionAndDisabledRoute(t *testing.T) {
	t.Parallel()

	catalog, routes := batchCatalogAndRoutes(t)
	repo := &fakeOutboxRepo{}
	p := newBatchTestProducer(t, catalog, routes, repo)

	err := p.EmitBatch(WithOutboxTx(context.Background(), &sql.Tx{}), []EmitRequest{{
		DefinitionKey: "unknown.event",
		Payload:       json.RawMessage(`{"valid":true}`),
	}})
	if !errors.Is(err, contract.ErrUnknownEventDefinition) {
		t.Fatalf("EmitBatch(unknown) error = %v; want ErrUnknownEventDefinition", err)
	}

	disabled := false
	disabledRoute := multiTestRoute(
		"transaction.created.kafka.primary",
		"transaction.created",
		"primary",
		"topic.transactions",
		contract.RouteRequired,
	)
	disabledRoute.Policy.Enabled = &disabled
	disabledRoutes, err := contract.NewRouteTable(disabledRoute)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}
	disabledCatalog, err := NewCatalog(EventDefinition{
		Key: "transaction.created", ResourceType: "transaction", EventType: "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}
	disabledProducer := newBatchTestProducer(t, disabledCatalog, disabledRoutes, repo)

	err = disabledProducer.EmitBatch(WithOutboxTx(context.Background(), &sql.Tx{}), []EmitRequest{{
		DefinitionKey: "transaction.created",
		Payload:       json.RawMessage(`{"valid":true}`),
	}})
	if !errors.Is(err, ErrEventDisabled) {
		t.Fatalf("EmitBatch(disabled route) error = %v; want ErrEventDisabled", err)
	}
}

func TestTransactionalBatchTxRejectsWrongContextValueType(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), txContextKey{}, "not-a-transaction")
	if _, err := transactionalBatchTx(ctx); !errors.Is(err, ErrOutboxTxUnsupported) {
		t.Fatalf("transactionalBatchTx() error = %v; want ErrOutboxTxUnsupported", err)
	}
}

func batchCatalogAndRoutes(t *testing.T) (Catalog, contract.RouteTable) {
	t.Helper()

	catalog, err := NewCatalog(
		EventDefinition{Key: "transaction.created", ResourceType: "transaction", EventType: "created"},
		EventDefinition{Key: "order.submitted", ResourceType: "order", EventType: "submitted"},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	routes, err := contract.NewRouteTable(
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "topic.transactions.replica", contract.RouteRequired),
		multiTestRoute("order.submitted.kafka.primary", "order.submitted", "primary", "topic.orders", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "topic.transactions", contract.RouteRequired),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	return catalog, routes
}

func newBatchTestProducer(
	t *testing.T,
	catalog Catalog,
	routes contract.RouteTable,
	repo *fakeOutboxRepo,
	extraOptions ...EmitterOption,
) *Producer {
	t.Helper()

	options := []EmitterOption{WithLogger(log.NewNop()), WithCatalog(catalog)}
	if repo != nil {
		options = append(options, WithOutboxRepository(repo))
	}
	options = append(options, extraOptions...)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc-batch-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)},
		},
		routes,
		catalog,
		options...,
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return p
}
