//go:build unit

package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-commons/v6/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/google/uuid"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

type nonBatchOutboxRepo struct {
	outbox.OutboxRepository
}

func TestOutboxAdapterWritesStableEnvelopeRows(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.Write(context.Background(), envelope); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("repo.Create was not called")
	}
	if row.EventType != StreamingOutboxEventType {
		t.Fatalf("row.EventType = %q; want %q", row.EventType, StreamingOutboxEventType)
	}

	var got OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &got); err != nil {
		t.Fatalf("json.Unmarshal row.Payload err = %v", err)
	}
	if got.Destination.Name != event.Topic() {
		t.Fatalf("envelope.Destination.Name = %q; want %q", got.Destination.Name, event.Topic())
	}
}

func TestOutboxAdapterWriteWithTxUsesRepositoryTransaction(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.WriteWithTx(context.Background(), &sql.Tx{}, envelope); err != nil {
		t.Fatalf("WriteWithTx err = %v", err)
	}

	if got := repo.createdWithTxCount(); got != 1 {
		t.Fatalf("createdWithTxCount = %d; want 1", got)
	}
	if got := repo.createdCount(); got != 0 {
		t.Fatalf("createdCount = %d; want 0", got)
	}
}

func TestOutboxAdapterNilRepositoryReturnsConfiguredSentinel(t *testing.T) {
	var nilWriter *libCommonsOutboxWriter
	if err := nilWriter.Write(context.Background(), OutboxEnvelope{}); !errors.Is(err, ErrOutboxNotConfigured) {
		t.Fatalf("Write err = %v; want ErrOutboxNotConfigured", err)
	}
}

func TestOutboxAdapterRejectsOversizedSerializedEnvelope(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.Payload = json.RawMessage(`"` + strings.Repeat("x", maxPayloadBytes-1) + `"`)
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.Write(context.Background(), envelope); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Write err = %v; want ErrPayloadTooLarge", err)
	}
	if got := repo.createdCount(); got != 0 {
		t.Fatalf("createdCount = %d; want 0", got)
	}
}

func TestOutboxAdapterWriteBatchWithTxRejectsInvalidWiringAndRows(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults()
	validEnvelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())
	repositoryFailure := errors.New("repository unavailable")

	tests := []struct {
		name      string
		writer    *libCommonsOutboxWriter
		tx        *sql.Tx
		envelopes []OutboxEnvelope
		want      error
	}{
		{
			name:      "nil writer",
			writer:    nil,
			tx:        &sql.Tx{},
			envelopes: []OutboxEnvelope{validEnvelope},
			want:      ErrOutboxNotConfigured,
		},
		{
			name:      "nil repository",
			writer:    &libCommonsOutboxWriter{},
			tx:        &sql.Tx{},
			envelopes: []OutboxEnvelope{validEnvelope},
			want:      ErrOutboxNotConfigured,
		},
		{
			name:      "nil transaction",
			writer:    &libCommonsOutboxWriter{repo: &fakeOutboxRepo{}},
			envelopes: []OutboxEnvelope{validEnvelope},
			want:      ErrOutboxTxUnsupported,
		},
		{
			name: "repository without batch support",
			writer: &libCommonsOutboxWriter{repo: &nonBatchOutboxRepo{
				OutboxRepository: &fakeOutboxRepo{},
			}},
			tx:        &sql.Tx{},
			envelopes: []OutboxEnvelope{validEnvelope},
			want:      ErrOutboxTxUnsupported,
		},
		{
			name:      "invalid envelope",
			writer:    &libCommonsOutboxWriter{repo: &fakeOutboxRepo{}},
			tx:        &sql.Tx{},
			envelopes: []OutboxEnvelope{{}},
			want:      contract.ErrInvalidOutboxEnvelope,
		},
		{
			name: "repository failure",
			writer: &libCommonsOutboxWriter{repo: &fakeOutboxRepo{
				createManyTxErr: repositoryFailure,
			}},
			tx:        &sql.Tx{},
			envelopes: []OutboxEnvelope{validEnvelope},
			want:      repositoryFailure,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.writer.WriteBatchWithTx(context.Background(), tt.tx, tt.envelopes)
			if !errors.Is(err, tt.want) {
				t.Fatalf("WriteBatchWithTx() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}

func TestPublishRouteOutbox_GuardsTransactionAndWriteFailures(t *testing.T) {
	t.Parallel()

	event := sampleEvent()
	event.ApplyDefaults()
	route := multiTestRoute(
		"transaction.created.kafka.primary",
		"transaction.created",
		"primary",
		event.Topic(),
		contract.RouteRequired,
	)
	policy := DefaultDeliveryPolicy()

	var nilProducer *Producer
	if err := nilProducer.publishRouteOutbox(context.Background(), event, "transaction.created", route, policy); !errors.Is(err, ErrNilProducer) {
		t.Fatalf("nil publishRouteOutbox() error = %v; want ErrNilProducer", err)
	}

	unconfigured := &Producer{logger: log.NewNop()}
	if err := unconfigured.publishRouteOutbox(context.Background(), event, "transaction.created", route, policy); !errors.Is(err, ErrOutboxNotConfigured) {
		t.Fatalf("unconfigured publishRouteOutbox() error = %v; want ErrOutboxNotConfigured", err)
	}

	capture := &captureRouteOutboxWriter{}
	p := &Producer{logger: log.NewNop(), outboxWriter: capture}
	wrongTxContext := context.WithValue(context.Background(), txContextKey{}, "not-a-transaction")
	if err := p.publishRouteOutbox(wrongTxContext, event, "transaction.created", route, policy); !errors.Is(err, ErrOutboxTxUnsupported) {
		t.Fatalf("wrong transaction publishRouteOutbox() error = %v; want ErrOutboxTxUnsupported", err)
	}

	if err := p.publishRouteOutbox(WithOutboxTx(context.Background(), &sql.Tx{}), event, "transaction.created", route, policy); !errors.Is(err, ErrOutboxTxUnsupported) {
		t.Fatalf("non-transactional writer publishRouteOutbox() error = %v; want ErrOutboxTxUnsupported", err)
	}

	if err := p.publishRouteOutbox(WithOutboxTx(context.Background(), nil), event, "transaction.created", route, policy); err != nil {
		t.Fatalf("nil transaction fallback publishRouteOutbox() error = %v", err)
	}

	writeFailure := errors.New("write failed")
	failing := &Producer{logger: log.NewNop(), outboxWriter: &failingRouteOutboxWriter{err: writeFailure}}
	if err := failing.publishRouteOutbox(nil, event, "transaction.created", route, policy); !errors.Is(err, writeFailure) {
		t.Fatalf("failed write publishRouteOutbox() error = %v; want repository failure", err)
	}

	invalidRoute := route
	invalidRoute.Key = ""
	if err := p.publishRouteOutbox(context.Background(), event, "transaction.created", invalidRoute, policy); !errors.Is(err, contract.ErrInvalidOutboxEnvelope) {
		t.Fatalf("invalid envelope publishRouteOutbox() error = %v; want ErrInvalidOutboxEnvelope", err)
	}
}

// TestIsNilInterface_TypedNil pins a regression contract: a typed-nil
// interface (`var w OutboxWriter = (*libCommonsOutboxWriter)(nil)`) must be
// detected as nil so the WithOutboxRepository / WithOutboxWriter options
// correctly clear the writer instead of installing a useless typed-nil
// shell that would NPE on first Emit.
//
// The standard Go gotcha: `var w OutboxWriter = (*Writer)(nil); w == nil`
// returns FALSE because the interface carries a non-nil type tag. Only
// reflect.ValueOf(w).IsNil() returns TRUE.
func TestIsNilInterface_TypedNil(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want bool
	}{
		{
			name: "untyped nil",
			v:    nil,
			want: true,
		},
		{
			name: "typed nil OutboxWriter",
			v:    OutboxWriter((*libCommonsOutboxWriter)(nil)),
			want: true,
		},
		{
			name: "typed nil pointer to concrete writer",
			v:    (*libCommonsOutboxWriter)(nil),
			want: true,
		},
		{
			name: "non-nil concrete writer",
			v:    &libCommonsOutboxWriter{},
			want: false,
		},
		{
			name: "non-nil OutboxWriter interface",
			v:    OutboxWriter(&libCommonsOutboxWriter{}),
			want: false,
		},
		{
			name: "typed nil map",
			v:    (map[string]int)(nil),
			want: true,
		},
		{
			name: "non-nil empty map",
			v:    map[string]int{},
			want: false,
		},
		{
			name: "typed nil slice",
			v:    ([]byte)(nil),
			want: true,
		},
		{
			name: "non-nil int (not a nilable kind)",
			v:    42,
			want: false,
		},
		{
			name: "non-nil string (not a nilable kind)",
			v:    "hello",
			want: false,
		},
	}

	for _, tt := range tests {
		if got := isNilInterface(tt.v); got != tt.want {
			t.Errorf("isNilInterface(%T %v) = %v; want %v", tt.v, tt.v, got, tt.want)
		}
	}
}
