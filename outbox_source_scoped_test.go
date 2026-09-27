//go:build unit

package streaming_test

import (
	"context"
	"database/sql"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// sharedOutboxTable is one outbox table several binaries of a service write
// to. It claims like the lib-commons Postgres repository: a claim moves a
// PENDING row to PROCESSING, and ListPendingByTypes claims only the named
// types, which is what makes a dispatcher's WithPriorityEventTypes scope
// exclusive.
type sharedOutboxTable struct {
	mu      sync.Mutex
	rows    []*outbox.OutboxEvent
	lastErr map[uuid.UUID]string
}

var _ outbox.MultiTypePendingRepository = (*sharedOutboxTable)(nil)

func (r *sharedOutboxTable) Create(_ context.Context, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	row := *event
	row.Status = outbox.OutboxStatusPending
	r.rows = append(r.rows, &row)

	return &row, nil
}

func (r *sharedOutboxTable) CreateWithTx(ctx context.Context, _ outbox.Tx, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	return r.Create(ctx, event)
}

func (r *sharedOutboxTable) CreateManyWithTx(ctx context.Context, _ outbox.Tx, events []*outbox.OutboxEvent) ([]*outbox.OutboxEvent, error) {
	created := make([]*outbox.OutboxEvent, 0, len(events))

	for _, event := range events {
		row, err := r.Create(ctx, event)
		if err != nil {
			return nil, err
		}

		created = append(created, row)
	}

	return created, nil
}

func (r *sharedOutboxTable) claim(limit int, match func(*outbox.OutboxEvent) bool) []*outbox.OutboxEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	var claimed []*outbox.OutboxEvent

	for _, row := range r.rows {
		if len(claimed) == limit {
			break
		}

		if row.Status == outbox.OutboxStatusPending && match(row) {
			row.Status = outbox.OutboxStatusProcessing
			claimed = append(claimed, row)
		}
	}

	return claimed
}

func (r *sharedOutboxTable) ListPending(_ context.Context, limit int) ([]*outbox.OutboxEvent, error) {
	return r.claim(limit, func(*outbox.OutboxEvent) bool { return true }), nil
}

func (r *sharedOutboxTable) ListPendingByType(_ context.Context, eventType string, limit int) ([]*outbox.OutboxEvent, error) {
	return r.claim(limit, func(row *outbox.OutboxEvent) bool { return row.EventType == eventType }), nil
}

func (r *sharedOutboxTable) ListPendingByTypes(_ context.Context, eventTypes []string, limit int) ([]*outbox.OutboxEvent, error) {
	return r.claim(limit, func(row *outbox.OutboxEvent) bool { return slices.Contains(eventTypes, row.EventType) }), nil
}

func (r *sharedOutboxTable) setStatus(id uuid.UUID, status, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, row := range r.rows {
		if row.ID == id {
			row.Status = status
		}
	}

	if errMsg != "" {
		if r.lastErr == nil {
			r.lastErr = map[uuid.UUID]string{}
		}

		r.lastErr[id] = errMsg
	}
}

func (r *sharedOutboxTable) MarkPublished(_ context.Context, id uuid.UUID, _ time.Time) error {
	r.setStatus(id, outbox.OutboxStatusPublished, "")

	return nil
}

func (r *sharedOutboxTable) MarkFailed(_ context.Context, id uuid.UUID, errMsg string, _ int) error {
	r.setStatus(id, outbox.OutboxStatusFailed, errMsg)

	return nil
}

func (r *sharedOutboxTable) MarkInvalid(_ context.Context, id uuid.UUID, errMsg string) error {
	r.setStatus(id, outbox.OutboxStatusInvalid, errMsg)

	return nil
}

func (*sharedOutboxTable) ListTenants(context.Context) ([]string, error) { return nil, nil }

func (*sharedOutboxTable) GetByID(context.Context, uuid.UUID) (*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*sharedOutboxTable) ListFailedForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*sharedOutboxTable) ResetForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*sharedOutboxTable) ResetStuckProcessing(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

// snapshot returns every row's event type and status, in write order.
func (r *sharedOutboxTable) snapshot() []outbox.OutboxEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]outbox.OutboxEvent, len(r.rows))
	for i, row := range r.rows {
		out[i] = *row
	}

	return out
}

func (r *sharedOutboxTable) errorsByStatus(status string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []string

	for _, row := range r.rows {
		if row.Status == status {
			out = append(out, r.lastErr[row.ID])
		}
	}

	return out
}

// outboxBinary is one binary of a multi-binary service: its own source, its
// own signing key, its own RabbitMQ publisher, the service's shared table.
type outboxBinary struct {
	source    string
	ring      *streaming.Keyring
	publisher *capturingRabbitMQPublisher
	producer  *streaming.Producer
}

func newOutboxBinary(t *testing.T, source string, table outbox.OutboxRepository, extra ...func(*streaming.Builder) *streaming.Builder) *outboxBinary {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	require.NoError(t, err)

	ring := builderSigningRing(t, "k-"+source, source)
	publisher := &capturingRabbitMQPublisher{}

	builder := streaming.NewBuilder().
		Source(source).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.rabbitmq.bus",
			DefinitionKey: "transaction.created",
			Target:        "rabbitmq-bus",
			Destination:   streaming.RabbitMQRoute("events", "tx.created"),
			Requirement:   streaming.RouteRequired,
		}).
		RabbitMQTarget("rabbitmq-bus", publisher).
		SignEnvelopes(ring, "k-"+source).
		OutboxRepository(table)

	for _, apply := range extra {
		builder = apply(builder)
	}

	emitter, err := builder.Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	producer, ok := emitter.(*streaming.Producer)
	require.True(t, ok, "Build returned %T", emitter)

	return &outboxBinary{source: source, ring: ring, publisher: publisher, producer: producer}
}

func sourceScoped(b *streaming.Builder) *streaming.Builder { return b.SourceScopedOutbox() }

var outboxOnly = streaming.DeliveryPolicyOverride{Direct: streaming.DirectModeSkip, Outbox: streaming.OutboxModeAlways}

func (b *outboxBinary) emitToOutbox(t *testing.T, n int) {
	t.Helper()

	for range n {
		require.NoError(t, b.producer.Emit(context.Background(), streaming.EmitRequest{
			DefinitionKey:  "transaction.created",
			TenantID:       "tenant-1",
			Payload:        transportSigningBody,
			PolicyOverride: outboxOnly,
		}))
	}
}

// relay runs one cycle of this binary's outbox dispatcher, wired the way the
// README tells a service to: its own handler registry, the caller-error
// classifier, and a claim scope of this producer's own outbox event type.
func (b *outboxBinary) relay(t *testing.T, table outbox.OutboxRepository) outbox.DispatchResult {
	t.Helper()

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, b.producer.RegisterOutboxRelay(registry))

	dispatcher, err := outbox.NewDispatcher(table, registry, nil, nil,
		outbox.WithPriorityEventTypes(b.producer.OutboxEventType()),
		outbox.WithRetryClassifier(outbox.RetryClassifierFunc(streaming.IsCallerError)),
		outbox.WithPublishMaxAttempts(1),
	)
	require.NoError(t, err)

	return dispatcher.DispatchOnceResult(context.Background())
}

// assertPublishedOnlyItsOwn checks every record this binary's publisher
// received carries its own ce-source and verifies under its own key.
func (b *outboxBinary) assertPublishedOnlyItsOwn(t *testing.T, want int) {
	t.Helper()

	b.publisher.mu.Lock()
	defer b.publisher.mu.Unlock()

	require.Len(t, b.publisher.headers, want, "records published by %s", b.source)

	verifier := transportVerifier(t, b.ring)

	for i, headers := range b.publisher.headers {
		assert.Equal(t, []byte(b.source), headers["ce-source"], "record %d of %s", i, b.source)
		require.NoError(t, verifier.Verify(headers, b.publisher.bodies[i]), "record %d of %s", i, b.source)
	}
}

func TestOutboxEventTypeForSource(t *testing.T) {
	t.Parallel()

	got, err := streaming.OutboxEventTypeForSource("svc-a")
	require.NoError(t, err)
	assert.Equal(t, streaming.StreamingOutboxEventType+".svc-a", got)

	_, err = streaming.OutboxEventTypeForSource("")
	require.ErrorIs(t, err, streaming.ErrMissingSource)

	_, err = streaming.OutboxEventTypeForSource("svc.a")
	require.ErrorIs(t, err, streaming.ErrInvalidSource)
}

func TestSourceScopedOutbox_EveryWritePathUsesTheQualifiedType(t *testing.T) {
	t.Parallel()

	table := &sharedOutboxTable{}
	binary := newOutboxBinary(t, "svc-scoped-writes", table, sourceScoped)

	want, err := streaming.OutboxEventTypeForSource("svc-scoped-writes")
	require.NoError(t, err)
	require.Equal(t, want, binary.producer.OutboxEventType())

	request := streaming.EmitRequest{
		DefinitionKey:  "transaction.created",
		TenantID:       "tenant-1",
		Payload:        transportSigningBody,
		PolicyOverride: outboxOnly,
	}

	// Write, WriteWithTx and WriteBatchWithTx, in that order.
	require.NoError(t, binary.producer.Emit(context.Background(), request))
	require.NoError(t, binary.producer.Emit(streaming.WithOutboxTx(context.Background(), &sql.Tx{}), request))
	require.NoError(t, binary.producer.EmitBatch(streaming.WithOutboxTx(context.Background(), &sql.Tx{}), []streaming.EmitRequest{request}))

	rows := table.snapshot()
	require.Len(t, rows, 3)

	for i, row := range rows {
		assert.Equal(t, want, row.EventType, "row %d", i)
	}
}

func TestSourceScopedOutbox_OptionMatchesTheBuilderSetter(t *testing.T) {
	t.Parallel()

	binary := newOutboxBinary(t, "svc-scoped-option", &sharedOutboxTable{}, func(b *streaming.Builder) *streaming.Builder {
		return b.Options(streaming.WithSourceScopedOutbox())
	})

	assert.Equal(t, streaming.StreamingOutboxEventType+".svc-scoped-option", binary.producer.OutboxEventType())
}

// Without the mode nothing changes: the rows and the relay keep the stable
// type byte for byte, so a service that never opts in sees no difference.
func TestSourceScopedOutbox_OffKeepsTheStableType(t *testing.T) {
	t.Parallel()

	table := &sharedOutboxTable{}
	binary := newOutboxBinary(t, "svc-unscoped", table)

	assert.Equal(t, "lerian.streaming.publish", binary.producer.OutboxEventType())

	binary.emitToOutbox(t, 1)

	rows := table.snapshot()
	require.Len(t, rows, 1)
	assert.Equal(t, "lerian.streaming.publish", rows[0].EventType)
}

func TestSourceScopedOutbox_RelayRegistersOnlyTheQualifiedType(t *testing.T) {
	t.Parallel()

	table := &sharedOutboxTable{}
	a := newOutboxBinary(t, "svc-reg-a", table, sourceScoped)
	b := newOutboxBinary(t, "svc-reg-b", table, sourceScoped)

	// Two source-scoped producers in one process share one registry without
	// colliding, because neither claims the stable type.
	registry := outbox.NewHandlerRegistry()
	require.NoError(t, a.producer.RegisterOutboxRelay(registry))
	require.NoError(t, b.producer.RegisterOutboxRelay(registry))

	a.emitToOutbox(t, 1)

	rows := table.snapshot()
	require.Len(t, rows, 1)

	own := rows[0]
	require.NoError(t, registry.Handle(context.Background(), &own))
	a.assertPublishedOnlyItsOwn(t, 1)

	stable := rows[0]
	stable.EventType = streaming.StreamingOutboxEventType
	require.ErrorIs(t, registry.Handle(context.Background(), &stable), outbox.ErrHandlerNotRegistered)
}

// The defect this mode exists for: two binaries of one service, one table,
// relays claiming by the stable type. Binary A's relay claims B's rows too,
// and its signing key refuses them, so B's facts are not published by the
// relay that claimed them.
func TestSharedOutboxTable_WithoutSourceScopingARelayClaimsAnotherBinarysRows(t *testing.T) {
	t.Parallel()

	table := &sharedOutboxTable{}
	a := newOutboxBinary(t, "svc-shared-a", table)
	b := newOutboxBinary(t, "svc-shared-b", table)

	a.emitToOutbox(t, 2)
	b.emitToOutbox(t, 2)

	result := a.relay(t, table)
	assert.Equal(t, 4, result.Processed, "A's relay claimed B's rows too")
	assert.Equal(t, 2, result.Failed)

	a.assertPublishedOnlyItsOwn(t, 2)
	b.assertPublishedOnlyItsOwn(t, 0)

	failed := table.errorsByStatus(outbox.OutboxStatusFailed)
	require.Len(t, failed, 2)

	for _, msg := range failed {
		assert.Contains(t, msg, streaming.ErrSigningSourceMismatch.Error())
	}
}

func TestSharedOutboxTable_SourceScopedRelaysPublishOnlyTheirOwnRows(t *testing.T) {
	t.Parallel()

	table := &sharedOutboxTable{}
	a := newOutboxBinary(t, "svc-shared-a", table, sourceScoped)
	b := newOutboxBinary(t, "svc-shared-b", table, sourceScoped)

	a.emitToOutbox(t, 2)
	b.emitToOutbox(t, 3)

	resultA := a.relay(t, table)
	assert.Equal(t, outbox.DispatchResult{Processed: 2, Published: 2}, resultA)

	resultB := b.relay(t, table)
	assert.Equal(t, outbox.DispatchResult{Processed: 3, Published: 3}, resultB)

	a.assertPublishedOnlyItsOwn(t, 2)
	b.assertPublishedOnlyItsOwn(t, 3)

	for i, row := range table.snapshot() {
		assert.Equal(t, outbox.OutboxStatusPublished, row.Status, "row %d (%s)", i, row.EventType)
	}
}
