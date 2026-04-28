//go:build unit

package streaming

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// fakeOutboxRepo is a test-only outbox.OutboxRepository. It captures Create
// and CreateWithTx calls separately so tests can assert which path was
// taken, and exposes per-call errors via createErr / createTxErr. Only the
// two create paths are used by T4; the remaining interface methods are
// stubbed to satisfy the contract without doing anything useful.
type fakeOutboxRepo struct {
	mu sync.Mutex

	created   []*outbox.OutboxEvent
	createErr error

	createdWithTx []*outbox.OutboxEvent
	createTxErr   error
}

func (f *fakeOutboxRepo) Create(_ context.Context, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return nil, f.createErr
	}

	f.created = append(f.created, event)

	return event, nil
}

func (f *fakeOutboxRepo) CreateWithTx(
	_ context.Context,
	_ outbox.Tx,
	event *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createTxErr != nil {
		return nil, f.createTxErr
	}

	f.createdWithTx = append(f.createdWithTx, event)

	return event, nil
}

// Unused interface methods; return zero values so the compiler accepts the
// type as an outbox.OutboxRepository. T4 never calls these.
func (f *fakeOutboxRepo) ListPending(context.Context, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListPendingByType(context.Context, string, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) ListTenants(context.Context) ([]string, error) { return nil, nil }
func (f *fakeOutboxRepo) GetByID(context.Context, uuid.UUID) (*outbox.OutboxEvent, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) MarkPublished(context.Context, uuid.UUID, time.Time) error { return nil }
func (f *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string, int) error  { return nil }
func (f *fakeOutboxRepo) ListFailedForRetry(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetForRetry(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetStuckProcessing(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}
func (f *fakeOutboxRepo) MarkInvalid(context.Context, uuid.UUID, string) error { return nil }

func (f *fakeOutboxRepo) createdCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.created)
}

func (f *fakeOutboxRepo) createdWithTxCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.createdWithTx)
}

func (f *fakeOutboxRepo) firstCreated() *outbox.OutboxEvent {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.created) == 0 {
		return nil
	}

	return f.created[0]
}

// --- T4 tests. ---

// TestProducer_Emit_CircuitOpen_WithOutbox_WritesRowAndReturnsNil is the
// primary T4 acceptance scenario. With the breaker OPEN and an
// OutboxRepository configured, Emit must:
//
//  1. Return nil to the caller (no error surfaces).
//  2. Write exactly one outbox row via Create (NOT CreateWithTx in v1).
//  3. Populate EventType with the topic name so the Dispatcher can route
//     it back via a registered handler.
//  4. Populate Payload with the JSON marshaling of the streaming.Event so
//     the replay handler can reconstruct the original message.
//  5. NOT call CreateWithTx in the absence of an ambient tx.
//  6. Not touch the broker.
func TestProducer_Emit_CircuitOpen_WithOutbox_WritesRowAndReturnsNil(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	fakeMgr := newFakeCBManager()
	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	// Drive the breaker OPEN via the listener.
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	if got := p.cbStateFlag.Load(); got != flagCBOpen {
		t.Fatalf("circuitState after OPEN transition = %d; want %d", got, flagCBOpen)
	}

	request := sampleRequest()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, request); err != nil {
		t.Fatalf("Emit (CB open, outbox wired) err = %v; want nil", err)
	}

	// Exactly one outbox row written via Create.
	if got := fakeRepo.createdCount(); got != 1 {
		t.Errorf("outbox.Create call count = %d; want 1", got)
	}

	if got := fakeRepo.createdWithTxCount(); got != 0 {
		t.Errorf("outbox.CreateWithTx call count = %d; want 0 (no ambient tx)", got)
	}

	row := fakeRepo.firstCreated()
	if row == nil {
		t.Fatal("outbox row is nil; Create was not called with an event")
	}

	// EventType is the stable streaming relay type; the concrete Kafka topic
	// lives inside the versioned payload envelope.
	wantType := StreamingOutboxEventType
	if row.EventType != wantType {
		t.Errorf("row.EventType = %q; want %q", row.EventType, wantType)
	}

	var envelope OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &envelope); err != nil {
		t.Fatalf("unmarshal row.Payload err = %v", err)
	}
	if envelope.Topic != "lerian.streaming.transaction.created" {
		t.Errorf("envelope.Topic = %q; want lerian.streaming.transaction.created", envelope.Topic)
	}
	if envelope.DefinitionKey != request.DefinitionKey {
		t.Errorf("envelope.DefinitionKey = %q; want %q", envelope.DefinitionKey, request.DefinitionKey)
	}
	if envelope.Policy != DefaultDeliveryPolicy() {
		t.Errorf("envelope.Policy = %+v; want %+v", envelope.Policy, DefaultDeliveryPolicy())
	}

	if envelope.Event.TenantID != request.TenantID {
		t.Errorf("round-tripped TenantID = %q; want %q", envelope.Event.TenantID, request.TenantID)
	}
	if envelope.Event.ResourceType != "transaction" {
		t.Errorf("round-tripped ResourceType = %q; want transaction", envelope.Event.ResourceType)
	}
	if envelope.Event.EventType != "created" {
		t.Errorf("round-tripped EventType = %q; want created", envelope.Event.EventType)
	}

	// ID is a non-nil UUID.
	if row.ID == uuid.Nil {
		t.Error("row.ID = uuid.Nil; want a fresh UUID")
	}

	// AggregateID is deterministic from the partition key.
	wantAgg := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(request.TenantID))
	if row.AggregateID != wantAgg {
		t.Errorf("row.AggregateID = %v; want %v (deterministic from tenant)", row.AggregateID, wantAgg)
	}

	// Broker must not have seen any records — the circuit-open path
	// MUST bypass publishDirect entirely.
	consumer := newConsumer(t, cluster, "lerian.streaming.transaction.created")

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)

	var count int
	fetches.EachRecord(func(_ *kgo.Record) { count++ })

	if count != 0 {
		t.Errorf("records fetched = %d; want 0 (circuit-open path must not touch broker)", count)
	}
}

// TestProducer_Emit_CircuitOpen_NoOutbox_ReturnsErrCircuitOpen preserves
// T3 behavior: with no outbox configured, circuit-open still surfaces
// ErrCircuitOpen to the caller. Regression guard.
func TestProducer_Emit_CircuitOpen_NoOutbox_ReturnsErrCircuitOpen(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	fakeMgr := newFakeCBManager()

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		// Deliberately NO WithOutboxRepository — this is the T3 fallback.
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	err = emitter.Emit(context.Background(), sampleRequest())
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Emit err = %v; want ErrCircuitOpen (no outbox wired)", err)
	}
}

func TestProducer_Emit_CircuitOpen_NilOutboxRepositoryBehavesAsNoOutbox(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	fakeMgr := newFakeCBManager()

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(nil),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	err = emitter.Emit(context.Background(), sampleRequest())
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Emit err = %v; want ErrCircuitOpen", err)
	}
}

type typedNilOutboxWriter struct{}

func (*typedNilOutboxWriter) Write(context.Context, OutboxEnvelope) error {
	return nil
}

type recordingOutboxWriter struct {
	envelopes []OutboxEnvelope
}

func (w *recordingOutboxWriter) Write(_ context.Context, envelope OutboxEnvelope) error {
	w.envelopes = append(w.envelopes, envelope)

	return nil
}

func TestProducer_Emit_CircuitOpen_TypedNilOutboxWriterBehavesAsNoOutbox(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	fakeMgr := newFakeCBManager()

	var writer *typedNilOutboxWriter
	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxWriter(writer),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	err = emitter.Emit(context.Background(), sampleRequest())
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Emit err = %v; want ErrCircuitOpen", err)
	}
}

func TestProducer_Emit_CircuitOpen_CustomOutboxWriterRoutesEnvelope(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	fakeMgr := newFakeCBManager()
	writer := &recordingOutboxWriter{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxWriter(writer),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	req := sampleRequest()
	if err := emitter.Emit(context.Background(), req); err != nil {
		t.Fatalf("Emit err = %v; want nil from custom outbox writer", err)
	}

	if got := len(writer.envelopes); got != 1 {
		t.Fatalf("custom writer envelope count = %d; want 1", got)
	}

	envelope := writer.envelopes[0]
	if err := envelope.Validate(); err != nil {
		t.Fatalf("custom writer envelope Validate err = %v", err)
	}
	if envelope.DefinitionKey != req.DefinitionKey {
		t.Errorf("envelope.DefinitionKey = %q; want %q", envelope.DefinitionKey, req.DefinitionKey)
	}
	if envelope.AggregateID == uuid.Nil {
		t.Error("envelope.AggregateID = uuid.Nil; want deterministic aggregate id")
	}
	wantEvent := sampleEvent()
	if want := wantEvent.Topic(); envelope.Topic != want {
		t.Errorf("envelope.Topic = %q; want %q", envelope.Topic, want)
	}
	if envelope.Event.TenantID != req.TenantID {
		t.Errorf("envelope.Event.TenantID = %q; want %q", envelope.Event.TenantID, req.TenantID)
	}
	if envelope.Event.ResourceType != wantEvent.ResourceType {
		t.Errorf("envelope.Event.ResourceType = %q; want %q", envelope.Event.ResourceType, wantEvent.ResourceType)
	}
	if envelope.Event.EventType != wantEvent.EventType {
		t.Errorf("envelope.Event.EventType = %q; want %q", envelope.Event.EventType, wantEvent.EventType)
	}
	if string(envelope.Event.Payload) != string(req.Payload) {
		t.Errorf("envelope.Event.Payload = %s; want %s", envelope.Event.Payload, req.Payload)
	}
	if envelope.Policy != DefaultDeliveryPolicy() {
		t.Errorf("envelope.Policy = %+v; want %+v", envelope.Policy, DefaultDeliveryPolicy())
	}
}

// TestProducer_Emit_CircuitOpen_OutboxFailure_SurfacesError: if the outbox
// write itself fails, the caller MUST see the error. Silent drop here
// would lose the event completely — the single most unacceptable failure
// mode in a streaming pipeline.
func TestProducer_Emit_CircuitOpen_OutboxFailure_SurfacesError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	fakeMgr := newFakeCBManager()
	fakeRepo := &fakeOutboxRepo{
		createErr: errors.New("db down"),
	}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	err = emitter.Emit(context.Background(), sampleRequest())
	if err == nil {
		t.Fatal("Emit err = nil; want non-nil error (outbox failure must surface)")
	}

	if !strings.Contains(err.Error(), "db down") {
		t.Errorf("Emit err = %q; want substring %q (root cause must propagate)", err.Error(), "db down")
	}
}

// TestProducer_PublishToOutbox_WithAmbientTx_CallsCreateWithTx exercises
// the ambient-tx branch through the exported WithOutboxTx helper.
//
// We use a non-nil &sql.Tx{} as a pointer witness — it's unusable for
// real SQL (zero-value), but the fake repo only checks identity, so
// this is sufficient to prove the branch routes to CreateWithTx.
func TestProducer_PublishToOutbox_WithAmbientTx_CallsCreateWithTx(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	fakeTx := &sql.Tx{}
	ctx := WithOutboxTx(context.Background(), fakeTx)

	event := sampleEvent()
	(&event).ApplyDefaults()

	policy := DefaultDeliveryPolicy().Normalize()

	if err := p.publishToOutbox(ctx, event, event.Topic(), policy, "transaction.created"); err != nil {
		t.Fatalf("publishToOutbox(ctx with tx) err = %v", err)
	}

	// CreateWithTx path taken exactly once; Create path not taken.
	if got := fakeRepo.createdWithTxCount(); got != 1 {
		t.Errorf("fakeRepo.createdWithTxCount = %d; want 1", got)
	}

	if got := fakeRepo.createdCount(); got != 0 {
		t.Errorf("fakeRepo.createdCount = %d; want 0 (tx branch should skip Create)", got)
	}
}

// TestProducer_PublishToOutbox_WithAmbientTx_PropagatesError covers the
// error path of the CreateWithTx branch so a repository failure inside a
// caller's transaction still surfaces. Together with the happy-path test
// above, this completes v1.1-readiness coverage on the ambient-tx flow.
func TestProducer_PublishToOutbox_WithAmbientTx_PropagatesError(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	fakeRepo := &fakeOutboxRepo{
		createTxErr: errors.New("tx rolled back"),
	}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	ctx := WithOutboxTx(context.Background(), &sql.Tx{})

	event := sampleEvent()
	(&event).ApplyDefaults()

	policy := DefaultDeliveryPolicy().Normalize()

	err = p.publishToOutbox(ctx, event, event.Topic(), policy, "transaction.created")
	if err == nil {
		t.Fatal("publishToOutbox err = nil; want non-nil (tx-path error must surface)")
	}

	if !strings.Contains(err.Error(), "tx rolled back") {
		t.Errorf("err = %q; want substring %q", err.Error(), "tx rolled back")
	}
}

type nonTransactionalOutboxWriter struct{}

func (nonTransactionalOutboxWriter) Write(context.Context, OutboxEnvelope) error {
	return nil
}

func TestProducer_PublishToOutbox_WithAmbientTx_RequiresTransactionalWriter(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxWriter(nonTransactionalOutboxWriter{}),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	event := sampleEvent()
	event.ApplyDefaults()
	policy := DefaultDeliveryPolicy().Normalize()

	err = p.publishToOutbox(WithOutboxTx(context.Background(), &sql.Tx{}), event, event.Topic(), policy, "transaction.created")
	if !errors.Is(err, ErrOutboxTxUnsupported) {
		t.Fatalf("publishToOutbox err = %v; want ErrOutboxTxUnsupported", err)
	}
}

// TestProducer_PublishToOutbox_NilReceiver is the defensive nil-receiver
// guard. Unreachable in practice (publishToOutbox is unexported and only
// called through Emit which itself guards against nil), but promised by
// the godoc contract.
func TestProducer_PublishToOutbox_NilReceiver(t *testing.T) {
	t.Parallel()

	var p *Producer
	ev := sampleEvent()
	policy := DefaultDeliveryPolicy().Normalize()

	err := p.publishToOutbox(context.Background(), ev, ev.Topic(), policy, "")
	if !errors.Is(err, ErrNilProducer) {
		t.Errorf("nil.publishToOutbox err = %v; want ErrNilProducer", err)
	}
}

// TestProducer_PublishToOutbox_NoRepoConfigured asserts the
// ErrOutboxNotConfigured sentinel fires when publishToOutbox is called on
// a Producer that wasn't built with WithOutboxRepository. This branch is
// theoretically unreachable from Emit (which checks p.outbox != nil before
// calling) but covers any future caller that forgets the guard.
func TestProducer_PublishToOutbox_NoRepoConfigured(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	ev := sampleEvent()
	policy := DefaultDeliveryPolicy().Normalize()

	err = p.publishToOutbox(context.Background(), ev, ev.Topic(), policy, "")
	if !errors.Is(err, ErrOutboxNotConfigured) {
		t.Errorf("publishToOutbox without repo err = %v; want ErrOutboxNotConfigured", err)
	}
}

// TestProducer_PublishToOutbox_MarshalFailure forces the json.Marshal
// branch to fail. Event.Payload is json.RawMessage, which implements
// json.Marshaler — an invalid RawMessage causes json.Marshal(event) to
// return an error. Reachable from publishToOutbox only by bypassing
// preFlight (which is what the unexported call path lets us do for this
// test). Ensures the marshal-error branch is not silently swallowed.
func TestProducer_PublishToOutbox_MarshalFailure(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	// json.RawMessage.MarshalJSON returns an error for non-JSON bytes.
	// We bypass preFlight here by calling publishToOutbox directly — this
	// is the defense-in-depth branch in case a future caller reaches
	// publishToOutbox without preFlight first.
	bad := sampleEvent()
	bad.Payload = []byte("{bad-json")

	policy := DefaultDeliveryPolicy().Normalize()

	err = p.publishToOutbox(context.Background(), bad, bad.Topic(), policy, "")
	if err == nil {
		t.Fatal("publishToOutbox(bad json) err = nil; want marshal error")
	}

	if !strings.Contains(err.Error(), "marshal") {
		t.Errorf("err = %q; want substring %q", err.Error(), "marshal")
	}

	// The outbox repo must NOT have been called — we bail before any
	// write when marshaling fails.
	if got := fakeRepo.createdCount(); got != 0 {
		t.Errorf("fakeRepo.createdCount = %d; want 0 (no write on marshal failure)", got)
	}
}

// TestProducer_DeriveAggregateID_DeterministicForSameTenant: two events
// with the same TenantID + same PartitionKey semantics must collapse to
// the same AggregateID. This is the hash property operators rely on to
// correlate outbox rows to Kafka partitions.
func TestProducer_DeriveAggregateID_DeterministicForSameTenant(t *testing.T) {
	t.Parallel()

	e1 := sampleEvent()
	e2 := sampleEvent()

	if got1, got2 := deriveAggregateID(e1), deriveAggregateID(e2); got1 != got2 {
		t.Errorf("deriveAggregateID(same tenant) gave %v and %v; want identical", got1, got2)
	}
}

// TestProducer_DeriveAggregateID_DifferentForDifferentTenants: two events
// with different partition keys must produce different aggregate IDs —
// otherwise operators couldn't distinguish tenant streams in the outbox
// table.
func TestProducer_DeriveAggregateID_DifferentForDifferentTenants(t *testing.T) {
	t.Parallel()

	e1 := sampleEvent()
	e1.TenantID = "tenant-a"

	e2 := sampleEvent()
	e2.TenantID = "tenant-b"

	if got1, got2 := deriveAggregateID(e1), deriveAggregateID(e2); got1 == got2 {
		t.Errorf("deriveAggregateID(different tenants) collided on %v; want distinct", got1)
	}
}

// TestProducer_DeriveAggregateID_SystemEventRandomized: SystemEvent=true
// events produce random UUIDs (not derived from partition key). Two
// consecutive calls MUST differ. This is the escape hatch that keeps
// every "system:*" event from collapsing into a single aggregate.
func TestProducer_DeriveAggregateID_SystemEventRandomized(t *testing.T) {
	t.Parallel()

	e := sampleEvent()
	e.TenantID = ""
	e.SystemEvent = true

	a1 := deriveAggregateID(e)
	a2 := deriveAggregateID(e)

	if a1 == a2 {
		t.Errorf("deriveAggregateID(SystemEvent=true) repeated %v; want distinct UUIDs", a1)
	}

	if a1 == uuid.Nil || a2 == uuid.Nil {
		t.Errorf("deriveAggregateID(SystemEvent=true) returned Nil UUID")
	}
}

// TestIsCallerError_OutboxSentinels_ReturnFalse confirms
// ErrOutboxNotConfigured and ErrNilOutboxRegistry are infrastructure-
// level, not caller-correctable. Callers cannot "fix" them from their
// side — they are bootstrap/wiring faults.
func TestIsCallerError_OutboxSentinels_ReturnFalse(t *testing.T) {
	t.Parallel()

	cases := []error{
		ErrOutboxNotConfigured,
		ErrNilOutboxRegistry,
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Error(), func(t *testing.T) {
			t.Parallel()
			if IsCallerError(tc) {
				t.Errorf("IsCallerError(%v) = true; want false (infra fault)", tc)
			}
		})
	}
}

// TestWithOutboxTx_NilTxStoresNothing locks the documented contract: calling
// WithOutboxTx with a nil *sql.Tx still attaches a value to the context (for
// type consistency), but the retrieval path in publishToOutbox guards on
// `tx != nil` so a nil-tx context is indistinguishable from a context without
// any WithOutboxTx call. This prevents a nil pointer deref in CreateWithTx.
func TestWithOutboxTx_NilTxStoresNothing(t *testing.T) {
	t.Parallel()

	ctx := WithOutboxTx(context.Background(), nil)

	// Retrieval with the (unexported) key mirrors what publishToOutbox does
	// internally. The type-asserted value is a typed-nil *sql.Tx — the
	// publishToOutbox guard `ok && tx != nil` is what ensures the
	// transactional branch is NOT taken.
	raw := ctx.Value(txContextKey{})
	tx, ok := raw.(*sql.Tx)

	if !ok {
		t.Fatalf("ctx.Value(txContextKey) type assertion = !ok; stored value = %v", raw)
	}
	if tx != nil {
		t.Errorf("ctx.Value(txContextKey) *sql.Tx = %v; want nil (the nil-tx guard path)", tx)
	}
}
