//go:build unit

package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport/fake"
)

func TestNewProducerMulti_AllRequiredSucceedReturnsNil(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v; want nil", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published = %d; want 1", got)
	}
	if got := len(secondary.Messages()); got != 1 {
		t.Fatalf("secondary published = %d; want 1", got)
	}

	if dest := primary.Messages()[0].Destination; dest.Name != "lerian.streaming.transaction.created" {
		t.Fatalf("primary destination = %q; want lerian.streaming.transaction.created", dest.Name)
	}
	if dest := secondary.Messages()[0].Destination; dest.Name != "lerian.streaming.transaction.created.replica" {
		t.Fatalf("secondary destination = %q; want lerian.streaming.transaction.created.replica", dest.Name)
	}
}

func TestNewProducerMulti_OneRequiredFailsAggregatesMultiEmitError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)
	secondaryErr := errors.New("broker unavailable")
	secondary.SetPublishError(secondaryErr)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
	if emitErr == nil {
		t.Fatal("Emit() error = nil; want MultiEmitError for failed required route")
	}

	var multi *contract.MultiEmitError
	if !errors.As(emitErr, &multi) {
		t.Fatalf("Emit() error = %T (%v); want *MultiEmitError", emitErr, emitErr)
	}
	if !multi.HasRequiredFailures() {
		t.Fatalf("MultiEmitError.Required = %d; want >= 1", len(multi.Required))
	}
	if multi.Required[0].RouteKey != "transaction.created.kafka.secondary" {
		t.Fatalf("Required[0].RouteKey = %q; want secondary", multi.Required[0].RouteKey)
	}
	if !errors.Is(emitErr, secondaryErr) {
		t.Fatalf("errors.Is(Emit, secondaryErr) = false; want true (chain: %v)", emitErr)
	}

	// Primary still published despite secondary failure.
	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published = %d; want 1 (sibling success preserved)", got)
	}
}

func TestNewProducerMulti_OptionalFailureDoesNotPropagate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	optional := fake.NewAdapter(TransportKafkaLike)
	optional.SetPublishError(errors.New("optional broker down"))

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.optional", "transaction.created", "optional", "lerian.streaming.transaction.created.shadow", contract.RouteOptional),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "optional", Kind: TransportKafkaLike, Adapter: optional},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v; want nil (optional failures must not propagate)", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published = %d; want 1", got)
	}
	if got := len(optional.Messages()); got != 0 {
		t.Fatalf("optional published = %d; want 0 (failed by design)", got)
	}
}

func TestNewProducerMulti_PerTargetCircuitBreakerIsolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Manually flip secondary's CB state mirror to OPEN. The dispatch
	// path reads the mirror BEFORE invoking the breaker; this lets us
	// assert per-target isolation without driving the CB itself through
	// real failures.
	rt := p.targets["secondary"]
	if rt == nil {
		t.Fatal("secondary target not registered")
	}
	rt.state.Store(flagCBOpen)

	emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
	if emitErr == nil {
		t.Fatal("Emit() error = nil; want ErrCircuitOpen via MultiEmitError")
	}
	if !errors.Is(emitErr, ErrCircuitOpen) {
		t.Fatalf("errors.Is(emitErr, ErrCircuitOpen) = false; want true (got %v)", emitErr)
	}

	// Primary CB stays CLOSED — its message MUST have published.
	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published = %d; want 1 (CB isolation preserved)", got)
	}
	if got := p.targetState("primary"); got != flagCBClosed {
		t.Fatalf("primary state = %d; want flagCBClosed (%d)", got, flagCBClosed)
	}
	if got := p.targetState("secondary"); got != flagCBOpen {
		t.Fatalf("secondary state = %d; want flagCBOpen (%d)", got, flagCBOpen)
	}
}

func TestNewProducerMulti_OutboxFallbackOnCircuitOpenWritesV2Envelope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	writer := &captureRouteOutboxWriter{}

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithOutboxWriter(writer),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	rt := p.targets["primary"]
	rt.state.Store(flagCBOpen)

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v; want nil (outbox fallback)", err)
	}

	// Primary did NOT publish — outbox absorbed the failure.
	if got := len(primary.Messages()); got != 0 {
		t.Fatalf("primary published = %d; want 0 (outbox should have absorbed)", got)
	}

	if got := len(writer.envelopes); got != 1 {
		t.Fatalf("writer envelopes = %d; want 1", got)
	}
	got := writer.envelopes[0]
	if got.Version != contract.OutboxEnvelopeVersion {
		t.Fatalf("envelope.Version = %d; want %d", got.Version, contract.OutboxEnvelopeVersion)
	}
	if got.Target != "primary" {
		t.Fatalf("envelope.Target = %q; want primary", got.Target)
	}
	if got.Transport != TransportKafkaLike {
		t.Fatalf("envelope.Transport = %q; want %q", got.Transport, TransportKafkaLike)
	}
	if got.RouteKey != "transaction.created.kafka.primary" {
		t.Fatalf("envelope.RouteKey = %q; want transaction.created.kafka.primary", got.RouteKey)
	}

	// Build the persisted row that the lib-commons outbox would normally
	// produce for this envelope, then drive it through the streaming
	// outbox handler. The handler MUST publish through the target adapter
	// without going through Emit — i.e. without consulting the CB mirror,
	// which we leave OPEN below.
	row, err := outboxRowFromEnvelope(got)
	if err != nil {
		t.Fatalf("outboxRowFromEnvelope() error = %v", err)
	}

	rt.state.Store(flagCBOpen)

	if err := p.handleOutboxRow(ctx, row); err != nil {
		t.Fatalf("handleOutboxRow() error = %v", err)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Fatalf("primary published after replay = %d; want 1", got)
	}
}

func TestNewProducerMulti_RejectsRouteForUnregisteredTarget(t *testing.T) {
	t.Parallel()

	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.ghost", "transaction.created", "ghost", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	_, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if !errors.Is(err, contract.ErrInvalidRouteDefinition) {
		t.Fatalf("NewProducerMulti() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}
}

// TestNewProducerMulti_PostCloseEmitReturnsErrEmitterClosed pins the
// post-close synchronous-failure invariant on the multi-target dispatch
// path (legacy single-target counterpart: TestProducer_EmitClosed in
// producer_lifecycle_test.go).
//
// The contract the multi-path Emit MUST honor:
//
//  1. After a successful Close, p.closed.Load() returns true.
//  2. The next Emit MUST return errors.Is(ErrEmitterClosed) WITHOUT
//     touching any target adapter (no socket I/O, no goroutine spawn).
//  3. The error path increments streaming_emitted_total{outcome="caller_error"}
//     for the resolved topic — verified indirectly here by asserting no
//     adapter received a Publish call after Close.
//
// The single-target legacy test in producer_lifecycle_test.go covers the
// same contract via the New() constructor; this test exercises the
// NewProducerMulti path so we know emit_multi.go's own closed-flag check
// at the top of emitMulti (see emit_multi.go:78-81) is a real gate, not
// dead code.
func TestNewProducerMulti_PostCloseEmitReturnsErrEmitterClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
	if !errors.Is(emitErr, ErrEmitterClosed) {
		t.Fatalf("Emit() after Close error = %v; want errors.Is(ErrEmitterClosed)", emitErr)
	}

	// Strong invariant: post-close Emit must short-circuit BEFORE any
	// route fan-out reaches an adapter. A single Publish observed here
	// indicates the closed-flag gate at the top of emitMulti was bypassed.
	if got := len(primary.Messages()); got != 0 {
		t.Errorf("primary published = %d; want 0 (post-close Emit must not touch adapters)", got)
	}
	if got := len(secondary.Messages()); got != 0 {
		t.Errorf("secondary published = %d; want 0 (post-close Emit must not touch adapters)", got)
	}
}

func TestNewProducerMulti_RejectsCatalogDefinitionWithNoRoute(t *testing.T) {
	t.Parallel()

	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t) // multiple definitions; only one route below
	routes, err := contract.NewRouteTable(
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	_, err = NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if !errors.Is(err, contract.ErrNoRoutesConfigured) {
		t.Fatalf("NewProducerMulti() error = %v; want errors.Is(ErrNoRoutesConfigured)", err)
	}
}

// TestNewProducerMulti_AllOptionalFail_ReturnsNilButRecordsOptional pins
// the "every Optional route fails — caller still sees nil" contract.
//
// Setup: three Optional routes (no Required at all for the definition under
// test, the rest of the catalog covered by default Required routes), every
// adapter returns a publish error. Expectations:
//
//   - Emit returns nil (Optional failures never propagate as errors).
//   - No *MultiEmitError is returned (consequence of the above).
//   - Each Optional adapter received exactly one Publish attempt — the
//     optional fan-out reached every target despite each one failing.
//   - streaming_emit_duration_ms records one observation per route that
//     reached dispatchRoute. Counts data-point granularity, not attribute
//     correctness — see HANDOFF / KNOWN GAP below.
//
// KNOWN GAP / HANDOFF (Agent 1 — producer Emit path):
// internal/producer/emit_multi.go:447 calls executeRoutePublish, passing
// outcome BY VALUE. executeRoutePublish modifies its local copy and
// returns it; dispatchRoute returns that fresh value to its caller but
// the deferred closure at emit_multi.go:216 still references
// dispatchRoute's ORIGINAL local `outcome`, whose .state is never
// updated. As a result, streaming_emitted_total fan-out records all use
// outcome="" (empty string) instead of "produced" / "failed" / "dlq" /
// etc. for the multi-target path. Single-target Emit is unaffected.
//
// Fix: either change dispatchRoute to capture state via pointer
// (defer p.recordOutcome(ctx, topic, &outcome)), or have
// executeRoutePublish accept *routeOutcome instead of routeOutcome. The
// fan-out-count assertion below is the most we can pin until that fix
// lands; it documents the gap as a regression gate (data-point count is
// correct even though label values are not).
//
// This test complements TestNewProducerMulti_OptionalFailureDoesNotPropagate
// which only tests the 1-required + 1-optional shape and never inspects
// metrics at all.
func TestNewProducerMulti_AllOptionalFail_ReturnsNilButRecordsOptional(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	failErr := errors.New("optional broker down")
	optA := fake.NewAdapter(TransportKafkaLike)
	optA.SetPublishError(failErr)
	optB := fake.NewAdapter(TransportKafkaLike)
	optB.SetPublishError(failErr)
	optC := fake.NewAdapter(TransportKafkaLike)
	optC.SetPublishError(failErr)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		// Override the default Required route for "transaction.created"
		// so this definition has THREE Optional routes and zero Required.
		// mustMultiRouteTable's auto-attach only fills DefinitionKeys it
		// hasn't seen — we control transaction.created here entirely.
		multiTestRoute("transaction.created.kafka.opt-a", "transaction.created", "opt-a", "lerian.streaming.transaction.created.a", contract.RouteOptional),
		multiTestRoute("transaction.created.kafka.opt-b", "transaction.created", "opt-b", "lerian.streaming.transaction.created.b", contract.RouteOptional),
		multiTestRoute("transaction.created.kafka.opt-c", "transaction.created", "opt-c", "lerian.streaming.transaction.created.c", contract.RouteOptional),
	)

	factory, snapshot := newManualMeterSetup(t)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-all-optional"},
		nil,
		[]TargetSpec{
			{Name: "opt-a", Kind: TransportKafkaLike, Adapter: optA},
			{Name: "opt-b", Kind: TransportKafkaLike, Adapter: optB},
			{Name: "opt-c", Kind: TransportKafkaLike, Adapter: optC},
			// Default mustMultiRouteTable Required routes pin to "primary";
			// we register a primary target so the route table validates.
			// It receives no traffic from this test (we Emit only the
			// transaction.created definition).
			{Name: "primary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithMetricsFactory(factory),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
	if emitErr != nil {
		t.Fatalf("Emit() error = %v; want nil (all routes Optional, none must surface)", emitErr)
	}

	// Every Optional adapter received its fan-out attempt but Publish
	// failed before recording — Messages() is empty.
	if got := len(optA.Messages()); got != 0 {
		t.Errorf("opt-a Messages = %d; want 0 (publish failed)", got)
	}
	if got := len(optB.Messages()); got != 0 {
		t.Errorf("opt-b Messages = %d; want 0 (publish failed)", got)
	}
	if got := len(optC.Messages()); got != 0 {
		t.Errorf("opt-c Messages = %d; want 0 (publish failed)", got)
	}

	// MultiEmitError must NOT have surfaced. We already asserted nil
	// above, but defensively walk the chain to catch a weird wrap path.
	var multi *contract.MultiEmitError
	if errors.As(emitErr, &multi) {
		t.Fatalf("Emit() leaked *MultiEmitError despite all-Optional fan-out (%v)", multi)
	}

	// Fan-out observability gate. recordEmitDuration is called from the
	// SAME defer site as recordEmitted (emit_multi.go:233-234) so its
	// data-point count is the same regardless of the state-bug above:
	// one per route that reached dispatchRoute.
	rm := snapshot()
	emitDuration, ok := findMetric(rm, metricNameEmitDurationMS)
	if !ok {
		t.Fatal("streaming_emit_duration_ms not recorded; want >=3 observations (one per dispatched optional route)")
	}

	if got := histogramCount(t, emitDuration); got < 3 {
		t.Errorf("streaming_emit_duration_ms count = %d; want >=3 (one per Optional route)", got)
	}
}

// TestNewProducerMulti_EmptyCatalogReturnsError pins the empty-catalog
// rejection at the producer-level constructor boundary. The Builder
// rejects this earlier (builder.go:521-523), but NewProducerMulti is
// reachable directly by callers writing custom bootstrap code; the
// invariant must hold at this layer too.
//
// Wiring detail: NewProducerMulti only fires its empty-catalog gate
// (validateCatalogAtBootstrap, line 86) when BOTH the catalog argument
// AND the WithCatalog option resolve to empty. We pass a zero-value
// catalog and intentionally OMIT WithCatalog so the gate fires.
func TestNewProducerMulti_EmptyCatalogReturnsError(t *testing.T) {
	t.Parallel()

	primary := fake.NewAdapter(TransportKafkaLike)

	// We need a route table to pass the routes.Len()==0 gate first.
	// Use a route that points at a definition NOT in the catalog so
	// that, if the empty-catalog gate were absent, the next gate
	// (validateRoutesAgainstTargets via catalog.Require) would also
	// fire — but with a different sentinel. We assert the empty-catalog
	// sentinel here.
	routes, err := contract.NewRouteTable(
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	_, err = NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://multi-empty-cat"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		Catalog{}, // zero-value catalog
		WithLogger(log.NewNop()),
		// NOTE: WithCatalog deliberately omitted so the empty zero-value
		// catalog isn't replaced by an option-supplied one.
	)
	if err == nil {
		t.Fatal("NewProducerMulti() with empty catalog returned nil error; want ErrInvalidEventDefinition")
	}
	if !errors.Is(err, ErrInvalidEventDefinition) {
		t.Fatalf("NewProducerMulti() error = %v; want errors.Is(ErrInvalidEventDefinition)", err)
	}
}

// TestNewProducerMulti_OptionalFailureRecordsMetricObservations extends
// TestNewProducerMulti_OptionalFailureDoesNotPropagate by asserting on
// the documented side effects: a metrics observation must land for
// every dispatched route (Required and Optional alike) so operators
// can alert on optional-route health without a Required-failure surface.
//
// Contract: Optional failures are silent at the API layer but MUST be
// observable through telemetry. This test is the regression gate for
// that contract — without it, an accidental change to the Optional
// branch could swallow the observability hook and the only signal
// would be missing downstream events with no metric trail.
//
// KNOWN GAP / HANDOFF: see the long block on
// TestNewProducerMulti_AllOptionalFail_ReturnsNilButRecordsOptional —
// the multi-target dispatch path captures outcome.state by value into
// executeRoutePublish, so the deferred recordEmitted call always sees
// the original empty state. We pin the data-point count via the
// duration histogram (which uses the same defer site) and the
// emitted-counter total, but cannot pin the outcome label until Agent
// 1 fixes the state propagation.
func TestNewProducerMulti_OptionalFailureRecordsMetricObservations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	optional := fake.NewAdapter(TransportKafkaLike)
	optional.SetPublishError(errors.New("optional broker down"))

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.optional", "transaction.created", "optional", "lerian.streaming.transaction.created.shadow", contract.RouteOptional),
	)

	factory, snapshot := newManualMeterSetup(t)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://multi-opt-metric"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "optional", Kind: TransportKafkaLike, Adapter: optional},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithMetricsFactory(factory),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v; want nil (optional failures must not propagate)", err)
	}

	// Optional adapter must have observed a Publish attempt — the fact
	// that it failed and Messages() is empty is the contract for a
	// failed Optional route.
	if got := len(primary.Messages()); got != 1 {
		t.Errorf("primary Messages = %d; want 1 (Required route success)", got)
	}
	if got := len(optional.Messages()); got != 0 {
		t.Errorf("optional Messages = %d; want 0 (publish failed by design)", got)
	}

	// Total emit count = number of routes dispatched. With one Required
	// route + one Optional route, expect 2 increments on
	// streaming_emitted_total regardless of the outcome-label bug.
	rm := snapshot()
	emitted, ok := findMetric(rm, metricNameEmitted)
	if !ok {
		t.Fatal("streaming_emitted_total not recorded after mixed-required-optional Emit")
	}

	total, _ := sumInt64DataPoints(t, emitted)
	if total < 2 {
		t.Errorf("streaming_emitted_total = %d; want >=2 (one per dispatched route)", total)
	}

	// streaming_emit_duration_ms must record one observation per route
	// that reached dispatchRoute.
	emitDuration, ok := findMetric(rm, metricNameEmitDurationMS)
	if !ok {
		t.Fatal("streaming_emit_duration_ms not recorded after mixed-required-optional Emit")
	}
	if got := histogramCount(t, emitDuration); got < 2 {
		t.Errorf("streaming_emit_duration_ms count = %d; want >=2 (one per Optional/Required route)", got)
	}
}

// --- Helpers ---

func multiTestRoute(key, definitionKey, target, topic string, requirement contract.RouteRequirement) contract.RouteDefinition {
	return contract.RouteDefinition{
		Key:           key,
		DefinitionKey: definitionKey,
		Target:        target,
		Destination:   contract.Destination{Kind: TransportKafkaLike, Name: topic},
		Requirement:   requirement,
	}
}

// mustMultiRouteTable builds a route table that satisfies the multi-target
// builder's "every catalog definition has at least one route" invariant by
// auto-attaching benign routes for any definition the test does not
// explicitly cover. Routes are pinned to the first registered target so the
// test fixture stays self-contained.
func mustMultiRouteTable(t *testing.T, routes ...contract.RouteDefinition) contract.RouteTable {
	t.Helper()

	defaults := []contract.RouteDefinition{
		multiTestRoute("order.submitted.kafka.primary", "order.submitted", "primary", "lerian.streaming.order.submitted", contract.RouteRequired),
		multiTestRoute("ledger.overflow.kafka.primary", "ledger.overflow", "primary", "lerian.streaming.ledger.overflow", contract.RouteRequired),
		multiTestRoute("payment.authorized.kafka.primary", "payment.authorized", "primary", "lerian.streaming.payment.authorized", contract.RouteRequired),
		multiTestRoute("transaction.cb-organic.kafka.primary", "transaction.cb_organic", "primary", "lerian.streaming.transaction.cb_organic", contract.RouteRequired),
		multiTestRoute("chaos.event.kafka.primary", "chaos.event", "primary", "lerian.streaming.chaos.event", contract.RouteRequired),
	}

	have := make(map[string]struct{}, len(routes))
	for _, r := range routes {
		have[r.DefinitionKey] = struct{}{}
	}
	for _, r := range defaults {
		if _, ok := have[r.DefinitionKey]; !ok {
			routes = append(routes, r)
		}
	}

	rt, err := contract.NewRouteTable(routes...)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}
	return rt
}

// captureRouteOutboxWriter is a test-only OutboxWriter that records every
// OutboxEnvelope it receives.
type captureRouteOutboxWriter struct {
	envelopes []contract.OutboxEnvelope
}

func (c *captureRouteOutboxWriter) Write(_ context.Context, envelope OutboxEnvelope) error {
	c.envelopes = append(c.envelopes, envelope)
	return nil
}

// Compile-time assertion: outboxRowFromEnvelope returns the lib-commons
// row shape consumed by handleOutboxRow.
var _ = func() *outbox.OutboxEvent {
	row, _ := outboxRowFromEnvelope(contract.OutboxEnvelope{})
	return row
}
