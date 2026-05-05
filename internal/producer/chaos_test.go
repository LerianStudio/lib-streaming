//go:build chaos

// Package streaming chaos tests (T9 chaos).
//
// Chaos tests run toxiproxy between the Producer and a real Redpanda broker
// to simulate network degradation. Build tag: chaos. Dual-gate: CHAOS=1
// environment variable + `!testing.Short()`.
//
// How to run:
//
//	CHAOS=1 go test -tags=chaos -timeout=10m ./...
//
// Requires Docker for both the Redpanda and Toxiproxy containers. Skips
// cleanly when Docker is unavailable (same skipIfNoDocker pattern as the
// integration suite).
//
// Scenario (TestChaos_BrokerLatency_CircuitOpensAndOutboxCatches):
//
//  1. NORMAL: emit N healthy events; CB stays CLOSED.
//  2. INJECT: add 2s latency toxic; emit more events; some timeout → feed CB.
//  3. DEGRADED: CB transitions to OPEN within 5s; outbox absorbs emits.
//  4. RESTORE: remove latency toxic.
//  5. RECOVER: drain outbox via the replay handler; CB eventually CLOSED.

package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// chaosRedpandaImage pins the Redpanda tag used by chaos tests. Matches the
// integration suite so the chaos + integration runs share an image cache.
const chaosRedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.18"

// chaosToxiproxyImage pins the Toxiproxy tag. 2.x line is the only one with
// the HTTP API the client library expects.
const chaosToxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

// chaosSource is the CloudEvents source used in the chaos scenario. Matches
// the integration-test style so logs/spans collected during a chaos run can
// be visually correlated.
const chaosSource = "//lerian.test/streaming-chaos"

// chaosSkipIfDisabled implements the dual-gate CHAOS=1 + !testing.Short()
// requirement. Returns true when the test should be skipped.
func chaosSkipIfDisabled(t *testing.T) bool {
	t.Helper()

	if testing.Short() {
		t.Skip("chaos test skipped under -short")
		return true
	}

	if os.Getenv("CHAOS") != "1" {
		t.Skip("chaos test skipped; set CHAOS=1 to enable")
		return true
	}

	return false
}

// chaosSkipIfNoDocker mirrors the integration suite's skipIfNoDocker helper
// without importing the integration-tagged file. Any container start error
// containing well-known Docker-unavailable markers → t.Skip.
func chaosSkipIfNoDocker(t *testing.T, err error) bool {
	t.Helper()

	if err == nil {
		return false
	}

	msg := err.Error()
	if strings.Contains(msg, "Cannot connect to the Docker daemon") ||
		strings.Contains(msg, "docker socket") ||
		strings.Contains(msg, "Is the docker daemon running") ||
		strings.Contains(msg, "provider not implemented") {
		t.Skipf("Docker not available in this environment: %v", err)
		return true
	}

	return false
}

// chaosInMemoryOutbox is a minimal thread-safe OutboxRepository for the
// chaos scenario. Stores events in memory and exposes them for replay.
// Satisfies outbox.OutboxRepository interface methods the chaos path
// touches — unused methods stub to zero values. Naming chose
// "chaosInMemoryOutbox" to avoid colliding with outbox_handler_test.go's
// own fakes (which live under a different build tag).
type chaosInMemoryOutbox struct {
	mu    sync.Mutex
	rows  []*outbox.OutboxEvent
	drain []*outbox.OutboxEvent // mirrors rows after replay for verification
}

func (c *chaosInMemoryOutbox) Create(
	_ context.Context,
	event *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Defensive: a fresh copy so subsequent mutations through the same
	// pointer don't leak into our captured list.
	eventCopy := *event
	c.rows = append(c.rows, &eventCopy)

	return &eventCopy, nil
}

func (c *chaosInMemoryOutbox) CreateWithTx(
	ctx context.Context,
	_ outbox.Tx,
	event *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	return c.Create(ctx, event)
}

func (c *chaosInMemoryOutbox) ListPending(
	_ context.Context,
	limit int,
) ([]*outbox.OutboxEvent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	take := len(c.rows)
	if limit > 0 && limit < take {
		take = limit
	}

	out := make([]*outbox.OutboxEvent, take)
	copy(out, c.rows[:take])

	return out, nil
}

func (*chaosInMemoryOutbox) ListPendingByType(
	_ context.Context,
	_ string,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*chaosInMemoryOutbox) ListTenants(_ context.Context) ([]string, error) {
	return nil, nil
}

func (c *chaosInMemoryOutbox) GetByID(
	_ context.Context,
	id uuid.UUID,
) (*outbox.OutboxEvent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range c.rows {
		if r.ID == id {
			return r, nil
		}
	}

	return nil, nil
}

func (c *chaosInMemoryOutbox) MarkPublished(
	_ context.Context,
	id uuid.UUID,
	_ time.Time,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, r := range c.rows {
		if r.ID == id {
			c.drain = append(c.drain, r)
			c.rows = append(c.rows[:i], c.rows[i+1:]...)
			return nil
		}
	}

	return nil
}

func (*chaosInMemoryOutbox) MarkFailed(
	_ context.Context,
	_ uuid.UUID,
	_ string,
	_ int,
) error {
	return nil
}

func (*chaosInMemoryOutbox) ListFailedForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*chaosInMemoryOutbox) ResetForRetry(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*chaosInMemoryOutbox) ResetStuckProcessing(
	_ context.Context,
	_ int,
	_ time.Time,
	_ int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*chaosInMemoryOutbox) MarkInvalid(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) error {
	return nil
}

// pendingCount returns the number of outbox rows not yet drained.
func (c *chaosInMemoryOutbox) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.rows)
}

// drainedCount returns the number of rows that were published through
// MarkPublished — i.e., successfully replayed.
func (c *chaosInMemoryOutbox) drainedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.drain)
}

// snapshotPending returns a copy of the current pending rows so the caller
// can iterate without holding the lock.
func (c *chaosInMemoryOutbox) snapshotPending() []*outbox.OutboxEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]*outbox.OutboxEvent, len(c.rows))
	copy(out, c.rows)

	return out
}

// TestChaos_BrokerLatency_CircuitOpensAndOutboxCatches is the T9 chaos
// scenario. Runs the 5-phase drill described at the top of this file.
//
// Fixture topology:
//
//	test process
//	     │
//	     ▼  (advertised broker = toxiproxy host:port)
//	┌────────────┐                  ┌─────────────┐
//	│ toxiproxy  │ ── upstream ─►   │  redpanda   │
//	│ :8666 (in) │   redpanda-      │  :9092 (in) │
//	└────────────┘   chaos:9092     └─────────────┘
//	      ▲
//	      │ host:<random-mapped-port>
//
// Critical: Redpanda advertises the toxiproxy host:port (NOT its own
// host-mapped Kafka port) on the OUTSIDE listener. franz-go reads that
// advertised address from metadata responses and uses it for every
// subsequent produce — so produces traverse the proxy, not just bootstrap.
// This is why we bypass tcredpanda.Run (which would auto-render
// redpanda.yaml advertising redpanda's own port) and roll the container
// directly via testcontainers.GenericContainer (see startChaosRedpanda).
//
// Phase 0 starts toxiproxy first to discover its random host port, then
// passes that port as the advertised address when starting redpanda. All
// test traffic — topic admin, producer publishes, consumer fetches —
// traverses the same toxiproxy listener, so a single toxic affects the
// entire client surface during INJECT, and removing it in RESTORE returns
// the entire path to baseline latency.
func TestChaos_BrokerLatency_CircuitOpensAndOutboxCatches(t *testing.T) {
	if chaosSkipIfDisabled(t) {
		return
	}

	// --- Phase 0: infra setup. ---
	ctx := context.Background()

	// Shared docker network so toxiproxy can dial redpanda by container name.
	nw, err := tcnetwork.New(ctx)
	if chaosSkipIfNoDocker(t, err) {
		return
	}
	require.NoError(t, err, "network create")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = nw.Remove(cleanupCtx)
	})

	// Toxiproxy alias so internal traffic between the test stack stays
	// network-resolved by name. Redpanda alias serves the same purpose for
	// toxiproxy's upstream resolution.
	redpandaAlias := "redpanda-chaos"
	toxiproxyAlias := "toxiproxy-chaos"

	// Toxiproxy container FIRST: we need its random host-mapped port for
	// 8666/tcp before redpanda starts, since redpanda's advertised address
	// must match. Toxiproxy doesn't require its upstream (redpanda) to be
	// reachable at start; the proxy listener accepts connections immediately
	// and lazy-dials upstream on the first client connection.
	toxiproxyReq := testcontainers.ContainerRequest{
		Image:        chaosToxiproxyImage,
		ExposedPorts: []string{"8474/tcp", "8666/tcp"},
		WaitingFor:   wait.ForListeningPort("8474/tcp").WithStartupTimeout(60 * time.Second),
		Networks:     []string{nw.Name},
		NetworkAliases: map[string][]string{
			nw.Name: {toxiproxyAlias},
		},
		Cmd: []string{"-host=0.0.0.0"},
	}
	tpContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: toxiproxyReq,
		Started:          true,
	})
	if chaosSkipIfNoDocker(t, err) {
		return
	}
	require.NoError(t, err, "toxiproxy run")
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = tpContainer.Terminate(cleanupCtx)
	})

	// Resolve toxiproxy port mappings: the API port for the client library
	// and the Kafka proxy port that redpanda will advertise.
	tpHost, err := tpContainer.Host(ctx)
	require.NoError(t, err, "toxiproxy host")

	tpAPIPort, err := tpContainer.MappedPort(ctx, "8474/tcp")
	require.NoError(t, err, "toxiproxy api port")

	tpKafkaPort, err := tpContainer.MappedPort(ctx, "8666/tcp")
	require.NoError(t, err, "toxiproxy kafka port")

	// Now start redpanda. The advertised Kafka address points at the
	// toxiproxy host:port resolved above, so every Kafka client (admin,
	// producer, consumer) routes through the proxy on every operation —
	// not just bootstrap.
	rp := startChaosRedpanda(t, ctx, chaosRedpandaImage, nw, redpandaAlias, tpHost, int(tpKafkaPort.Num()))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = rp.Terminate(cleanupCtx)
	})

	// Configure toxiproxy: listen on 0.0.0.0:8666 inside the container,
	// forward to redpanda's network alias on its bound listener port (9092).
	tpClient := toxiproxy.NewClient(net.JoinHostPort(tpHost, tpAPIPort.Port()))

	proxy, err := tpClient.CreateProxy(
		"redpanda-kafka",
		"0.0.0.0:8666",
		redpandaAlias+":9092",
	)
	require.NoError(t, err, "create toxiproxy proxy")

	t.Cleanup(func() {
		_ = proxy.Delete()
	})

	// Verify the broker can answer through the toxiproxy path before we
	// start the chaos drill. Same address franz-go will use as seed.
	proxiedBroker := net.JoinHostPort(tpHost, tpKafkaPort.Port())
	chaosWaitForBroker(t, proxiedBroker)

	// Pre-create topic to avoid UNKNOWN_TOPIC_OR_PARTITION on the first
	// produce. Goes through the proxy (no toxic injected yet) — same path
	// the producer will use.
	chaosTopic := topicPrefix + "chaos.event"
	chaosEnsureTopic(t, proxiedBroker, chaosTopic)
	chaosEnsureTopic(t, proxiedBroker, dlqTopic(chaosTopic))

	// --- Phase 1: NORMAL — 100 healthy emits. ---

	store := &chaosInMemoryOutbox{}

	cfg := Config{
		Enabled:               true,
		Brokers:               []string{proxiedBroker},
		ClientID:              "chaos-producer",
		BatchLingerMs:         0,
		BatchMaxBytes:         defaultBatchMaxBytes,
		MaxBufferedRecords:    defaultMaxBufferedRecords,
		Compression:           defaultCompression,
		RecordRetries:         1,
		RecordDeliveryTimeout: 3 * time.Second,
		RequiredAcks:          defaultRequiredAcks,
		CBFailureRatio:        0.5,
		CBMinRequests:         5,
		CBTimeout:             10 * time.Second,
		CloseTimeout:          3 * time.Second,
		CloudEventsSource:     chaosSource,
	}

	p, err := NewProducer(ctx, cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(store),
	)
	require.NoError(t, err, "NewProducer")
	t.Cleanup(func() { _ = p.Close() })

	emitted := int64(0)
	const normalEmits = 100

	healthyEvent := func(i int) Event {
		return Event{
			TenantID:     "tenant-chaos",
			ResourceType: "chaos",
			EventType:    "event",
			Source:       chaosSource,
			Payload:      json.RawMessage(fmt.Sprintf(`{"seq":%d}`, i)),
		}
	}

	for i := 0; i < normalEmits; i++ {
		emitCtx, emitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoError(t, p.Emit(emitCtx, eventToRequest(healthyEvent(i))), "NORMAL emit %d", i)
		emitCancel()
		atomic.AddInt64(&emitted, 1)
	}

	require.Equal(t, int32(flagCBClosed), p.targetState("primary"),
		"after 100 healthy emits the breaker must be CLOSED")

	// --- Phase 2: INJECT — 4s latency toxic on downstream packets. ---

	// Toxic semantics worth understanding: this delays packets in the
	// broker→client direction by 4s. The produce REQUEST still flows fast
	// (upstream untouched), and Redpanda actually persists the record. Only
	// the produce ack response is delayed — by the time it arrives, our
	// 2s per-emit ctx (below) has already cancelled the wait. The Producer
	// sees ctx.DeadlineExceeded, the CB feeds it as a failure, and after
	// the configured threshold is met the breaker trips OPEN.
	//
	// We deliberately do NOT rely on RecordDeliveryTimeout to enforce the
	// failure: kgo treats RecordDeliveryTimeout as a pre-flight cap on
	// queued records, and does not preempt an in-flight produce that is
	// just receiving a slow response. The ctx cancellation in our adapter's
	// select on ctx.Done() is the load-bearing mechanism.
	_, err = proxy.AddToxic(
		"slow_responses",
		"latency",
		"downstream",
		1.0,
		toxiproxy.Attributes{
			"latency": 4000, // 4s downstream packet latency, > 2s per-emit ctx
		},
	)
	require.NoError(t, err, "add latency toxic")

	// Emit more events. Each will block on RecordDeliveryTimeout (3s) and
	// surface a timeout error. We don't assert per-emit result — the CB
	// transition is the load-bearing signal, checked in phase 3.
	const injectEmits = 50

	for i := 0; i < injectEmits; i++ {
		// 2s per-emit ctx is well below the 4s toxic latency. The select in
		// the kafka adapter's Publish takes the ctx.Done() branch at 2s,
		// returns context.DeadlineExceeded, and the CB feeds it as a failure.
		// Tight ctx also bounds Phase 2's wall-clock time: ~5 emits to trip
		// (default ConsecutiveFailures=5) × 2s ≈ 10s before early-exit.
		emitCtx, emitCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = p.Emit(emitCtx, eventToRequest(healthyEvent(1000+i)))
		emitCancel()

		// Early exit once CB flips to OPEN so we don't spend the full
		// injectEmits × 2s budget when the breaker has already done its job.
		if p.targetState("primary") == flagCBOpen {
			break
		}
	}

	// --- Phase 3: DEGRADED — CB transitions to OPEN within 5s. ---

	observedOpen := false
	pollCBDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(pollCBDeadline) {
		if p.targetState("primary") == flagCBOpen {
			observedOpen = true
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	require.True(t, observedOpen, "CB never transitioned to OPEN within 5s under latency injection")

	// With outbox wired, a subsequent Emit succeeds (returns nil) because
	// the outbox absorbs the hit.
	outboxCtx, outboxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, p.Emit(outboxCtx, eventToRequest(healthyEvent(5000))), "Emit during CB OPEN must succeed via outbox")
	outboxCancel()

	require.Positive(t, store.pendingCount(), "outbox must contain at least one pending row")

	// --- Phase 4: RESTORE — remove latency toxic. ---

	require.NoError(t, proxy.RemoveToxic("slow_responses"), "remove latency toxic")

	// --- Phase 5: RECOVER — drive CB through HALF-OPEN to CLOSED, drain outbox. ---

	// Register the replay handler. RegisterOutboxRelay wires publishDirect
	// (NOT Emit) so replays bypass the breaker and cannot re-enqueue.
	registry := outbox.NewHandlerRegistry()
	require.NoError(t, p.RegisterOutboxRelay(registry), "RegisterOutboxRelay")

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()

	// Drive CB recovery in two stages — both load-bearing:
	//
	//  1. OPEN→HALF-OPEN: gobreaker transitions lazily when CBTimeout has
	//     elapsed AND something calls breaker.State() or Execute(). The
	//     dispatchRoute fast-path at emit_multi.go:407 SKIPS cb.Execute
	//     when the mirror is OPEN, so Emit alone never drives this
	//     transition. The Producer's background CB recovery goroutine
	//     (cb_recovery.go) calls manager.GetState on every target every
	//     ~CBTimeout/4 (clamped), forcing the lazy check; the listener
	//     then updates the mirror to HALF-OPEN. This test relies on that
	//     production behaviour — no manual poke from the test.
	//
	//  2. HALF-OPEN→CLOSED: once the mirror reflects HALF-OPEN, the
	//     fast-path falls through to cb.Execute. Each successful probe
	//     Emit increments ConsecutiveSuccesses; reaching MaxRequests=3
	//     transitions the breaker to CLOSED. The listener updates the
	//     mirror, the loop's next read sees CLOSED and exits.
	//
	// Probe Emits below drive stage 2 once stage 1 has flipped the mirror.
	// While the mirror is still OPEN, probes are outbox-absorbed (no harm,
	// just doesn't advance recovery — the background goroutine is doing
	// that work in parallel).
	probeEmits := 0
	successfulProbeSeqs := make([]int, 0)
	pollClosedDeadline := time.Now().Add(20 * time.Second)
	observedClosed := false

	for time.Now().Before(pollClosedDeadline) {
		if p.targetState("primary") == flagCBClosed {
			observedClosed = true
			break
		}

		probeSeq := 9990 + probeEmits
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := p.Emit(probeCtx, eventToRequest(healthyEvent(probeSeq))); err == nil {
			probeEmits++
			successfulProbeSeqs = append(successfulProbeSeqs, probeSeq)
		}
		probeCancel()

		time.Sleep(200 * time.Millisecond)
	}

	require.True(t, observedClosed, "CB never returned to CLOSED after restore + drain")

	// Drain outbox AFTER recovery loop completes. This catches:
	//   - Phase 3's explicit "during CB OPEN" outbox emit (1 row).
	//   - Every Phase 5 probe Emit absorbed by the outbox while the mirror
	//     was still OPEN (i.e., before the background recovery goroutine
	//     transitioned the mirror to HALF-OPEN via manager.GetState).
	// Probes that went through cb.Execute (HALF-OPEN/CLOSED) are NOT in
	// the outbox — they published directly. Both classes of records end
	// up in Kafka after this drain, so the consumer assertion below checks
	// every required `seq` value explicitly.
	snapshot := store.snapshotPending()
	for _, row := range snapshot {
		require.NoError(t, registry.Handle(drainCtx, row), "drain row %s", row.ID)
		require.NoError(t, store.MarkPublished(drainCtx, row.ID, time.Now().UTC()),
			"MarkPublished row %s", row.ID)
	}

	require.Zero(t, store.pendingCount(), "outbox must be empty after recovery drain")
	require.Positive(t, store.drainedCount(), "at least one row must have been drained")

	// Zero event loss verification: consume from the topic and assert every
	// required sequence is present. Goes through the proxy (toxic removed in
	// phase 4) — same path the producer used.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(proxiedBroker),
		kgo.ConsumeTopics(chaosTopic),
		kgo.ConsumerGroup("chaos-consumer-"+strings.ReplaceAll(uuid.NewString(), "-", "")[:12]),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "consumer kgo.NewClient")
	t.Cleanup(func() { consumer.Close() })

	// Required sequence math:
	//   - normalEmits (100): Phase 1 publishes, all in Kafka.
	//   - 5000: Phase 3's explicit outbox emit during CB OPEN; drained
	//     post-recovery → in Kafka.
	//   - successfulProbeSeqs: Phase 5 successful Emits. Each one is
	//     either outbox-absorbed (then drained → Kafka) or published
	//     directly via cb.Execute in HALF-OPEN/CLOSED → Kafka. Either way:
	//     Kafka.
	// Phase 2 emits before CB trip are excluded — the broker may or may
	// not have persisted them depending on whether ctx cancellation aborted
	// the in-flight TCP send. Depending on them would make the assertion
	// flaky. Counting alone is insufficient because duplicate Phase 2
	// records could satisfy a lower bound while a required Phase 1/outbox/
	// probe event is missing.
	requiredSeqs := make(map[int]struct{}, normalEmits+1+len(successfulProbeSeqs))
	for i := 0; i < normalEmits; i++ {
		requiredSeqs[i] = struct{}{}
	}
	requiredSeqs[5000] = struct{}{}
	for _, seq := range successfulProbeSeqs {
		requiredSeqs[seq] = struct{}{}
	}
	wantAtLeast := int64(len(requiredSeqs))
	seenRequiredSeqs := make(map[int]struct{}, len(requiredSeqs))

	received := int64(0)
	consumerCtx, consumerCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer consumerCancel()

	for len(seenRequiredSeqs) < len(requiredSeqs) {
		fetches := consumer.PollFetches(consumerCtx)
		if consumerCtx.Err() != nil {
			break
		}

		fetches.EachRecord(func(record *kgo.Record) {
			received++

			var payload struct {
				Seq int `json:"seq"`
			}
			require.NoErrorf(t, json.Unmarshal(record.Value, &payload),
				"decode chaos record payload %q", string(record.Value))

			if _, ok := requiredSeqs[payload.Seq]; ok {
				seenRequiredSeqs[payload.Seq] = struct{}{}
			}
		})
	}

	missingSeqs := make([]int, 0, len(requiredSeqs)-len(seenRequiredSeqs))
	for seq := range requiredSeqs {
		if _, ok := seenRequiredSeqs[seq]; !ok {
			missingSeqs = append(missingSeqs, seq)
		}
	}
	sort.Ints(missingSeqs)

	require.Emptyf(t, missingSeqs,
		"zero-event-loss: missing required seq values after receiving %d records", received)
	require.GreaterOrEqualf(t, received, wantAtLeast,
		"zero-event-loss: received %d records; want at least %d", received, wantAtLeast)
}

// chaosWaitForBroker polls ListTopics until the broker responds successfully
// through the given address (host:port). Used after toxiproxy setup to
// confirm the proxy forwards correctly before we start the drill.
func chaosWaitForBroker(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		admin := kadm.NewClient(cl)
		pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		_, err = admin.ListTopics(pingCtx)
		cancel()
		cl.Close()

		if err == nil {
			return
		}

		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	require.NoErrorf(t, lastErr, "broker %s never became reachable through toxiproxy", addr)
}

// chaosEnsureTopic creates the named topic if it doesn't already exist.
// Uses the DIRECT broker address so topic setup isn't affected by any
// toxiproxy state.
func chaosEnsureTopic(t *testing.T, broker, topic string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err, "admin kgo.NewClient")
	defer cl.Close()

	admin := kadm.NewClient(cl)

	adminCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err = admin.CreateTopic(adminCtx, 1, 1, nil, topic)
	if err == nil {
		return
	}

	if strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		return
	}

	require.NoErrorf(t, err, "CreateTopic %s", topic)
}

// startChaosRedpanda starts a Redpanda container directly via
// testcontainers.GenericContainer (NOT testcontainers-go/modules/redpanda)
// because the latter auto-renders redpanda.yaml with the broker's own
// host-mapped port as the advertised_kafka_api address. For chaos testing
// we need the broker to advertise the toxiproxy host:port so franz-go
// continues to traverse the proxy on every produce — not just bootstrap.
//
// The container binds the OUTSIDE Kafka listener on 0.0.0.0:9092 inside
// the container (so toxiproxy can reach it via the network alias) but
// advertises (advertisedHost, advertisedPort) to clients. Clients dial
// that advertised address for every metadata-driven follow-up call,
// ensuring all produce traffic flows through the toxiproxy listener at
// the host port.
//
// The cmd here is the canonical Redpanda dev-container start sequence
// minus schema-registry/pandaproxy (chaos doesn't need either) and with
// auto_create_topics_enabled set via --set so the chaos producer doesn't
// race the explicit topic creation.
func startChaosRedpanda(
	t *testing.T,
	ctx context.Context,
	image string,
	sharedNet *testcontainers.DockerNetwork,
	networkAlias string,
	advertisedHost string,
	advertisedPort int,
) testcontainers.Container {
	t.Helper()

	cmd := []string{
		"redpanda", "start",
		"--mode", "dev-container",
		"--smp", "1",
		"--memory", "1G",
		"--reserve-memory", "0M",
		"--overprovisioned",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", "external://0.0.0.0:9092",
		"--advertise-kafka-addr", fmt.Sprintf("external://%s:%d", advertisedHost, advertisedPort),
		"--rpc-addr", "0.0.0.0:33145",
		"--advertise-rpc-addr", fmt.Sprintf("%s:33145", networkAlias),
		"--set", "redpanda.auto_create_topics_enabled=true",
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"9092/tcp", "9644/tcp"},
		Networks:     []string{sharedNet.Name},
		NetworkAliases: map[string][]string{
			sharedNet.Name: {networkAlias},
		},
		Cmd: cmd,
		WaitingFor: wait.ForAll(
			wait.ForLog("Successfully started Redpanda!").WithStartupTimeout(120*time.Second),
			wait.ForListeningPort("9092/tcp").WithStartupTimeout(120*time.Second),
		),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start chaos redpanda")

	return c
}

// Sanity compile-time check: *chaosInMemoryOutbox satisfies the repository
// contract Producer consumes. If outbox.OutboxRepository ever grows a new
// method, this line fails the build and forces us to add a stub.
var _ outbox.OutboxRepository = (*chaosInMemoryOutbox)(nil)
