package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
	"github.com/LerianStudio/lib-streaming/internal/transport/kafka"
)

// errTerminalQuarantine is the stable cause stamped on a DLQ forensic header
// when a record is quarantined. The routing decision is made upstream by
// classify-by-source; the DLQ error-class/message headers are metadata only, so
// a single marker keeps the publisher seam free of the original error value.
var errTerminalQuarantine = errors.New("streaming consumer: record quarantined to DLQ (terminal/poison)")

// Consumer metric names (free-form labels kept off to bound cardinality; see
// docs/design/consumer.md §6). Recorded best-effort — a metrics factory is
// optional, so recordMetric no-ops when none is wired.
const (
	metricDLQTotal           = "streaming_consumer_dlq_total"
	metricDLQPublishFailed   = "streaming_consumer_dlq_publish_failed_total"
	metricFetchError         = "streaming_consumer_fetch_error_total"
	metricFetchErrorDataLoss = "streaming_consumer_fetch_error_data_loss_total"
	metricSystemEvent        = "streaming_consumer_system_event_total"
	metricPartitionHalted    = "streaming_consumer_partition_halted_total"
)

// tenantContextKey is the unexported context key under which the validated
// tenant id is seeded onto the handler ctx. A tenant-aware downstream repo reads
// it via its own getter; the library never exposes the raw key.
type tenantContextKey struct{}

// contextWithTenant returns ctx carrying tenantID. Tenant id derives ONLY from
// the validated ce-tenantid header (never the payload).
func contextWithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

// sanitize strips secrets/broker-credentials from an error message before it is
// logged. Payloads and BorrowerCPF never reach a log because only error strings
// (already broker/transport text) are logged, never rec.Value.
func sanitize(err error) error {
	if err == nil {
		return nil
	}

	return errors.New(contract.SanitizeBrokerURL(err.Error()))
}

// Runner is the PUBLIC consumer surface (aliased as streaming.Consumer). It is
// an interface — mirroring the producer's public Emitter (contract.Emitter) — so
// Build can return either the real runtime or the disabled-mode noopConsumer
// from the same Enabled kill switch (producer pattern: producer.go:175-176).
type Runner interface {
	// Run drives the poll loop until ctx-cancel/Close; blocks; goleak-clean.
	Run(ctx context.Context) error
	// Close stops the loop and releases the group. Idempotent.
	Close() error
	// Healthy reports readiness.
	Healthy(ctx context.Context) error
}

// Compile-time assertions: both the real runtime and the disabled-mode no-op
// satisfy the public Runner surface, so Build can return either.
var (
	_ Runner = (*consumerRuntime)(nil)
	_ Runner = (*noopConsumer)(nil)
)

// consumerRuntime is the hardened at-least-once group consumer runtime. It is
// hidden under internal/consumer so applications depend on the root streaming
// facade (api_consumer.go aliases) rather than this package directly — mirroring
// the producer's internal/producer boundary.
//
// The at-least-once state machine is documented in docs/design/consumer.md.
// In one line: poll (rebalance-blocked) -> per partition in offset order
// {success: stage commit; transient: IN-LOOP retry (budget, dwell-capped) then on
// SUSTAINED transient seek-back + halt partition + cross-poll backoff (NEVER DLQ);
// terminal/poison ONLY: DLQ-publish + commit} -> AllowRebalance -> backoff if any
// halt. Empty TenantID is a valid single-tenant scope (mirrors producer v1.6.2):
// system events and empty-tenant business events dispatch IDENTICALLY with an
// empty TenantID; only a codec-decode fault or a handler-terminal verdict DLQs.
type consumerRuntime struct {
	cfg     ConsumerConfig
	client  GroupClient
	handler Handler

	dlq        dlqPublisher
	classifier Classifier
	codec      codecFunc

	logger  log.Logger
	metrics *metrics.MetricsFactory
	tracer  trace.Tracer

	// stop is closed by Close to break the poll loop. closeOnce makes Close
	// idempotent; closed records that Close already ran so Run can distinguish a
	// Close-driven exit from a ctx-cancel exit.
	stop      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	// lastPollOK records the most recent poll-cycle completion for Healthy.
	lastPollOK atomic.Bool
}

// New constructs the real consumer runtime from validated config and resolved
// collaborators, returning it as the public Runner. The public builder (root
// api_consumer.go) is the only intended caller; it owns env/option resolution,
// the Enabled kill switch (returns NewNoop() when disabled), and typed-nil
// guards before reaching here.
//
// The DLQ publisher is the only seam Build wires from the kafka adapter; the
// dlqPublisher republishes poison.
func New(cfg ConsumerConfig, client GroupClient, handler Handler, opts ...Option) (Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if transport.IsNilInterface(client) {
		return nil, ErrNilGroupClient
	}

	if transport.IsNilInterface(handler) {
		return nil, ErrNilHandler
	}

	c := &consumerRuntime{
		cfg:     cfg,
		client:  client,
		handler: handler,
		codec:   defaultCodec,
		logger:  log.NewNop(),
		stop:    make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	// A DLQ publisher is mandatory: terminal/poison records MUST quarantine
	// rather than silently drop. Build wires the transport-seam publisher; if a
	// caller reaches New without one it is a wiring bug, fail closed.
	if transport.IsNilInterface(c.dlq) {
		return nil, ErrNilDLQPublisher
	}

	return c, nil
}

// Consumer runtime sentinels surfaced by New when a collaborator is missing.
var (
	// ErrNilGroupClient is returned when New is reached with a nil GroupClient.
	ErrNilGroupClient = errors.New("streaming consumer: group client is required")
	// ErrNilDLQPublisher is returned when New is reached with no DLQ publisher;
	// terminal/poison must quarantine, never silently drop.
	ErrNilDLQPublisher = errors.New("streaming consumer: DLQ publisher is required")
)

// Build is the production constructor called by the root streaming.ConsumerBuilder.
// It owns the wiring that the unexported runtime types cannot expose across the
// package boundary: it constructs the franz-go group client (BlockRebalanceOnPoll
// + DisableAutoCommit + TLS/SASL via kafkasec), builds a SECOND kafka adapter
// over the SAME brokers/TLS/SASL config for the internal transport-seam DLQ
// publisher (never the public Emitter, whose catalog/payload/header gates reject
// poison — docs/design/consumer.md §1), and hands all of it to New.
//
// When cfg.Enabled is false it returns the no-op consumer so callers wire
// NewConsumer() unconditionally and toggle with one env var.
func Build(ctx context.Context, cfg ConsumerConfig, handler Handler, opts ...Option) (Runner, error) {
	if !cfg.Enabled {
		return NewNoop(), nil
	}

	// Normalize a blank suffix to the safe default before validation/wiring so a
	// directly-constructed ConsumerConfig (not via NewConsumer/LoadConsumerConfig)
	// never derives <topic><""> == the source topic. A whitespace-only suffix is
	// rejected by Validate below.
	if cfg.DLQTopicSuffix == "" {
		cfg.DLQTopicSuffix = DefaultDLQTopicSuffix
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if transport.IsNilInterface(handler) {
		return nil, ErrNilHandler
	}

	client, err := newKgoGroupClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// A SEPARATE kafka adapter over the same config drives the DLQ publish. It
	// shares the consume client's brokers/TLS/SASL but is its own PRODUCE-ONLY
	// franz-go client — buildDLQKgoOpts deliberately omits the consumer-group /
	// subscribe / block-rebalance options so the DLQ client never joins the
	// consumer's group as a non-polling phantom member (a rebalance hazard that
	// starves the real consumer's assignment).
	dlqOpts, err := buildDLQKgoOpts(cfg)
	if err != nil {
		client.Close()
		return nil, err
	}

	dlqAdapter, err := kafka.NewAdapter(dlqOpts...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("streaming consumer: DLQ adapter init: %s", contract.SanitizeBrokerURL(err.Error()))
	}

	dlq := &transportDLQPublisher{
		adapter: dlqAdapter,
		suffix:  cfg.DLQTopicSuffix,
		groupID: cfg.Group,
	}

	// The internal DLQ seam is authoritative: it is applied LAST so Build's
	// constructed adapter always wins. The dlqPublisher option is unexported
	// (test-only) and no longer overrides it here.
	prod := []Option{
		WithDLQPublisher(dlq),
	}

	runner, err := New(cfg, client, handler, append(append([]Option(nil), opts...), prod...)...)
	if err != nil {
		client.Close()
		// Best-effort cleanup on a construction failure; the close error is not
		// actionable when New already failed.
		_ = dlqAdapter.Close(context.Background())

		return nil, err
	}

	return runner, nil
}

// Run drives the poll loop until ctx is canceled or Close is called. It is
// SafeGo-friendly and goleak-clean: every spawned goroutine (none in v1 beyond
// the loop itself) exits before Run returns.
//
// Run blocks. The consuming service launches it via runtime.SafeGo or a
// lib-commons launcher entry.
func (c *consumerRuntime) Run(ctx context.Context) error {
	if c == nil {
		return ErrNilGroupClient
	}

	for {
		// Cheap pre-poll shutdown check so Close/ctx-cancel between cycles exits
		// without blocking another poll. PollFetches also unblocks on ctx-cancel
		// and on Close (synthetic ErrClientClosed fetch, drained below). A
		// ctx-cancel / Close is a CLEAN shutdown — Run returns nil (goleak +
		// Launcher contract), never the cancellation error. No poll happened this
		// iteration, so there is no BlockRebalanceOnPoll freeze to release here.
		if c.shuttingDown(ctx) {
			return nil
		}

		stop, halted := c.pollCycle(ctx)
		if stop {
			return nil
		}

		// Sustained-transient partitions were seeked back; pause before the next
		// poll re-delivers them so we don't hot-spin re-fetching uncommitted
		// records. The group is unblocked during this wait (pollCycle released the
		// rebalance via its deferred AllowRebalance).
		if len(halted) > 0 {
			c.alertHalted(ctx, halted)

			if !c.sleep(ctx, c.cfg.HaltBackoff) {
				return nil
			}
		}
	}
}

// pollCycle runs ONE poll iteration: poll, drain fetch errors, process records,
// and (on a non-shutdown fetch error) back off. It returns stop=true when a
// shutdown signal arrived, and the set of partitions halted this cycle.
//
// The single PollFetches here freezes the group rebalance (BlockRebalanceOnPoll).
// AllowRebalance is DEFERRED immediately after the poll so the freeze is released
// no matter how the cycle exits — including the drainFetchErrors stop path, the
// COMMON shutdown shape (ctx canceled during PollFetches -> synthetic fetch ->
// drain sees ErrClientClosed/ctx -> stop). Releasing on the stop path is safe
// (no seek-backs were staged); on the normal path the defer fires AFTER
// processFetches, so it still runs strictly after every seek-back is staged
// (Req 3). The deferred call pairs with EVERY PollFetches and runs exactly once
// per poll.
func (c *consumerRuntime) pollCycle(ctx context.Context) (stop bool, halted map[topicPartition]struct{}) {
	fetches := c.client.PollFetches(ctx)

	// Req 3: release the rebalance frozen by BlockRebalanceOnPoll exactly once
	// per cycle, on EVERY return path. processFetches (with its seek-backs) runs
	// before this deferred call on the normal path; the stop path has no
	// seek-backs, so releasing is safe — and it unblocks a subsequent Close() ->
	// LeaveGroup that would otherwise hang on the frozen rebalance.
	defer c.client.AllowRebalance()

	// Req 6: drain partition-level FETCH errors FIRST — they arrive only via
	// Errors()/EachError, never through record iteration. A shutdown signal
	// (ErrClientClosed / ctx-cancel) exits Run cleanly (goleak); any other
	// fetch error is logged/metered/alerted and triggers a cross-poll backoff.
	stop, fetchErr := c.drainFetchErrors(ctx, fetches)
	if stop {
		return true, nil
	}

	halted = c.processFetches(ctx, fetches)
	// A fetch-error cycle (auth / data-loss / other non-shutdown error) must leave
	// Healthy() reporting NOT-ok — the group is not cleanly fetching. A clean cycle
	// marks the consumer healthy.
	c.lastPollOK.Store(!fetchErr)

	// Req 6: a non-shutdown fetch error must not hot-spin the poll loop.
	if fetchErr {
		if !c.sleep(ctx, c.cfg.HaltBackoff) {
			return true, halted
		}
	}

	return false, halted
}

// shuttingDown reports whether the loop should stop: ctx canceled or Close
// signaled. Both are clean shutdowns — the caller returns nil, never the ctx
// error.
func (c *consumerRuntime) shuttingDown(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}

	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

// topicPartition identifies a partition within the per-cycle halt set (Req 4).
type topicPartition struct {
	topic     string
	partition int32
}

// processFetches walks every partition of one poll, applies the per-record
// disposition state machine in ascending-offset order, stages commit watermarks,
// performs seek-backs, and commits the staged watermarks at the end of the
// cycle. It returns the set of partitions halted this cycle (sustained
// transients) so Run can apply the cross-poll backoff.
//
// staged holds the per-partition MAX commit watermark (rec.Offset+1). franz-go's
// CommitRecords is itself a per-partition watermark, but we compute the max
// explicitly so a partition that halts mid-batch never stages a watermark past
// the halted offset (Req 1, within-batch layer).
func (c *consumerRuntime) processFetches(ctx context.Context, fetches kgo.Fetches) map[topicPartition]struct{} {
	halted := make(map[topicPartition]struct{})
	staged := make(map[topicPartition]*kgo.Record)

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		tp := topicPartition{topic: p.Topic, partition: p.Partition}

		// Req 4: EachPartition may visit one partition multiple times per poll.
		// Once halted (seek-back staged), skip every later record of it this
		// cycle — processing them would stage a watermark past the seek-back.
		if _, ok := halted[tp]; ok {
			return
		}

		for _, rec := range p.Records {
			if _, ok := halted[tp]; ok {
				break
			}

			disp, retryCount := c.handleRecord(ctx, rec)

			switch disp {
			case dispositionCommit:
				stageWatermark(staged, tp, rec)
			case dispositionDLQ:
				if c.routeDLQ(ctx, rec, retryCount) {
					// Commit ONLY AFTER the DLQ publish is acknowledged: the
					// quarantine copy is durable before the original is dropped.
					stageWatermark(staged, tp, rec)
				} else {
					// Fail-closed: DLQ publish failed -> do NOT commit past this
					// record. Halt the partition so it is re-attempted next poll.
					halted[tp] = struct{}{}
				}
			case dispositionRetry:
				// Sustained transient (in-loop budget exhausted): seek the
				// partition back so a later same-partition success cannot leapfrog
				// this uncommitted failure across polls (Req 1, cross-poll layer),
				// halt it for the rest of this cycle (Req 4), and break. NEVER DLQ.
				c.seekBack(rec)

				halted[tp] = struct{}{}
			case dispositionStop:
				// Shutdown surfaced mid-handle (ctx cancel). Do NOT DLQ; do NOT
				// stage a watermark past it. Halting the partition this cycle
				// leaves the offset for re-delivery on a clean restart.
				halted[tp] = struct{}{}
			}
		}
	})

	c.commitStaged(ctx, staged)

	return halted
}

// handleRecord runs the per-record guard chain (docs/design/consumer.md §7b) and
// returns the single disposition plus the in-loop retry count consumed (for the
// DLQ retry-count header). Guards run UPSTREAM of classify; only records that
// actually reach Handle are classified sourceHandler.
func (c *consumerRuntime) handleRecord(ctx context.Context, rec *kgo.Record) (disposition, int) {
	ev, err := c.codec(rec.Headers)
	if err != nil {
		// Codec decode fault: malformed CloudEvent, can never parse, not
		// reclassifiable -> always terminal -> DLQ.
		return c.classify(err, sourceCodec), 0
	}

	// Empty tenant is NOT a DLQ reason. Mirroring the producer (v1.6.2
	// "fix(producer): treat empty tenantId as valid single-tenant scope"), a
	// successfully-decoded event ALWAYS dispatches. A system
	// event and an empty-tenant business event are handled IDENTICALLY: both
	// dispatch with an empty TenantID on ctx. Tenant isolation is preserved
	// downstream — a multi-tenant handler that needs a tenant fails closed via
	// its OWN seeder, returning a terminal error that THEN routes to DLQ as the
	// handler's verdict (sourceHandler), never a lib blanket rule.
	if ev.SystemEvent {
		// Observability only (no longer control flow): a system event is just an
		// empty-tenant dispatch with a label. Cheap counter, kept.
		c.recordSystemEvent(ctx, ev)
	}

	return c.handleWithRetry(ctx, rec, ev)
}

// handleWithRetry dispatches the record to Handle and, on a transient handler
// error, retries IN-LOOP up to RetryBudget with bounded ctx-aware backoff whose
// AGGREGATE dwell is hard-capped by RetryInLoopMaxDwell (GAP 4 — the member holds
// BlockRebalanceOnPoll for the batch, so a slow in-loop retry risks a kick). It
// returns dispositionCommit on success, dispositionRetry when the in-loop budget
// is exhausted (a SUSTAINED transient -> seek-back + halt upstream; NEVER DLQ),
// dispositionDLQ on a terminal handler/codec error, or dispositionStop on
// shutdown. The second return value is the number of in-loop retries consumed.
func (c *consumerRuntime) handleWithRetry(ctx context.Context, rec *kgo.Record, ev contract.Event) (disposition, int) {
	deadline := time.Now().Add(c.cfg.RetryInLoopMaxDwell)
	backoff := c.cfg.RetryBackoffInitial

	for attempt := 0; ; attempt++ {
		err := c.dispatch(ctx, rec, ev)

		// Shutdown landing mid-Handle: if Run's ctx is cancelled AND the handler
		// returned an error (a cancel-honoring handler does), that error is
		// shutdown-induced, not poison — stop and let the record re-deliver on
		// restart, never DLQ it. Gate on err != nil so a handler that already
		// SUCCEEDED still commits: an unconditional check would skip the commit for
		// an already-processed record and force a needless duplicate on restart.
		if err != nil && ctx.Err() != nil {
			return dispositionStop, attempt
		}

		disp := c.classify(err, sourceHandler)
		if disp != dispositionRetry {
			return disp, attempt
		}

		// Transient handler error. Stop retrying in-loop if the budget or the
		// aggregate dwell cap is reached, or if shutting down — the partition is
		// then seeked back and re-delivered fresh on the next poll.
		if attempt >= c.cfg.RetryBudget || ctx.Err() != nil {
			return dispositionRetry, attempt
		}

		wait := backoff
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}

		if wait <= 0 {
			// Aggregate dwell cap hit: defer to the cross-poll halt path.
			return dispositionRetry, attempt
		}

		if !c.sleep(ctx, wait) {
			return dispositionStop, attempt
		}

		if backoff < c.cfg.RetryBackoffMax {
			backoff *= 2
			if backoff > c.cfg.RetryBackoffMax {
				backoff = c.cfg.RetryBackoffMax
			}
		}
	}
}

// dispatch seeds the tenant id onto ctx + span (for non-system events) and calls
// the service handler. tenantId derives ONLY from the validated ce-tenantid the
// codec parsed, NEVER from the payload body (the single biggest operational
// invariant). System events carry an empty TenantID by design.
func (c *consumerRuntime) dispatch(ctx context.Context, rec *kgo.Record, ev contract.Event) error {
	hctx := ctx
	if ev.TenantID != "" {
		hctx = contextWithTenant(ctx, ev.TenantID)

		if span := trace.SpanFromContext(hctx); span.IsRecording() {
			span.SetAttributes(attribute.String("tenant.id", ev.TenantID))
		}
	}

	return c.handler.Handle(hctx, ev, rec.Value)
}

// routeDLQ republishes a terminal/poison record to its DLQ topic synchronously
// and reports whether the publish was acknowledged. On success the caller stages
// the commit watermark (commit strictly AFTER the quarantine copy is durable).
// On failure it is fail-closed: routeDLQ seeks the partition back (so franz-go's
// in-session cursor does not advance past the un-quarantined record) and the
// caller halts the partition + skips the commit, so the record is re-attempted
// on the next poll. A poison record never silently drops.
func (c *consumerRuntime) routeDLQ(ctx context.Context, rec *kgo.Record, retryCount int) (published bool) {
	// The original handler/codec error is not threaded into the publisher (the
	// disposition already consumed it); a stable terminal marker populates the
	// DLQ error-class/message forensic headers. The routing decision was made
	// upstream by classify-by-source — the DLQ error class is metadata only.
	if err := c.dlq.PublishDLQ(ctx, rec, errTerminalQuarantine, retryCount); err != nil {
		c.seekBack(rec)
		c.logger.Log(ctx, log.LevelError, "streaming consumer: DLQ publish failed",
			log.String("topic", rec.Topic),
			log.Int("partition", int(rec.Partition)),
			log.Err(sanitize(err)),
		)
		c.recordMetric(ctx, metricDLQPublishFailed)

		return false
	}

	c.recordMetric(ctx, metricDLQTotal)

	return true
}

// drainFetchErrors processes partition-level FETCH errors (Req 6). franz-go
// surfaces these via Fetches.Errors() / Fetches.EachError ONLY — record
// iteration (EachPartition/EachRecord) yields only successfully-fetched records,
// so an errored partition is silently skipped unless drained here.
//
// It returns shouldStop=true when a shutdown signal arrived — kgo.ErrClientClosed
// (injected by PollFetches as a synthetic fetch on partition -1, reaching us ONLY
// through Errors()) or ctx cancellation — so Run can return cleanly and the poll
// goroutine exits goleak-clean (Req 5). For any OTHER fetch error it logs +
// meters + alerts (a *kgo.ErrDataLoss means franz-go auto-reset the cursor past
// lost data — unrecoverable but MUST be observable; auth/batch-parse/group-session
// errors surface here too) and signals a non-shutdown error so Run applies an
// ctx-aware backoff instead of hot-spinning the poll. A fetch error is NEVER a
// silent no-op.
func (c *consumerRuntime) drainFetchErrors(ctx context.Context, fetches kgo.Fetches) (shouldStop, fetchErr bool) {
	if ctx.Err() != nil {
		return true, false
	}

	// EachError's callback takes THREE positional args (topic, partition, err) —
	// NOT func(FetchError) (franz-go v1.21.3 record_and_fetch.go:536).
	fetches.EachError(func(topic string, partition int32, err error) {
		switch {
		case errors.Is(err, kgo.ErrClientClosed) || errors.Is(err, context.Canceled):
			// Shutdown: ErrClientClosed is injected by PollFetches as a synthetic
			// fetch on partition -1, reaching us ONLY through Errors(). Run exits
			// cleanly so the poll goroutine is goleak-clean (Req 5).
			shouldStop = true

		default:
			fetchErr = true

			var dl *kgo.ErrDataLoss
			if errors.As(err, &dl) {
				// Unrecoverable but MUST be observable: franz-go detected the
				// offset out of range and auto-reset the cursor past lost data.
				c.recordMetric(ctx, metricFetchErrorDataLoss)
				c.logger.Log(ctx, log.LevelError, "streaming consumer: DATA LOSS — cursor auto-reset past lost records (unrecoverable, ALERT)",
					log.String("topic", topic),
					log.Int("partition", int(partition)),
					log.Err(sanitize(err)),
				)

				return
			}

			// auth / batch-parse / group-session and other fetch errors.
			c.recordMetric(ctx, metricFetchError)
			c.logger.Log(ctx, log.LevelError, "streaming consumer: fetch error",
				log.String("topic", topic),
				log.Int("partition", int(partition)),
				log.Err(sanitize(err)),
			)
		}
	})

	return shouldStop, fetchErr
}

// Close stops the loop and releases the group. Idempotent. It closes the stop
// channel (unblocking Run between cycles) and closes the franz-go client, which
// injects the synthetic ErrClientClosed fetch that breaks an in-flight
// PollFetches and drains the loop goleak-clean. client.Close() bounds itself
// internally; CloseTimeout is reserved for a future explicit drain budget.
func (c *consumerRuntime) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error

	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.stop)
		c.client.Close()

		// Close the DLQ publisher's own produce-side client so it (and any buffered
		// quarantine writes) is flushed and released, not leaked. Bounded by
		// CloseTimeout so a wedged DLQ broker cannot hang shutdown. A close failure
		// can mean buffered quarantine writes were lost, so surface it (logged AND
		// returned) instead of swallowing it on the shutdown path.
		if !transport.IsNilInterface(c.dlq) {
			ctx, cancel := context.WithTimeout(context.Background(), c.dlqCloseTimeout())
			defer cancel()

			if err := c.dlq.Close(ctx); err != nil {
				c.logger.Log(ctx, log.LevelError, "streaming consumer: DLQ publisher close failed",
					log.Err(sanitize(err)),
				)

				closeErr = fmt.Errorf("close DLQ publisher: %w", err)
			}
		}
	})

	return closeErr
}

// dlqCloseTimeout returns the bound applied to the DLQ publisher flush+close on
// shutdown, falling back to a conservative default when CloseTimeout is unset.
func (c *consumerRuntime) dlqCloseTimeout() time.Duration {
	if c.cfg.CloseTimeout > 0 {
		return c.cfg.CloseTimeout
	}

	return defaultCloseTimeout
}

// Healthy reports consumer readiness: not closed, and the poll loop has
// completed at least one cycle (so the group is joined and fetching). A consumer
// that has never completed a poll is reported not-ready rather than falsely
// healthy.
func (c *consumerRuntime) Healthy(ctx context.Context) error {
	if c == nil {
		return ErrNilGroupClient
	}

	_ = ctx

	if c.closed.Load() {
		return contract.ErrEmitterClosed
	}

	if !c.lastPollOK.Load() {
		return ErrNotReady
	}

	return nil
}

// ErrNotReady is returned by Healthy before the first poll cycle completes.
var ErrNotReady = errors.New("streaming consumer: not ready (no completed poll cycle)")

// noopConsumer is the disabled-mode (Enabled=false) Consumer. It mirrors the
// producer's NoopEmitter (internal/emitter/noop.go:8, selected at
// internal/producer/producer.go:175-176): Run blocks until ctx-cancel, Close and
// Healthy are no-ops. Build returns this when cfg.Enabled is false so callers can
// wire NewConsumer() unconditionally and toggle behavior with a single env var.
//
// It satisfies the public Runner interface (Run/Close/Healthy), so Build returns
// it directly when disabled (see NewNoop).
type noopConsumer struct{}

// Run blocks until ctx is canceled, then returns ctx.Err()'s nil-on-cancel
// contract: a clean (nil) shutdown. No goroutines, goleak-clean.
func (noopConsumer) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Close is a no-op for the disabled consumer.
func (noopConsumer) Close() error { return nil }

// Healthy always reports ready for the disabled consumer.
func (noopConsumer) Healthy(ctx context.Context) error {
	_ = ctx
	return nil
}

// NewNoop returns the disabled-mode consumer as a Runner (mirroring the
// producer's NewNoopEmitter() returning the Emitter interface). The public Build
// calls this when ConsumerConfig.Enabled is false.
func NewNoop() Runner { return &noopConsumer{} }

// --- internal decision helpers (signatures only; logic in a later wave) ---

// seekBack forces partition tp's in-session cursor back to rec so a later
// same-partition success cannot commit past this uncommitted earlier failure
// across polls (Req 1). Uses rec.LeaderEpoch + rec.Offset.
func (c *consumerRuntime) seekBack(rec *kgo.Record) {
	// BARE rec.Offset (re-consume THIS record), NOT rec.Offset+1 (the commit
	// watermark). The two are deliberately distinct: SetOffsets re-delivers the
	// failed record, CommitRecords marks it consumed. Epoch is rec.LeaderEpoch so
	// franz-go validates the seek against the same leader epoch the record came
	// from. Safe only because BlockRebalanceOnPoll froze the rebalance and Run
	// calls AllowRebalance strictly after every seek-back is staged (Req 3).
	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {
			rec.Partition: {Epoch: rec.LeaderEpoch, Offset: rec.Offset},
		},
	})
}

// stageWatermark records the per-partition MAX commit watermark (rec.Offset+1).
// Keeping the max ensures a partition never stages a watermark below an earlier
// staged success in the same cycle, and (with the within-batch halt) never above
// a halted offset (Req 1).
func stageWatermark(staged map[topicPartition]*kgo.Record, tp topicPartition, rec *kgo.Record) {
	if cur, ok := staged[tp]; !ok || rec.Offset > cur.Offset {
		staged[tp] = rec
	}
}

// commitStaged commits the staged per-partition watermarks. CommitRecords is
// itself a per-partition max(offset+1) watermark; passing the single
// highest-offset record per partition commits exactly that watermark.
func (c *consumerRuntime) commitStaged(ctx context.Context, staged map[topicPartition]*kgo.Record) {
	if len(staged) == 0 {
		return
	}

	recs := make([]*kgo.Record, 0, len(staged))
	for _, rec := range staged {
		recs = append(recs, rec)
	}

	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		// A failed commit is not fatal: the records re-deliver next session and
		// are re-processed (at-least-once). Log so it is never silent.
		c.logger.Log(ctx, log.LevelError, "streaming consumer: commit failed",
			log.Err(sanitize(err)),
		)
	}
}

// sleep waits d (ctx-aware), returning false if ctx is canceled or Close is
// signaled during the wait so the caller exits the loop cleanly. A non-positive
// d returns true immediately (no wait, keep going).
func (c *consumerRuntime) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-c.stop:
		return false
	case <-timer.C:
		return true
	}
}

// recordSystemEvent meters + logs that a system event was dispatched with an
// empty TenantID. Observability only — a system event is no longer a tenant-
// routing control branch; it dispatches identically to an empty-tenant business
// event. The counter just labels the platform-level subset.
func (c *consumerRuntime) recordSystemEvent(ctx context.Context, ev contract.Event) {
	c.recordMetric(ctx, metricSystemEvent)
	c.logger.Log(ctx, log.LevelInfo, "streaming consumer: system event dispatched (empty tenant)",
		log.String("resource_type", ev.ResourceType),
		log.String("event_type", ev.EventType),
	)
}

// alertHalted meters + logs that one or more partitions are halted on a
// sustained transient, so an operator can intervene on a long downstream outage.
func (c *consumerRuntime) alertHalted(ctx context.Context, halted map[topicPartition]struct{}) {
	c.recordMetric(ctx, metricPartitionHalted)

	for tp := range halted {
		c.logger.Log(ctx, log.LevelWarn, "streaming consumer: partition halted on sustained transient (head-of-line blocked, ALERT)",
			log.String("topic", tp.topic),
			log.Int("partition", int(tp.partition)),
		)
	}
}

// recordMetric increments a counter best-effort. A metrics factory is optional;
// when none is wired (tests, disabled telemetry) this is a no-op. Errors from
// the factory are swallowed — a metric failure must never break the poll loop.
func (c *consumerRuntime) recordMetric(ctx context.Context, name string) {
	if c.metrics == nil {
		return
	}

	counter, err := c.metrics.Counter(metrics.Metric{Name: name})
	if err != nil || counter == nil {
		return
	}

	_ = counter.Add(ctx, 1)
}

// errSource names the ORIGIN of a non-nil error so classify can apply the
// correct safe default per source. Codec and handler faults need DIFFERENT
// defaults: a "transient, retry" default (right for a transport fault) is
// exactly WRONG for a codec/handler fault — it would WEDGE the partition
// head-of-line on a record that can never succeed. So classify takes the source
// explicitly. Transport/fetch errors never reach classify (they are handled in
// drainFetchErrors), so only codec and handler remain.
type errSource int

const (
	// sourceCodec: a CloudEvents decode fault (ErrMissingRequiredHeader /
	// ErrUnsupportedSpecVersion, cloudevents.go:153,159). ALWAYS terminal — a
	// malformed CloudEvent can never parse; retry is pointless.
	sourceCodec errSource = iota
	// sourceHandler: a Handler.Handle return error. FAIL-CLOSED: terminal -> DLQ
	// by default unless the service Classifier reclassifies it as transient.
	sourceHandler
)

// classify decides the disposition of a non-nil error by its SOURCE, not by one
// taxonomy (the 8th-hole fix; Fred-decided 2026-06-27, docs/design/consumer.md
// §7a). err == nil yields dispositionCommit (the success path). The two
// sources have DIFFERENT safe defaults:
//
//   - sourceCodec -> ALWAYS dispositionDLQ. A malformed CloudEvent is poison; it
//     can never parse, so retry is pointless and it is not reclassifiable.
//   - sourceHandler -> run the service Classifier; if it returns true (a known
//     downstream-transient) -> dispositionRetry. DEFAULT (Classifier returns
//     false, or no Classifier is supplied, or it does not recognize the error)
//     -> dispositionDLQ. THIS IS FAIL-CLOSED.
//
// FAIL-CLOSED RATIONALE (wedge > quarantine in badness): an unrecognized handler
// error -> DLQ quarantines ONE record (per-record blast radius, alertable,
// replayable, nothing lost). Fail-open would WEDGE the whole partition head-of-
// line (unbounded blast radius, silent, no alert). The optional Classifier FLIPS
// ROLE here: it is no longer the only path to DLQ; it RECLASSIFIES a known
// handler-transient (Midaz/Postgres down) BACK to retry. Money-path consumers
// MUST supply one (else a transient outage over-quarantines — recoverable: the
// DLQ is replayable). Codec poison -> DLQ is non-negotiable regardless.
//
// SCOPE: classify operates on the handler/codec/transport ERROR. Empty TenantID
// is NOT a classify input — a successfully-decoded event ALWAYS dispatches
// (docs/design/consumer.md §7b), mirroring the producer (v1.6.2: empty tenant is
// a valid single-tenant scope). A system event and an empty-tenant business
// event dispatch IDENTICALLY with an empty TenantID. A tenant-scoped handler that
// needs a tenant fails closed via its OWN seeder and returns a HANDLER error that
// THEN reaches classify (source=sourceHandler) and routes to DLQ as the HANDLER's
// verdict — never a lib blanket rule. The codec does NOT validate tenant
// (cloudevents.go:232,237 returns "" + nil err); the runtime no longer adds a
// tenant guard.
//
// DELIBERATE DIVERGENCE FROM THE PRODUCER (docs/design/consumer.md §7a): the
// producer's isDLQRoutable (internal/producer/dlq_helpers.go:43-50) treats
// ClassValidation as NOT DLQ-routable — at PRODUCE time a validation fault is the
// caller's own bug, rejected synchronously. At CONSUME time the same fault is on
// a record ALREADY in the topic — it cannot be rejected synchronously, so it MUST
// go to DLQ or it wedges the partition / loses data. A TRANSIENT error is NEVER
// classified to DLQ: it returns dispositionRetry and a sustained transient seeks
// back + blocks (GAP 3), it does not quarantine.
func (c *consumerRuntime) classify(err error, source errSource) disposition {
	if err == nil {
		return dispositionCommit
	}

	switch source {
	case sourceCodec:
		// Malformed CloudEvent: poison, can never parse, not reclassifiable.
		return dispositionDLQ

	case sourceHandler:
		// Handler-return error: FAIL-CLOSED. The optional Classifier RECLASSIFIES
		// a known downstream-transient (Midaz/Postgres down) BACK to retry; the
		// DEFAULT (no Classifier, or it returns false / does not recognize the
		// error) quarantines ONE record rather than wedging the partition.
		if c.classifier != nil && c.classifier(err) {
			return dispositionRetry
		}

		return dispositionDLQ

	default:
		return dispositionDLQ // fail-closed default (quarantine > wedge)
	}
}

// disposition is the per-record verdict produced by classify.
type disposition int

const (
	// dispositionCommit is the SUCCESS verdict (err == nil): stage the commit
	// watermark (rec.Offset+1) for this record. Returned by classify(nil, _).
	dispositionCommit disposition = iota
	// dispositionRetry: transient/retryable error. Retried IN-LOOP up to
	// RetryBudget with aggregate backoff hard-capped (RetryInLoopMaxDwell) below
	// the rebalance timeout (GAP 4). On a SUSTAINED transient (in-loop budget
	// exhausted) the runtime seeks back + halts the partition + cross-poll
	// backoff (HaltBackoff) — block beats lose. A transient NEVER goes to DLQ
	// (GAP 3).
	dispositionRetry
	// dispositionDLQ: terminal/poison ONLY (codec-decode fault, or a handler
	// terminal verdict — bad payload, unknown/drifted topic, nil uuid, illegal
	// transition, not-found) — publish to DLQ, then stage commit, then alert.
	// Empty ce-tenantid is NOT a DLQ reason (it dispatches as a single-tenant
	// scope). There is NO budget-exhausted -> DLQ path: budget-exhausted
	// transients seek back and block (GAP 3).
	dispositionDLQ
	// dispositionStop: shutdown (ctx-canceled / ClientClosed) — stop the loop,
	// do NOT DLQ.
	dispositionStop
)
