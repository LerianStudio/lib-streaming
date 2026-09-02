package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-streaming/v3/obs"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/v4/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport/kafka"
)

// DLQ cause kinds, stamped on x-lerian-dlq-cause-kind. Low-cardinality by
// design: an operator filters and alerts on this, then reads the sanitized
// underlying error from x-lerian-dlq-error-message.
//
// They exist because every DLQ entry used to carry the SAME message. A
// consumer's DLQ filling up told an operator that something was terminal, and
// nothing else — a codec fault (the producer's wire format drifted), a source
// mismatch (a foreign write, or a misconfigured allowlist), an unhandled key
// (this consumer's registrations drifted behind the producer's catalog) and a
// genuine business rejection were indistinguishable, and they have four
// different owners and four different fixes.
const (
	// dlqCauseCodec: the CloudEvents headers would not decode. The record is
	// poison and can never parse; the producer's wire format is the suspect.
	dlqCauseCodec = "codec"
	// dlqCauseHandler: the service handler returned a terminal error. The
	// business rejection is the suspect.
	dlqCauseHandler = "handler"
	// dlqCauseSourceMismatch: the event's ce-source was not an expected
	// producer. Either a foreign write to the topic, or an ExpectSources
	// allowlist that drifted from what actually publishes there.
	dlqCauseSourceMismatch = "source_mismatch"
	// dlqCauseUnhandledKey: no handler registered for the event key. Fires on
	// EVERY unmatched key from a COMMANDS queue (always strict — a command is
	// work addressed to this consumer), and on a fact stream only under the
	// opt-in UnmatchedError policy. Either way this consumer's On(...)
	// registrations have drifted behind the producer's catalog.
	dlqCauseUnhandledKey = "unhandled_key"
)

// ErrUnexpectedSource is returned when a record's ce-source is not one of the
// consumer's expected producers. It means either a producer misconfiguration or
// a foreign write to a topic this consumer reads, so it quarantines rather than
// dispatching.
//
// The check lives in the RUNTIME, ahead of the handler, not in the Dispatcher.
// It used to be dispatch-only, which left a whole-stream Handler(...) — the
// mode most exposed on a shared-ACL topic, since it sees every record — with no
// verification at all, and made ExpectSources a build error in that mode with
// no in-API opt-out for a fleet-wide env var.
var ErrUnexpectedSource = errors.New("streaming consumer: event ce-source is not an expected producer")

// quarantineCause carries WHY a record is going to the DLQ, from the gate that
// decided it down to the publisher that stamps the forensic headers.
type quarantineCause struct {
	kind string
	err  error
}

// handlerQuarantineCause buckets a handler-return error into its cause kind.
// The dispatcher's two structural rejections (source mismatch, unhandled key)
// arrive as handler errors but are NOT business rejections, and lumping them
// under "handler" would point an operator at the wrong owner.
func handlerQuarantineCause(err error) quarantineCause {
	switch {
	case errors.Is(err, ErrUnexpectedSource):
		return quarantineCause{kind: dlqCauseSourceMismatch, err: err}
	case errors.Is(err, ErrUnhandledEvent):
		return quarantineCause{kind: dlqCauseUnhandledKey, err: err}
	default:
		return quarantineCause{kind: dlqCauseHandler, err: err}
	}
}

// Consumer metric names (free-form labels kept off to bound cardinality; see
// docs/design/consumer.md §6). Recorded best-effort — a metrics recorder is
// optional, so recordMetric no-ops when none is wired.
const (
	metricDLQTotal           = "streaming_consumer_dlq_total"
	metricDLQPublishFailed   = "streaming_consumer_dlq_publish_failed_total"
	metricFetchError         = "streaming_consumer_fetch_error_total"
	metricFetchErrorDataLoss = "streaming_consumer_fetch_error_data_loss_total"
	metricSystemEvent        = "streaming_consumer_system_event_total"
	metricPartitionHalted    = "streaming_consumer_partition_halted_total"
	metricUnmatchedTotal     = "streaming_consumer_unmatched_total"
)

// maxUnmatchedEventKeyLabels bounds the distinct event_key label values on
// streaming_consumer_unmatched_total. The keys a consumer legitimately sees are
// bounded by its producers' catalogs, but the topic is writable by anything the
// ce-source allowlist admits, so the label is capped and the overflow folds
// into "other". An unbounded label here would be a metrics-backend hazard
// dressed up as observability.
const maxUnmatchedEventKeyLabels = 64

// unmatchedEventKeyOverflow is the label used once maxUnmatchedEventKeyLabels
// distinct keys have been metered.
const unmatchedEventKeyOverflow = "other"

// The two unmatched-event log lines, as constants so a test can pin the exact
// string rather than a substring that drifts.
const (
	// unmatchedNoHandlerMessage fires once per distinct unmatched key.
	unmatchedNoHandlerMessage = "streaming consumer: no handler registered for event key — records are being skipped and committed"
	// unmatchedLabelOverflowMessage fires ONCE, at the boundary where the
	// event_key label stops naming keys. Without it the "other" bucket
	// silently swallows every later drift and the metric looks like it just
	// went quiet.
	unmatchedLabelOverflowMessage = `streaming consumer: unmatched event-key label overflow; further keys metered as "other"`
	// unmatchedOverflowKeyMessage names an unmatched key seen PAST the label
	// cap, rate-limited by unmatchedOverflowLogInterval.
	//
	// It exists because the per-key warning used to live inside the below-cap
	// branch: once 64 distinct keys had been seen, every new one metered as
	// "other" and was named NOWHERE. The real fleet carries 143 event keys
	// across the four launch producers, so any two-app consumer burns the cap in
	// minutes — and a producer shipping a new event on day 30 was then invisible
	// in both signals at once.
	unmatchedOverflowKeyMessage = "streaming consumer: unmatched event key seen past the metric label cap (named here because the metric can no longer name it)"
)

// unmatchedOverflowLogInterval bounds unmatchedOverflowKeyMessage globally.
//
// The cap protects the metrics backend's cardinality budget; this protects the
// log from the same pressure. One line per window is enough to notice drift and
// far too few to flood — and rate-limiting on a timestamp means no unbounded
// set of seen keys has to be retained just to decide what is "new".
const unmatchedOverflowLogInterval = 30 * time.Second

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

	logger  obs.Logger
	metrics obs.MetricsRecorder
	tracer  trace.Tracer

	// stop is closed by Close to break the poll loop. closeOnce makes Close
	// idempotent; closed records that Close already ran so Run can distinguish a
	// Close-driven exit from a ctx-cancel exit.
	stop      chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	// lastPollOK records the most recent poll-cycle completion for Healthy.
	lastPollOK atomic.Bool

	// unmatchedSeen records the event keys already metered/logged as
	// unmatched, so the log fires once per key rather than once per record,
	// and so the metric's event_key label stays bounded.
	unmatchedSeen  sync.Map
	unmatchedCount atomic.Int64
	// unmatchedOverflowOnce guards the single boundary warning fired when the
	// event_key label cap is first exceeded.
	unmatchedOverflowOnce sync.Once
	// unmatchedOverflowLogAt is the UnixNano of the last past-the-cap key line,
	// which rate-limits that line globally. A timestamp rather than a seen-set:
	// retaining every overflow key to decide "new" would reintroduce, in memory,
	// exactly the unbounded growth the label cap exists to prevent.
	unmatchedOverflowLogAt atomic.Int64

	// haltMu guards haltStreaks, which the poll goroutine writes once per cycle
	// and Healthy reads from whatever goroutine serves readiness.
	haltMu sync.Mutex
	// haltStreaks counts CONSECUTIVE halted cycles per partition. It is bounded
	// by the consumer's own assignment, and a partition drops out the moment it
	// makes progress.
	haltStreaks map[topicPartition]haltStreak

	// dispatcher is the handler when it is a *Dispatcher, resolved once at
	// construction. New binds this runtime's unmatched recorder onto it —
	// which means a Dispatcher REUSED across two Build calls has its
	// observeUnmatched rebound to the LAST consumer, and the first consumer
	// then meters nothing. Build one Dispatcher per consumer.
	dispatcher *Dispatcher

	// commandTopics is the set of subscribed topics carrying STRICT unmatched
	// semantics, resolved once from cfg.Commands. Read per record on the guard
	// chain, which is why it is a set rather than a slice scan.
	commandTopics map[string]struct{}
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
		cfg:           cfg,
		client:        client,
		handler:       handler,
		codec:         defaultCodec,
		logger:        log.NewNop(),
		stop:          make(chan struct{}),
		haltStreaks:   make(map[topicPartition]haltStreak),
		commandTopics: cfg.CommandTopics(),
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

	// Give the dispatcher a voice for the events it drops. UnmatchedIgnore is
	// the right default and stays the default; what changes is that it stops
	// being invisible. A typo'd On("loan.disbursd") otherwise builds clean,
	// commits every record, reports Healthy, and processes nothing forever.
	//
	// This MUTATES the caller-owned Dispatcher. Reusing one Dispatcher across
	// two Build calls rebinds observeUnmatched to the last consumer, leaving
	// the first one metering nothing — build one Dispatcher per consumer.
	if d, ok := handler.(*Dispatcher); ok {
		c.dispatcher = d

		d.ObserveUnmatched(c.recordUnmatched)
	}

	return c, nil
}

// recordUnmatched meters and logs one event the dispatcher had no handler for.
//
// The metric carries the event key so an operator can see WHICH stream is going
// unread, bounded by maxUnmatchedEventKeyLabels. The log fires once per key —
// per record would drown the log in exactly the high-volume case that makes the
// signal matter.
func (c *consumerRuntime) recordUnmatched(ctx context.Context, eventKey string) {
	label := eventKey

	if _, seen := c.unmatchedSeen.Load(eventKey); !seen {
		if c.unmatchedCount.Load() >= maxUnmatchedEventKeyLabels {
			label = unmatchedEventKeyOverflow

			c.unmatchedOverflowOnce.Do(func() {
				c.logger.Log(ctx, obs.LevelWarn, unmatchedLabelOverflowMessage,
					"distinct_event_keys", maxUnmatchedEventKeyLabels,
				)
			})

			// The metric can no longer name this key, so the log must. The two
			// signals are decoupled deliberately: the cap protects the metrics
			// backend's cardinality budget, and going blind on key NAMES was
			// never part of that bargain.
			c.logOverflowKey(ctx, eventKey)
		} else if _, loaded := c.unmatchedSeen.LoadOrStore(eventKey, struct{}{}); !loaded {
			c.unmatchedCount.Add(1)

			c.logger.Log(ctx, obs.LevelWarn, unmatchedNoHandlerMessage,
				"event_key", eventKey,
				"policy", string(c.unmatchedPolicy()),
			)
		}
	}

	c.recordMetricWithLabels(ctx, metricUnmatchedTotal, map[string]string{"event_key": label})
}

// sourceAccepted reports whether source is one of the producing applications
// this consumer accepts. An EMPTY allowlist accepts everything: verification
// stays opt-in for the raw Topics(...) escape hatch, whose producers were never
// named. Subscribing by Apps(...) fills the allowlist automatically, so the
// ergonomic path gets verification for free.
func (c *consumerRuntime) sourceAccepted(source string) bool {
	if len(c.cfg.ExpectSources) == 0 {
		return true
	}

	return slices.Contains(c.cfg.ExpectSources, source)
}

// logOverflowKey names one unmatched key seen past the label cap, at most once
// per unmatchedOverflowLogInterval across the whole consumer.
//
// The CAS makes the window a real global bound under concurrent partitions: a
// loser of the race skips its line rather than queueing behind it.
func (c *consumerRuntime) logOverflowKey(ctx context.Context, eventKey string) {
	now := time.Now().UnixNano()

	last := c.unmatchedOverflowLogAt.Load()
	if last != 0 && now-last < int64(unmatchedOverflowLogInterval) {
		return
	}

	if !c.unmatchedOverflowLogAt.CompareAndSwap(last, now) {
		return
	}

	c.logger.Log(ctx, obs.LevelWarn, unmatchedOverflowKeyMessage,
		"event_key", eventKey,
		"policy", string(c.unmatchedPolicy()),
		"distinct_event_keys_capped_at", maxUnmatchedEventKeyLabels,
	)
}

// unmatchedPolicy reports the dispatcher's unmatched policy for logging, or
// the ignore default when the handler is not a dispatcher.
func (c *consumerRuntime) unmatchedPolicy() UnmatchedPolicy {
	if c.dispatcher != nil {
		return c.dispatcher.unmatched
	}

	return UnmatchedIgnore
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

	// The quarantine target is THIS consumer's own DLQ, derived from its own
	// ce-source (cfg.Validate proved it legal above). It is deliberately NOT
	// derived per-record from the producer's topic: a consumer writing into a
	// producer's DLQ needs a write grant on every producer it reads, and lands
	// its failures on someone else's operational surface.
	dlq := &transportDLQPublisher{
		adapter:  dlqAdapter,
		dlqTopic: contract.AppDLQTopic(cfg.Source),
		groupID:  cfg.Group,
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

	// Create the topics this consumer OWNS (its own DLQ, and its own commands
	// queue when it is the app being commanded) before Run starts polling. It
	// runs AFTER New because New is where the caller's Option-supplied logger is
	// resolved, and the WARN-not-fail posture is worthless against a nop logger.
	//
	// Ordering is not load-bearing: franz-go refreshes metadata on a cadence, so
	// a subscription picks up a topic created moments later. What matters is that
	// it happens before the first quarantine needs the DLQ to exist.
	if built, ok := runner.(*consumerRuntime); ok {
		dlqAdapter.EnsureTopics(ctx, built.logger, ownedTopics(cfg)...)
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
//
// The poll is bounded by a CHILD deadline (pollWait) so a cycle completes on a
// quiet topic. PollFetches otherwise blocks until records arrive, so on a topic
// with zero traffic no cycle ever finished, lastPollOK was never stored, and
// Healthy returned ErrNotReady forever — a first activation in a traffic-less
// environment could not become Ready at all. An idle window with a joined group
// and no fetch errors IS a healthy cycle.
//
// The child deadline is NOT a shutdown. drainFetchErrors reads the PARENT ctx,
// so parent-cancel and Close keep their exact current semantics (stop the loop,
// Run returns nil) and a deadline expiry is classified as an empty clean cycle
// instead. Passing the child to drainFetchErrors would turn every quiet window
// into a shutdown; passing it to processFetches would abort handler work that a
// deadline was never meant to bound.
func (c *consumerRuntime) pollCycle(ctx context.Context) (stop bool, halted map[topicPartition]string) {
	pollCtx, cancelPoll := context.WithTimeout(ctx, c.pollWait())
	fetches := c.client.PollFetches(pollCtx)

	cancelPoll()

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

	halted, seen := c.processFetches(ctx, fetches)

	// Fold this cycle's halts into the consecutive-cycle streaks Healthy reads.
	// The drainFetchErrors stop path returned above without touching them, and a
	// mid-handle shutdown halt cannot accumulate a streak either — Run exits on
	// the same signal that produced it, so it never gets a second cycle.
	c.trackHalts(seen, halted)

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

// Halt reasons. Low-cardinality by design: they label the partition-halted
// metric and name the cause in the readiness error, and they have different
// owners — a stuck downstream, a broken DLQ path, and a shutdown are three
// different pages.
const (
	// haltReasonSustainedTransient: the in-loop retry budget was exhausted on a
	// reclassified transient. The partition seeks back and blocks head-of-line
	// ("block beats lose"); the downstream is the suspect.
	haltReasonSustainedTransient = "sustained_transient"
	// haltReasonDLQPublishFailed: a terminal record could not be quarantined.
	// Fail-closed, so the record is re-attempted rather than committed past —
	// which means this one wedges until the DLQ path is fixed.
	haltReasonDLQPublishFailed = "dlq_publish_failed"
	// haltReasonShutdown: ctx-cancel landed mid-handle. Not a wedge; Run
	// returns on the same signal, so it can never accumulate a streak.
	haltReasonShutdown = "shutdown"
)

// haltedCyclesUnhealthy is how many CONSECUTIVE poll cycles a partition must
// stay halted before Healthy reports it.
//
// One is too eager: a single sustained-transient cycle is an ordinary
// downstream hiccup, and failing readiness on it would flap the pod out of the
// load balancer for a blip that resolves itself. Three cycles (each separated
// by HaltBackoff) means the partition made no progress across the whole
// recovery window — a real wedge, not jitter.
const haltedCyclesUnhealthy = 3

// haltStreak counts consecutive halted cycles for one partition and remembers
// why it is halted.
type haltStreak struct {
	cycles int
	reason string
}

// ErrPartitionHalted is returned by Healthy when a partition has been halted
// across haltedCyclesUnhealthy consecutive poll cycles.
//
// It exists because readiness could not see a wedge at all: it was
// !closed && lastPollOK, and both stay true while a poison record whose DLQ
// publish keeps failing redelivers forever. The consumer polled cleanly,
// processed nothing, and reported green — under one topic per app, with the
// producing application's entire catalog stuck behind it.
var ErrPartitionHalted = errors.New("streaming consumer: partition halted across consecutive poll cycles (head-of-line blocked)")

// processFetches walks every partition of one poll, applies the per-record
// disposition state machine in ascending-offset order, stages commit watermarks,
// performs seek-backs, and commits the staged watermarks at the end of the
// cycle. It returns the set of partitions halted this cycle (sustained
// transients) so Run can apply the cross-poll backoff, plus the set of
// partitions this poll actually delivered records for — trackHalts needs both,
// because a partition MISSING from the batch has made no progress and its halt
// streak must survive.
//
// staged holds the per-partition MAX commit watermark (rec.Offset+1). franz-go's
// CommitRecords is itself a per-partition watermark, but we compute the max
// explicitly so a partition that halts mid-batch never stages a watermark past
// the halted offset (Req 1, within-batch layer).
func (c *consumerRuntime) processFetches(ctx context.Context, fetches kgo.Fetches) (map[topicPartition]string, map[topicPartition]struct{}) {
	halted := make(map[topicPartition]string)
	seen := make(map[topicPartition]struct{})
	staged := make(map[topicPartition]*kgo.Record)

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		tp := topicPartition{topic: p.Topic, partition: p.Partition}

		// Delivered records is the only evidence of a fetch round-trip for this
		// partition. An entry with none proves nothing happened, so it must not
		// count as progress against a halt streak.
		if len(p.Records) > 0 {
			seen[tp] = struct{}{}
		}

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

			disp, retryCount, cause := c.handleRecord(ctx, rec)

			switch disp {
			case dispositionCommit:
				stageWatermark(staged, tp, rec)
			case dispositionDLQ:
				if c.routeDLQ(ctx, rec, retryCount, cause) {
					// Commit ONLY AFTER the DLQ publish is acknowledged: the
					// quarantine copy is durable before the original is dropped.
					stageWatermark(staged, tp, rec)
				} else {
					// Fail-closed: DLQ publish failed -> do NOT commit past this
					// record. Halt the partition so it is re-attempted next poll.
					halted[tp] = haltReasonDLQPublishFailed
				}
			case dispositionRetry:
				// Sustained transient (in-loop budget exhausted): seek the
				// partition back so a later same-partition success cannot leapfrog
				// this uncommitted failure across polls (Req 1, cross-poll layer),
				// halt it for the rest of this cycle (Req 4), and break. NEVER DLQ.
				c.seekBack(rec)

				halted[tp] = haltReasonSustainedTransient
			case dispositionStop:
				// Shutdown surfaced mid-handle (ctx cancel). Do NOT DLQ; do NOT
				// stage a watermark past it. Halting the partition this cycle
				// leaves the offset for re-delivery on a clean restart.
				halted[tp] = haltReasonShutdown
			}
		}
	})

	c.commitStaged(ctx, staged)

	return halted, seen
}

// handleRecord runs the per-record guard chain (docs/design/consumer.md §7b) and
// returns the single disposition plus the in-loop retry count consumed (for the
// DLQ retry-count header). Guards run UPSTREAM of classify; only records that
// actually reach Handle are classified sourceHandler.
func (c *consumerRuntime) handleRecord(ctx context.Context, rec *kgo.Record) (disposition, int, quarantineCause) {
	ev, err := c.codec(rec.Headers)
	if err != nil {
		// Codec decode fault: malformed CloudEvent, can never parse, not
		// reclassifiable -> always terminal -> DLQ.
		return c.classify(err, sourceCodec), 0, quarantineCause{kind: dlqCauseCodec, err: err}
	}

	// Source verification, ahead of BOTH handler modes. A record whose
	// ce-source is not an accepted producer is either a foreign write to a
	// topic this consumer reads or an allowlist that drifted from what actually
	// publishes there — never something to hand a business handler.
	//
	// It runs here rather than inside the Dispatcher so a whole-stream
	// Handler(...) gets the same guarantee. That mode needs it MOST: it
	// receives every record on a topic whose write ACL it does not control.
	if !c.sourceAccepted(ev.Source) {
		err := fmt.Errorf("%w: got %q, want one of %v", ErrUnexpectedSource, ev.Source, c.cfg.ExpectSources)

		return dispositionDLQ, 0, quarantineCause{kind: dlqCauseSourceMismatch, err: err}
	}

	// Empty tenant is NOT a DLQ reason. Mirroring the producer (v1.6.2
	// "fix(producer): treat empty tenantId as valid single-tenant scope"), a
	// successfully-decoded event ALWAYS dispatches. A system
	// event and an empty-tenant business event are handled IDENTICALLY: both
	// dispatch with an empty TenantID on ctx. Tenant isolation is preserved
	// downstream — a multi-tenant handler that needs a tenant fails closed via
	// its OWN seeder, returning a terminal error that THEN routes to DLQ as the
	// handler's verdict (sourceHandler), never a lib blanket rule.
	// STRICT unmatched semantics on a commands queue, ahead of dispatch.
	//
	// A command is work addressed to THIS consumer, so a key it has no handler
	// for is undelivered work — not the ignorable majority of a producer's fact
	// firehose. Skipping and committing it is the failure this queue exists to
	// prevent: a producer shipping a new command key before its consumer
	// deploys the handler would lose every one of them, forever, while both
	// sides report healthy.
	//
	// It runs here, not in the Dispatcher, because the verdict is a property of
	// the TOPIC the record arrived on and Handler.Handle never sees the topic.
	// That is also what makes the policy per-topic: the same consumer stays
	// lenient on the fact streams it also subscribes to.
	if err := c.unhandledCommand(rec, ev); err != nil {
		return dispositionDLQ, 0, quarantineCause{kind: dlqCauseUnhandledKey, err: err}
	}

	if ev.SystemEvent {
		// Observability only (no longer control flow): a system event is just an
		// empty-tenant dispatch with a label. Cheap counter, kept.
		c.recordSystemEvent(ctx, ev)
	}

	return c.handleWithRetry(ctx, rec, ev)
}

// unhandledCommand returns an ErrUnhandledEvent-wrapped error when rec arrived
// on a STRICT commands queue carrying an event key this consumer registered no
// handler for, and nil in every other case.
//
// Nil when the record is not from a commands queue (fact streams keep the
// lenient UnmatchedPolicy verdict) and nil when there is no dispatcher — a
// whole-stream Handler has no registry to ask, which is exactly why combining
// Handler(...) with Commands(...) is refused at Build (ErrHandlerAndCommandsBothSet)
// rather than silently downgraded here.
func (c *consumerRuntime) unhandledCommand(rec *kgo.Record, ev contract.Event) error {
	if len(c.commandTopics) == 0 || c.dispatcher == nil {
		return nil
	}

	if _, strict := c.commandTopics[rec.Topic]; !strict {
		return nil
	}

	key := contract.EventKey(ev.ResourceType, ev.EventType)
	if c.dispatcher.Handles(ev.Source, key) {
		return nil
	}

	return fmt.Errorf("%w: %q from %q on the commands queue %q — a command with no handler is undelivered work, never skipped",
		ErrUnhandledEvent, key, ev.Source, rec.Topic)
}

// handleWithRetry dispatches the record to Handle and, on a transient handler
// error, retries IN-LOOP up to RetryBudget with bounded ctx-aware backoff whose
// AGGREGATE dwell is hard-capped by RetryInLoopMaxDwell (GAP 4 — the member holds
// BlockRebalanceOnPoll for the batch, so a slow in-loop retry risks a kick). It
// returns dispositionCommit on success, dispositionRetry when the in-loop budget
// is exhausted (a SUSTAINED transient -> seek-back + halt upstream; NEVER DLQ),
// dispositionDLQ on a terminal handler/codec error, or dispositionStop on
// shutdown. The second return value is the number of in-loop retries consumed.
func (c *consumerRuntime) handleWithRetry(ctx context.Context, rec *kgo.Record, ev contract.Event) (disposition, int, quarantineCause) {
	deadline := time.Now().Add(c.cfg.RetryInLoopMaxDwell)
	backoff := c.cfg.RetryBackoffInitial

	for attempt := 0; ; attempt++ {
		err := c.dispatch(ctx, rec, ev)

		// Shutdown landing mid-Handle: stop (re-deliver, never DLQ) ONLY when the
		// handler error is itself a cancellation caused by Run's ctx being
		// cancelled. Gating on the error's NATURE (not merely "ctx cancelled +
		// any error") keeps fail-closed intact: a real terminal/business error
		// that merely COINCIDES with shutdown must still classify to the DLQ, and
		// a successful handle (err == nil) must still commit. errors.Is(err,
		// ctx.Err()) catches a handler that propagated the ctx error verbatim;
		// the explicit Canceled/DeadlineExceeded cover a freshly-wrapped one.
		if err != nil && ctx.Err() != nil &&
			(errors.Is(err, ctx.Err()) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return dispositionStop, attempt, quarantineCause{}
		}

		disp := c.classify(err, sourceHandler)
		if disp != dispositionRetry {
			return disp, attempt, handlerQuarantineCause(err)
		}

		// Transient handler error. Stop retrying in-loop if the budget or the
		// aggregate dwell cap is reached, or if shutting down — the partition is
		// then seeked back and re-delivered fresh on the next poll.
		if attempt >= c.cfg.RetryBudget || ctx.Err() != nil {
			return dispositionRetry, attempt, quarantineCause{}
		}

		wait := backoff
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}

		if wait <= 0 {
			// Aggregate dwell cap hit: defer to the cross-poll halt path.
			return dispositionRetry, attempt, quarantineCause{}
		}

		if !c.sleep(ctx, wait) {
			return dispositionStop, attempt, quarantineCause{}
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
func (c *consumerRuntime) routeDLQ(ctx context.Context, rec *kgo.Record, retryCount int, cause quarantineCause) (published bool) {
	// The underlying error and its cause kind BOTH travel to the publisher.
	// Stamping one stable marker on every entry made a filling DLQ say only
	// "something was terminal" — a codec fault, a foreign write, a drifted
	// event-key registration and a genuine business rejection have four
	// different owners, and an operator could not tell them apart.
	//
	// The error travels UNWRAPPED-BUT-INTACT: PublishDLQ sanitizes it (broker
	// credentials stripped) at the point it becomes a header value, which is
	// also where the adapter classifies it. Flattening it here would strip the
	// sentinel chain both of them read.
	//
	// cause is always fully populated on this path: dispositionDLQ is only ever
	// returned by classify for a NON-NIL error, and both of its callers pair it
	// with a kind (dlqCauseCodec, or handlerQuarantineCause's four-way bucket).
	// The former nil/empty fallbacks were unreachable, and the kind fallback
	// attributed unknown causes to "handler" — pointing an operator at the
	// service team for a fault that was never theirs.
	kind := cause.kind

	if err := c.dlq.PublishDLQ(ctx, rec, cause.err, kind, retryCount); err != nil {
		c.seekBack(rec)
		c.logger.Log(ctx, obs.LevelError, "streaming consumer: DLQ publish failed",
			"topic", rec.Topic,
			"partition", int(rec.Partition),
			"error", sanitize(err),
		)
		c.recordMetric(ctx, metricDLQPublishFailed)

		return false
	}

	c.recordMetricWithLabels(ctx, metricDLQTotal, map[string]string{"cause_kind": kind})

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

		case errors.Is(err, context.DeadlineExceeded):
			// IDLE WINDOW, not an error and not a shutdown: pollCycle's own child
			// deadline expired with no records on a quiet topic. Leave shouldStop
			// and fetchErr alone so the cycle completes clean — that is what lets
			// Healthy pass without traffic.
			//
			// Only OUR child ctx can put this error here. franz-go stamps a ctx
			// error into a fetch solely via NewErrFetch(ctx.Err()) on the ctx the
			// caller passed to PollFetches; a parent deadline would already have
			// been caught by the ctx.Err() guard above and stopped the loop, and
			// broker-side fetch faults surface as kerr/net errors, never as this
			// sentinel.

		default:
			fetchErr = true

			var dl *kgo.ErrDataLoss
			if errors.As(err, &dl) {
				// Unrecoverable but MUST be observable: franz-go detected the
				// offset out of range and auto-reset the cursor past lost data.
				c.recordMetric(ctx, metricFetchErrorDataLoss)
				c.logger.Log(ctx, obs.LevelError, "streaming consumer: DATA LOSS — cursor auto-reset past lost records (unrecoverable, ALERT)",
					"topic", topic,
					"partition", int(partition),
					"error", sanitize(err),
				)

				return
			}

			// auth / batch-parse / group-session and other fetch errors.
			c.recordMetric(ctx, metricFetchError)
			c.logger.Log(ctx, obs.LevelError, "streaming consumer: fetch error",
				"topic", topic,
				"partition", int(partition),
				"error", sanitize(err),
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
				c.logger.Log(ctx, obs.LevelError, "streaming consumer: DLQ publisher close failed",
					"error", sanitize(err),
				)

				closeErr = fmt.Errorf("close DLQ publisher: %w", err)
			}
		}
	})

	return closeErr
}

// pollWait returns the per-cycle deadline applied to a single PollFetches,
// falling back to the bounded default when PollTimeout is unset.
//
// Zero MUST resolve here rather than meaning "block": a fluent
// NewConsumer()...Build() seeds DefaultBuilderConfig, and a caller assembling
// ConsumerConfig by hand leaves the field at its zero value — both were the
// shapes that deadlocked readiness on a quiet topic.
func (c *consumerRuntime) pollWait() time.Duration {
	if c.cfg.PollTimeout > 0 {
		return c.cfg.PollTimeout
	}

	return defaultPollTimeout
}

// dlqCloseTimeout returns the bound applied to the DLQ publisher flush+close on
// shutdown, falling back to a conservative default when CloseTimeout is unset.
func (c *consumerRuntime) dlqCloseTimeout() time.Duration {
	if c.cfg.CloseTimeout > 0 {
		return c.cfg.CloseTimeout
	}

	return defaultCloseTimeout
}

// Healthy reports consumer readiness: not closed, the poll loop has completed
// at least one cycle (so the group is joined and fetching), and no partition is
// wedged.
//
// "Completed a cycle" does NOT mean "received a record". An idle poll window —
// the per-cycle deadline expiring with a joined group and no fetch errors — is a
// completed, healthy cycle. It has to be: PollFetches blocks until records
// arrive, so requiring traffic made readiness unreachable on an empty topic and
// a first activation in any traffic-less environment could never go Ready.
//
// The third condition is the one that was missing. Polling cleanly is not the
// same as making progress: a poison record whose DLQ publish keeps failing, or
// a downstream outage holding a partition back, leaves !closed && lastPollOK
// both true forever while nothing is processed. The consumer reported green,
// the pod stayed in the load balancer, and under one topic per app the
// producing application's whole catalog sat behind it.
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

	return c.wedgedPartition()
}

// ErrNotReady is returned by Healthy before the first poll cycle completes —
// the consumer is still joining the group, or the most recent cycle ended in a
// fetch error.
//
// It is NOT what a quiet topic reports. A cycle completes on the PollTimeout
// deadline with zero records, so on an empty topic this clears within one poll
// window rather than persisting until the first event is produced.
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
		c.logger.Log(ctx, obs.LevelError, "streaming consumer: commit failed",
			"error", sanitize(err),
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
	c.logger.Log(ctx, obs.LevelInfo, "streaming consumer: system event dispatched (empty tenant)",
		"resource_type", ev.ResourceType,
		"event_type", ev.EventType,
	)
}

// trackHalts folds one poll cycle's halt set into the per-partition consecutive
// -cycle streaks Healthy reads.
//
// CONSECUTIVE is the whole point: a partition that halts, recovers, and halts
// again is making progress, and treating that as a wedge would flap readiness
// on ordinary downstream jitter. So a streak drops out entirely — rather than
// decaying — the moment the partition makes progress.
//
// Progress is "seen AND not halted", never "not halted". A seek-back discards
// the partition's buffered records, so franz-go needs a fresh fetch round-trip
// before re-delivering them, and a hot sibling partition can win that race —
// leaving the wedged partition absent from a poll batch it never recovered in.
// Reading absence as recovery oscillated the streak 1 -> 0 -> 1 forever and the
// threshold was never reached. An idle partition never enters the map at all,
// so nothing else changes.
func (c *consumerRuntime) trackHalts(seen map[topicPartition]struct{}, halted map[topicPartition]string) {
	c.haltMu.Lock()
	defer c.haltMu.Unlock()

	if c.haltStreaks == nil {
		c.haltStreaks = make(map[topicPartition]haltStreak)
	}

	for tp := range c.haltStreaks {
		_, delivered := seen[tp]
		if _, still := halted[tp]; delivered && !still {
			delete(c.haltStreaks, tp)
		}
	}

	for tp, reason := range halted {
		streak := c.haltStreaks[tp]
		streak.cycles++
		streak.reason = reason

		c.haltStreaks[tp] = streak
	}
}

// wedgedPartition returns a readiness error for the first partition (in
// deterministic order) halted for haltedCyclesUnhealthy consecutive cycles, or
// nil when none is.
//
// It names the topic, the partition, and the cause, because a health check that
// only says "not ready" sends an operator to read logs for what it already
// knew.
func (c *consumerRuntime) wedgedPartition() error {
	c.haltMu.Lock()
	defer c.haltMu.Unlock()

	worst, found := topicPartition{}, false
	streak := haltStreak{}

	for tp, s := range c.haltStreaks {
		if s.cycles < haltedCyclesUnhealthy {
			continue
		}

		// Deterministic pick so repeated probes report the same partition.
		if !found || tp.topic < worst.topic || (tp.topic == worst.topic && tp.partition < worst.partition) {
			worst, streak, found = tp, s, true
		}
	}

	if !found {
		return nil
	}

	return fmt.Errorf("%w: topic=%q partition=%d cause=%s consecutive_cycles=%d",
		ErrPartitionHalted, worst.topic, worst.partition, streak.reason, streak.cycles)
}

// alertHalted meters + logs that one or more partitions are halted, so an
// operator can intervene on a long downstream outage or a broken DLQ path.
func (c *consumerRuntime) alertHalted(ctx context.Context, halted map[topicPartition]string) {
	for tp, reason := range halted {
		// The reason is a closed three-value set, so it is safe as a label and
		// it is the one thing that routes the page: a stuck downstream, a broken
		// DLQ path, and a shutdown have three different owners.
		c.recordMetricWithLabels(ctx, metricPartitionHalted, map[string]string{"reason": reason})

		c.logger.Log(ctx, obs.LevelWarn, "streaming consumer: partition halted (head-of-line blocked, ALERT)",
			"topic", tp.topic,
			"partition", int(tp.partition),
			"reason", reason,
		)
	}
}

// recordMetric increments a counter best-effort. A metrics recorder is optional;
// when none is wired (tests, disabled telemetry) this is a no-op. Errors from
// the factory are swallowed — a metric failure must never break the poll loop.
func (c *consumerRuntime) recordMetric(ctx context.Context, name string) {
	c.recordMetricWithLabels(ctx, name, nil)
}

// recordMetricWithLabels is recordMetric with a bounded label set. Callers own
// the cardinality bound on every label value they pass.
func (c *consumerRuntime) recordMetricWithLabels(ctx context.Context, name string, labels map[string]string) {
	if c.metrics == nil {
		return
	}

	_ = c.metrics.AddCounter(ctx, name, "", "1", labels, 1)
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
		// STRUCTURAL LIBRARY VERDICTS BYPASS THE CLASSIFIER. ErrUnhandledEvent
		// and ErrUnexpectedSource are synthesized by THIS library, not by the
		// service: no handler is registered for the key, or the record came
		// from a source this consumer refuses. Neither can ever become
		// satisfiable by waiting, so neither is reclassifiable — exactly like a
		// codec fault, which is why they short-circuit in the same place.
		//
		// Handing them to the service Classifier was a wedge waiting to happen.
		// The common classifier shape is "retry anything that is not my own
		// business rule", which turns a never-satisfiable verdict into a
		// transient: retried to exhaustion, seeked back, partition halted,
		// redelivered, forever — and under one topic per app that is the
		// producing application's ENTIRE catalog stuck behind one record, with
		// nothing reaching the DLQ where an operator would have seen it.
		if errors.Is(err, ErrUnhandledEvent) || errors.Is(err, ErrUnexpectedSource) {
			return dispositionDLQ
		}

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
