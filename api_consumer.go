package streaming

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/LerianStudio/lib-observability/v2/metrics"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/v3/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
)

// Handler is the only interface a consuming service implements. The library
// owns commit, retry, seek-back, DLQ, tenant propagation, and rebalance safety;
// tenant filtering/enforcement is the handler's own responsibility (see below).
//
//	type myHandler struct{}
//	func (myHandler) Handle(ctx context.Context, ev streaming.Event, payload []byte) error {
//	    // ev.TenantID is set from ce-tenantid by the library — filter on it
//	    // before any tenant-scoped business logic (cross-tenant leak otherwise).
//	    return nil
//	}
type Handler = consumer.Handler

// Classifier optionally RECLASSIFIES a known HANDLER-return error as transient
// (retryable) instead of the fail-closed default. Handler-return errors default
// to TERMINAL -> DLQ when no Classifier recognizes them; a money-path consumer
// MUST supply one marking its known-transient downstream faults (Midaz/Postgres
// down, etc.) as retry, else a transient outage over-quarantines into the DLQ.
// Return true for an error that should be retried (a recoverable downstream
// blip), false to let it take the fail-closed terminal path. It governs ONLY a
// service handler's OWN errors — transport errors are classified by the
// transport seam, codec-decode errors are always terminal, and the library's
// own structural verdicts (ErrUnexpectedSource, ErrUnhandledEvent) quarantine
// without ever reaching it. That last exclusion matters: the common classifier
// shape is "retry anything that is not my business rule", and routing a
// never-satisfiable verdict through it wedges the partition forever. See
// docs/design/consumer.md §7a.
type Classifier = consumer.Classifier

// Consumer is the hardened at-least-once group consumer surface (Run/Close/
// Healthy). Construct it with NewConsumer().…​.Build(ctx); drive it with
// Run(ctx); stop it with Close. It is an interface — mirroring the producer's
// public Emitter — so Build returns either the real runtime or the disabled-mode
// no-op from the Enabled kill switch.
type Consumer = consumer.Runner

// ConsumerOption is the functional-option type for advanced consumer wiring
// not covered by a dedicated builder method.
type ConsumerOption = consumer.Option

// ConsumerConfig is the full runtime configuration for a Consumer — the
// inbound counterpart to the producer's Config. Every field maps to a
// STREAMING_CONSUMER_* environment variable read by LoadConsumerConfig.
type ConsumerConfig = consumer.ConsumerConfig

// LoadConsumerConfig reads every STREAMING_CONSUMER_* environment variable,
// applies defaults, and validates the result when Enabled=true. The second
// return value carries human-readable warnings and is never nil.
//
// TLS and SASL are deliberately absent from the environment surface — wire
// them programmatically through ConsumerBuilder.TLS / .SASL. Secrets do not
// belong in env-string config.
//
// Pair it with ConsumerBuilder.FromConfig to go from environment to a running
// consumer without restating a single knob:
//
//	cfg, warnings, err := streaming.LoadConsumerConfig()
//	if err != nil { return err }
//	c, err := streaming.NewConsumer().FromConfig(cfg).On("loan.disbursed", h).Build(ctx)
func LoadConsumerConfig() (ConsumerConfig, []string, error) {
	return consumer.LoadConsumerConfig()
}

// HandlerFunc is a single-event handler registered under a (producing app,
// event key) pair via ConsumerBuilder.OnFrom, or via ConsumerBuilder.On for a
// single-producer consumer. Same signature as Handler.Handle.
type HandlerFunc = consumer.HandlerFunc

// UnmatchedPolicy decides what happens to an event on the subscribed stream
// that has no registered handler. See UnmatchedIgnore / UnmatchedError.
type UnmatchedPolicy = consumer.UnmatchedPolicy

const (
	// UnmatchedIgnore skips and commits an unhandled event. DEFAULT, and the
	// only safe default under one-topic-per-app: subscribing to a producer's
	// app stream delivers EVERY event that producer emits, and a consumer
	// legitimately cares about a handful of them.
	UnmatchedIgnore = consumer.UnmatchedIgnore
	// UnmatchedError quarantines an unhandled event (ErrUnhandledEvent takes
	// the fail-closed terminal path to the DLQ). Opt in only when the consumer
	// genuinely owns every event on the stream.
	UnmatchedError = consumer.UnmatchedError
)

// Consumer runtime sentinels. Both are synthesized BY the library, so both
// quarantine outright and NEITHER is offered to the service Classifier: they
// are structural and can never become satisfiable by waiting, exactly like a
// codec fault.
var (
	// ErrUnhandledEvent surfaces under UnmatchedError for an (app, event key)
	// pair with no registered handler.
	ErrUnhandledEvent = consumer.ErrUnhandledEvent
	// ErrUnexpectedSource surfaces when a record's ce-source is not one of the
	// consumer's expected producers — a producer misconfiguration or a foreign
	// write to a topic this consumer reads. Verified in the runtime, ahead of
	// either handler mode.
	ErrUnexpectedSource = consumer.ErrUnexpectedSource
)

// WithConsumerLogger sets the structured logger.
func WithConsumerLogger(l log.Logger) ConsumerOption { return consumer.WithLogger(l) }

// WithConsumerMetricsFactory wires the metrics factory for consumer instruments.
func WithConsumerMetricsFactory(f *metrics.MetricsFactory) ConsumerOption {
	return consumer.WithMetricsFactory(f)
}

// WithConsumerTracer overrides the tracer used for poll/handle spans.
func WithConsumerTracer(t trace.Tracer) ConsumerOption { return consumer.WithTracer(t) }

// ConsumerBuilder assembles a Consumer programmatically, mirroring the
// producer's streaming.Builder idiom. Build constructs the franz-go group
// client (with BlockRebalanceOnPoll + DisableAutoCommit) and the at-least-once
// runtime.
//
// Under one-topic-per-app the ergonomic path is Source + Apps + OnFrom: declare
// this application's own identity, name the producing applications you consume,
// register one handler per (producer, event) pair you care about, and the
// library subscribes to the right topics, verifies each record came from an
// expected producer, and dispatches.
//
//	c, err := streaming.NewConsumer().
//	    Brokers(cfg.Brokers...).
//	    Group("my-service").
//	    Source(cfg.CloudEventsSource).                     // REQUIRED; names this app's own DLQ
//	    Apps("lender", "matcher").                         // -> lerian.streaming.{lender,matcher}
//	    OnFrom("lender", "loan.disbursed", onLenderLoan).  // "<resourceType>.<eventType>"
//	    OnFrom("matcher", "loan.disbursed", onMatcherLoan).
//	    TLS(tlsCfg).
//	    SASL(mech).
//	    RetryBudget(3).
//	    Classifier(isTransient).
//	    Build(ctx)
//	if err != nil { return err }
//	go func() { _ = c.Run(ctx) }()  // SafeGo in production
//	defer c.Close()
//
// Those two loan.disbursed registrations are the point: lender's and matcher's
// are different facts with different payloads. A single-producer consumer keeps
// the terse form — Apps("lender").On("loan.disbursed", h) binds to the sole app
// — and a bare On under SEVERAL apps fails the build rather than binding to
// whichever record happens to arrive.
//
// Every other event on those streams is skipped and committed
// (UnmatchedIgnore); call UnmatchedPolicy(streaming.UnmatchedError) to
// quarantine unknown keys instead. A record whose ce-source is not one of the
// named Apps never reaches a handler — that check used to be hand-rolled in
// every consuming repo.
//
// Handler(...) remains for consumers that want the raw stream: it takes the
// whole record set itself and does its own selection, and gets the same
// ce-source verification. Handler and On/OnFrom are mutually exclusive.
//
// There is deliberately no DLQ(emitter) knob: the DLQ must not flow through the
// public Emitter (its catalog/payload/header gates reject the very poison it
// must quarantine). The consumer constructs its own DLQ publisher internally
// over the internal transport seam, reusing the same Brokers/TLS/SASL config it
// consumes with. See docs/design/consumer.md §1 and §6.
type ConsumerBuilder struct {
	cfg        consumer.ConsumerConfig
	handler    Handler
	dispatcher *consumer.Dispatcher
	// dispatchWanted records that the caller asked for per-event dispatch,
	// which ONLY On(...) / OnFrom(...) do. UnmatchedPolicy also allocates the
	// dispatcher because it has somewhere to write, but allocating it is not a
	// request for it: reading intent off `dispatcher != nil` made
	// .Handler(h).UnmatchedPolicy(...) fail with "Handler and On are mutually
	// exclusive" when the caller had never written On.
	dispatchWanted bool
	// unmatchedSet records that UnmatchedPolicy was called. The dispatcher's
	// own field cannot answer this — UnmatchedIgnore is both the default and a
	// legal explicit choice — and the knob is inert under a whole-stream
	// Handler, so Build has to be able to tell "set" from "defaulted".
	unmatchedSet bool
	// expectSources holds the fluent ExpectSources(...) entries. It lives on
	// the builder rather than the dispatcher because source verification is a
	// RUNTIME concern now, valid in both handler modes — parking it on the
	// dispatcher was what made ExpectSources meaningless (and then a build
	// error) for a whole-stream Handler.
	expectSources []string
	classifier    Classifier
	opts          []ConsumerOption
}

// NewConsumer returns a ConsumerBuilder defaulted to ENABLED — an explicitly
// fluent-built consumer is meant to run (mirrors the producer default at
// api.go:771). Config-driven callers that gate on a deployment flag use
// .Enabled(cfg.Flag); the env path (LoadConsumerConfig) keeps its own
// STREAMING_CONSUMER_ENABLED kill switch.
func NewConsumer() *ConsumerBuilder {
	// Seed the same defaults LoadConsumerConfig applies so a minimal fluent build
	// (Brokers/Group/Topics/Handler only) passes Validate and runs. Without these,
	// the zero-value backoff/dwell/timeout fields would fail validation and the
	// default-enabled builder would be unusable.
	b := &ConsumerBuilder{cfg: consumer.DefaultBuilderConfig()}

	return b
}

// FromConfig adopts a whole ConsumerConfig — typically the one LoadConsumerConfig
// read from STREAMING_CONSUMER_* — as the builder's starting point, including
// its Enabled kill switch. Fluent setters called AFTER it override individual
// fields; setters called before it are discarded.
//
// This is the seam that makes the STREAMING_CONSUMER_* surface reachable at
// all: without it, an operator could set STREAMING_CONSUMER_APPS and every
// sibling variable and have nothing read them.
//
// Handlers are NOT part of ConsumerConfig — follow FromConfig with On(...) or
// Handler(...).
func (b *ConsumerBuilder) FromConfig(cfg ConsumerConfig) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = cfg

	return b
}

// Enabled gates whether Build yields a real runtime (true) or the disabled-mode
// no-op (false). Defaults to true via NewConsumer; pass .Enabled(cfg.Flag) to
// drive it from config.
func (b *ConsumerBuilder) Enabled(v bool) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Enabled = v

	return b
}

// Brokers sets the bootstrap broker list.
func (b *ConsumerBuilder) Brokers(brokers ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Brokers = append([]string(nil), brokers...)

	return b
}

// Group sets the consumer group id.
func (b *ConsumerBuilder) Group(group string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Group = group

	return b
}

// Source sets THIS application's ce-source — the same identity its producer
// side publishes under, and the same STREAMING_CLOUDEVENTS_SOURCE value, because
// one service has one identity.
//
// It is REQUIRED for an enabled consumer, and it does one job: it names the
// consumer's OWN dead-letter topic, "lerian.streaming.<source>.dlq". A consumer
// quarantines into its own DLQ, never the producer's — so a Kafka ACL grants
// every application exactly two writes (its topic and its .dlq) whether it
// produces, consumes, or both, and a filling DLQ names the team that owns the
// fix rather than the team whose events happened to be poison.
//
// Held to the same strict source rule the producer enforces: one dot-free
// lowercase segment. Config-driven callers get it for free through FromConfig.
func (b *ConsumerBuilder) Source(source string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Source = source

	return b
}

// Topics sets the RAW subscription list — the escape hatch for topics this
// library did not derive (legacy streams, third-party producers). It composes
// with Apps; use Apps for lib-streaming producers.
func (b *ConsumerBuilder) Topics(topics ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Topics = append([]string(nil), topics...)

	return b
}

// Apps subscribes by PRODUCING APPLICATION name (ce-source). Each app resolves
// to its one topic, "lerian.streaming.<app>", so a consumer never hardcodes
// the derivation.
//
// Naming apps here also arms source verification: when the consumer dispatches
// by event key (see On), an event whose ce-source is not one of these apps is
// rejected with ErrUnexpectedSource instead of reaching a handler.
//
// Each name is validated against the same strict source contract the producer
// enforces; a name no producer could legally publish under is a Build error
// rather than a subscription to a topic that stays empty forever.
func (b *ConsumerBuilder) Apps(apps ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.Apps = append([]string(nil), apps...)

	return b
}

// On registers a handler for one event key, "<resourceType>.<eventType>" — the
// pair the producer's catalog spells and its manifest advertises. Snake_case
// resource types travel verbatim; there is no '_'->'-' translation in v3.
//
// It binds to the consumer's SOLE producing application. A consumer that names
// more than one app in Apps(...) must use OnFrom instead: with two producers in
// scope a bare event key does not say whose event it is, and two apps really do
// publish the same event names. Bare On there fails the build rather than
// letting one app's payload reach the other app's handler.
//
// On and Handler are mutually exclusive: On builds a dispatching handler that
// selects per event, Handler takes the whole stream. Registering the same key
// twice keeps the last handler.
func (b *ConsumerBuilder) On(eventKey string, handler HandlerFunc) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.ensureDispatcher().On(eventKey, handler)

	return b
}

// OnFrom registers a handler for one event key from ONE named producing
// application — the explicit form, and the only one a multi-app consumer can
// use.
//
//	streaming.NewConsumer().
//	    Apps("lender", "matcher").
//	    OnFrom("lender", "loan.disbursed", onLenderDisbursed).
//	    OnFrom("matcher", "loan.disbursed", onMatcherDisbursed).
//
// Those two "loan.disbursed" events are different facts with different
// payloads. v3 put the producing app into ce-type precisely so they stop
// colliding; OnFrom is how a consumer spends that.
//
// app must be one of the applications the consumer accepts (Apps(...), or an
// explicit ExpectSources(...) list). Naming any other fails the build — that
// handler could never receive a record.
func (b *ConsumerBuilder) OnFrom(app, eventKey string, handler HandlerFunc) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.ensureDispatcher().OnFrom(app, eventKey, handler)

	return b
}

// ensureDispatcher lazily allocates the dispatcher and records that the caller
// asked for per-event dispatch. Only On / OnFrom call it.
func (b *ConsumerBuilder) ensureDispatcher() *consumer.Dispatcher {
	if b.dispatcher == nil {
		b.dispatcher = consumer.NewDispatcher()
	}

	b.dispatchWanted = true

	return b.dispatcher
}

// UnmatchedPolicy sets what happens to an event with no registered handler.
// Defaults to UnmatchedIgnore. It applies to On(...) dispatch only; combining
// it with a whole-stream Handler(...) is a build error rather than a silently
// inert setting.
func (b *ConsumerBuilder) UnmatchedPolicy(policy UnmatchedPolicy) *ConsumerBuilder {
	if b == nil {
		return b
	}

	if b.dispatcher == nil {
		b.dispatcher = consumer.NewDispatcher()
	}

	b.unmatchedSet = true

	b.dispatcher.OnUnmatched(policy)

	return b
}

// ExpectSources declares the producing applications accepted by source
// verification, by ce-source.
//
// Precedence and shape, exactly:
//
//   - An explicit list REPLACES the allowlist Apps(...) would have implied.
//     Repeated calls APPEND to each other, so the union of every call is the
//     final list.
//   - Every entry is validated at Build against the same strict source rule
//     the producer enforces. A hyphen/underscore typo matches no real producer
//     and would quarantine 100% of the stream, so it fails the build instead.
//   - The list must COVER every app named in Apps(...). Subscribing to an
//     app's topic while refusing its ce-source is a bug, not a filter — use
//     On(...) to select events, not this.
//   - With no explicit list and only Apps(...), the Apps become the allowlist.
//   - With no explicit list and only raw Topics(...), verification is off and
//     any ce-source dispatches.
//   - With no explicit list and BOTH Apps and Topics, Build FAILS: neither
//     defaulting to Apps (which would DLQ the whole raw stream) nor skipping
//     the check is a defensible guess.
//
// Verification runs in the RUNTIME, ahead of either handler mode, so this
// applies to a whole-stream Handler(...) exactly as it does to On(...) dispatch
// — and a Handler is the mode that needs it most, since it receives every
// record on a topic whose write ACL it does not own.
func (b *ConsumerBuilder) ExpectSources(sources ...string) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.expectSources = append(b.expectSources, sources...)

	return b
}

// TLS sets the TLS config for broker dials. Validated at Build (shared with the
// producer via the wave-2 internal/kafkasec extraction).
func (b *ConsumerBuilder) TLS(cfg *tls.Config) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithTLSConfig(cfg)

	return b
}

// SASL sets the SASL mechanism. SASL requires TLS unless AllowPlaintextSASL.
func (b *ConsumerBuilder) SASL(mechanism sasl.Mechanism) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithSASL(mechanism)

	return b
}

// AllowPlaintextSASL permits SASL without TLS for local/dev brokers only.
func (b *ConsumerBuilder) AllowPlaintextSASL() *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg = b.cfg.WithAllowPlaintextSASL()

	return b
}

// Handler wires a service-supplied handler that receives EVERY event on the
// subscribed streams and does its own selection. Mutually exclusive with On;
// one of the two is required.
func (b *ConsumerBuilder) Handler(h Handler) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.handler = h

	return b
}

// RetryBudget sets the IN-LOOP transient-failure retry count within a single
// poll cycle (the connection-blip absorber; typically resolves in 0-1 attempts).
// It is NOT "retries before DLQ": transients NEVER reach the DLQ. When the
// in-loop budget is exhausted (a SUSTAINED transient) the runtime seeks back and
// blocks its partition head-of-line — block beats lose. Only classified
// TERMINAL/poison reaches the DLQ; handler-return AND codec-decode errors
// default to terminal (fail-closed) unless the optional Classifier reclassifies
// a handler error as transient. See docs/design/consumer.md §2/§7a.
func (b *ConsumerBuilder) RetryBudget(n int) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.RetryBudget = n

	return b
}

// Classifier wires the optional handler-error reclassifier (transient flip off
// the fail-closed terminal default; see the Classifier type).
func (b *ConsumerBuilder) Classifier(fn Classifier) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.classifier = fn

	return b
}

// CloseTimeout bounds graceful drain on Close.
func (b *ConsumerBuilder) CloseTimeout(d time.Duration) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.cfg.CloseTimeout = d

	return b
}

// Options appends arbitrary ConsumerOptions (logger, metrics, tracer, custom
// codec). Parity escape hatch for options without a dedicated method.
func (b *ConsumerBuilder) Options(opts ...ConsumerOption) *ConsumerBuilder {
	if b == nil {
		return b
	}

	b.opts = append(b.opts, opts...)

	return b
}

// Build validates the builder, constructs the franz-go group client, constructs
// the internal DLQ publisher over the transport seam (same Brokers/TLS/SASL as
// the consume client), and returns the at-least-once Consumer runtime.
//
// When cfg.Enabled is false, Build returns a no-op Consumer (consumer.NewNoop)
// whose Run blocks until ctx-cancel and whose Close/Healthy are no-ops —
// mirroring the producer's NoopEmitter kill-switch.
func (b *ConsumerBuilder) Build(ctx context.Context) (Consumer, error) {
	if b == nil {
		return nil, consumer.ErrNilHandler
	}

	// Handler resolution runs only for an ENABLED consumer. The disabled-mode
	// kill switch must stay a pure no-op: a service that wires
	// .Enabled(false) has deliberately not supplied handlers, and failing its
	// Build would defeat the point of the switch.
	var handler Handler

	if b.cfg.Enabled {
		resolved, err := b.resolveHandler()
		if err != nil {
			return nil, err
		}

		handler = resolved
	}

	// Fold the builder-level classifier into the option list so the runtime
	// reclassifier seam stays single-sourced (consumer.WithClassifier).
	opts := b.opts
	if b.classifier != nil {
		opts = append(opts, consumer.WithClassifier(b.classifier))
	}

	// consumer.Build owns the full production wiring: the Enabled kill switch,
	// cfg.Validate, the franz-go group client (BlockRebalanceOnPoll +
	// DisableAutoCommit + TLS/SASL via kafkasec), the internal transport-seam DLQ
	// publisher over the same config (NOT the public Emitter), and the transport
	// error-source classifier. The handler typed-nil guard lives there too.
	return consumer.Build(ctx, b.cfg, handler, opts...)
}

// resolveHandler picks the handler Build hands to the runtime — the
// caller-supplied whole-stream Handler, or the dispatcher assembled from On /
// OnFrom / UnmatchedPolicy — and settles the ce-source allowlist the runtime
// verifies against, which is now common to both modes.
//
// Dispatch intent is read from dispatchWanted, which ONLY On / OnFrom set.
//
// A whole-stream Handler still rejects the two genuinely dispatch-only knobs
// rather than ignoring them: On (it is the other answer to "who selects
// events") and UnmatchedPolicy (it decides what the DISPATCHER does with a key
// it has no handler for, and a raw Handler receives everything and selects for
// itself). ExpectSources is NOT in that set any more — verification moved to
// the runtime, so it is valid and functional here too.
//
// When the caller did not name expected sources explicitly, the Apps list
// becomes the allowlist. That is the ergonomic payoff of subscribing by
// application: source verification — which every consuming repo hand-rolled in
// v2 — comes for free and cannot drift from the subscription it guards.
func (b *ConsumerBuilder) resolveHandler() (Handler, error) {
	hasHandler := !transport.IsNilInterface(b.handler)

	if hasHandler {
		if b.dispatchWanted {
			return nil, consumer.ErrHandlerAndDispatchBothSet
		}

		if b.unmatchedSet {
			return nil, consumer.ErrHandlerAndUnmatchedPolicyBothSet
		}

		if err := b.resolveExpectedSources(); err != nil {
			return nil, err
		}

		return b.handler, nil
	}

	if !b.dispatchWanted {
		// Neither wired. Let the runtime's own ErrNilHandler gate speak; it is
		// the same failure a v2 consumer would have hit.
		return nil, consumer.ErrNilHandler
	}

	if len(b.dispatcher.EventKeys()) == 0 {
		return nil, fmt.Errorf("%w: dispatching consumer has no On(...) handlers registered", consumer.ErrNilHandler)
	}

	if err := b.resolveExpectedSources(); err != nil {
		return nil, err
	}

	// Bind resolves bare On(...) registrations against the accepted apps and
	// rejects the two shapes that cannot work: an ambiguous bare On under
	// several producers, and an OnFrom naming an app nothing publishes from
	// here.
	if err := b.dispatcher.Bind(b.cfg.ExpectSources...); err != nil {
		return nil, err
	}

	return b.dispatcher, nil
}

// resolveExpectedSources settles the ce-source allowlist onto cfg.ExpectSources,
// where the runtime reads it. It runs in BOTH handler modes.
//
// Three shapes, three outcomes:
//
//   - Explicit ExpectSources: REPLACES anything Apps would have implied. Every
//     entry is validated with the same strict source rule the producer
//     enforces (a hyphen/underscore typo would otherwise quarantine 100% of a
//     stream while the consumer reported healthy), and the list must cover
//     every app named in Apps — subscribing to an app's topic while refusing
//     its source is always a bug, never a filter.
//   - Apps only: the Apps list becomes the allowlist, verification for free.
//   - Apps AND raw Topics with no explicit list: REFUSED. Defaulting to Apps
//     would DLQ every record from the raw topics, whose producers were never
//     named; skipping verification would silently drop the check the Apps
//     subscription paid for. Neither is a defensible guess.
//
// STREAMING_CONSUMER_EXPECT_SOURCES (adopted via FromConfig) counts as an
// explicit list — it is the env-only way out of the Apps+Topics refusal — and a
// fluent ExpectSources(...) call overrides it. Failures name whichever of the
// two the list actually came from, so nobody hunts a fluent call that is not
// there when the value arrived from a fleet-wide environment variable.
func (b *ConsumerBuilder) resolveExpectedSources() error {
	explicit, origin := b.expectSources, "ExpectSources(...)"

	if len(explicit) == 0 && len(b.cfg.ExpectSources) > 0 {
		explicit, origin = b.cfg.ExpectSources, "STREAMING_CONSUMER_EXPECT_SOURCES"
	}

	if len(explicit) == 0 {
		if len(b.cfg.Apps) > 0 && len(b.cfg.Topics) > 0 {
			return fmt.Errorf("%w: apps=%v topics=%v — set ExpectSources(...) or STREAMING_CONSUMER_EXPECT_SOURCES",
				consumer.ErrAmbiguousSourceVerification, b.cfg.Apps, b.cfg.Topics)
		}

		b.cfg.ExpectSources = dedupSources(b.cfg.Apps)

		return nil
	}

	allowed := make(map[string]struct{}, len(explicit))

	for _, source := range explicit {
		if err := ValidateSource(source); err != nil {
			return fmt.Errorf("%w: %s entry %q: %w", consumer.ErrInvalidExpectSource, origin, source, err)
		}

		allowed[source] = struct{}{}
	}

	for _, app := range b.cfg.Apps {
		if _, ok := allowed[app]; !ok {
			return fmt.Errorf("%w: app %q is subscribed but not in %s %v",
				consumer.ErrExpectSourcesMissingApp, app, origin, explicit)
		}
	}

	b.cfg.ExpectSources = dedupSources(explicit)

	return nil
}

// dedupSources returns a copy of sources with repeats removed, first-seen order
// preserved.
//
// The ce-source allowlist is a SET: ExpectSources documents the UNION of every
// call, and Apps names producing applications rather than occurrences. Cloning
// it verbatim let a repeat survive into Dispatcher.Bind, which decides bare-On
// ambiguity on the list length — so a single-producer consumer whose app was
// named twice (STREAMING_CONSUMER_APPS="lender,lender" is an ordinary CSV
// paste) failed the build as ambiguous between lender and lender.
func dedupSources(sources []string) []string {
	out := make([]string, 0, len(sources))
	seen := make(map[string]struct{}, len(sources))

	for _, source := range sources {
		if _, dup := seen[source]; dup {
			continue
		}

		seen[source] = struct{}{}
		out = append(out, source)
	}

	return out
}
