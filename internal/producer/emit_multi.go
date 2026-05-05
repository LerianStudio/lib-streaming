package producer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

// routeOutcome captures the terminal state of one route's publish attempt.
// Used to drive metric/span recording and to decide whether a route counts
// as "accepted" for the all-or-error required-route semantic.
type routeOutcome struct {
	routeKey     string
	requirement  contract.RouteRequirement
	target       string
	transport    contract.TransportKind
	destination  string
	state        string // "produced" | "outboxed" | "circuit_open" | "dlq" | "failed" | "caller_error" | "outbox_failed"
	class        contract.ErrorClass
	cause        error
	durationMs   int64
	dlqDelivered bool // true when DLQ publish was attempted and reported success
}

// routeAccepted reports whether the outcome counts toward the all-or-error
// "Required must succeed" rule. Produced and outboxed are accepted; every
// other terminal state is a failure for required routes.
func (o routeOutcome) routeAccepted() bool {
	switch o.state {
	case outcomeProduced, outcomeOutboxed:
		return true
	default:
		return false
	}
}

// emitMulti is the multi-target dispatch path. Resolves routes for the
// definition key, fans out one publish attempt per route in deterministic
// order, classifies outcomes, and aggregates Required-route failures into a
// MultiEmitError when the all-or-error semantic is violated.
//
// Optional-route failures are recorded (metrics/log/span) but never surfaced
// in the returned error.
func (p *Producer) emitMulti(ctx context.Context, request contract.EmitRequest) error {
	resolved, err := p.resolveEventAllowDisabled(request)
	if err != nil {
		p.metrics.recordEmitted(ctx, metricTopicUnresolved, outcomeCallerError)
		return err
	}

	event := resolved.Event
	topic := resolved.Topic
	policy := resolved.Policy

	if p.closed.Load() {
		p.metrics.recordEmitted(ctx, topic, outcomeCallerError)
		return ErrEmitterClosed
	}

	// Caller-side validation — same gate as single-target Emit. Preflight
	// rejections never produce a span and do not enter route fan-out.
	if err := p.preFlightWithPayload(event, true); err != nil {
		p.metrics.recordEmitted(ctx, topic, outcomeCallerError)
		return err
	}

	routes := contract.RoutesUnsafe(&p.routes, resolved.DefinitionKey)
	if len(routes) == 0 {
		// No routes for this definition — caller misconfigured the
		// RouteTable or skipped a definition entirely. Surface as
		// ErrNoRoutesConfigured so the caller sees a precise wiring
		// signal instead of a silent drop.
		p.metrics.recordEmitted(ctx, topic, outcomeCallerError)
		return fmt.Errorf("%w: definition %q", contract.ErrNoRoutesConfigured, resolved.DefinitionKey)
	}

	// Per-transport payload cap. preFlightWithPayload already enforced the
	// global 1 MiB limit; here we narrow it for any required route whose
	// transport family advertises a smaller cap (SQS / EventBridge: 256
	// KiB). Optional routes are NOT considered — a payload that exceeds
	// an optional route's cap surfaces as a per-route failure at dispatch
	// time, not as a synchronous all-or-error rejection.
	//
	// Returning ErrPayloadTooLarge synchronously prevents the event from
	// landing in the outbox and perpetually failing replay against the
	// smaller-cap transport.
	if err := p.enforceRoutePayloadCap(ctx, event, topic, routes); err != nil {
		return err
	}

	// One span per Emit. The span lives across every route's publish
	// attempt so all outcomes share one trace context, matching the
	// single-route span discipline.
	ctx, span := p.tracer.Start(ctx, emitSpanName, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	p.setEmitSpanAttributes(span, event, topic, resolved.DefinitionKey, policy)
	span.SetAttributes(attribute.Int("streaming.routes.total", len(routes)))

	requiredCount := 0

	for i := range routes {
		if routes[i].Requirement == contract.RouteRequired {
			requiredCount++
		}
	}

	span.SetAttributes(attribute.Int("streaming.routes.required", requiredCount))

	// Routes come pre-sorted from contract.RoutesUnsafe (definition key,
	// then route key — the canonical order populated by NewRouteTable).
	// We read them directly without copying or re-sorting; the slice is
	// owned by the immutable RouteTable and dispatchRoute never mutates
	// the elements it receives.
	outcomes := make([]routeOutcome, 0, len(routes))

	headers := buildCloudEventsTransportHeaders(event)
	for i := range routes {
		outcomes = append(outcomes, p.dispatchRoute(ctx, span, event, topic, resolved.DefinitionKey, policy, headers, routes[i]))
	}

	return p.aggregateRouteOutcomes(span, resolved, outcomes)
}

// enforceRoutePayloadCap applies the smallest per-transport payload cap
// across every REQUIRED route to the event payload. Returns
// ErrPayloadTooLarge synchronously when the payload would not fit; the
// caller treats this as a caller-error path and records the metric.
//
// Optional routes are skipped: they are best-effort by definition, and a
// payload that exceeds an optional route's cap surfaces at dispatch time
// as a per-route failure that does not fail the Emit. Required-route
// caps are the all-or-error contract surface, so a synchronous cap
// rejection here is exactly the right place.
func (p *Producer) enforceRoutePayloadCap(ctx context.Context, event Event, topic string, routes []contract.RouteDefinition) error {
	minCap := contract.MaxPayloadBytes // 1 MiB starting point.
	capped := false

	for i := range routes {
		if routes[i].Requirement != contract.RouteRequired {
			continue
		}

		if c := contract.MaxPayloadBytesForKind(routes[i].Destination.Kind); c < minCap {
			minCap = c
			capped = true
		}
	}

	// No required route narrowed the cap — fall back to preFlight's 1 MiB
	// limit, which has already accepted the payload.
	if !capped || len(event.Payload) <= minCap {
		return nil
	}

	p.metrics.recordEmitted(ctx, topic, outcomeCallerError)

	return &EmitError{
		ResourceType: event.ResourceType,
		EventType:    event.EventType,
		TenantID:     event.TenantID,
		Topic:        topic,
		Class:        contract.ClassValidation,
		Cause:        contract.ErrPayloadTooLarge,
	}
}

// dispatchRoute resolves the per-route policy, builds the transport message,
// and runs the publish attempt through the route's target circuit breaker.
// Returns the routeOutcome describing the terminal state — caller never sees
// a returned error from this method, by design: outcomes (not errors) are
// the unit of record for multi-target dispatch.
func (p *Producer) dispatchRoute(
	ctx context.Context,
	span trace.Span,
	event Event,
	topic string,
	definitionKey string,
	basePolicy contract.DeliveryPolicy,
	headers []transport.Header,
	route contract.RouteDefinition,
) routeOutcome {
	start := time.Now()

	outcome := routeOutcome{
		routeKey:    route.Key,
		requirement: route.Requirement,
		target:      route.Target,
		transport:   route.Destination.Kind,
		destination: describeDestination(route.Destination),
	}

	defer func() {
		outcome.durationMs = time.Since(start).Milliseconds()
		// Per-route metrics fan-out: a definition wired to N routes
		// increments streaming_emitted_total by N per logical Emit
		// (one counter event per route attempt, not per Emit). This is
		// intentional — operators reading the dashboard see how many
		// publish attempts were made across the dispatch fan-out, which
		// is the actionable signal for routing health.
		//
		// Keyed on topic + outcome state for cardinality discipline,
		// matching the single-target metric contract. The route_key
		// and target labels are deliberately NOT emitted here: route
		// cardinality is bounded only at construction time and the
		// route count per service can grow without limit, so adding
		// either label would balloon dashboards. The same rule applies
		// to tenant_id (already excluded from every streaming_* metric
		// by AGENTS.md). Tenant identity lives on spans only.
		p.metrics.recordEmitted(ctx, topic, outcome.state)
		p.metrics.recordEmitDuration(ctx, topic, outcome.state, outcome.durationMs)
	}()

	// Per-route delivery policy resolution. The route's own policy
	// override layers on top of the already-resolved base policy
	// (definition default → config override → call override → ROUTE
	// override). Validate independently — a malformed route policy is a
	// caller error and propagates as such.
	routePolicy, err := applyRoutePolicy(basePolicy, route)
	if err != nil {
		outcome.state = outcomeCallerError
		outcome.class = contract.ClassValidation
		outcome.cause = err

		span.AddEvent("route.policy_invalid", trace.WithAttributes(
			attribute.String("route.key", route.Key),
			attribute.String("route.target", route.Target),
		))

		return outcome
	}

	if !routePolicy.HasDeliveryPath() {
		// Disabled-by-policy: not a failure per se, but not "produced"
		// either. Record as caller_error so dashboards can spot
		// disabled routes; the route does NOT count toward required-
		// route acceptance.
		outcome.state = outcomeCallerError
		outcome.class = contract.ClassValidation
		outcome.cause = ErrEventDisabled

		return outcome
	}

	rt, ok := p.targets[route.Target]
	if !ok || rt == nil {
		// Route references a target that was never registered. This is
		// a caller misconfiguration but we cannot surface it as
		// ErrInvalidRouteDefinition because the table validated at
		// build time — the only path here is post-construction state
		// corruption, so classify as validation but record cause.
		outcome.state = outcomeFailed
		outcome.class = contract.ClassValidation
		outcome.cause = fmt.Errorf("%w: target %q not registered", contract.ErrMissingTarget, route.Target)

		return outcome
	}

	// Post-construction invariants on the targetRuntime. NewProducerMulti
	// guarantees both rt.cb and rt.adapter are non-nil for every entry
	// in p.targets. Reaching dispatch with either nil is a state-
	// corruption invariant violation — fire the trident so the break is
	// visible on dashboards, then short-circuit with the documented
	// ErrNilProducer cause so the public contract stays unchanged.
	if rt.cb == nil || rt.adapter == nil {
		a := p.newAsserter("emit_multi.dispatch_route")
		_ = a.NotNil(ctx, rt.cb, "target circuit breaker must be initialized post-construction",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
		)
		_ = a.NotNil(ctx, rt.adapter, "target adapter must be initialized post-construction",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
		)

		outcome.state = outcomeFailed
		outcome.class = contract.ClassValidation
		outcome.cause = fmt.Errorf("%w: target %q runtime corrupted (cb or adapter nil)",
			ErrNilProducer, route.Target)

		return outcome
	}

	// Post-construction invariant: the route's destination kind matches
	// the target's adapter kind. validateRoutesAgainstTargets enforces
	// this at NewProducerMulti time (producer_multi.go:226-228) and the
	// RouteTable is documented immutable, so reaching dispatch with a
	// kind mismatch is a state-corruption invariant violation — an
	// unreachable code path under normal flow. Fire the trident so the
	// break is visible on dashboards, then short-circuit with the
	// documented ErrInvalidDestination cause so the public contract
	// stays unchanged on the assertion-fires path.
	if rt.kind != route.Destination.Kind {
		a := p.newAsserter("emit_multi.dispatch_route_kind_check")
		_ = a.Never(ctx, "route destination kind must match target adapter kind (validated at NewProducerMulti, RouteTable is immutable)",
			"producer_id", p.producerID,
			"route_key", route.Key,
			"target", route.Target,
			"route_kind", string(route.Destination.Kind),
			"target_kind", string(rt.kind),
		)

		outcome.state = outcomeCallerError
		outcome.class = contract.ClassValidation
		outcome.cause = fmt.Errorf("%w: destination kind %q does not match target %q transport %q",
			contract.ErrInvalidDestination, route.Destination.Kind, route.Target, rt.kind)

		return outcome
	}

	// Build the per-route transport message.
	//
	// Lifetime invariant (matches the toKgoHeaders documentation in
	// internal/transport/kafka/adapter.go:236-267):
	//
	//   - event.Payload is owned by the caller for the duration of Emit
	//     and reused verbatim across every route. The Kafka adapter hands
	//     it to franz-go's Produce and waits synchronously on the produce
	//     callback before returning, so no goroutine retains it past
	//     Publish. The SQS / RabbitMQ adapters document that body is "a
	//     fresh slice the adapter does not retain". The EventBridge
	//     adapter embeds it inside a json.RawMessage and marshals
	//     synchronously. None of the adapters retain Payload past Publish.
	//
	//   - headers is the canonical []transport.Header slice built ONCE per
	//     Emit by buildCloudEventsTransportHeaders and is package-owned
	//     read-only across every route. SQS / RabbitMQ / EventBridge each
	//     deep-copy header bytes inside their own buildAttributes /
	//     buildHeaders / mapHeaders, so reusing the slice across routes
	//     is correctness-safe. The Kafka adapter relies on the same
	//     lifetime invariant documented at toKgoHeaders.
	//
	// Cloning Payload + headers per route was reintroducing exactly the
	// per-Emit allocation that the kgo→transport→kgo triple-conversion
	// fix removed; for an N-route definition this was N×(payload + len
	// headers) extra allocations on every Emit.
	partKey := event.PartitionKey()
	if p.partFn != nil {
		partKey = p.partFn(event)
	}

	message := transport.TransportMessage{
		Destination: route.Destination.Normalize(),
		TenantID:    event.TenantID,
		Key:         partKey,
		Payload:     event.Payload,
		Headers:     headers,
	}

	// Outbox-always policy short-circuits the broker entirely.
	if routePolicy.OutboxAlways() {
		obxErr := p.publishRouteOutbox(ctx, event, definitionKey, route, routePolicy)
		if obxErr != nil {
			outcome.state = outcomeOutboxFailed
			// publishRouteOutbox can fail with DB connection errors
			// (broker-adjacent infra), with envelope-validation errors
			// (caller-correctable), or with serialization errors. Run
			// the result through the target adapter's Classify — it's
			// the broker-adjacent classifier and yields the right class
			// for both infra (BrokerUnavailable) and caller-correctable
			// (Validation) failures. ClassValidation is the wrong default:
			// a DB outage would otherwise be mislabeled as a caller bug
			// and bypass the breaker / DLQ semantics tied to infra
			// classes.
			outcome.class = rt.adapter.Classify(obxErr)
			outcome.cause = obxErr
			span.RecordError(obxErr)

			return outcome
		}

		outcome.state = outcomeOutboxed

		span.AddEvent("route.outbox_routed", trace.WithAttributes(
			attribute.String("route.key", route.Key),
			attribute.String("route.target", route.Target),
			attribute.String("reason", "policy_always"),
		))
		p.metrics.recordOutboxRouted(ctx, topic, "policy_always")

		return outcome
	}

	// Pre-CB circuit-state mirror check. A subsequent cb.Execute may
	// short-circuit with gobreaker.ErrOpenState if the breaker flipped
	// between this check and the Execute call; the Execute branch below
	// handles that race by routing to the same fallback path.
	//
	// IMPORTANT: outcome is passed BY VALUE to executeRoutePublish and
	// handleRouteCircuitOpen, so any field mutation inside those helpers
	// applies to a copy. The deferred metrics closure above reads
	// dispatchRoute's LOCAL outcome variable, so we must reassign it
	// here from the helper's return value before returning. Without
	// this assignment, the deferred recordEmitted/recordEmitDuration
	// would log a still-empty outcome.state for every route that exits
	// through these branches.
	if rt.state.Load() == flagCBOpen {
		outcome = p.handleRouteCircuitOpen(ctx, span, event, topic, definitionKey, route, routePolicy, outcome)
		return outcome
	}

	if !routePolicy.DirectAllowed() {
		outcome.state = outcomeCallerError
		outcome.class = contract.ClassValidation
		outcome.cause = ErrEventDisabled

		return outcome
	}

	outcome = p.executeRoutePublish(ctx, span, rt, event, topic, definitionKey, route, routePolicy, message, outcome)

	return outcome
}

// executeRoutePublish runs the actual publish attempt for a single route
// through the per-target circuit breaker, classifies the resulting error,
// and routes the failure to DLQ when policy permits. Split out from
// dispatchRoute to keep cyclomatic complexity bounded.
func (p *Producer) executeRoutePublish(
	ctx context.Context,
	span trace.Span,
	rt *targetRuntime,
	event Event,
	topic string,
	definitionKey string,
	route contract.RouteDefinition,
	routePolicy contract.DeliveryPolicy,
	message transport.TransportMessage,
	outcome routeOutcome,
) routeOutcome {
	firstAttempt := time.Now().UTC()

	_, execErr := rt.cb.Execute(func() (any, error) {
		if p.closed.Load() {
			return nil, ErrEmitterClosed
		}

		return nil, rt.adapter.Publish(ctx, message)
	})
	if execErr == nil {
		outcome.state = outcomeProduced

		return outcome
	}

	if errors.Is(execErr, gobreaker.ErrOpenState) || errors.Is(execErr, gobreaker.ErrTooManyRequests) {
		return p.handleRouteCircuitOpen(ctx, span, event, topic, definitionKey, route, routePolicy, outcome)
	}

	cls := rt.adapter.Classify(execErr)
	outcome.class = cls
	outcome.cause = execErr

	if routePolicy.DLQAllowed() && isDLQRoutable(cls) {
		if delivered, dlqErr := p.publishRouteDLQ(ctx, rt, event, route, execErr, firstAttempt); dlqErr == nil && delivered {
			outcome.dlqDelivered = true
			outcome.state = outcomeDLQ

			p.metrics.recordDLQ(ctx, topic, string(cls))

			return outcome
		}
	}

	if contract.IsCallerError(execErr) {
		outcome.state = outcomeCallerError

		return outcome
	}

	outcome.state = outcomeFailed

	span.RecordError(execErr)

	return outcome
}

// handleRouteCircuitOpen runs the circuit-open fallback for one route.
// Outbox-fallback policy + outbox writer wired → write a v2 envelope and
// mark outboxed. Otherwise → outcomeCircuitOpen (which counts as a failure
// for required routes; optional routes consume it without surfacing).
func (p *Producer) handleRouteCircuitOpen(
	ctx context.Context,
	span trace.Span,
	event Event,
	topic string,
	definitionKey string,
	route contract.RouteDefinition,
	policy contract.DeliveryPolicy,
	outcome routeOutcome,
) routeOutcome {
	if policy.OutboxFallbackOnCircuitOpen() && p.outboxWriter != nil {
		// Resolve the target runtime to access its Classify; reaching
		// here from dispatchRoute means rt is non-nil (asserted just
		// before). Read it again rather than threading a parameter
		// through to keep the signature stable.
		rt := p.targets[route.Target]

		obxErr := p.publishRouteOutbox(ctx, event, definitionKey, route, policy)
		if obxErr != nil {
			outcome.state = outcomeOutboxFailed
			// See dispatchRoute's outbox-always branch for the same
			// rationale: an outbox write failure is broker-adjacent
			// infra (DB unavailable), not a caller bug. Defer to the
			// target adapter's classifier so the resulting class
			// matches the rest of the route's failure taxonomy.
			if rt != nil && rt.adapter != nil {
				outcome.class = rt.adapter.Classify(obxErr)
			} else {
				// Defensive: if we somehow lost rt between dispatch
				// and here (impossible under documented invariants),
				// fall back to BrokerUnavailable rather than
				// mislabeling as caller-correctable.
				outcome.class = contract.ClassBrokerUnavailable
			}

			outcome.cause = obxErr
			span.RecordError(obxErr)

			return outcome
		}

		outcome.state = outcomeOutboxed

		span.AddEvent("route.outbox_routed", trace.WithAttributes(
			attribute.String("route.key", route.Key),
			attribute.String("route.target", route.Target),
			attribute.String("reason", "circuit_open"),
		))
		p.metrics.recordOutboxRouted(ctx, topic, "circuit_open")

		return outcome
	}

	outcome.state = outcomeCircuitOpen
	outcome.class = contract.ClassBrokerUnavailable
	outcome.cause = ErrCircuitOpen

	return outcome
}

// aggregateRouteOutcomes inspects every per-route outcome, decides whether
// the all-or-error semantic was satisfied, and either returns nil or
// constructs a MultiEmitError with the Required-route failures.
//
// ctx is intentionally NOT a parameter: aggregation is pure-data and the
// span is the only observability surface needed. Callers cancelling ctx
// cannot meaningfully abort an aggregation that has zero I/O.
func (p *Producer) aggregateRouteOutcomes(
	span trace.Span,
	resolved resolvedEvent,
	outcomes []routeOutcome,
) error {
	requiredFailures := make([]routeOutcome, 0)
	optionalFailures := make([]routeOutcome, 0)

	for _, o := range outcomes {
		if o.routeAccepted() {
			continue
		}

		if o.requirement == contract.RouteOptional {
			optionalFailures = append(optionalFailures, o)
			continue
		}

		requiredFailures = append(requiredFailures, o)
	}

	// Optional-failure observability: drop a span event so trace viewers
	// surface the partial degradation, but never propagate an error.
	for _, o := range optionalFailures {
		span.AddEvent("route.optional_failed", trace.WithAttributes(
			attribute.String("route.key", o.routeKey),
			attribute.String("route.target", o.target),
			attribute.String("route.state", o.state),
			attribute.String("error.type", string(o.class)),
		))
	}

	if len(requiredFailures) == 0 {
		return nil
	}

	multi := &contract.MultiEmitError{
		DefinitionKey: resolved.DefinitionKey,
		EventID:       resolved.Event.EventID,
		TenantID:      resolved.Event.TenantID,
		Required:      make([]contract.RouteError, 0, len(requiredFailures)),
	}

	for _, o := range requiredFailures {
		multi.Required = append(multi.Required, contract.RouteError{
			RouteKey:      o.routeKey,
			DefinitionKey: resolved.DefinitionKey,
			Target:        o.target,
			Transport:     o.transport,
			Destination:   o.destination,
			Required:      true,
			Class:         o.class,
			Cause:         o.cause,
		})

		span.AddEvent("route.required_failed", trace.WithAttributes(
			attribute.String("route.key", o.routeKey),
			attribute.String("route.target", o.target),
			attribute.String("route.state", o.state),
			attribute.String("error.type", string(o.class)),
		))
	}

	if len(optionalFailures) > 0 {
		multi.Optional = make([]contract.RouteError, 0, len(optionalFailures))
		for _, o := range optionalFailures {
			multi.Optional = append(multi.Optional, contract.RouteError{
				RouteKey:      o.routeKey,
				DefinitionKey: resolved.DefinitionKey,
				Target:        o.target,
				Transport:     o.transport,
				Destination:   o.destination,
				Required:      false,
				Class:         o.class,
				Cause:         o.cause,
			})
		}
	}

	span.SetAttributes(
		attribute.Int("streaming.routes.required_failed", len(requiredFailures)),
		attribute.Int("streaming.routes.optional_failed", len(optionalFailures)),
	)

	return multi
}

// describeDestination returns a stable, sanitized one-line description of a
// destination suitable for log lines, trace attributes, and *RouteError
// messages. This is the LOG/TRACE/ERROR-MESSAGE renderer — for human and
// operator consumption only. It is NOT the manifest wire renderer.
//
// The manifest wire format is produced by destinationDisplay in
// internal/manifest/manifest.go. The two renderers intentionally differ
// for two transports:
//
//   - RabbitMQ: this renderer uses "name:address";
//     destinationDisplay uses "name/address".
//   - Custom:   this renderer uses "name address" (space-separated);
//     destinationDisplay uses "name|address" (pipe-separated).
//
// Do NOT unify these two functions. destinationDisplay is pinned to the
// streaming.ManifestVersion wire contract and any change to its output
// requires a manifest version bump (see manifest.go). describeDestination
// has no wire commitment and may evolve freely for readability of logs
// and traces.
func describeDestination(dest contract.Destination) string {
	switch dest.Kind {
	case contract.TransportKafkaLike:
		return dest.Name
	case contract.TransportEventBridge:
		return dest.Name
	case contract.TransportSQS:
		return contract.SanitizeBrokerURL(dest.Address)
	case contract.TransportRabbitMQ:
		return dest.Name + ":" + dest.Address
	case contract.TransportCustom:
		if dest.Name != "" && dest.Address != "" {
			return dest.Name + " " + contract.SanitizeBrokerURL(dest.Address)
		}

		if dest.Address != "" {
			return contract.SanitizeBrokerURL(dest.Address)
		}

		return dest.Name
	default:
		return string(dest.Kind)
	}
}

// applyRoutePolicy layers the route's per-route policy override onto the
// already-resolved base policy. The route override is the LAST precedence
// layer (after definition default, config override, and call override) so a
// route can downgrade or restrict policy without affecting siblings.
//
// The override itself is validated once at NewRouteDefinition (route.go's
// `route.Policy.Validate()`); we do NOT re-run override.Validate() here on
// the hot path. The merged policy IS revalidated because the cross-field
// rule (`Direct=skip` requires `Outbox=always`) is only checked on
// DeliveryPolicy.Validate, not on DeliveryPolicyOverride.Validate — an
// override that sets only Direct=skip would otherwise produce an invalid
// merged policy when the base policy has Outbox != always.
func applyRoutePolicy(base contract.DeliveryPolicy, route contract.RouteDefinition) (contract.DeliveryPolicy, error) {
	override := route.Policy
	if override == (contract.DeliveryPolicyOverride{}) {
		// No route-level override — return base unchanged.
		return base, nil
	}

	policy := base
	if override.Enabled != nil {
		policy.Enabled = *override.Enabled
	}

	if override.Direct != "" {
		policy.Direct = override.Direct
	}

	if override.Outbox != "" {
		policy.Outbox = override.Outbox
	}

	if override.DLQ != "" {
		policy.DLQ = override.DLQ
	}

	if err := policy.Validate(); err != nil {
		return contract.DeliveryPolicy{}, fmt.Errorf("%w: route %q: %w", contract.ErrInvalidRouteDefinition, route.Key, err)
	}

	return policy, nil
}
