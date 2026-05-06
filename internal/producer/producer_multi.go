package producer

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
	"github.com/LerianStudio/lib-streaming/internal/transport/kafka"
)

// TargetSpec is the per-target wiring fed to NewProducerMulti. The
// transport adapter is constructed up front by the caller (typically the
// public Builder.Build, which dispatches to per-kind factory functions).
type TargetSpec struct {
	Name    string
	Kind    contract.TransportKind
	Adapter transport.TransportAdapter
}

// MultiProducerConfig carries the runtime knobs that are NOT per-target.
// Today only CloudEventsSource and CB tuning live here; per-target broker
// wiring belongs on TargetSpec.Adapter.
type MultiProducerConfig struct {
	Source string
	// CloseTimeout caps Close/Flush per target. Zero means "use the
	// 30s default" (matches Config.CloseTimeout default).
	CloseTimeout time.Duration

	// CBFailureRatio / CBMinRequests / CBTimeout layer onto the HTTP
	// preset for every per-target circuit breaker. Zero values fall back
	// to the preset.
	CBFailureRatio float64
	CBMinRequests  int
	CBTimeout      time.Duration
}

// NewProducerMulti constructs a multi-target Producer from per-target
// TransportAdapters and an explicit RouteTable. Used by the public Builder
// when multiple targets and/or non-Kafka transport kinds are configured.
//
// Invariants:
//   - At least one target.
//   - Every route's Target field references a registered target.
//   - Every route's Destination.Kind matches the target's Kind.
//   - Catalog is non-empty and every PolicyOverride key matches a
//     definition.
//
// Constructor failures close every adapter that was successfully created so
// no socket leaks under partial-init failures.
func NewProducerMulti(
	ctx context.Context,
	mpc MultiProducerConfig,
	policyOverrides map[string]contract.DeliveryPolicyOverride,
	targets []TargetSpec,
	routes contract.RouteTable,
	catalog contract.Catalog,
	opts ...EmitterOption,
) (*Producer, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target required", contract.ErrMissingTarget)
	}

	if routes.Len() == 0 {
		return nil, contract.ErrNoRoutesConfigured
	}

	resolvedOpts := resolveEmitterOptions(opts)
	logger := resolvedOpts.logger

	// Reuse the catalog from the multi-target build path. Caller must
	// thread the catalog through opts (WithCatalog) for legacy code that
	// reads p.catalog directly (preFlight via resolveEvent), but since
	// resolveEvent is on the multi-path too, we ALSO seed it from the
	// explicit catalog argument when WithCatalog was omitted.
	if resolvedOpts.catalog.Len() == 0 {
		resolvedOpts.catalog = catalog
	}

	if err := validateCatalogAtBootstrap(resolvedOpts.catalog, policyOverrides, resolvedOpts.allowSystemEvents); err != nil {
		return nil, err
	}

	if err := validateRoutesAgainstTargets(ctx, logger, routes, targets, resolvedOpts.catalog); err != nil {
		return nil, err
	}

	closeTimeout := resolveCloseTimeout(resolvedOpts.closeTimeout, mpc.CloseTimeout)

	cbCfg := buildCBConfigFromMulti(mpc)

	// Manager: reuse caller-supplied or build one from the logger.
	cbManager := resolvedOpts.cbManager
	if isNilInterface(cbManager) {
		mgr, err := circuitbreaker.NewManager(logger)
		if err != nil {
			return nil, fmt.Errorf("streaming: init circuit breaker manager: %w", err)
		}

		cbManager = mgr
	}

	p := &Producer{
		cbManager:          cbManager,
		tracer:             resolveTracer(resolvedOpts.tracer),
		logger:             logger,
		metrics:            newStreamingMetrics(resolvedOpts.metricsFactory, logger),
		producerID:         generateProducerID(),
		partFn:             resolvedOpts.partitionKeyFn,
		closeTimeout:       closeTimeout,
		outboxWriter:       resolvedOpts.outboxWriter,
		stop:               make(chan struct{}),
		allowSystemEvents:  resolvedOpts.allowSystemEvents,
		catalog:            resolvedOpts.catalog,
		policyOverrides:    cloneDeliveryPolicyOverrides(policyOverrides),
		targets:            make(map[string]*targetRuntime, len(targets)),
		routes:             routes,
		cloudEventsSource:  mpc.Source,
		cbRecoveryInterval: resolveCBRecoveryInterval(cbCfg.Timeout),
	}

	// Build per-target runtimes BEFORE registering the shared CB listener.
	// Registering the listener first would create a window in which
	// foreign Producers sharing the same cbManager could fire transitions
	// while p.targets and p.primaryTargetName are still empty — the
	// listener would observe no matching target and silently drop the
	// notification, or worse, fire recordCircuitState for events that
	// aren't ours if the gauge gating ever loosens.
	for _, spec := range targets {
		if isNilInterface(spec.Adapter) {
			rollbackTargetAdapters(p)
			return nil, fmt.Errorf("streaming: target %q has no adapter", spec.Name)
		}

		if spec.Adapter.Kind() != spec.Kind {
			// Construction-time mirror of the runtime invariant in
			// dispatchRoute's route-kind assertion (see emit_multi.go).
			// The runtime mirror is "unreachable under normal flow" — and
			// IS observed today. The construction-time predecessor that
			// would catch the bug at bootstrap was silent until this
			// asserter landed. p.producerID is populated on the struct
			// literal above this loop, so we surface it on the trident
			// alongside spec.Name for replica correlation.
			a := p.newAsserter("producer_multi.adapter_kind_match")
			_ = a.That(ctx, false,
				"target adapter kind must match TargetSpec kind",
				"producer_id", p.producerID,
				"target", spec.Name,
				"spec_kind", string(spec.Kind),
				"adapter_kind", string(spec.Adapter.Kind()),
			)

			rollbackTargetAdapters(p)

			_ = spec.Adapter.Close(context.Background())

			return nil, fmt.Errorf("%w: target %q kind %q does not match adapter kind %q",
				contract.ErrInvalidRouteDefinition, spec.Name, spec.Kind, spec.Adapter.Kind())
		}

		serviceName := targetCBServiceName(p.producerID, spec.Name)

		cb, err := p.cbManager.GetOrCreate(serviceName, cbCfg)
		if err != nil {
			rollbackTargetAdapters(p)
			return nil, fmt.Errorf("streaming: register circuit breaker %q: %w", serviceName, err)
		}

		rt := &targetRuntime{
			name:          spec.Name,
			kind:          spec.Kind,
			adapter:       spec.Adapter,
			cb:            cb,
			cbServiceName: serviceName,
		}
		p.targets[spec.Name] = rt
	}

	// Pin the primary target — the one whose CB transitions drive the
	// single-dimension streaming_circuit_state gauge. Convention: the
	// FIRST entry in the caller-supplied targets slice. Selecting on
	// insertion order (rather than on map iteration) keeps the choice
	// deterministic and caller-controlled: tests, NewProducer, and
	// Builder.Build all materialise targets in a stable order.
	//
	// len(targets) > 0 is guaranteed by the up-front guard at the top of
	// this constructor; the redundant check below is a defensive belt
	// that surfaces an invariant violation as a wiring error rather than
	// a silent index-out-of-range panic if the guard is ever weakened.
	if len(targets) == 0 {
		rollbackTargetAdapters(p)
		return nil, fmt.Errorf("%w: invariant: at least one target after construction", contract.ErrNilProducer)
	}

	p.primaryTargetName = targets[0].Name

	// Register the listener LAST. p.targets and p.primaryTargetName are
	// now fully populated; the listener will correctly route any
	// transition — for our breakers OR for foreign breakers — to the
	// right per-target mirror (or ignore foreign traffic). Use a single
	// shared listener across targets; it filters by service name
	// internally.
	p.cbManager.RegisterStateChangeListener(&streamingStateListener{producer: p})

	// Background CB recovery goroutine: starts AFTER listener registration
	// so any transitions fired by the very first GetState poke are
	// observed by the listener and mirrored onto rt.state. Without the
	// listener wired first, an OPEN→HALF-OPEN transition triggered by the
	// initial poke would update gobreaker's state but never propagate to
	// the mirror. See cb_recovery.go for the full rationale.
	p.startCBRecoveryLoop()

	return p, nil
}

// validateRoutesAgainstTargets enforces the cross-product invariant that
// every route resolves to a registered target whose adapter kind matches
// the route's destination kind, AND every catalog definition has at least
// one route. Catches misconfigurations at construction time so callers see
// a precise wiring error before the first Emit.
//
// ctx and logger are used by the asserter trident so each construction-time
// failure branch fires a loud signal alongside its runtime mirror (see
// dispatchRoute's rt-nil and route-kind-check assertions in emit_multi.go).
// Without these tridents, the construction-time predecessors of the
// runtime invariants were silent — operators only saw telemetry on the
// unreachable runtime path.
//
// Each branch fires under a distinct operation label so dashboards can
// distinguish "unknown target", "unknown definition", "kind mismatch",
// and "orphan definition" without parsing wrapped sentinels.
func validateRoutesAgainstTargets(ctx context.Context, logger log.Logger, routes contract.RouteTable, targets []TargetSpec, catalog contract.Catalog) error {
	targetByName := make(map[string]contract.TransportKind, len(targets))
	for _, spec := range targets {
		targetByName[spec.Name] = spec.Kind
	}

	for _, route := range routes.Definitions() {
		kind, ok := targetByName[route.Target]
		if !ok {
			a := newAsserterForLogger(logger, "producer_multi.validate_routes_target_unknown")
			_ = a.That(ctx, false, "route target must reference a registered target",
				"route_key", route.Key,
				"target", route.Target,
			)

			return fmt.Errorf("%w: route %q references unknown target %q", contract.ErrInvalidRouteDefinition, route.Key, route.Target)
		}

		if kind != route.Destination.Kind {
			a := newAsserterForLogger(logger, "producer_multi.validate_routes_kind_mismatch")
			_ = a.That(ctx, false, "route destination kind must match target adapter kind",
				"route_key", route.Key,
				"target", route.Target,
				"route_kind", string(route.Destination.Kind),
				"target_kind", string(kind),
			)

			return fmt.Errorf("%w: route %q destination kind %q does not match target %q transport %q",
				contract.ErrInvalidRouteDefinition, route.Key, route.Destination.Kind, route.Target, kind)
		}

		if _, err := catalog.Require(route.DefinitionKey); err != nil {
			a := newAsserterForLogger(logger, "producer_multi.validate_routes_unknown_definition")
			_ = a.That(ctx, false, "route DefinitionKey must reference a catalog entry",
				"route_key", route.Key,
				"target", route.Target,
				"definition_key", route.DefinitionKey,
			)

			return fmt.Errorf("%w: %w", contract.ErrInvalidRouteDefinition, err)
		}
	}

	// Every catalog entry MUST have at least one route, otherwise
	// emit-time would surface ErrNoRoutesConfigured for any caller using
	// that key. Catching at construction is far better.
	for _, def := range catalog.Definitions() {
		if len(routes.Routes(def.Key)) == 0 {
			a := newAsserterForLogger(logger, "producer_multi.validate_routes_orphan_definition")
			_ = a.That(ctx, false, "catalog entry must have at least one route",
				"definition_key", def.Key,
			)

			return fmt.Errorf("%w: definition %q has no routes", contract.ErrNoRoutesConfigured, def.Key)
		}
	}

	return nil
}

// buildCBConfigFromMulti maps the MultiProducerConfig knobs onto the same
// circuit-breaker config used by the legacy single-target path. Falls back
// to the HTTP preset when fields are zero-valued.
func buildCBConfigFromMulti(mpc MultiProducerConfig) circuitbreaker.Config {
	cbCfg := circuitbreaker.HTTPServiceConfig()

	if mpc.CBFailureRatio > 0 {
		cbCfg.FailureRatio = mpc.CBFailureRatio
	}

	// CBMinRequests is `int` in MultiProducerConfig but uint32 in
	// circuitbreaker.Config. Bounded by [1, math.MaxUint32] so gosec G115
	// can prove the int->uint32 conversion is safe (matches the legacy
	// buildCBConfig pattern).
	if mpc.CBMinRequests > 0 && mpc.CBMinRequests <= math.MaxUint32 {
		cbCfg.MinRequests = uint32(mpc.CBMinRequests)
	}

	if mpc.CBTimeout > 0 {
		cbCfg.Timeout = mpc.CBTimeout
	}

	return cbCfg
}

// rollbackTargetAdapters closes every adapter that was successfully wired
// before a constructor failure. Called from NewProducerMulti.
func rollbackTargetAdapters(p *Producer) {
	if p == nil || len(p.targets) == 0 {
		return
	}

	for _, rt := range p.targets {
		if rt == nil || rt.adapter == nil {
			continue
		}

		_ = rt.adapter.Close(context.Background())
	}
}

// TransportAdapterFactory builds a TransportAdapter for a given target
// configuration. Used by the public Builder to plug in third-party
// transports (SQS, RabbitMQ, EventBridge, custom). Each factory owns the
// translation from the caller's per-target options to its concrete adapter.
//
// The opts argument is a per-target opaque struct provided by the caller via
// Builder.RegisterTransport — typically a config struct exported from the
// transport's own package.
type TransportAdapterFactory func(ctx context.Context, opts TransportAdapterOptions) (transport.TransportAdapter, error)

// TransportAdapterOptions is the input to TransportAdapterFactory. It carries
// the operator-supplied target name plus the polymorphic configuration
// payload registered alongside the route table.
type TransportAdapterOptions struct {
	Name    string
	Brokers []string
	Logger  log.Logger
	// Extra carries caller-supplied configuration data forwarded by the
	// public Builder. Factories type-assert on the expected concrete type.
	Extra any
}

// BuildKafkaAdapter spins up a Kafka transport adapter using the legacy
// franz-go option pipeline for a single multi-target Producer leg. The
// public Builder dispatches here for TransportKafkaLike unless a custom
// factory is registered.
//
// This helper performs only transport-level validation on cfg (brokers,
// compression, acks, batch sizing). Producer-level Config requirements
// (CloudEventsSource, etc.) are intentionally NOT enforced here — they
// belong to the Producer envelope, not to a per-target adapter.
func BuildKafkaAdapter(ctx context.Context, cfg Config, opts ...EmitterOption) (transport.TransportAdapter, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrMissingBrokers
	}

	resolvedOpts := resolveEmitterOptions(opts)

	kgoOpts, err := buildKgoOpts(cfg, *resolvedOpts)
	if err != nil {
		return nil, err
	}

	adapter, err := kafka.NewAdapter(kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("streaming: kgo client init: %s", sanitizeBrokerURL(err.Error()))
	}

	if isNilInterface(adapter) {
		return nil, fmt.Errorf("streaming: kafka adapter factory returned nil: %w", ErrNilProducer)
	}

	// ctx is propagated into kgo.NewClient via kgoOpts (see kafka.NewAdapter)
	// — no explicit use needed here. Kept in the signature to match the
	// TransportAdapterFactory contract used by Builder.RegisterTransport.
	return adapter, nil
}

// _ pins kgo as an explicit dep — BuildKafkaAdapter routes through
// kafka.NewAdapter which uses kgo.Opt internally; the explicit reference
// here keeps imports aligned even if helpers shuffle later.
var _ = kgo.NewClient
