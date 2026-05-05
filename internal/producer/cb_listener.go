package producer

import (
	"context"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

// streamingStateListener satisfies circuitbreaker.StateChangeListener for a
// single *Producer. The Manager notifies ALL registered listeners on every
// state transition across every service, so OnStateChange resolves the
// transition to one of this Producer's per-target runtimes (or drops the
// notification entirely when it belongs to a sibling Producer sharing the
// same Manager).
//
// State model: each targetRuntime owns its own atomic state mirror
// (rt.state). The listener updates exactly that mirror per transition —
// there is no Producer-level mirror.
//
// Cardinality discipline: streaming_circuit_state is a single gauge (no
// `target` label) so it must be driven by a single CB. Convention: the
// primary target — Producer.primaryTargetName, which NewProducerMulti
// pins to the first registered target. Per-target state remains
// observable via:
//   - the rt.state atomic (read-only, internal use, hot path);
//   - structured log lines emitted on every transition with a `target`
//     field;
//   - span events emitted by dispatchRoute when a per-target breaker
//     short-circuits.
//
// Adding a `target` label to the gauge would balloon dashboard cardinality
// for an N-target deployment with K active targets per Producer. Per-
// target observability flows through traces and logs instead.
//
// The Manager already runs each OnStateChange call in a
// SafeGoWithContextAndComponent goroutine with a 10-second context deadline
// (see circuitbreaker/manager.go). So we do NOT spawn our own goroutine and
// we do NOT hold any lock. OnStateChange performs a single atomic store
// onto rt.state plus an optional metric record (primary only) and a single
// log call — bounded in time, panic-safe through
// runtime.RecoverAndLogWithContext, and consistent with TRD §9 "no I/O in
// the listener".
//
// The constraint that makes this work: the circuit-breaker Manager's 10s
// listener deadline is plenty because the listener only touches atomics +
// logger. See TRD R5.
type streamingStateListener struct {
	// producer is the *Producer we mirror state onto. Never nil in normal
	// construction — listener registration happens after Producer is
	// assembled. Guarded defensively in OnStateChange and an asserter fires
	// the observability trident when the guard trips (see T-002).
	producer *Producer

	// fallbackLogger is used by OnStateChange only when producer is nil (an
	// invariant violation) so the asserter has somewhere to emit the trident
	// log. Normal construction leaves this nil; the asserter falls back to
	// log.NewNop() in that case. Tests that intentionally construct a
	// listener with producer=nil set this to a capture logger to observe the
	// assertion firing.
	fallbackLogger log.Logger
}

// OnStateChange is invoked by the circuit-breaker Manager on every state
// transition for any registered service. This implementation:
//
//  1. Recovers from panics via runtime.RecoverAndLogWithContext
//     (belt-and-suspenders; the Manager also wraps us in SafeGo).
//  2. Resolves the transition to a per-target runtime via cbServiceName.
//     Notifications for breakers belonging to sibling Producers (sharing
//     the same Manager) are dropped silently.
//  3. Mirrors the State enum onto rt.state.
//  4. Records the streaming_circuit_state gauge ONLY when the resolved
//     target is the Producer's primary — keeps the gauge single-dimension
//     while still reflecting at least one real CB.
//  5. Emits a single structured log line per transition.
//
// No I/O. No broker calls. No outbox writes. Keeping this function sub-
// millisecond is a TRD R5 requirement; tests assert it.
func (l *streamingStateListener) OnStateChange(
	ctx context.Context,
	serviceName string,
	from circuitbreaker.State,
	to circuitbreaker.State,
) {
	if l == nil {
		// Receiver-nil DX guard: a nil listener pointer cannot fire an
		// asserter (the fallback logger lives on the receiver). Returning
		// silently here matches the receiver-nil contract on every other
		// method in this package.
		return
	}

	if l.producer == nil {
		// Post-registration invariant violation: the listener was registered
		// with a live Producer and should never see nil here. Fire the
		// observability trident so silent listener detach becomes a loud
		// signal. The asserter is constructed with the fallback logger
		// because p.newAsserter is unreachable on nil p.
		logger := l.fallbackLogger
		if logger == nil {
			logger = log.NewNop()
		}

		a := assert.New(ctx, logger, asserterComponent, "cb_state_listener.on_state_change")
		_ = a.NotNil(ctx, l.producer, "state listener producer must be non-nil post-registration",
			"service", serviceName,
			"from", string(from),
			"to", string(to),
		)

		return
	}

	// Defense in depth — the Manager already wraps listeners in SafeGo with
	// its own recover, but a panic in a logger implementation would otherwise
	// poison the Manager goroutine. Use the context-aware variant so this inner
	// recovery still emits the runtime observability trident.
	defer runtime.RecoverAndLogWithContext(ctx, l.producer.logger, "streaming", "cb_state_listener")

	// Resolve the transition to one of our per-target runtimes. A nil
	// match means the notification belongs to a sibling Producer sharing
	// this Manager — drop it silently.
	targetRT := l.producer.targetRuntimeByServiceName(serviceName)
	if targetRT == nil {
		return
	}

	flag, recognized := stateToFlag(to)

	// Producer identity (producer_id) goes on every log line per TRD §7.3
	// so ops tooling can correlate CB transitions with emit log lines. The
	// "service" field remains for back-compat with any pre-existing
	// dashboards that key on it.
	producerIDField := log.String("producer_id", l.producer.producerID)
	targetField := log.String("target", targetRT.name)

	if !recognized {
		// Unknown state shouldn't happen for a registered breaker; log a
		// warning so operators notice if the circuitbreaker package ever
		// introduces a new state without updating this switch. NOT writing
		// to the metric gauge here — writing an unknown numeric would
		// corrupt the gauge semantics (0/1/2 closed/half-open/open).
		l.producer.logger.Log(ctx, log.LevelWarn, "streaming: circuit state unknown",
			producerIDField,
			targetField,
			log.String("service", serviceName),
			log.String("from", string(from)),
			log.String("to", string(to)),
		)

		return
	}

	// Single-mirror update. The atomic.Store happens BEFORE the log call
	// so a logger panic inside RecoverAndLogWithContext cannot lose the
	// state update — a TRD R5 invariant pinned by
	// TestProducer_CBListener_PanicSafe.
	targetRT.state.Store(flag)

	// Cardinality discipline: only the primary target drives the
	// single-dimension streaming_circuit_state gauge. Per-target state
	// remains observable via rt.state and structured logs.
	if targetRT.name == l.producer.primaryTargetName {
		l.producer.metrics.recordCircuitState(ctx, flag)
	}

	level := log.LevelInfo
	msg := "streaming: circuit closed"

	switch to {
	case circuitbreaker.StateHalfOpen:
		msg = "streaming: circuit half-open"
	case circuitbreaker.StateOpen:
		level = log.LevelWarn
		msg = "streaming: circuit open"
	case circuitbreaker.StateClosed:
		// default msg already set
	case circuitbreaker.StateUnknown:
		// Exhaustive-switch guard. stateToFlag filters this before mutation.
	}

	l.producer.logger.Log(ctx, level, msg,
		producerIDField,
		targetField,
		log.String("service", serviceName),
		log.String("from", string(from)),
		log.String("to", string(to)),
	)
}

// stateToFlag maps the circuitbreaker.State enum to the int32 flag values
// stored in targetRuntime.state. Returns recognized=false for StateUnknown
// so callers can short-circuit before mutating the mirror.
func stateToFlag(state circuitbreaker.State) (int32, bool) {
	switch state {
	case circuitbreaker.StateClosed:
		return flagCBClosed, true
	case circuitbreaker.StateHalfOpen:
		return flagCBHalfOpen, true
	case circuitbreaker.StateOpen:
		return flagCBOpen, true
	case circuitbreaker.StateUnknown:
		return flagCBClosed, false
	default:
		return flagCBClosed, false
	}
}
