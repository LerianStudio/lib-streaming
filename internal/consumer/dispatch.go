package consumer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// ErrUnhandledEvent is returned for an event whose (app, event key) pair has no
// registered handler: ALWAYS on a commands queue (Commands(...), where
// strictness is not configurable), and on a fact stream only under the opt-in
// UnmatchedError policy.
//
// It is a HANDLER-return error by shape, but the LIBRARY synthesizes it, so the
// runtime quarantines it outright rather than offering it to the service
// Classifier — see consumer.go's classify.
var ErrUnhandledEvent = errors.New("streaming consumer: no handler registered for event key")

// UnmatchedPolicy decides what a Dispatcher does with an event whose key has
// no registered handler.
//
// It governs FACT streams only — the topics named by Apps(...) and the raw
// Topics(...) escape hatch. A COMMANDS queue (Commands(...)) is always strict:
// an unmatched key there quarantines with cause kind "unhandled_key",
// regardless of this setting and with no knob to turn it off, because a
// command is work addressed to this consumer rather than noise on someone
// else's firehose. The strict verdict is applied by the runtime, which knows
// the record's topic; this policy is applied by the dispatcher, which does not.
type UnmatchedPolicy string

const (
	// UnmatchedIgnore skips the event and commits it. THIS IS THE DEFAULT for
	// fact streams, and the only safe one under one-topic-per-app: a consumer
	// subscribed to a producer's fact stream receives EVERY fact that producer
	// emits, and will legitimately care about a handful of them. Erroring on
	// the rest would fail-closed the producer's entire sibling stream into the
	// DLQ.
	UnmatchedIgnore UnmatchedPolicy = "ignore"

	// UnmatchedError returns ErrUnhandledEvent, which the runtime treats as a
	// terminal error and quarantines. Opt into it only when the consumer
	// genuinely owns every fact on the stream and an unknown key means the
	// producer's catalog drifted ahead of this consumer. A commands queue is
	// already strict without it.
	UnmatchedError UnmatchedPolicy = "error"
)

// HandlerFunc is a single-event handler registered under an event key.
// It has the same signature as Handler.Handle.
type HandlerFunc func(ctx context.Context, event contract.Event, payload []byte) error

// dispatchKey is what a Dispatcher routes on: the PRODUCING APPLICATION plus
// the event key.
//
// The app segment is not decoration. v3 put the app into ce-type precisely
// because two services emit byte-identical event names — "loan.disbursed" from
// lender and from matcher are different facts with different payloads — and a
// key without the app collapses them into one handler. The real fleet has
// byte-identical vocabularies across apps, so this is the common case, not a
// corner one, and getting it wrong is silent: the wrong handler parses the
// wrong payload and writes the result.
type dispatchKey struct {
	app      string
	eventKey string
}

// anyApp is the dispatchKey app segment meaning "any producing application".
// It is what a bare On(...) binds to when source verification is off — the raw
// Topics(...) escape hatch, where there is no allowlist to bind against.
const anyApp = ""

// Dispatcher is a Handler that routes each record to a per-event handler
// registered under its producing application and its
// "<resourceType>.<eventType>" key.
//
// It exists because of the topic collapse. In v2 a consumer expressed "I want
// loan.disbursed" by subscribing to that event's own topic. In v3 one
// subscription delivers the producer's whole stream, so selection moves here —
// into the library, once — instead of being re-implemented in every consuming
// repo.
//
// Registration has two forms and one rule:
//
//   - On(eventKey, fn) binds to the consumer's SOLE producing application. It
//     is the terse common case and it fails the build when the consumer
//     subscribes to more than one app, because there it is ambiguous.
//   - OnFrom(app, eventKey, fn) names the producer explicitly. It is the only
//     form that works for a multi-app consumer, and it is what lets the same
//     event name from two apps reach two different handlers.
//
// Construct with NewDispatcher, register, and hand it to the consumer builder,
// which calls Bind to resolve the bare registrations and reject the impossible
// ones. A Dispatcher is NOT safe for concurrent registration; build it fully
// during bootstrap, then let the runtime call Handle concurrently — the read
// path takes no locks and never mutates.
//
// Source VERIFICATION is not here: the runtime checks ce-source against the
// expected-source allowlist before any handler mode is invoked, so a
// whole-stream Handler gets the same protection a dispatching consumer does.
type Dispatcher struct {
	handlers  map[dispatchKey]HandlerFunc
	unmatched UnmatchedPolicy
	// observeUnmatched, when set, is called for every event whose key has no
	// registered handler. The consumer runtime wires it at Build so the
	// dispatcher can meter and log without owning a metrics factory of its
	// own. Nil is the valid standalone-dispatcher case.
	//
	// Ignoring unmatched events is the correct DEFAULT — an app stream carries
	// every event its producer emits — but the silent version of it is a trap:
	// a typo'd On("loan.disbursd") builds clean, commits the entire stream,
	// reports Healthy, and processes nothing, forever. This is the seam that
	// makes that visible.
	observeUnmatched func(ctx context.Context, eventKey string)
}

// Compile-time assertion: a Dispatcher must satisfy the Handler surface the
// runtime dispatches through.
var _ Handler = (*Dispatcher)(nil)

// NewDispatcher returns an empty Dispatcher with the UnmatchedIgnore default.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers:  make(map[dispatchKey]HandlerFunc),
		unmatched: UnmatchedIgnore,
	}
}

// On registers handler for an event key, i.e. "<resourceType>.<eventType>" —
// the same pair the producer's catalog spells (EventDefinition.EventKey) and
// the manifest advertises.
//
// It binds to the consumer's SOLE producing application, resolved at Build. A
// consumer that subscribes to more than one app must use OnFrom instead: with
// two producers in scope, a bare event key does not say whose event it is, and
// two apps really do publish the same names.
//
// Underscores are fine: a snake_case ResourceType such as "loan_contract"
// travels verbatim, with none of the '_'->'-' translation v2 forced on
// consumers. Registering the same (app, key) twice keeps the LAST handler; a
// nil handler leaves the pair unregistered, so it follows the unmatched policy.
func (d *Dispatcher) On(eventKey string, handler HandlerFunc) *Dispatcher {
	return d.register(anyApp, eventKey, handler)
}

// OnFrom registers handler for one event key from ONE named producing
// application — the explicit form, and the only one a multi-app consumer can
// use.
//
// It is what makes homonyms tractable: lender's "loan.disbursed" and matcher's
// "loan.disbursed" are different facts, and OnFrom sends each to its own
// handler instead of letting whichever registered last swallow both.
//
// app must be one of the applications the consumer accepts (Apps(...), or an
// explicit ExpectSources(...) list); naming any other fails the build rather
// than registering a handler nothing can ever reach.
func (d *Dispatcher) OnFrom(app, eventKey string, handler HandlerFunc) *Dispatcher {
	return d.register(app, eventKey, handler)
}

// register is the shared body of On / OnFrom.
func (d *Dispatcher) register(app, eventKey string, handler HandlerFunc) *Dispatcher {
	if d == nil {
		return d
	}

	if d.handlers == nil {
		d.handlers = make(map[dispatchKey]HandlerFunc)
	}

	key := dispatchKey{app: app, eventKey: eventKey}

	if handler == nil {
		delete(d.handlers, key)
		return d
	}

	d.handlers[key] = handler

	return d
}

// OnUnmatched sets the policy for events with no registered handler. An
// unrecognized value falls back to the safe UnmatchedIgnore default.
func (d *Dispatcher) OnUnmatched(policy UnmatchedPolicy) *Dispatcher {
	if d == nil {
		return d
	}

	if policy != UnmatchedError {
		policy = UnmatchedIgnore
	}

	d.unmatched = policy

	return d
}

// Bind resolves the registrations against the applications this consumer
// accepts, and is where an ambiguous or unreachable registration becomes a
// build failure instead of a silent runtime surprise. The consumer builder
// calls it once, after the expected-source allowlist is settled.
//
// Three outcomes:
//
//   - Exactly one app in scope: every bare On(...) binds to it. The terse
//     single-producer case keeps working exactly as written.
//   - More than one app in scope: a bare On(...) is ambiguous and FAILS
//     (ErrBareOnWithMultipleApps). Two producers publishing the same event name
//     is the normal case in this fleet, and binding to whichever one happened
//     to arrive would corrupt data silently.
//   - No app in scope (raw Topics with verification off): bare registrations
//     stay wildcards and match any source, because there is no allowlist to
//     bind against.
//
// Any OnFrom(app, ...) naming an application outside the scope fails
// (ErrUnknownDispatchApp): it could never receive a record.
func (d *Dispatcher) Bind(sources ...string) error {
	if d == nil {
		return nil
	}

	bare := d.bareKeys()

	switch {
	case len(bare) > 0 && len(sources) > 1:
		return fmt.Errorf("%w: bare On(...) registered for %v while subscribing to %v — use OnFrom(app, eventKey, handler)",
			ErrBareOnWithMultipleApps, bare, sources)

	case len(bare) > 0 && len(sources) == 1:
		for _, eventKey := range bare {
			handler := d.handlers[dispatchKey{anyApp, eventKey}]

			delete(d.handlers, dispatchKey{anyApp, eventKey})

			d.handlers[dispatchKey{sources[0], eventKey}] = handler
		}
	}

	if len(sources) == 0 {
		return nil
	}

	for _, key := range d.sortedKeys() {
		if key.app == anyApp || slices.Contains(sources, key.app) {
			continue
		}

		return fmt.Errorf("%w: OnFrom(%q, %q, ...) but this consumer accepts only %v",
			ErrUnknownDispatchApp, key.app, key.eventKey, sources)
	}

	return nil
}

// bareKeys returns the event keys registered without an app, sorted so a build
// error names them deterministically.
func (d *Dispatcher) bareKeys() []string {
	keys := make([]string, 0, len(d.handlers))

	for key := range d.handlers {
		if key.app == anyApp {
			keys = append(keys, key.eventKey)
		}
	}

	slices.Sort(keys)

	return keys
}

// sortedKeys returns every registered (app, event key) pair in a deterministic
// order, so a build failure always names the same one first.
func (d *Dispatcher) sortedKeys() []dispatchKey {
	keys := slices.Collect(maps.Keys(d.handlers))

	slices.SortFunc(keys, func(a, b dispatchKey) int {
		if c := strings.Compare(a.app, b.app); c != 0 {
			return c
		}

		return strings.Compare(a.eventKey, b.eventKey)
	})

	return keys
}

// EventKeys returns the distinct event keys registered, across every app,
// sorted. The builder reads only its length, to fail a build that wired a
// dispatcher with no handlers at all.
func (d *Dispatcher) EventKeys() []string {
	if d == nil {
		return nil
	}

	seen := make(map[string]struct{}, len(d.handlers))

	for key := range d.handlers {
		seen[key.eventKey] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}

// Handles reports whether a handler is registered for (app, eventKey), using
// the same two-step lookup Handle performs: the exact (app, key) pair first,
// then the wildcard registration a bare On(...) leaves behind when source
// verification is off.
//
// The runtime asks it BEFORE dispatching a record from a strict COMMANDS
// queue. Asking here rather than reading Handle's return keeps the strict
// verdict distinguishable from a handler that legitimately returned
// ErrUnhandledEvent of its own, and keeps the unmatched-fact metering — which
// counts records that were skipped and committed — from claiming a record that
// was quarantined instead.
func (d *Dispatcher) Handles(app, eventKey string) bool {
	if d == nil {
		return false
	}

	if handler, ok := d.handlers[dispatchKey{app: app, eventKey: eventKey}]; ok && handler != nil {
		return true
	}

	handler, ok := d.handlers[dispatchKey{app: anyApp, eventKey: eventKey}]

	return ok && handler != nil
}

// Handle routes the event to the handler registered for its producing
// application and event key, falling back to a wildcard registration when
// source verification is off.
//
// The event's ce-source has already been verified by the runtime before Handle
// is reached, so an event arriving here came from an accepted producer.
func (d *Dispatcher) Handle(ctx context.Context, event contract.Event, payload []byte) error {
	if d == nil {
		return ErrNilHandler
	}

	key := contract.EventKey(event.ResourceType, event.EventType)

	handler, ok := d.handlers[dispatchKey{app: event.Source, eventKey: key}]
	if !ok {
		handler, ok = d.handlers[dispatchKey{app: anyApp, eventKey: key}]
	}

	if !ok || handler == nil {
		if d.observeUnmatched != nil {
			d.observeUnmatched(ctx, key)
		}

		if d.unmatched == UnmatchedError {
			return fmt.Errorf("%w: %q from %q", ErrUnhandledEvent, key, event.Source)
		}

		return nil
	}

	return handler(ctx, event, payload)
}

// ObserveUnmatched wires the callback invoked for every event with no
// registered handler. The consumer runtime calls it at Build with a recorder
// that meters streaming_consumer_unmatched_total and logs each key the first
// time it is seen. Passing nil disables observation.
func (d *Dispatcher) ObserveUnmatched(fn func(ctx context.Context, eventKey string)) *Dispatcher {
	if d == nil {
		return d
	}

	d.observeUnmatched = fn

	return d
}
