package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// Dispatch sentinels. Both are HANDLER-return errors, so they flow through the
// runtime's normal fail-closed disposition machinery (terminal -> DLQ unless a
// Classifier reclassifies them).
var (
	// ErrUnhandledEvent is returned for an event whose key has no registered
	// handler, under the opt-in UnmatchedError policy only.
	ErrUnhandledEvent = errors.New("streaming consumer: no handler registered for event key")

	// ErrUnexpectedSource is returned when an event's ce-source is not one of
	// the dispatcher's expected producers. It means either a producer
	// misconfiguration or a foreign write to the application's topic, so it
	// quarantines rather than dispatching.
	ErrUnexpectedSource = errors.New("streaming consumer: event ce-source is not an expected producer")
)

// UnmatchedPolicy decides what a Dispatcher does with an event whose key has
// no registered handler.
type UnmatchedPolicy string

const (
	// UnmatchedIgnore skips the event and commits it. THIS IS THE DEFAULT, and
	// it is the only safe one under one-topic-per-app: a consumer subscribed to
	// a producer's app stream receives EVERY event that producer emits, and
	// will legitimately care about a handful of them. Erroring on the rest
	// would fail-closed the producer's entire sibling stream into the DLQ.
	UnmatchedIgnore UnmatchedPolicy = "ignore"

	// UnmatchedError returns ErrUnhandledEvent, which the runtime treats as a
	// terminal handler error and quarantines. Opt into it only when the
	// consumer genuinely owns every event on the stream and an unknown key
	// means the producer's catalog drifted ahead of this consumer.
	UnmatchedError UnmatchedPolicy = "error"
)

// HandlerFunc is a single-event handler registered under an event key.
// It has the same signature as Handler.Handle.
type HandlerFunc func(ctx context.Context, event contract.Event, payload []byte) error

// Dispatcher is a Handler that routes each record to a per-event handler
// registered under its "<resourceType>.<eventType>" key, after verifying the
// event came from an expected producer.
//
// It exists because of the topic collapse. In v2 a consumer expressed "I want
// loan.disbursed" by subscribing to that event's own topic, and hand-rolled a
// ce-source check on top. In v3 one subscription delivers the producer's whole
// stream, so selection and source verification move here — into the library,
// once — instead of being re-implemented in every consuming repo.
//
// Construct with NewDispatcher, register with On, and hand it to the consumer
// builder. A Dispatcher is NOT safe for concurrent registration; build it
// fully during bootstrap, then let the runtime call Handle concurrently — the
// read path takes no locks and never mutates.
type Dispatcher struct {
	handlers map[string]HandlerFunc
	// expectedSources, when non-empty, is the allowlist of ce-source values
	// this dispatcher accepts. Empty means "accept any", so verification stays
	// opt-in for the raw .Topics(...) escape hatch.
	expectedSources []string
	unmatched       UnmatchedPolicy
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

// NewDispatcher returns an empty Dispatcher with the UnmatchedIgnore default
// and no source verification.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlers:  make(map[string]HandlerFunc),
		unmatched: UnmatchedIgnore,
	}
}

// On registers handler for an event key, i.e. "<resourceType>.<eventType>" —
// the same pair the producer's catalog spells (EventDefinition.EventKey) and
// the manifest advertises. Registering the same key twice keeps the LAST
// handler; a nil handler leaves the key unregistered, so it follows the
// unmatched policy.
//
// Underscores are fine: a snake_case ResourceType such as "loan_contract"
// travels verbatim, with none of the '_'->'-' translation v2 forced on
// consumers.
func (d *Dispatcher) On(eventKey string, handler HandlerFunc) *Dispatcher {
	if d == nil {
		return d
	}

	if d.handlers == nil {
		d.handlers = make(map[string]HandlerFunc)
	}

	if handler == nil {
		delete(d.handlers, eventKey)
		return d
	}

	d.handlers[eventKey] = handler

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

// ExpectSources declares the producing applications this dispatcher accepts,
// by ce-source. An event from any other source is rejected with
// ErrUnexpectedSource before any handler runs.
//
// The consumer builder populates this automatically from Apps(...), so a
// consumer that subscribes by application name gets the check for free. Called
// with no arguments (or never called), verification is off.
func (d *Dispatcher) ExpectSources(sources ...string) *Dispatcher {
	if d == nil {
		return d
	}

	d.expectedSources = append(d.expectedSources, sources...)

	return d
}

// EventKeys returns the registered keys. Used by the builder to fail a build
// that wired a dispatcher with no handlers at all.
func (d *Dispatcher) EventKeys() []string {
	if d == nil {
		return nil
	}

	keys := make([]string, 0, len(d.handlers))
	for key := range d.handlers {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}

// Handle verifies the event's source, then routes it to the handler registered
// under its event key.
//
// Order is deliberate: the source check runs FIRST, so a foreign write to the
// application's topic is quarantined even when its event key happens to be one
// this consumer handles, and even when the unmatched policy would otherwise
// have ignored it silently.
func (d *Dispatcher) Handle(ctx context.Context, event contract.Event, payload []byte) error {
	if d == nil {
		return ErrNilHandler
	}

	if !d.sourceAccepted(event.Source) {
		return fmt.Errorf("%w: got %q, want one of %v", ErrUnexpectedSource, event.Source, d.expectedSources)
	}

	key := contract.EventKey(event.ResourceType, event.EventType)

	handler, ok := d.handlers[key]
	if !ok || handler == nil {
		if d.observeUnmatched != nil {
			d.observeUnmatched(ctx, key)
		}

		if d.unmatched == UnmatchedError {
			return fmt.Errorf("%w: %q", ErrUnhandledEvent, key)
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

// sourceAccepted reports whether source is in the expected set. An empty set
// accepts everything (verification is opt-in).
func (d *Dispatcher) sourceAccepted(source string) bool {
	if len(d.expectedSources) == 0 {
		return true
	}

	return slices.Contains(d.expectedSources, source)
}

// ExpectedSources returns the declared producer allowlist. The root builder
// reads it to decide whether to default the allowlist from the subscribed
// Apps, so an explicit ExpectSources call is never silently overwritten.
func (d *Dispatcher) ExpectedSources() []string {
	if d == nil {
		return nil
	}

	return slices.Clone(d.expectedSources)
}
