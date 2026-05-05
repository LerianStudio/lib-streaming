package contract

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// RouteError is the per-route failure record carried by MultiEmitError. One
// RouteError is built per route that did not reach an accepted terminal
// state in a multi-target Emit. The Cause is the underlying *EmitError or
// sentinel; errors.Is on a RouteError walks into Cause.
type RouteError struct {
	// RouteKey is the canonical route key (RouteDefinition.Key).
	RouteKey string
	// DefinitionKey is the catalog definition key the route is bound to.
	DefinitionKey string
	// Target is the runtime target name the route resolved to.
	Target string
	// Transport is the transport family of the route's destination.
	Transport TransportKind
	// Destination is a sanitized human-readable description of the route
	// destination. SanitizeBrokerURL is applied at construction so it never
	// carries credentials.
	Destination string
	// Required reports whether the route was Required at the time of failure.
	Required bool
	// Class is the classified error bucket for this route's failure.
	Class ErrorClass
	// Cause is the underlying error returned by the per-route publish attempt.
	Cause error
}

// nilString is the sentinel a nil-receiver Error() returns. Centralized
// to satisfy goconst across the contract package.
const nilString = "<nil>"

// Error returns a sanitized one-line diagnostic.
func (r *RouteError) Error() string {
	if r == nil {
		return nilString
	}

	cause := ""
	if r.Cause != nil {
		cause = ": " + SanitizeBrokerURL(r.Cause.Error())
	}

	return "streaming route " + r.RouteKey +
		" target=" + r.Target +
		" transport=" + string(r.Transport) +
		" destination=" + r.Destination +
		" class=" + string(r.Class) +
		cause
}

// Unwrap returns the wrapped Cause.
func (r *RouteError) Unwrap() error {
	if r == nil {
		return nil
	}

	return r.Cause
}

// MultiEmitError aggregates failures from a single multi-target Emit. It is
// returned only when at least one Required route did not reach an accepted
// terminal state. Optional-route failures NEVER produce a MultiEmitError on
// their own — they surface via metrics, logs, span events, and (when
// configured) DLQ.
//
// Unwrap returns errors.Join over the underlying causes of every Required
// failure, so errors.Is still walks into the chain.
type MultiEmitError struct {
	// DefinitionKey is the catalog definition key Emit attempted to publish.
	DefinitionKey string
	// EventID is the resolved Event.EventID (after ApplyDefaults).
	EventID string
	// TenantID is the Event.TenantID at publish time. Empty for system events.
	TenantID string
	// Required is the set of Required-route failures that prevented the
	// Emit from reaching the all-or-error semantic. Always non-empty when
	// MultiEmitError is constructed.
	Required []RouteError
	// Optional carries Optional-route failures recorded for diagnostic
	// completeness. NEVER consulted by IsCallerError.
	Optional []RouteError

	// errorOnce memoizes the sorted, formatted Error() output. Error() is
	// called both directly (test assertions, log lines) and transitively
	// via fmt-style wrap chains, so a non-trivial number of failure modes
	// invoke it 2–3 times for the same instance. Pre-v1.1.0 every call
	// re-allocated, re-sorted, and re-formatted; the sync.Once collapses
	// the work to a single pass per instance while preserving the
	// stable-by-RouteKey ordering tests assert on.
	//
	// Safe for concurrent Error() calls (sync.Once guarantees one-shot
	// execution under contention) and for the zero-value MultiEmitError
	// (Once{} is ready-to-use). The cached string is private — every
	// public-facing render goes through Error().
	errorOnce sync.Once
	errorStr  string
}

// Error returns a sanitized aggregated string. Routes are listed in
// deterministic RouteKey order so log lines and test assertions remain
// stable.
//
// The sort + format pass runs ONCE per *MultiEmitError instance via
// sync.Once and the result is cached on the receiver. Repeat callers
// (log line, fmt wrap chain, errors.As inspection) get the cached
// string without re-sorting or re-allocating.
//
// Pre-v1.1.0 Error() sorted a defensive copy on every call. The
// sync.Once memoization preserves the public ordering contract while
// making repeated formatting effectively free.
func (e *MultiEmitError) Error() string {
	if e == nil {
		return nilString
	}

	e.errorOnce.Do(func() {
		e.errorStr = e.formatError()
	})

	return e.errorStr
}

// formatError builds the deterministic Error() string. Extracted so the
// sync.Once initializer keeps a clean closure; never called directly by
// other methods.
func (e *MultiEmitError) formatError() string {
	var b strings.Builder
	b.WriteString("streaming multi-emit failed: definition=")
	b.WriteString(e.DefinitionKey)

	if len(e.Required) > 0 {
		// Sort a one-shot defensive copy. e.Required is left untouched so
		// callers iterating it after Error() observe the original order
		// (which may already be sorted by aggregateRouteOutcomes — see
		// HANDOFF note in the package).
		sorted := append([]RouteError(nil), e.Required...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return sorted[i].RouteKey < sorted[j].RouteKey
		})

		b.WriteString(" required_failures=[")

		for i := range sorted {
			if i > 0 {
				b.WriteString("; ")
			}

			re := sorted[i]
			b.WriteString((&re).Error())
		}

		b.WriteString("]")
	}

	return b.String()
}

// Unwrap returns errors.Join over the Cause of every Required failure so
// errors.Is can match individual sentinels carried by any required route.
func (e *MultiEmitError) Unwrap() error {
	if e == nil || len(e.Required) == 0 {
		return nil
	}

	causes := make([]error, 0, len(e.Required))
	for i := range e.Required {
		if e.Required[i].Cause != nil {
			causes = append(causes, e.Required[i].Cause)
		}
	}

	if len(causes) == 0 {
		return nil
	}

	return errors.Join(causes...)
}

// HasRequiredFailures reports whether the error carries at least one
// Required-route failure. Used by the runtime to decide whether to surface
// the MultiEmitError to the caller.
func (e *MultiEmitError) HasRequiredFailures() bool {
	return e != nil && len(e.Required) > 0
}

// IsMultiEmitErrorCallerError reports whether err is a *MultiEmitError
// whose aggregate semantics indicate caller-correctable failure.
//
// Returns:
//   - callerCorrectable: true ONLY when err is *MultiEmitError AND every
//     Required failure has a caller-correctable cause AND
//     HasRequiredFailures() is true. A single infrastructure-class failure
//     flips this to false. Optional failures are NOT consulted — they
//     cannot turn an aggregate caller error into an infra one.
//   - matched: true when err is *MultiEmitError, regardless of the
//     callerCorrectable verdict. Distinguishes the empty-Required edge
//     case (matched=true, callerCorrectable=false) from "not a
//     MultiEmitError at all" (matched=false).
//
// IMPORTANT: callers MUST inspect BOTH return values. Discarding `matched`
// silently loses the empty-Required edge case where the helper returns
// (false, true) — i.e. "this IS a MultiEmitError, but with no required
// failures, so the all-or-error semantic was satisfied". Treating that as
// "not caller-correctable" without the second flag would cause callers to
// skip MultiEmitError-specific diagnostics.
//
// Use the public errors.go IsCallerError as the preferred entry point —
// it folds this two-value return into the standard one-value caller-error
// classification used everywhere else in the codebase.
func IsMultiEmitErrorCallerError(err error) (bool, bool) {
	if err == nil {
		return false, false
	}

	var multi *MultiEmitError
	if !errors.As(err, &multi) || multi == nil {
		return false, false
	}

	if !multi.HasRequiredFailures() {
		return false, true
	}

	for i := range multi.Required {
		cause := multi.Required[i].Cause
		if cause == nil {
			return false, true
		}

		if !IsCallerError(cause) {
			return false, true
		}
	}

	return true, true
}
