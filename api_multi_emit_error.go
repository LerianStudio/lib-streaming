package streaming

import "github.com/LerianStudio/lib-streaming/v2/internal/contract"

// RouteError is the per-route failure record carried by MultiEmitError.
//
// Returned in the Required (or Optional) slice of *MultiEmitError. errors.Is
// walks through RouteError.Cause so callers can match wrapped sentinels
// (ErrCircuitOpen, ErrEmitterClosed, ErrInvalidDestination, *EmitError, …).
type RouteError = contract.RouteError

// MultiEmitError aggregates Required-route failures from a single
// multi-target Emit. It is returned only when at least one Required route
// did not reach an accepted terminal state; Optional routes never trigger
// surfacing on their own.
//
// Unwrap returns errors.Join over every Required Cause, so:
//
//	errors.Is(multiErr, ErrCircuitOpen)  // true if ANY required route was circuit-open
//	errors.As(multiErr, &emitErr)        // captures the FIRST *EmitError in the chain
type MultiEmitError = contract.MultiEmitError
