package streaming

import (
	"context"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

// asserterComponent is the component label every streaming asserter carries.
// All assertion_failed_total metrics emitted by this package aggregate under
// component="streaming" with operation set per call site. This keeps the
// dashboard axis consistent with the streaming_* metric cardinality
// discipline: operation is bounded by call-site count; tenant_id MUST NOT
// be added.
const asserterComponent = "streaming"

// newAsserter returns a *assert.Asserter scoped to component="streaming" and
// the supplied operation label. One asserter per operation per ring:using-assert
// §2 — callers construct on demand at the invariant site rather than sharing
// a package singleton, because singleton component+operation pairs collapse
// every assertion into one metric dimension.
//
// The asserter emits the observability trident (log + span event + metric)
// on failure. The metric counter assertion_failed_total is wired via
// assert.InitAssertionMetrics, which the consuming service calls once at
// bootstrap after telemetry initialization. lib-streaming does NOT own
// bootstrap and does NOT call InitAssertionMetrics itself — without the
// consumer bootstrap hook, the log + span-event layers still fire but the
// metric counter stays at zero. See AGENTS.md for the consumer contract.
//
// Nil-receiver safe. On a nil *Producer the asserter is backed by
// log.NewNop() so assertion calls at nil-guarded boundaries still return a
// usable asserter rather than panicking.
func (p *Producer) newAsserter(operation string) *assert.Asserter {
	logger := log.NewNop()
	if p != nil && p.logger != nil {
		logger = p.logger
	}

	// assert.New's ctx is only used as a fallback when the per-method call
	// passes a nil ctx; each NotNil/NoError/That call site in the library
	// passes its own ctx, which takes precedence. Using context.Background()
	// here decouples asserter construction from any specific request ctx —
	// asserter lifetime is per call site and shorter than the caller's ctx.
	return assert.New(context.Background(), logger, asserterComponent, operation)
}
