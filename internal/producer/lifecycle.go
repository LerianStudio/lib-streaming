package producer

import (
	"context"
	"time"
)

// healthPingTimeout caps the per-Healthy broker Ping. 500ms is aligned with
// github.com/LerianStudio/lib-commons/v5/commons/rabbitmq and the `net/http.HealthWithDependencies` conventions —
// readiness probes should return fast.
const healthPingTimeout = 500 * time.Millisecond

// Close is idempotent: subsequent calls return nil without re-flushing. The
// first call flips the atomic, flushes the underlying transport adapter under
// a deadline derived from Producer.closeTimeout, then closes the adapter.
//
// Flush errors surface as returned errors so the caller can decide whether
// to proceed with shutdown or wait — but the transport adapter close is called
// regardless so socket resources always get reclaimed.
//
// Nil-receiver safe.
func (p *Producer) Close() error {
	if p == nil {
		return nil
	}

	return p.CloseContext(context.Background())
}

// CloseContext is the paired ctx-aware variant. The provided context is
// IGNORED for transport cleanup: per-target Flush and Close run under fresh
// producer-owned bounded background contexts so shutdown cleanup is not
// aborted by the caller's request lifecycle. A fresh deadline derived from
// Producer.closeTimeout caps each transport operation so Close cannot hang
// indefinitely. This preserves the "Close never blocks service shutdown
// forever" contract.
//
// Closes the internal stop channel so any in-flight (*Producer).RunContext
// unblocks — converging ctx-cancel-driven and Close-driven shutdown on the
// same exit path. stopOnce protects against a double-close panic when
// RunContext and an external Close race.
//
// Nil-ctx safe: ctx is accepted but not consumed.
func (p *Producer) CloseContext(_ context.Context) error {
	if p == nil {
		return nil
	}

	// CompareAndSwap guarantees exactly-one Flush + Close even under
	// concurrent Close callers.
	if !p.closed.CompareAndSwap(false, true) {
		// Already closed — still signal stop in case the prior path
		// missed it (defensive; the constructor always seeds p.stop).
		p.signalStop()

		return nil
	}

	// Unblock any in-flight RunContext before Flush so the Launcher
	// goroutine can observe the shutdown and exit cleanly. Safe to call
	// before Flush — RunContext's own CloseContext invocation is a no-op
	// under the CAS guard above.
	p.signalStop()

	// Drain every registered target adapter under fresh bounded contexts.
	return p.closeTargets(context.Background())
}

type sanitizedError struct {
	err error
}

func (e sanitizedError) Error() string {
	if e.err == nil {
		return ""
	}

	return sanitizeBrokerURL(e.err.Error())
}

func (e sanitizedError) Unwrap() error {
	return e.err
}

func sanitizeError(err error) error {
	if err == nil {
		return nil
	}

	return sanitizedError{err: err}
}

// signalStop closes the stop channel exactly once. Safe for concurrent
// callers via sync.Once. Nil-safe on the channel field so Producers
// constructed without the stop chan (theoretically possible in tests that
// build a *Producer directly) don't panic.
func (p *Producer) signalStop() {
	if p == nil {
		return
	}

	p.stopOnce.Do(func() {
		if p.stop == nil {
			// Invariant violation: NewProducer unconditionally assigns
			// p.stop = make(chan struct{}), so a nil channel here means the
			// *Producer was hand-built bypassing the constructor. Without
			// this signal a running RunContext hangs forever on shutdown.
			// Fire the observability trident; close(p.stop) is still
			// guarded by sync.Once so no double-close panic if someone
			// later assigns a channel.
			a := p.newAsserter("lifecycle.signal_stop")
			_ = a.NotNil(context.Background(), p.stop, "producer stop channel must be initialized post-construction",
				"producer_id", p.producerID,
			)

			return
		}

		close(p.stop)
	})
}

// Healthy reports readiness. Returns nil when the Producer is not closed and
// every registered target adapter responds to its Ping within
// healthPingTimeout. Otherwise returns a *HealthError with a typed State()
// so consumers of net/http.HealthWithDependencies can differentiate
// Degraded from Down.
//
// State semantics:
//   - Healthy: every target ping succeeds.
//   - Degraded: at least one target ping fails BUT either some target
//     remains healthy or an outbox writer is wired. Events whose policy
//     permits outbox fallback can still be captured durably, so readiness
//     consumers can keep serving traffic.
//   - Down: all targets fail AND no outbox wired. Emits will return
//     ErrCircuitOpen once the breakers trip. Readiness consumers should
//     route away.
//
// Nil-receiver safe. Nil-ctx safe: falls back to context.Background if ctx
// is nil (RunContext applies the same defense; symmetry across lifecycle
// entry points).
func (p *Producer) Healthy(ctx context.Context) error {
	if p == nil {
		return NewHealthError(Down, ErrNilProducer)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if p.closed.Load() {
		return NewHealthError(Down, ErrEmitterClosed)
	}

	return p.healthyTargets(ctx)
}
