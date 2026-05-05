//go:build unit || integration || chaos

package producer

// Test-only helpers exposing the per-target CB mirror to test code.
//
// Build tag rationale: targetState is read from chaos_test.go (build tag
// `chaos`); setTargetState is written from unit-tagged tests. The combined
// `unit || integration || chaos` tag covers both audiences without leaking
// either symbol into a release binary.

// targetState returns the cached circuit-state mirror for tests.
func (p *Producer) targetState(targetName string) int32 {
	if p == nil {
		return flagCBClosed
	}

	rt, ok := p.targets[targetName]
	if !ok || rt == nil {
		return flagCBClosed
	}

	return rt.state.Load()
}

// setTargetState writes a circuit-state flag onto the named target's state
// mirror. Test-only counterpart to targetState. No-op when p is nil or the
// target is not registered, matching the reader's nil-handling shape.
//
// Used by tests that need to force the per-target CB mirror (e.g. to drive
// the outbox-fallback branch in dispatchRoute) without having to thread a
// fake circuitbreaker.Manager through construction.
func (p *Producer) setTargetState(targetName string, flag int32) {
	if p == nil {
		return
	}

	rt, ok := p.targets[targetName]
	if !ok || rt == nil {
		return
	}

	rt.state.Store(flag)
}
