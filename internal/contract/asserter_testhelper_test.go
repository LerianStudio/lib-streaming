//go:build unit

package contract

import (
	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v3/obs"
)

// setContractAsserterLogger overrides the package-default logger that
// backs every contract-package asserter. Returns the previous logger so
// tests can defer restoration:
//
//	prev := setContractAsserterLogger(captureLogger)
//	t.Cleanup(func() { setContractAsserterLogger(prev) })
//
// The underlying var is an atomic.Pointer[obs.Logger] (see route.go), so
// the swap+restore sequence is race-free even when multiple tests in the
// same package run in parallel against unrelated assertion sites — though
// any test that swaps the logger and then asserts on capture output MUST
// NOT call t.Parallel() because the swap is a global pointer flip. See
// event_topic_assert_test.go's discipline as the reference pattern.
//
// Package-private test helper. Production callers cannot reach it because
// it lives in a `_test.go` file (test-only build constraint).
func setContractAsserterLogger(logger obs.Logger) obs.Logger {
	if logger == nil {
		logger = log.NewNop()
	}

	previous := *contractAsserterLogger.Load()
	contractAsserterLogger.Store(&logger)

	return previous
}
