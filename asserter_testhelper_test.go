package streaming

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

// captureLogger is a minimal log.Logger that records every Log call so tests
// can assert on the trident's log layer when an asserter fires. Concurrency
// safe — the circuit-breaker listener runs in a Manager-owned goroutine.
//
// We define this in a shared *_test.go file so every T-002..T-008 test can
// use the same capture shape without reinventing it.
type captureLogger struct {
	mu      sync.Mutex
	entries []captureEntry
}

type captureEntry struct {
	Level  log.Level
	Msg    string
	Fields []log.Field
}

func newCaptureLogger() *captureLogger {
	return &captureLogger{}
}

func (c *captureLogger) Log(_ context.Context, level log.Level, msg string, fields ...log.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Take a defensive copy of fields — the caller owns the backing slice and
	// the assert package reuses builder slices in some paths.
	f := make([]log.Field, len(fields))
	copy(f, fields)

	c.entries = append(c.entries, captureEntry{Level: level, Msg: msg, Fields: f})
}

func (c *captureLogger) With(_ ...log.Field) log.Logger { return c }
func (c *captureLogger) WithGroup(_ string) log.Logger  { return c }
func (c *captureLogger) Enabled(_ log.Level) bool       { return true }
func (c *captureLogger) Sync(_ context.Context) error   { return nil }

// containsMessage returns true if any captured entry's message contains the
// supplied substring. Used to verify an asserter-emitted log line fired.
func (c *captureLogger) containsMessage(needle string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range c.entries {
		if strings.Contains(e.Msg, needle) {
			return true
		}
	}

	return false
}

// swapWriterAsserterLogger swaps writerAsserterLogger for the duration of the
// test and restores the previous value via t.Cleanup. Tests that construct a
// libCommonsOutboxWriter with a nil repo use this to observe the asserter's
// trident log layer.
//
// NOT parallel-safe: the swap mutates a package-level variable. Tests using
// this helper MUST NOT call t.Parallel(). The underlying reason is that the
// outbox writer has no *Producer reference to carry a per-instance logger;
// the package variable is the only wiring path.
func swapWriterAsserterLogger(t *testing.T, l log.Logger) {
	t.Helper()
	prev := writerAsserterLogger
	writerAsserterLogger = l
	t.Cleanup(func() { writerAsserterLogger = prev })
}
