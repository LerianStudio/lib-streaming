//go:build unit

package contract

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
)

// captureContractLogger records every Log call so the asserter trident's
// log layer is observable in contract-package tests. Mirrors the
// internal/producer captureLogger shape but stays package-private here.
type captureContractLogger struct {
	mu      sync.Mutex
	entries []string
}

func newCaptureContractLogger() *captureContractLogger {
	return &captureContractLogger{}
}

func (c *captureContractLogger) Log(_ context.Context, _ log.Level, msg string, _ ...log.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, msg)
}

func (c *captureContractLogger) With(_ ...log.Field) log.Logger { return c }
func (c *captureContractLogger) WithGroup(_ string) log.Logger  { return c }
func (c *captureContractLogger) Enabled(_ log.Level) bool       { return true }
func (c *captureContractLogger) Sync(_ context.Context) error   { return nil }

func (c *captureContractLogger) containsMessage(needle string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, msg := range c.entries {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// TestEvent_Topic_EmptySchema_DoesNotFireAssertion pins T8 invariant: an
// empty SchemaVersion is the documented default and MUST NOT fire any
// trident. NewEventDefinition normalizes empty to "1.0.0" upstream;
// callers building Event{} directly see the base form silently. We do
// NOT call t.Parallel() because the swap is a global pointer flip.
func TestEvent_Topic_EmptySchema_DoesNotFireAssertion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	e := Event{
		ResourceType: "transaction",
		EventType:    "created",
		// SchemaVersion intentionally empty.
	}

	got := e.Topic()
	if got != "lerian.streaming.transaction.created" {
		t.Errorf("Topic() = %q; want base form for empty schema", got)
	}

	if cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("trident fired on empty SchemaVersion; empty is the documented default and MUST stay silent")
	}
}

// TestEvent_Topic_MalformedSchema_DoesNotFireAssertion pins T8: Topic() is
// a zero-allocation hot-path helper and does NOT fire the asserter trident
// on malformed semver. The construction-time gate in NewEventDefinition
// (operation="event_definition.schema_version") catches malformed semver
// before any Event reaches Topic() in production. Callers building raw
// Event{} structs (tests, benchmarks) see the base form silently.
func TestEvent_Topic_MalformedSchema_DoesNotFireAssertion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	e := Event{
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "two-point-oh", // unparseable
	}

	got := e.Topic()
	if got != "lerian.streaming.transaction.created" {
		t.Errorf("Topic() = %q; want base form (public contract preserved on malformed schema)", got)
	}

	if cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("Topic() must not fire trident; the construction-time gate in NewEventDefinition is the single source of truth for SchemaVersion shape")
	}
}

// TestEvent_Topic_MajorGTE2_DoesNotFireAssertion pins T8: a parseable
// schema with major >= 2 returns the suffixed form and does NOT fire
// the trident — successful parse is the happy path.
func TestEvent_Topic_MajorGTE2_DoesNotFireAssertion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	e := Event{
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "2.0.0",
	}

	got := e.Topic()
	if got != "lerian.streaming.transaction.created.v2" {
		t.Errorf("Topic() = %q; want .v2 suffix", got)
	}

	if cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("trident fired on parseable major>=2 SchemaVersion; happy path must stay silent")
	}
}

// TestEvent_Topic_MajorLT2_DoesNotFireAssertion pins T8: a parseable
// schema with major < 2 returns base form and does NOT fire the trident.
func TestEvent_Topic_MajorLT2_DoesNotFireAssertion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	e := Event{
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.5.0",
	}

	got := e.Topic()
	if got != "lerian.streaming.transaction.created" {
		t.Errorf("Topic() = %q; want base form for major < 2", got)
	}

	if cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("trident fired on parseable major<2 SchemaVersion; happy path must stay silent")
	}
}
