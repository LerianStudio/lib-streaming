//go:build unit

package contract

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
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

func (c *captureContractLogger) Log(_ context.Context, _ int, msg string, _ ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, msg)
}

func (c *captureContractLogger) With(_ ...any) log.Logger { return c }
func (c *captureContractLogger) WithGroup(_ string) log.Logger  { return c }
func (c *captureContractLogger) Enabled(_ int) bool       { return true }
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

// TestEvent_Topic_FiresNoAssertion pins that Topic() stays a silent,
// allocation-predictable hot-path helper for EVERY input shape: it must not
// fire the asserter trident even when handed a schema version or a source
// that upstream validation would reject.
//
// v2 had four separate cases here, one per semver branch of the topic
// derivation (empty / malformed / major<2 / major>=2). Those branches are
// gone: the schema version left the topic entirely, so the only thing left
// to pin is the silence itself. Rejection now happens at ValidateSource
// (config / Builder / preflight) and at NewEventDefinition's semver gate,
// where the trident and the error both belong.
//
// We do NOT call t.Parallel() because the logger swap is a global pointer flip.
func TestEvent_Topic_FiresNoAssertion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	for _, schemaVersion := range []string{"", "1.0.0", "2.3.1", "not-a-version"} {
		e := Event{
			Source:        "midaz-ledger",
			ResourceType:  "transaction",
			EventType:     "created",
			SchemaVersion: schemaVersion,
		}

		if got, want := e.Topic(), "lerian.streaming.midaz-ledger"; got != want {
			t.Errorf("Topic() with SchemaVersion=%q = %q; want %q", schemaVersion, got, want)
		}
	}

	// A source that ValidateSource genuinely REJECTS still must not make
	// Topic() shout; Topic() has no validation branch by design, and the
	// rejection is preflight's job.
	//
	// The inputs matter: "midaz-tx" PASSES ValidateSource, so using it here
	// never exercised the stated invariant at all. These two do not.
	for _, source := range []string{"//lerian.midaz/tx", "Lender"} {
		if err := ValidateSource(source); err == nil {
			t.Fatalf("ValidateSource(%q) = nil; this test needs a source that is actually rejected", source)
		}

		bad := Event{Source: source, ResourceType: "transaction", EventType: "created"}
		_ = bad.Topic()
	}

	if cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("trident fired from Topic(); the hot-path helper MUST stay silent for every input")
	}
}
