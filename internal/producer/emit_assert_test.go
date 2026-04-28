package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestClassifyNonEmitErrorFallback_FiresAssertion pins T-003: when
// publishDirect returns a non-*EmitError (a broken contract — publishDirect
// is documented to wrap every error in *EmitError), the fallback outcome
// label stays bounded at caller_error, AND an asserter fires so the
// contract break is visible instead of masquerading as a caller fault on
// dashboards.
//
// Before T-003: silent relabel to outcomeCallerError. Operators chase
// caller-correctable spikes that are actually lib-streaming bugs.
// After T-003: asserter.NoError fires the trident on the non-EmitError;
// the outcome is still set to outcomeCallerError so the metric cardinality
// is preserved.
func TestClassifyNonEmitErrorFallback_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	p := &Producer{logger: cap, producerID: "test-producer"}

	outcome := ""
	rawErr := errors.New("publishDirect contract broken: bare error")
	topic := "lerian.streaming.transaction.created"

	p.classifyNonEmitErrorFallback(context.Background(), topic, rawErr, &outcome)

	if outcome != outcomeCallerError {
		t.Errorf("outcome = %q, want %q (label must stay bounded after assertion fires)",
			outcome, outcomeCallerError)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter.NoError to fire on a non-*EmitError fallback")
	}

	// The assert package packs key/value pairs into a single "details" field
	// (structured format in the log). Verify the topic and error_type keys
	// both made it through.
	details := collectDetails(cap)
	if !strings.Contains(details, "topic") || !strings.Contains(details, topic) {
		t.Errorf("expected topic context in assertion details; got %q", details)
	}
	if !strings.Contains(details, "error_type") {
		t.Errorf("expected error_type context in assertion details; got %q", details)
	}
}

// collectDetails concatenates every captured "details" field into one string
// so the test can substring-match without needing the log.Field Value API.
// The assert package emits details as log.String("details", "<k=v lines>")
// (see lib-commons/v5 commons/assert/assert.go logAssertionStructured).
func collectDetails(cap *captureLogger) string {
	cap.mu.Lock()
	defer cap.mu.Unlock()

	var sb strings.Builder
	for _, e := range cap.entries {
		for _, f := range e.Fields {
			// Field rendering is implementation-specific; use fmt.Sprintf to
			// get a stable textual representation. Substring-only matching
			// keeps the test robust across log backends.
			fmt.Fprintf(&sb, "%v\n", f)
		}
	}

	return sb.String()
}
