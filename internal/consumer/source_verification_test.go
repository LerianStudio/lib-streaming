//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// sourceHeaders returns a valid CloudEvents header set whose ce-source is the
// supplied producing application.
func sourceHeaders(source string) []kgo.RecordHeader {
	headers := ceHeaders("tenantA", false)

	for i := range headers {
		if headers[i].Key == "ce-source" {
			headers[i].Value = []byte(source)
		}
	}

	return headers
}

// TestSourceVerification_RunsAheadOfEveryHandlerMode is the point of moving the
// ce-source check out of the Dispatcher and into the runtime.
//
// It used to be dispatch-only. A whole-stream Handler(...) — the mode that
// receives EVERY record on a topic whose write ACL it does not control, and
// therefore the mode a foreign write reaches first — got no verification at
// all, and asking for it was a hard build error. Both modes now check before a
// handler is ever called.
func TestSourceVerification_RunsAheadOfEveryHandlerMode(t *testing.T) {
	t.Parallel()

	newHandler := func(t *testing.T) Handler {
		t.Helper()

		return &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
			t.Error("handler ran for a foreign ce-source; verification must reject before dispatch")

			return nil
		}}
	}

	tests := []struct {
		name    string
		handler func(t *testing.T) Handler
	}{
		{"whole-stream Handler", newHandler},
		{
			"per-event dispatch",
			func(t *testing.T) Handler {
				t.Helper()

				return NewDispatcher().OnFrom("stranger", "loan.created", func(context.Context, contract.Event, []byte) error {
					t.Error("handler ran for a foreign ce-source; verification must reject before dispatch")

					return nil
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 4, sourceHeaders("stranger"))))
			dlq := &fakeDLQ{}

			r := newTestRuntimeCfg(t,
				func(cfg *ConsumerConfig) { cfg.ExpectSources = []string{"lender"} },
				client, tt.handler(t), dlq)

			runUntilClosed(t, r)

			if dlq.count() != 1 {
				t.Fatalf("DLQ count = %d; want 1 (a foreign ce-source must quarantine)", dlq.count())
			}

			cause, kind := dlq.lastCause()
			if kind != dlqCauseSourceMismatch {
				t.Errorf("cause kind = %q; want %q", kind, dlqCauseSourceMismatch)
			}

			if !errors.Is(cause, ErrUnexpectedSource) {
				t.Errorf("cause = %v; want it to wrap ErrUnexpectedSource", cause)
			}

			if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 5 {
				t.Errorf("committed watermark = %d; want 5 (commit after the quarantine copy is durable)", wm)
			}
		})
	}
}

// TestSourceVerification_AcceptsAnExpectedProducer is the other half: a record
// from a listed producer reaches the handler untouched.
func TestSourceVerification_AcceptsAnExpectedProducer(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 4, sourceHeaders("lender"))))
	dlq := &fakeDLQ{}

	r := newTestRuntimeCfg(t,
		func(cfg *ConsumerConfig) { cfg.ExpectSources = []string{"lender", "matcher"} },
		client, handler, dlq)

	runUntilClosed(t, r)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 for an expected producer", dlq.count())
	}

	if handler.callCount() != 1 {
		t.Errorf("handler called %d times; want 1", handler.callCount())
	}
}

// TestSourceVerification_EmptyAllowlistAcceptsAnything pins that verification
// stays OPT-IN for the raw Topics(...) escape hatch, whose producers were never
// named. Verifying against an empty list would quarantine 100% of that stream.
func TestSourceVerification_EmptyAllowlistAcceptsAnything(t *testing.T) {
	t.Parallel()

	handler := &fakeHandler{}
	client := newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 4, sourceHeaders("whoever"))))
	dlq := &fakeDLQ{}

	r := newTestRuntime(t, client, handler, dlq)
	runUntilClosed(t, r)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 (verification is opt-in)", dlq.count())
	}

	if handler.callCount() != 1 {
		t.Errorf("handler called %d times; want 1", handler.callCount())
	}
}
