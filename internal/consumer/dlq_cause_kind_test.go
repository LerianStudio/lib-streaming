//go:build unit

package consumer

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
	"github.com/LerianStudio/lib-streaming/v4/obs"
)

// TestRouteDLQ_StampsTheCauseKindThatQuarantinedTheRecord pins that every DLQ
// entry says WHY it is there.
//
// Every entry used to carry the same stable marker, so a filling DLQ told an
// operator that something was terminal and nothing else. A codec fault (the
// producer's wire format drifted), a source mismatch (a foreign write, or a
// drifted allowlist), an unhandled key (this consumer's registrations fell
// behind the producer's catalog) and a genuine business rejection have four
// different owners and four different fixes.
func TestRouteDLQ_StampsTheCauseKindThatQuarantinedTheRecord(t *testing.T) {
	t.Parallel()

	businessErr := errors.New("loan already settled")

	tests := []struct {
		name        string
		handler     Handler
		mutate      func(*ConsumerConfig)
		headers     []kgo.RecordHeader
		wantKind    string
		wantCauseIs error
	}{
		{
			name:     "codec fault",
			handler:  &fakeHandler{},
			headers:  []kgo.RecordHeader{{Key: "ce-id", Value: []byte("evt-1")}}, // no ce-specversion
			wantKind: dlqCauseCodec,
		},
		{
			name: "handler business rejection",
			handler: &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
				return businessErr
			}},
			headers:     ceHeaders("tenantA", false),
			wantKind:    dlqCauseHandler,
			wantCauseIs: businessErr,
		},
		{
			name: "source mismatch",
			handler: NewDispatcher().
				On("loan.created", func(context.Context, contract.Event, []byte) error { return nil }),
			mutate:      func(cfg *ConsumerConfig) { cfg.ExpectSources = []string{"some-other-app"} },
			headers:     ceHeaders("tenantA", false),
			wantKind:    dlqCauseSourceMismatch,
			wantCauseIs: ErrUnexpectedSource,
		},
		{
			name: "unhandled event key",
			handler: NewDispatcher().
				On("loan.settled", func(context.Context, contract.Event, []byte) error { return nil }).
				OnUnmatched(UnmatchedError),
			headers:     ceHeaders("tenantA", false),
			wantKind:    dlqCauseUnhandledKey,
			wantCauseIs: ErrUnhandledEvent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dlq := &fakeDLQ{}
			r := newTestRuntimeCfg(t, tt.mutate, newFakeGroupClient(), tt.handler, dlq)

			r.processFetches(context.Background(), fetchOf("t", 0, rec("t", 0, 1, tt.headers)))

			if dlq.count() != 1 {
				t.Fatalf("DLQ count = %d; want 1 (the record must quarantine)", dlq.count())
			}

			kind, cause := dlq.lastCause()

			if kind != tt.wantKind {
				t.Errorf("cause kind = %q; want %q", kind, tt.wantKind)
			}

			if cause == nil {
				t.Fatal("cause = nil; the DLQ entry must carry the underlying error")
			}

			if tt.wantCauseIs != nil && !errors.Is(cause, tt.wantCauseIs) {
				t.Errorf("cause = %v; want it to wrap %v", cause, tt.wantCauseIs)
			}
		})
	}
}

// TestPublishDLQ_StampsCauseKindHeader pins the wire contract: the cause kind
// reaches the DLQ record as x-lerian-dlq-cause-kind, where replay tooling and
// alert rules can read it.
func TestPublishDLQ_StampsCauseKindHeader(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "matcher")

	source := &kgo.Record{Topic: "lerian.streaming.lender", Partition: 3, Offset: 42, Value: []byte(`{}`)}

	if err := pub.PublishDLQ(context.Background(), source, ErrUnexpectedSource, dlqCauseSourceMismatch, 0); err != nil {
		t.Fatalf("PublishDLQ = %v; want nil", err)
	}

	messages := adapter.Messages()
	if len(messages) != 1 {
		t.Fatalf("published %d messages; want 1", len(messages))
	}

	got := ""

	for _, h := range messages[0].Headers {
		if h.Key == "x-lerian-dlq-cause-kind" {
			got = string(h.Value)
		}
	}

	if got != "source_mismatch" {
		t.Fatalf("x-lerian-dlq-cause-kind = %q; want source_mismatch", got)
	}
}

const handlerErrorLine = "streaming consumer: handler error"

// assertHandlerErrorLine asserts exactly one handler-error line at level, carrying
// every field in want.
func assertHandlerErrorLine(t *testing.T, spy *spyLogger, level int, want map[string]any) {
	t.Helper()

	if got := spy.levelsOf(handlerErrorLine); !slices.Equal(got, []int{level}) {
		t.Fatalf("%q levels = %v; want exactly one line at level %d", handlerErrorLine, got, level)
	}

	for key, w := range want {
		if got := spy.fieldValues(handlerErrorLine, key); len(got) != 1 || got[0] != w {
			t.Errorf("%s = %v; want %v", key, got, w)
		}
	}
}

// TestHandleWithRetry_LogsTheHandlerErrorBeforeTheDLQ pins that the reason a
// record was quarantined reaches the service log, not only the DLQ header: the
// header is readable only by whoever holds DLQ read access.
func TestHandleWithRetry_LogsTheHandlerErrorBeforeTheDLQ(t *testing.T) {
	t.Parallel()

	cause := "loan already settled via kafka://svc:s3cret@broker:9092 " + strings.Repeat("x", dlqheader.MaxErrorMessageBytes)
	handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
		return errors.New(cause)
	}}
	spy := newSpyLogger()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, newFakeGroupClient(), handler, dlq, WithLogger(spy))

	r.processFetches(context.Background(), fetchOf("t", 2, rec("t", 2, 41, ceHeaders("tenantA", false))))

	if dlq.count() != 1 {
		t.Fatalf("DLQ count = %d; want 1", dlq.count())
	}

	assertHandlerErrorLine(t, spy, obs.LevelError, map[string]any{
		"outcome": "dlq", "topic": "t", "partition": 2, "offset": int64(41), "event_key": "loan.created",
		"event_id": "evt-1", "tenant_id": "tenantA", "attempts": 1,
	})

	// Same scrub and cap as the DLQ header the record carries.
	got, _ := spy.fieldValues(handlerErrorLine, "error")[0].(string)
	if want := dlqheader.TruncateErrorMessage(contract.SanitizeBrokerURL(cause)); got != want || strings.Contains(got, "s3cret") {
		t.Errorf("error = %.80q... (%d bytes); want the scrubbed, capped cause (%d bytes)", got, len(got), len(want))
	}
}
