//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport/fake"
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
			wantCauseIs: nil,
		},
		{
			name: "source mismatch",
			handler: NewDispatcher().
				On("loan.created", func(context.Context, contract.Event, []byte) error { return nil }).
				ExpectSources("some-other-app"),
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
			r := newTestRuntime(t, newFakeGroupClient(), tt.handler, dlq)

			r.processFetches(context.Background(), fetchOf("t", 0, rec("t", 0, 1, tt.headers)))

			if dlq.count() != 1 {
				t.Fatalf("DLQ count = %d; want 1 (the record must quarantine)", dlq.count())
			}

			cause, kind := dlq.lastCause()

			if kind != tt.wantKind {
				t.Errorf("cause kind = %q; want %q", kind, tt.wantKind)
			}

			if cause == nil {
				t.Fatal("cause = nil; the DLQ entry must carry the underlying error")
			}

			if tt.wantCauseIs != nil && !errors.Is(cause, tt.wantCauseIs) {
				t.Errorf("cause = %v; want it to wrap %v", cause, tt.wantCauseIs)
			}

			if cause.Error() == errTerminalQuarantine.Error() {
				t.Error("cause is still the generic terminal marker; the real error was dropped")
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
	pub := &transportDLQPublisher{adapter: adapter, suffix: contract.DLQTopicSuffix, groupID: "g"}

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
