//go:build unit

package consumer

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

// cappedAdapter models a real broker's per-record size limit: any message whose
// payload plus header bytes exceeds maxBytes is refused with the same
// MESSAGE_TOO_LARGE verdict franz-go surfaces, both from the broker and from its
// own client-side preflight.
//
// The shared fake adapter cannot express this — it has one static publish error,
// so it cannot fail the fat attempt and accept the slim one, which is the entire
// behaviour under test.
type cappedAdapter struct {
	mu       sync.Mutex
	maxBytes int
	msgs     []transport.TransportMessage
}

func (*cappedAdapter) Kind() contract.TransportKind { return contract.TransportKafkaLike }

func (a *cappedAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	size := len(message.Payload)
	for _, h := range message.Headers {
		size += len(h.Key) + len(h.Value)
	}

	if a.maxBytes > 0 && size > a.maxBytes {
		return kerr.MessageTooLarge
	}

	a.msgs = append(a.msgs, transport.CloneMessage(message))

	return nil
}

func (*cappedAdapter) Healthy(context.Context) error { return nil }
func (*cappedAdapter) Flush(context.Context) error   { return nil }
func (*cappedAdapter) Close(context.Context) error   { return nil }

func (*cappedAdapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	return contract.ClassBrokerUnavailable
}

func (a *cappedAdapter) published() []transport.TransportMessage {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]transport.TransportMessage(nil), a.msgs...)
}

// headerValue returns the value of key on message, and whether it was present.
func headerValue(message transport.TransportMessage, key string) (string, bool) {
	for _, h := range message.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

// newTestDLQPublisher builds the production publisher targeting the DLQ of the
// consumer application named by source.
func newTestDLQPublisher(adapter transport.TransportAdapter, source string) *transportDLQPublisher {
	return &transportDLQPublisher{
		adapter:  adapter,
		dlqTopic: contract.AppDLQTopic(source),
		groupID:  "g",
	}
}

// TestPublishDLQ_QuarantinesIntoTheConsumersOwnDLQ pins the ownership rule.
//
// A consumer used to republish poison into the PRODUCER's DLQ, derived from the
// record's own topic. That put a write on a topic the consumer's application
// does not own — every consumer needed a write grant on every producer's DLQ,
// and a filling DLQ named the wrong team. Quarantine is now the consuming
// application's own act, on its own topic: every app writes exactly two names,
// its topic and its .dlq.
func TestPublishDLQ_QuarantinesIntoTheConsumersOwnDLQ(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "matcher")

	source := &kgo.Record{
		Topic:     "lerian.streaming.lender",
		Partition: 3,
		Offset:    42,
		Key:       []byte("tenant-abc|loan-1"),
		Value:     []byte(`{"ok":true}`),
		Headers:   []kgo.RecordHeader{{Key: "ce-id", Value: []byte("evt-1")}},
	}

	if err := pub.PublishDLQ(context.Background(), source, errors.New("terminal"), dlqCauseHandler, 0); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	msgs := adapter.Messages()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages; want 1", len(msgs))
	}

	if got, want := msgs[0].Destination.Name, "lerian.streaming.matcher.dlq"; got != want {
		t.Errorf("DLQ destination = %q; want %q (the CONSUMER's own DLQ, not the producer's)", got, want)
	}

	if got := msgs[0].Key; got != string(source.Key) {
		t.Errorf("DLQ message Key = %q; want %q (verbatim from rec.Key)", got, source.Key)
	}
}

// TestPublishDLQ_CarriesTheOriginCoordinates pins the forensic headers that
// locate the poison record in the source topic.
//
// They were always useful; consumer-owned quarantine makes them load-bearing.
// The DLQ topic no longer implies where the record came from, so topic +
// partition + offset are the only way back to the original — and the only way a
// payload-omitted entry is recoverable at all.
func TestPublishDLQ_CarriesTheOriginCoordinates(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "matcher")

	source := &kgo.Record{
		Topic:     "lerian.streaming.lender",
		Partition: 7,
		Offset:    918,
		Value:     []byte(`{"ok":true}`),
	}

	if err := pub.PublishDLQ(context.Background(), source, errors.New("terminal"), dlqCauseHandler, 2); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	msg := adapter.Messages()[0]

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"origin topic", dlqheader.SourceTopic, "lerian.streaming.lender"},
		{"origin partition", dlqheader.SourcePartition, "7"},
		{"origin offset", dlqheader.SourceOffset, "918"},
		{"cause kind", dlqheader.CauseKind, dlqCauseHandler},
		{"retry count", dlqheader.RetryCount, "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := headerValue(msg, tt.key)
			if !ok {
				t.Fatalf("header %s missing", tt.key)
			}

			if got != tt.want {
				t.Errorf("%s = %q; want %q", tt.key, got, tt.want)
			}
		})
	}
}

// TestPublishDLQ_BoundsTheErrorMessageHeader proves the sanitized handler error
// cannot grow the quarantine copy without limit.
func TestPublishDLQ_BoundsTheErrorMessageHeader(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "matcher")

	huge := errors.New(strings.Repeat("x", dlqheader.MaxErrorMessageBytes*4))
	source := &kgo.Record{Topic: "lerian.streaming.lender", Partition: 0, Offset: 1, Value: []byte(`{}`)}

	if err := pub.PublishDLQ(context.Background(), source, huge, dlqCauseHandler, 0); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	got, ok := headerValue(adapter.Messages()[0], dlqheader.ErrorMessage)
	if !ok {
		t.Fatal("error-message header missing")
	}

	if len(got) > dlqheader.MaxErrorMessageBytes {
		t.Errorf("error-message header = %d bytes; want <= %d", len(got), dlqheader.MaxErrorMessageBytes)
	}
}

// TestPublishDLQ_OversizeRecordQuarantinesWithoutItsPayload is the fix for the
// permanent partition wedge.
//
// A quarantine copy is strictly LARGER than the record it quarantines: same
// payload, same headers, plus the forensic set. A near-cap record that fails a
// handler therefore could never fit in the DLQ — the publish failed, the
// runtime held the partition back fail-closed, and the record redelivered
// forever. Under one topic per app that stalls the producing application's
// entire catalog behind one poison record, with Healthy() reporting green.
//
// The record is now quarantined WITHOUT its payload, marked as such, with the
// origin coordinates intact so the payload stays recoverable from the source
// topic.
func TestPublishDLQ_OversizeRecordQuarantinesWithoutItsPayload(t *testing.T) {
	t.Parallel()

	payload := []byte(strings.Repeat("p", 900))
	// Tight enough that payload + forensic headers cannot fit, loose enough that
	// the headers alone can.
	adapter := &cappedAdapter{maxBytes: 800}
	pub := newTestDLQPublisher(adapter, "matcher")

	source := &kgo.Record{Topic: "lerian.streaming.lender", Partition: 4, Offset: 99, Value: payload}

	if err := pub.PublishDLQ(context.Background(), source, errors.New("terminal"), dlqCauseHandler, 0); err != nil {
		t.Fatalf("PublishDLQ: %v; want the payload-omitted retry to succeed", err)
	}

	msgs := adapter.published()
	if len(msgs) != 1 {
		t.Fatalf("accepted %d messages; want 1 (the slim retry)", len(msgs))
	}

	if len(msgs[0].Payload) != 0 {
		t.Errorf("slim DLQ copy carries %d payload bytes; want 0", len(msgs[0].Payload))
	}

	if got, ok := headerValue(msgs[0], dlqheader.PayloadOmitted); !ok || got != "true" {
		t.Errorf("%s = %q (present=%v); want %q", dlqheader.PayloadOmitted, got, ok, "true")
	}

	if got, ok := headerValue(msgs[0], dlqheader.PayloadBytes); !ok || got != strconv.Itoa(len(payload)) {
		t.Errorf("%s = %q (present=%v); want %q", dlqheader.PayloadBytes, got, ok, strconv.Itoa(len(payload)))
	}

	// The coordinates that make the omitted payload recoverable must survive.
	if got, _ := headerValue(msgs[0], dlqheader.SourceOffset); got != "99" {
		t.Errorf("slim copy source offset = %q; want 99 — without it the payload is unrecoverable", got)
	}
}

// TestPublishDLQ_NonSizeFailureIsNotRetried proves the slim retry is scoped to
// the one failure it can actually fix. A broker outage must surface as a failed
// quarantine (fail-closed halt), not silently drop the payload from the copy.
func TestPublishDLQ_NonSizeFailureIsNotRetried(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	adapter.SetPublishError(contract.ErrNilProducer)

	pub := newTestDLQPublisher(adapter, "matcher")
	source := &kgo.Record{Topic: "lerian.streaming.lender", Offset: 1, Value: []byte(`{}`)}

	err := pub.PublishDLQ(context.Background(), source, errors.New("terminal"), dlqCauseHandler, 0)
	if !errors.Is(err, contract.ErrNilProducer) {
		t.Fatalf("PublishDLQ = %v; want the original publish error surfaced", err)
	}
}

// TestPublishDLQ_SlimRetryFailureSurfaces proves the fail-closed path still ends
// in a returned error when even the payload-omitted copy cannot be written —
// the runtime must halt the partition rather than commit past un-quarantined
// poison.
func TestPublishDLQ_SlimRetryFailureSurfaces(t *testing.T) {
	t.Parallel()

	// maxBytes below the forensic header block: nothing fits, slim included.
	adapter := &cappedAdapter{maxBytes: 1}
	pub := newTestDLQPublisher(adapter, "matcher")

	source := &kgo.Record{Topic: "lerian.streaming.lender", Offset: 1, Value: []byte(strings.Repeat("p", 900))}

	err := pub.PublishDLQ(context.Background(), source, errors.New("terminal"), dlqCauseHandler, 0)
	if err == nil {
		t.Fatal("PublishDLQ = nil; want an error when even the payload-omitted retry cannot be written")
	}

	if !errors.Is(err, kerr.MessageTooLarge) {
		t.Errorf("PublishDLQ = %v; want it to wrap the transport's size verdict", err)
	}
}

// TestTransportDLQPublisher_CloseDelegates proves Close flushes/closes the
// underlying produce-side adapter (finding #4): without it the second franz-go
// client Build creates for DLQ publishing leaks.
func TestTransportDLQPublisher_CloseDelegates(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "matcher")

	if err := pub.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !adapter.Closed() {
		t.Error("DLQ adapter not closed; transportDLQPublisher.Close must delegate to adapter.Close")
	}
}

// TestConsumerRuntimeClose_ClosesDLQ proves the runtime closes the DLQ publisher
// on Close (finding #4), so the DLQ client is not leaked alongside the consume
// client.
func TestConsumerRuntimeClose_ClosesDLQ(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if dlq.closeCount() != 1 {
		t.Errorf("DLQ Close called %d times; want 1", dlq.closeCount())
	}

	// Idempotent: a second Close must not re-close the DLQ.
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if dlq.closeCount() != 1 {
		t.Errorf("DLQ Close called %d times after second runtime Close; want 1 (idempotent)", dlq.closeCount())
	}
}

// TestConsumerRuntimeClose_ReturnsDLQError proves a DLQ publisher close failure
// is surfaced from Close() (wrapped, not swallowed). A failed DLQ close can mean
// buffered quarantine writes were lost, so the shutdown path must let callers see
// it rather than silently returning nil.
func TestConsumerRuntimeClose_ReturnsDLQError(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient()
	dlq := &fakeDLQ{closeErr: contract.ErrNilProducer}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	err := r.Close()
	if err == nil {
		t.Fatal("Close returned nil; want the DLQ close failure surfaced, not swallowed")
	}

	if !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("Close error = %v; want it to wrap the DLQ close error (%v)", err, contract.ErrNilProducer)
	}
}
