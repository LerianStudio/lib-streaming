//go:build unit

package consumer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

// recordingDiscardHandler captures every DiscardRecord handed to it and can be
// scripted to return an error.
type recordingDiscardHandler struct {
	mu      sync.Mutex
	records []dlqheader.DiscardRecord
	err     error
}

func (h *recordingDiscardHandler) HandleDiscard(_ context.Context, record dlqheader.DiscardRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record)

	return h.err
}

func (h *recordingDiscardHandler) last() (dlqheader.DiscardRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.records) == 0 {
		return dlqheader.DiscardRecord{}, false
	}

	return h.records[len(h.records)-1], true
}

func (h *recordingDiscardHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.records)
}

// quarantine runs ONE poison record through a real consumer runtime whose DLQ
// publisher is the production transportDLQPublisher, and returns the quarantine
// copy as a record on the DLQ topic — headers and payload exactly as a broker
// would hold them.
//
// The round trip through the REAL publisher is the point. A test that hand-wrote
// the nine forensic headers would prove only that the parser reads the strings
// the test wrote; this one fails if the writer and the reader ever disagree
// about a key, a value format, or which headers survive.
func quarantine(t *testing.T, source *kgo.Record, cause error, consumerApp string) *kgo.Record {
	t.Helper()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)

	client := newFakeGroupClient(fetchOf(source.Topic, source.Partition, source))
	handler := &fakeHandler{fn: func(context.Context, contract.Event, []byte) error { return cause }}

	r := newTestRuntime(t, client, handler, newTestDLQPublisher(adapter, consumerApp))

	runUntilClosed(t, r)

	msgs := adapter.Messages()
	if len(msgs) != 1 {
		t.Fatalf("quarantined %d records; want 1", len(msgs))
	}

	return dlqRecord(msgs[0], contract.AppDLQTopic(consumerApp))
}

// dlqRecord turns a published quarantine copy back into the record a consumer
// of the DLQ topic would fetch.
func dlqRecord(message transport.TransportMessage, dlqTopic string) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(message.Headers))
	for _, h := range message.Headers {
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}

	return &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Offset:    11,
		Key:       []byte(message.Key),
		Headers:   headers,
		Value:     message.Payload,
	}
}

// readDLQ runs one DLQ record through a consumer wired with a DiscardHandler and
// returns the handler plus the DLQ publisher, so a test can assert both what the
// reader received and that the reader quarantined nothing of its own.
func readDLQ(t *testing.T, record *kgo.Record, handler *recordingDiscardHandler, mutate func(*ConsumerConfig)) *fakeDLQ {
	t.Helper()

	client := newFakeGroupClient(fetchOf(record.Topic, record.Partition, record))
	dlq := &fakeDLQ{}

	r := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
		cfg.Topics = []string{record.Topic}

		if mutate != nil {
			mutate(cfg)
		}
	}, client, AsHandler(handler), dlq)

	runUntilClosed(t, r)

	return dlq
}

// TestDiscardHandler_ReceivesWhatDiedWhyAndFromWhere is the whole point of the
// discard seam: a service can drain its own ".dlq" and learn what the library
// stamped on the quarantine, which no Handler can see because the codec drops
// every non-ce-* header before Handle runs.
func TestDiscardHandler_ReceivesWhatDiedWhyAndFromWhere(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 3, 42, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("loan already settled"), "lender")

	handler := &recordingDiscardHandler{}

	dlq := readDLQ(t, entry, handler, nil)

	if handler.count() != 1 {
		t.Fatalf("discard handler ran %d times; want 1", handler.count())
	}

	if dlq.count() != 0 {
		t.Fatalf("the DLQ reader quarantined %d records of its own; want 0", dlq.count())
	}

	got, _ := handler.last()

	t.Run("where it came from", func(t *testing.T) {
		t.Parallel()

		if got.SourceTopic != "lerian.streaming.gateway" {
			t.Errorf("SourceTopic = %q; want %q", got.SourceTopic, "lerian.streaming.gateway")
		}

		if got.SourcePartition != 3 {
			t.Errorf("SourcePartition = %d; want 3", got.SourcePartition)
		}

		if got.SourceOffset != 42 {
			t.Errorf("SourceOffset = %d; want 42", got.SourceOffset)
		}
	})

	t.Run("why it died", func(t *testing.T) {
		t.Parallel()

		if got.CauseKind != dlqheader.CauseHandler {
			t.Errorf("CauseKind = %q; want %q", got.CauseKind, dlqheader.CauseHandler)
		}

		if !strings.Contains(got.ErrorMessage, "loan already settled") {
			t.Errorf("ErrorMessage = %q; want it to carry the handler's error", got.ErrorMessage)
		}

		if got.ErrorClass == "" {
			t.Error("ErrorClass is empty; want the transport's classification")
		}
	})

	t.Run("which tenant and what it was", func(t *testing.T) {
		t.Parallel()

		if got.EnvelopeError != nil {
			t.Fatalf("EnvelopeError = %v; want nil (the ce-* headers travel verbatim)", got.EnvelopeError)
		}

		if got.Event.TenantID != "tenant-abc" {
			t.Errorf("Event.TenantID = %q; want %q", got.Event.TenantID, "tenant-abc")
		}

		if key := contract.EventKey(got.Event.ResourceType, got.Event.EventType); key != "loan.created" {
			t.Errorf("event key = %q; want %q", key, "loan.created")
		}

		if got.Event.Source != "test-source" {
			t.Errorf("Event.Source = %q; want the ORIGINAL producer, not the quarantining app", got.Event.Source)
		}
	})

	t.Run("when and who quarantined it", func(t *testing.T) {
		t.Parallel()

		if got.ProducerID == "" {
			t.Error("ProducerID is empty; want the quarantining consumer group")
		}

		if got.FirstFailureAt.IsZero() {
			t.Error("FirstFailureAt is zero; want the quarantine stamp")
		}
	})

	t.Run("the payload is the real one", func(t *testing.T) {
		t.Parallel()

		if got.PayloadOmitted {
			t.Error("PayloadOmitted = true; want false — this record fit")
		}

		if string(got.Payload) != `{"ok":true}` {
			t.Errorf("Payload = %q; want the verbatim poison payload", got.Payload)
		}
	})
}

// TestDiscardHandler_DropOneHeaderLosesExactlyThatField is the mutation proof:
// every field the reader reports must come from its OWN header, not from the
// record's coordinates, a sibling header, or a fabricated default.
//
// It matters most for the origin triple. A reader that silently fell back to the
// DLQ record's own topic/partition/offset would look correct in every happy-path
// assertion above and point an operator at the quarantine queue instead of at
// the poison record — with no way to tell, because those coordinates are always
// populated.
func TestDiscardHandler_DropOneHeaderLosesExactlyThatField(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 3, 42, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("loan already settled"), "lender")

	tests := []struct {
		name string
		key  string
		zero func(dlqheader.DiscardRecord) bool
		what string
	}{
		{"origin topic", dlqheader.SourceTopic, func(r dlqheader.DiscardRecord) bool { return r.SourceTopic == "" }, "SourceTopic"},
		{"origin partition", dlqheader.SourcePartition, func(r dlqheader.DiscardRecord) bool { return r.SourcePartition == 0 }, "SourcePartition"},
		{"origin offset", dlqheader.SourceOffset, func(r dlqheader.DiscardRecord) bool { return r.SourceOffset == 0 }, "SourceOffset"},
		{"cause kind", dlqheader.CauseKind, func(r dlqheader.DiscardRecord) bool { return r.CauseKind == "" }, "CauseKind"},
		{"error class", dlqheader.ErrorClass, func(r dlqheader.DiscardRecord) bool { return r.ErrorClass == "" }, "ErrorClass"},
		{"error message", dlqheader.ErrorMessage, func(r dlqheader.DiscardRecord) bool { return r.ErrorMessage == "" }, "ErrorMessage"},
		{"retry count", dlqheader.RetryCount, func(r dlqheader.DiscardRecord) bool { return r.RetryCount == 0 }, "RetryCount"},
		{"first failure at", dlqheader.FirstFailureAt, func(r dlqheader.DiscardRecord) bool { return r.FirstFailureAt.IsZero() }, "FirstFailureAt"},
		{"producer id", dlqheader.ProducerID, func(r dlqheader.DiscardRecord) bool { return r.ProducerID == "" }, "ProducerID"},
	}

	// Sanity: with every header present, none of the nine fields reads as zero.
	// Without this the table below would pass against a parser that returns the
	// zero record for everything.
	whole := dlqheader.ParseRecord(withRetryCount(entry.Headers), entry.Value)

	for _, tt := range tests {
		if tt.zero(whole) {
			t.Fatalf("%s is zero with every header present; the mutation table would prove nothing", tt.what)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mutated := dlqheader.ParseRecord(dropHeader(withRetryCount(entry.Headers), tt.key), entry.Value)

			if !tt.zero(mutated) {
				t.Errorf("dropping %s left %s populated; the field is not sourced from its own header", tt.key, tt.what)
			}
		})
	}
}

// withRetryCount forces a non-zero retry count so the mutation table can tell
// "dropped" from "legitimately zero" for that one field. Every other forensic
// header is non-zero on a real quarantine already.
func withRetryCount(headers []kgo.RecordHeader) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(headers))

	for _, h := range headers {
		if h.Key == dlqheader.RetryCount {
			h.Value = []byte("2")
		}

		out = append(out, h)
	}

	return out
}

// TestDiscardHandler_MalformedEnvelopeIsDeliveredNotRequarantined pins the guard
// that keeps a DLQ reader from feeding itself.
//
// A "codec" quarantine is, by definition, a record whose CloudEvents envelope
// does not parse — and the quarantine copy is header-verbatim, so the entry on
// the DLQ topic does not parse either. On the normal path that is a terminal
// codec fault, which would republish the entry onto the very topic the reader is
// draining. Forever.
func TestDiscardHandler_MalformedEnvelopeIsDeliveredNotRequarantined(t *testing.T) {
	t.Parallel()

	// A record whose ce-* headers are missing entirely: the shape that made the
	// original consumer quarantine it with cause kind "codec".
	poison := &kgo.Record{
		Topic:     "lerian.streaming.gateway",
		Partition: 1,
		Offset:    7,
		Value:     []byte(`not json`),
		Headers:   []kgo.RecordHeader{{Key: "ce-id", Value: []byte("evt-9")}},
	}

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := newTestDLQPublisher(adapter, "lender")

	if err := pub.PublishDLQ(context.Background(), poison, errors.New("missing ce-specversion"), dlqheader.CauseCodec, 0); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	entry := dlqRecord(adapter.Messages()[0], contract.AppDLQTopic("lender"))
	handler := &recordingDiscardHandler{}

	dlq := readDLQ(t, entry, handler, nil)

	if dlq.count() != 0 {
		t.Fatalf("the reader quarantined %d records back onto the topic it drains; want 0", dlq.count())
	}

	if handler.count() != 1 {
		t.Fatalf("discard handler ran %d times; want 1 — an unparseable envelope is a DLQ's normal content", handler.count())
	}

	got, _ := handler.last()

	if got.EnvelopeError == nil {
		t.Error("EnvelopeError is nil; a reader must be able to tell a garbage envelope from a single-tenant one")
	}

	if got.CauseKind != dlqheader.CauseCodec {
		t.Errorf("CauseKind = %q; want %q — the forensic headers parse even when the envelope does not", got.CauseKind, dlqheader.CauseCodec)
	}

	if got.SourceTopic != "lerian.streaming.gateway" || got.SourceOffset != 7 {
		t.Errorf("origin = %s/%d; want lerian.streaming.gateway/7 — the route back must survive a dead envelope",
			got.SourceTopic, got.SourceOffset)
	}
}

// TestDiscardHandler_ForeignSourceIsNotQuarantined pins the second exemption. A
// quarantine copy carries the ORIGINAL producer's ce-source, never the reader's
// own application, so the source gate would reject a healthy DLQ entirely.
func TestDiscardHandler_ForeignSourceIsNotQuarantined(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	handler := &recordingDiscardHandler{}

	// An allowlist that does NOT contain the poison record's ce-source
	// ("test-source"), which is what any real DLQ reader's config looks like.
	dlq := readDLQ(t, entry, handler, func(cfg *ConsumerConfig) {
		cfg.ExpectSources = []string{"lender"}
	})

	if dlq.count() != 0 {
		t.Fatalf("source verification quarantined %d DLQ entries; want 0", dlq.count())
	}

	if handler.count() != 1 {
		t.Fatalf("discard handler ran %d times; want 1", handler.count())
	}
}

// TestDiscardHandler_TenantTravelsOnTheHandlerContext proves the reader gets the
// POISON record's tenant on ctx, from ce-tenantid, never from the payload.
func TestDiscardHandler_TenantTravelsOnTheHandlerContext(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-xyz", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	seen := make(chan string, 1)
	handler := &tenantSpyDiscardHandler{seen: seen}

	client := newFakeGroupClient(fetchOf(entry.Topic, entry.Partition, entry))

	r := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
		cfg.Topics = []string{entry.Topic}
	}, client, AsHandler(handler), &fakeDLQ{})

	runUntilClosed(t, r)

	select {
	case got := <-seen:
		if got != "tenant-xyz" {
			t.Errorf("tenant on handler ctx = %q; want %q", got, "tenant-xyz")
		}
	default:
		t.Fatal("discard handler never ran")
	}
}

type tenantSpyDiscardHandler struct{ seen chan string }

func (h *tenantSpyDiscardHandler) HandleDiscard(ctx context.Context, _ dlqheader.DiscardRecord) error {
	tid, _ := ctx.Value(tenantContextKey{}).(string)
	h.seen <- tid

	return nil
}

// TestDiscardHandler_ReturnedErrorStillQuarantines pins what is deliberately NOT
// lifted. The library exempts its own structural verdicts on this path; it does
// not override the service's. A reader that returns terminal on its own ".dlq"
// republishes onto the topic it drains — which is why the seam documents
// "return nil for anything you cannot use" rather than silently swallowing it.
func TestDiscardHandler_ReturnedErrorStillQuarantines(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	handler := &recordingDiscardHandler{err: errors.New("exception desk is down")}

	dlq := readDLQ(t, entry, handler, nil)

	if dlq.count() != 1 {
		t.Fatalf("DLQ count = %d; want 1 — a discard handler's own error is classified like any other", dlq.count())
	}

	_, kind := dlq.lastCause()
	if kind != dlqheader.CauseHandler {
		t.Errorf("cause kind = %q; want %q", kind, dlqheader.CauseHandler)
	}
}

// TestDiscardOnly_HandleIsRefused proves the unreachable Handler path fails
// loudly rather than dispatching into nothing, if a future refactor ever drops
// the runtime's discard resolution.
func TestDiscardOnly_HandleIsRefused(t *testing.T) {
	t.Parallel()

	err := AsHandler(&recordingDiscardHandler{}).Handle(context.Background(), contract.Event{}, nil)
	if !errors.Is(err, ErrDiscardHandlerMisrouted) {
		t.Errorf("Handle err = %v; want ErrDiscardHandlerMisrouted", err)
	}
}
