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

// recordingDiscard captures every delivery through the discard seam. It holds
// RAW headers because that is what the seam carries: the typed record is a root
// type, parsed at the facade, and this package never names it.
type recordingDiscard struct {
	mu       sync.Mutex
	headers  [][]kgo.RecordHeader
	payloads [][]byte
	tenants  []string
	err      error
}

func (d *recordingDiscard) dispatch(ctx context.Context, headers []kgo.RecordHeader, payload []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.headers = append(d.headers, headers)
	d.payloads = append(d.payloads, payload)

	tid, _ := ctx.Value(tenantContextKey{}).(string)
	d.tenants = append(d.tenants, tid)

	return d.err
}

func (d *recordingDiscard) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.headers)
}

// value returns the value of key on the most recent delivery.
func (d *recordingDiscard) value(key string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.headers) == 0 {
		return "", false
	}

	for _, h := range d.headers[len(d.headers)-1] {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

// quarantine runs ONE poison record through a real consumer runtime whose DLQ
// publisher is the production transportDLQPublisher, and returns the quarantine
// copy as a record on the DLQ topic — headers and payload exactly as a broker
// would hold them.
//
// The round trip through the REAL publisher is the point. A test that hand-wrote
// the forensic headers would prove only that the reader sees the strings the
// test wrote; this one fails if the writer and the reader ever disagree about a
// key, a value format, or which headers survive.
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

// readDLQ runs one DLQ record through a consumer wired with the discard seam and
// returns the fake DLQ publisher, so a test can assert both what the reader
// received and that the reader quarantined nothing of its own.
//
// The reader carries its OWN ce-source, which is the shape the library now
// requires: "test-consumer" quarantines into lerian.streaming.test-consumer.dlq,
// never into the topic it drains.
func readDLQ(t *testing.T, record *kgo.Record, discard *recordingDiscard, mutate func(*ConsumerConfig)) *fakeDLQ {
	t.Helper()

	client := newFakeGroupClient(fetchOf(record.Topic, record.Partition, record))
	dlq := &fakeDLQ{}

	r := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
		cfg.Topics = []string{record.Topic}

		if mutate != nil {
			mutate(cfg)
		}
	}, client, nil, dlq, WithDiscardDispatch(discard.dispatch))

	runUntilClosed(t, r)

	return dlq
}

// TestDiscardSeam_CarriesTheForensicHeadersThroughVerbatim is the whole point of
// the seam: the x-lerian-dlq-* keys a Handler can never see, because the codec
// drops every non-ce-* header before Handle runs, reach the reader intact.
//
// It asserts by KEY against the writer's own constants, which is what the root
// facade's parser reads; TestDLQHeaderConstants_MatchTheWriter pins the facade's
// literals to these same constants, so the two halves cannot drift apart.
func TestDiscardSeam_CarriesTheForensicHeadersThroughVerbatim(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 3, 42, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("loan already settled"), "lender")

	discard := &recordingDiscard{}

	dlq := readDLQ(t, entry, discard, nil)

	if discard.count() != 1 {
		t.Fatalf("discard seam ran %d times; want 1", discard.count())
	}

	if dlq.count() != 0 {
		t.Fatalf("the DLQ reader quarantined %d records of its own; want 0", dlq.count())
	}

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"origin topic", dlqheader.SourceTopic, "lerian.streaming.gateway"},
		{"origin partition", dlqheader.SourcePartition, "3"},
		{"origin offset", dlqheader.SourceOffset, "42"},
		{"cause kind", dlqheader.CauseKind, dlqheader.CauseHandler},
		{"retry count", dlqheader.RetryCount, "0"},
		{"the original tenant", "ce-tenantid", "tenant-abc"},
		{"the ORIGINAL producer, not the quarantining app", "ce-source", "test-source"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := discard.value(tt.key)
			if !ok {
				t.Fatalf("header %s missing from the delivered record", tt.key)
			}

			if got != tt.want {
				t.Errorf("%s = %q; want %q", tt.key, got, tt.want)
			}
		})
	}

	t.Run("the error message and the quarantining identity", func(t *testing.T) {
		t.Parallel()

		msg, _ := discard.value(dlqheader.ErrorMessage)
		if !strings.Contains(msg, "loan already settled") {
			t.Errorf("%s = %q; want the handler's error", dlqheader.ErrorMessage, msg)
		}

		if id, ok := discard.value(dlqheader.ProducerID); !ok || id == "" {
			t.Error("producer id missing; want the quarantining consumer group")
		}
	})

	t.Run("the payload is the real one", func(t *testing.T) {
		t.Parallel()

		discard.mu.Lock()
		defer discard.mu.Unlock()

		if string(discard.payloads[0]) != `{"ok":true}` {
			t.Errorf("payload = %q; want the verbatim poison payload", discard.payloads[0])
		}
	})
}

// TestDiscardSeam_IsNotArmedByAHandlersMethodSet is the F1 witness.
//
// The seam used to be an interface the runtime type-asserted on the handler. Any
// business handler that happened to carry a HandleDiscard method — a publicly
// writable shape, since the record type is exported — then took the discard path
// while wired with Handler(...) on an ORDINARY topic, which silently lifted both
// library verdicts there: a codec-fault poison record was handed over with an
// all-zero forensic record and committed (gone, no DLQ entry, no alert), and
// ce-source verification stopped.
//
// The seam is now a func only WithDiscardDispatch can install, so the arming is
// a property of what the caller built. This test pins that: the same dual-method
// handler, wired as a plain Handler with no discard option, still gets both
// verdicts.
func TestDiscardSeam_IsNotArmedByAHandlersMethodSet(t *testing.T) {
	t.Parallel()

	t.Run("a codec fault is still quarantined", func(t *testing.T) {
		t.Parallel()

		// No ce-specversion: the envelope cannot decode.
		poison := &kgo.Record{
			Topic: "lerian.streaming.gateway", Partition: 0, Offset: 4,
			Value:   []byte(`{}`),
			Headers: []kgo.RecordHeader{{Key: "ce-id", Value: []byte("evt-1")}},
		}

		handler := &dualMethodHandler{}
		dlq := &fakeDLQ{}

		r := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
			cfg.Topics = []string{poison.Topic}
		}, newFakeGroupClient(fetchOf(poison.Topic, 0, poison)), handler, dlq)

		runUntilClosed(t, r)

		if dlq.count() != 1 {
			t.Fatalf("a normal consumer quarantined %d codec faults; want 1 — the discard path was armed by a method set", dlq.count())
		}

		if _, kind := dlq.lastCause(); kind != dlqheader.CauseCodec {
			t.Errorf("cause kind = %q; want %q", kind, dlqheader.CauseCodec)
		}

		if handler.discardCalls() != 0 {
			t.Errorf("HandleDiscard ran %d times on a NORMAL consumer; want 0", handler.discardCalls())
		}
	})

	t.Run("a foreign ce-source is still refused", func(t *testing.T) {
		t.Parallel()

		foreign := rec("t", 0, 4, ceHeaders("tenant-abc", false))
		handler := &dualMethodHandler{}
		dlq := &fakeDLQ{}

		r := newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
			cfg.ExpectSources = []string{"lender"}
		}, newFakeGroupClient(fetchOf("t", 0, foreign)), handler, dlq)

		runUntilClosed(t, r)

		if dlq.count() != 1 {
			t.Fatalf("ce-source verification quarantined %d foreign writes; want 1", dlq.count())
		}

		if _, kind := dlq.lastCause(); kind != dlqheader.CauseSourceMismatch {
			t.Errorf("cause kind = %q; want %q", kind, dlqheader.CauseSourceMismatch)
		}
	})
}

// dualMethodHandler is a business Handler that ALSO carries a discard-shaped
// method — the accident that used to arm the discard path. Wired with
// Handler(...), it must behave as an ordinary handler and nothing else.
type dualMethodHandler struct {
	mu       sync.Mutex
	discards int
}

func (*dualMethodHandler) Handle(context.Context, contract.Event, []byte) error { return nil }

func (h *dualMethodHandler) HandleDiscard(_ context.Context, _ []kgo.RecordHeader, _ []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.discards++

	return nil
}

func (h *dualMethodHandler) discardCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.discards
}

// TestDiscardSeam_MalformedEnvelopeIsDeliveredNotRequarantined pins the guard
// that keeps a DLQ reader from treating its topic's normal content as poison.
//
// A "codec" quarantine is, by definition, a record whose CloudEvents envelope
// does not parse — and the quarantine copy is header-verbatim, so the entry on
// the DLQ topic does not parse either. On the normal path that is a terminal
// codec fault.
func TestDiscardSeam_MalformedEnvelopeIsDeliveredNotRequarantined(t *testing.T) {
	t.Parallel()

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
	discard := &recordingDiscard{}

	dlq := readDLQ(t, entry, discard, nil)

	if dlq.count() != 0 {
		t.Fatalf("the reader quarantined %d entries whose envelope was dead; want 0", dlq.count())
	}

	if discard.count() != 1 {
		t.Fatalf("discard seam ran %d times; want 1 — an unparseable envelope is a DLQ's normal content", discard.count())
	}

	if got, ok := discard.value(dlqheader.SourceOffset); !ok || got != "7" {
		t.Errorf("origin offset = %q; want 7 — the route back must survive a dead envelope", got)
	}
}

// TestDiscardSeam_ForeignSourceIsNotQuarantined pins the second exemption. A
// quarantine copy carries the ORIGINAL producer's ce-source, never the reader's
// own application, so the source gate would reject a healthy DLQ entirely.
func TestDiscardSeam_ForeignSourceIsNotQuarantined(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	discard := &recordingDiscard{}

	dlq := readDLQ(t, entry, discard, func(cfg *ConsumerConfig) {
		cfg.ExpectSources = []string{"lender"}
	})

	if dlq.count() != 0 {
		t.Fatalf("source verification quarantined %d DLQ entries; want 0", dlq.count())
	}

	if discard.count() != 1 {
		t.Fatalf("discard seam ran %d times; want 1", discard.count())
	}
}

// TestDiscardSeam_TenantTravelsOnTheHandlerContext proves the reader gets the
// POISON record's tenant on ctx, from ce-tenantid, never from the payload.
func TestDiscardSeam_TenantTravelsOnTheHandlerContext(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-xyz", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	discard := &recordingDiscard{}

	readDLQ(t, entry, discard, nil)

	discard.mu.Lock()
	defer discard.mu.Unlock()

	if len(discard.tenants) != 1 || discard.tenants[0] != "tenant-xyz" {
		t.Errorf("tenant on handler ctx = %v; want [tenant-xyz]", discard.tenants)
	}
}

// TestDiscardSeam_ReturnedErrorStillQuarantines pins what is deliberately NOT
// lifted. The library exempts its own structural verdicts on this path; it does
// not override the service's. What makes that safe is the construction-time
// refusal proved below: the reader's quarantine destination is never a topic it
// drains, so a terminal return lands somewhere else.
func TestDiscardSeam_ReturnedErrorStillQuarantines(t *testing.T) {
	t.Parallel()

	poison := rec("lerian.streaming.gateway", 0, 5, ceHeaders("tenant-abc", false))
	entry := quarantine(t, poison, errors.New("terminal"), "lender")

	discard := &recordingDiscard{err: errors.New("exception desk is down")}

	dlq := readDLQ(t, entry, discard, nil)

	if dlq.count() != 1 {
		t.Fatalf("DLQ count = %d; want 1 — a reader's own error is classified like any other", dlq.count())
	}

	if _, kind := dlq.lastCause(); kind != dlqheader.CauseHandler {
		t.Errorf("cause kind = %q; want %q", kind, dlqheader.CauseHandler)
	}
}

// TestRefuseSelfQuarantine_ClosesTheLoopAtConstruction is the F3 witness, and it
// asserts the republish DESTINATION rather than a recording fake.
//
// The destination is lerian.streaming.<Source>.dlq and the subscription is
// known at the same moment, so the self-feeding loop — republish onto the topic
// you just read from, redeliver, quarantine, forever, while reporting healthy —
// is refused instead of documented. It is refused in BOTH modes: with a plain
// Handler the loop needs no handler error at all (a codec fault is enough), and
// with a DLQ reader it reopens the moment the service's own handler returns
// terminal.
func TestRefuseSelfQuarantine_ClosesTheLoopAtConstruction(t *testing.T) {
	t.Parallel()

	ownDLQ := contract.AppDLQTopic("lender")

	t.Run("refused for a plain handler", func(t *testing.T) {
		t.Parallel()

		_, err := New(selfQuarantineConfig(), newFakeGroupClient(), &fakeHandler{}, WithDLQPublisher(&fakeDLQ{}))
		if !errors.Is(err, ErrSubscribedToOwnQuarantineTopic) {
			t.Errorf("New err = %v; want ErrSubscribedToOwnQuarantineTopic", err)
		}
	})

	t.Run("refused for a DLQ reader too", func(t *testing.T) {
		t.Parallel()

		discard := &recordingDiscard{}

		_, err := New(selfQuarantineConfig(), newFakeGroupClient(), nil,
			WithDLQPublisher(&fakeDLQ{}), WithDiscardDispatch(discard.dispatch))
		if !errors.Is(err, ErrSubscribedToOwnQuarantineTopic) {
			t.Errorf("New err = %v; want ErrSubscribedToOwnQuarantineTopic", err)
		}
	})

	t.Run("the accepted shape republishes somewhere it does not read", func(t *testing.T) {
		t.Parallel()

		// The documented fix: the reader carries its own ce-source, so its
		// quarantine destination is a topic it provisions and owns.
		cfg := selfQuarantineConfig()
		cfg.Source = "lender-dlq-desk"

		destination := contract.AppDLQTopic(cfg.Source)

		if destination == ownDLQ {
			t.Fatal("the fix produced the same destination; the test proves nothing")
		}

		for _, subscribed := range cfg.ResolvedTopics() {
			if destination == subscribed {
				t.Fatalf("republish destination %q is a subscribed topic; the loop is still open", destination)
			}
		}

		discard := &recordingDiscard{}

		if _, err := New(cfg, newFakeGroupClient(), nil,
			WithDLQPublisher(&fakeDLQ{}), WithDiscardDispatch(discard.dispatch)); err != nil {
			t.Fatalf("New: %v", err)
		}
	})
}

// selfQuarantineConfig is a valid consumer subscribed to its OWN quarantine
// destination — Source("lender") reading lerian.streaming.lender.dlq.
func selfQuarantineConfig() ConsumerConfig {
	cfg := DefaultBuilderConfig()
	cfg.Enabled = true
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "test-group"
	cfg.Source = "lender"
	cfg.Topics = []string{contract.AppDLQTopic("lender")}

	return cfg
}
