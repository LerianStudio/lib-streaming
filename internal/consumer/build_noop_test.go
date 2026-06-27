//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestDefaultBuilderConfig proves the root builder seed is ENABLED and carries
// every numeric/duration default LoadConsumerConfig applies, so a minimal fluent
// build (Brokers/Group/Topics/Handler only) passes Validate.
func TestDefaultBuilderConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig()

	if !cfg.Enabled {
		t.Error("DefaultBuilderConfig().Enabled = false; want true (fluent builds default to run)")
	}

	if cfg.RetryBudget != defaultRetryBudget {
		t.Errorf("RetryBudget = %d; want %d", cfg.RetryBudget, defaultRetryBudget)
	}

	if cfg.RetryBackoffInitial != defaultRetryBackoffInitial ||
		cfg.RetryBackoffMax != defaultRetryBackoffMax ||
		cfg.RetryInLoopMaxDwell != defaultRetryInLoopMaxDwell ||
		cfg.HaltBackoff != defaultHaltBackoff ||
		cfg.CloseTimeout != defaultCloseTimeout {
		t.Errorf("duration defaults not all applied: %+v", cfg)
	}

	if cfg.DLQTopicSuffix != DefaultDLQTopicSuffix {
		t.Errorf("DLQTopicSuffix = %q; want %q", cfg.DLQTopicSuffix, DefaultDLQTopicSuffix)
	}

	// Seeded config + the required fields must validate clean.
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Topics = []string{"t"}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("seeded DefaultBuilderConfig + required fields failed Validate: %v", err)
	}
}

// TestBuild_EnabledFalseYieldsNoop proves the Enabled kill switch returns the
// no-op WITHOUT validating or dialing — no brokers/handler needed.
func TestBuild_EnabledFalseYieldsNoop(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{Enabled: false}

	c, err := Build(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("Build(Enabled=false) = %v; want nil", err)
	}

	if _, ok := c.(*noopConsumer); !ok {
		t.Fatalf("Build(Enabled=false) returned %T; want *noopConsumer", c)
	}
}

// TestBuild_RealRuntimeConstructsOffline proves the enabled path constructs the
// real franz-go group client + internal DLQ adapter without dialing a broker
// (kgo.NewClient is non-dialing at construction). It exercises Build,
// newKgoGroupClient, buildConsumerKgoOpts/buildDLQKgoOpts, kafka.NewAdapter, and
// New end to end, then Closes to release both clients.
func TestBuild_RealRuntimeConstructsOffline(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "build-test"
	cfg.Topics = []string{"loan.created"}

	c, err := Build(context.Background(), cfg, &fakeHandler{})
	if err != nil {
		t.Fatalf("Build = %v; want nil (offline construction)", err)
	}

	defer func() { _ = c.Close() }()

	if _, ok := c.(*consumerRuntime); !ok {
		t.Fatalf("Build returned %T; want *consumerRuntime", c)
	}

	// Real runtime is NOT ready before the first poll (discriminates from no-op).
	if err := c.Healthy(context.Background()); err == nil {
		t.Error("Healthy() = nil before first poll; want ErrNotReady (real runtime)")
	}
}

// TestBuild_BlankSuffixNormalized proves Build re-defaults an empty DLQ suffix to
// ".dlq" before wiring, so a directly-constructed config never derives
// <topic><""> == the source topic.
func TestBuild_BlankSuffixNormalized(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Topics = []string{"t"}
	cfg.DLQTopicSuffix = "" // blank -> must be normalized

	c, err := Build(context.Background(), cfg, &fakeHandler{})
	if err != nil {
		t.Fatalf("Build with blank suffix = %v; want nil (normalized)", err)
	}

	if got := c.(*consumerRuntime).cfg.DLQTopicSuffix; got != DefaultDLQTopicSuffix {
		t.Errorf("normalized DLQTopicSuffix = %q; want %q", got, DefaultDLQTopicSuffix)
	}

	_ = c.Close()
}

// TestBuild_InvalidConfigRejected proves Build fails closed on a config that does
// not validate (the enabled path runs cfg.Validate before any client dial).
func TestBuild_InvalidConfigRejected(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig() // enabled, but no brokers/group/topics
	if _, err := Build(context.Background(), cfg, &fakeHandler{}); !errors.Is(err, ErrMissingBrokers) {
		t.Fatalf("Build(no brokers) = %v; want ErrMissingBrokers", err)
	}
}

// TestBuild_NilHandlerRejected proves the enabled path rejects a nil handler
// (fail-closed before constructing any client).
func TestBuild_NilHandlerRejected(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Topics = []string{"t"}

	if _, err := Build(context.Background(), cfg, nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("Build(nil handler) = %v; want ErrNilHandler", err)
	}
}

// TestNoopConsumer proves the disabled-mode no-op behaves as specified: Run
// blocks until ctx-cancel then returns nil (clean), Close is a no-op, Healthy is
// always ready. This is the kill-switch contract the builder depends on.
func TestNoopConsumer(t *testing.T) {
	t.Parallel()

	c := NewNoop()

	if err := c.Healthy(context.Background()); err != nil {
		t.Errorf("noop Healthy() = %v; want nil", err)
	}

	if err := c.Close(); err != nil {
		t.Errorf("noop Close() = %v; want nil", err)
	}

	// Run blocks until ctx-cancel, then returns nil (clean shutdown, goleak-safe).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("noop Run() = %v; want nil after ctx-cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("noop Run did not return after ctx-cancel")
	}
}

// TestNewKgoGroupClient proves the production group-client constructor builds a
// real franz-go client (offline) and its pass-throughs are nil-safe. The
// non-nil client's Close releases it; the nil-wrapper guards return zero values
// without panicking (the defensive k == nil / k.client == nil branches).
func TestNewKgoGroupClient(t *testing.T) {
	t.Parallel()

	cfg := kgoOptsTestConfig()

	client, err := newKgoGroupClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("newKgoGroupClient = %v; want nil (offline construction)", err)
	}

	if client == nil || client.client == nil {
		t.Fatal("newKgoGroupClient returned an empty wrapper")
	}

	// Pass-throughs on the real (non-dialing) client: SetOffsets/AllowRebalance
	// are cheap local-state ops; Close releases the client. PollFetches/
	// CommitRecords would dial, so they are NOT called here (covered via the
	// scripted fake elsewhere).
	client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		"t1": {0: {Epoch: 1, Offset: 0}},
	})
	client.AllowRebalance()
	client.Close()
}

// TestKgoGroupClient_NilGuards proves every pass-through on a nil/empty wrapper
// is defensive: no panic, and the error-returning methods fail closed.
func TestKgoGroupClient_NilGuards(t *testing.T) {
	t.Parallel()

	var k *kgoGroupClient // nil receiver

	if got := k.PollFetches(context.Background()); len(got) != 0 {
		t.Errorf("nil PollFetches = %v; want empty Fetches", got)
	}

	if err := k.CommitRecords(context.Background()); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("nil CommitRecords = %v; want ErrNilProducer", err)
	}

	// These must simply not panic.
	k.SetOffsets(nil)
	k.AllowRebalance()
	k.Close()

	// An empty (non-nil) wrapper with a nil inner client takes the same guards.
	empty := &kgoGroupClient{}
	if err := empty.CommitRecords(context.Background()); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("empty CommitRecords = %v; want ErrNilProducer", err)
	}

	empty.SetOffsets(nil)
	empty.AllowRebalance()
	empty.Close()
}

// TestConsumerOptions proves the construction-time options write the right
// runtime field. WithMetricsFactory/WithTracer/WithCodec are otherwise only
// reachable via the root facade aliases, so they show 0% in this package.
func TestConsumerOptions(t *testing.T) {
	t.Parallel()

	var sawCodec bool
	codec := func([]kgo.RecordHeader) (contract.Event, error) {
		sawCodec = true
		return contract.Event{}, nil
	}

	r := &consumerRuntime{}

	WithLogger(log.NewNop())(r)
	WithMetricsFactory(nil)(r)
	WithTracer(nil)(r)
	WithClassifier(func(error) bool { return true })(r)
	WithCodec(codec)(r)

	if r.logger == nil {
		t.Error("WithLogger did not set logger")
	}

	if r.classifier == nil {
		t.Error("WithClassifier did not set classifier")
	}

	if r.codec == nil {
		t.Fatal("WithCodec did not set codec")
	}

	if _, err := r.codec(nil); err != nil || !sawCodec {
		t.Error("WithCodec did not install the supplied codec func")
	}
}

// TestPublishDLQ_StampsForensicHeaders drives the production transportDLQPublisher
// against a SPY transport adapter (no broker): it asserts the destination suffix,
// key preservation, original-header passthrough, and the eight stamped forensic
// headers — the header-stamping contract of the DLQ seam.
func TestPublishDLQ_StampsForensicHeaders(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := &transportDLQPublisher{adapter: adapter, suffix: ".dlq", groupID: "svc-group"}

	source := &kgo.Record{
		Topic:     "loan.created",
		Partition: 5,
		Offset:    99,
		Key:       []byte("tenant-x|loan-7"),
		Value:     []byte(`{"poison":true}`),
		Headers: []kgo.RecordHeader{
			{Key: "ce-id", Value: []byte("evt-9")},
			{Key: "ce-tenantid", Value: []byte("tenant-x")},
		},
	}

	cause := errors.New("handler said no")
	if err := pub.PublishDLQ(context.Background(), source, cause, 2); err != nil {
		t.Fatalf("PublishDLQ = %v; want nil", err)
	}

	msgs := adapter.Messages()
	if len(msgs) != 1 {
		t.Fatalf("published %d; want 1", len(msgs))
	}

	m := msgs[0]

	if m.Destination.Name != "loan.created.dlq" {
		t.Errorf("destination = %q; want loan.created.dlq", m.Destination.Name)
	}

	if m.Key != "tenant-x|loan-7" {
		t.Errorf("key = %q; want verbatim source key", m.Key)
	}

	if string(m.Payload) != `{"poison":true}` {
		t.Errorf("payload = %q; want verbatim source value", m.Payload)
	}

	hdr := map[string]string{}
	for _, h := range m.Headers {
		hdr[h.Key] = string(h.Value)
	}

	// Original CE headers preserved verbatim.
	if hdr["ce-id"] != "evt-9" || hdr["ce-tenantid"] != "tenant-x" {
		t.Errorf("original CE headers not preserved: %v", hdr)
	}

	// The eight forensic headers (values that are deterministic here).
	wantContains := map[string]string{
		"x-lerian-dlq-source-topic":     "loan.created",
		"x-lerian-dlq-error-message":    "handler said no",
		"x-lerian-dlq-retry-count":      "2",
		"x-lerian-dlq-producer-id":      "svc-group",
		"x-lerian-dlq-source-partition": "5",
		"x-lerian-dlq-source-offset":    "99",
	}

	for k, want := range wantContains {
		if got, ok := hdr[k]; !ok || got != want {
			t.Errorf("forensic header %q = %q (present=%v); want %q", k, got, ok, want)
		}
	}

	// error-class and first-failure-at are present (values are adapter/time
	// dependent, so just assert presence).
	if _, ok := hdr["x-lerian-dlq-error-class"]; !ok {
		t.Error("missing x-lerian-dlq-error-class forensic header")
	}

	if _, ok := hdr["x-lerian-dlq-first-failure-at"]; !ok {
		t.Error("missing x-lerian-dlq-first-failure-at forensic header")
	}
}

// TestPublishDLQ_NilGuards proves the publisher fails closed on a nil adapter or
// a nil record rather than panicking or silently dropping poison.
func TestPublishDLQ_NilGuards(t *testing.T) {
	t.Parallel()

	// Nil adapter.
	none := &transportDLQPublisher{adapter: nil, suffix: ".dlq", groupID: "g"}
	if err := none.PublishDLQ(context.Background(), &kgo.Record{Topic: "t"}, nil, 0); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("PublishDLQ(nil adapter) = %v; want ErrNilProducer", err)
	}

	// Close on a nil-adapter publisher is a no-op.
	if err := none.Close(context.Background()); err != nil {
		t.Errorf("Close(nil adapter) = %v; want nil", err)
	}

	// Nil record with a real adapter.
	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := &transportDLQPublisher{adapter: adapter, suffix: ".dlq", groupID: "g"}

	if err := pub.PublishDLQ(context.Background(), nil, nil, 0); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("PublishDLQ(nil rec) = %v; want ErrNilProducer", err)
	}
}

// TestRun_CtxCancelCleanExit proves the top-level Run returns nil (clean) when
// ctx is canceled between cycles — the shuttingDown pre-poll guard. It uses a
// fake whose PollFetches blocks until ctx-cancel, so Run takes the ctx-cancel
// shutdown path rather than the ErrClientClosed one already covered by the
// invariants suite.
func TestRun_CtxCancelCleanExit(t *testing.T) {
	t.Parallel()

	client := &ctxBlockingClient{}
	r := newTestRuntime(t, client, &fakeHandler{}, &fakeDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- r.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after ctx-cancel = %v; want nil (clean exit)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on ctx-cancel")
	}
}

// ctxBlockingClient is a GroupClient whose PollFetches blocks until ctx is
// canceled, then returns an empty batch — modelling a real franz-go poll that
// unblocks on ctx-cancel. Drains the ctx-cancel shutdown path in Run.
type ctxBlockingClient struct{}

func (ctxBlockingClient) PollFetches(ctx context.Context) kgo.Fetches {
	<-ctx.Done()
	return kgo.Fetches{}
}
func (ctxBlockingClient) CommitRecords(context.Context, ...*kgo.Record) error { return nil }
func (ctxBlockingClient) SetOffsets(map[string]map[int32]kgo.EpochOffset)     {}
func (ctxBlockingClient) AllowRebalance()                                     {}
func (ctxBlockingClient) Close()                                              {}
