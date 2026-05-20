//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-observability/log"
)

// TestProducer_New_DisabledReturnsNoop proves the fail-safe: cfg.Enabled=false
// returns a NoopEmitter without attempting to connect. We check via type
// identity.
func TestProducer_New_DisabledReturnsNoop(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Enabled:           false,
		Brokers:           []string{"broker:9092"},
		Compression:       "lz4",
		RequiredAcks:      "all",
		CloudEventsSource: "//test",
	}

	emitter, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New(disabled) err = %v", err)
	}

	if _, ok := emitter.(*NoopEmitter); !ok {
		t.Errorf("New(disabled) returned %T; want *NoopEmitter", emitter)
	}
}

// TestProducer_New_NoBrokersReturnsErrMissingBrokers proves NoopEmitter is
// reserved for explicit Enabled=false. Enabled configs fail closed when the
// broker list is empty.
func TestProducer_New_NoBrokersReturnsErrMissingBrokers(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Enabled:           true,
		Brokers:           []string{},
		Compression:       "lz4",
		RequiredAcks:      "all",
		CloudEventsSource: "//test",
	}

	_, err := New(context.Background(), cfg)
	if !errors.Is(err, ErrMissingBrokers) {
		t.Fatalf("New(empty brokers) err = %v; want ErrMissingBrokers", err)
	}
}

// TestProducer_NewProducer_FailsOnInvalidConfig: NewProducer does NOT fall
// back to Noop. Invalid config surfaces as a returned error.
func TestProducer_NewProducer_FailsOnInvalidConfig(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Enabled:           true,
		Brokers:           []string{},
		Compression:       "lz4",
		RequiredAcks:      "all",
		CloudEventsSource: "//test",
	}

	_, err := NewProducer(context.Background(), cfg)
	if !errors.Is(err, ErrMissingBrokers) {
		t.Fatalf("NewProducer err = %v; want ErrMissingBrokers", err)
	}
}

func TestProducer_NewProducer_WithSASLWithoutTLS_DefaultRejectsPlaintext(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Enabled:               true,
		Brokers:               []string{"127.0.0.1:9092"},
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    1,
		Compression:           "lz4",
		RecordRetries:         1,
		RecordDeliveryTimeout: time.Second,
		RequiredAcks:          "all",
		CloseTimeout:          time.Second,
		CloudEventsSource:     "//test",
	}

	_, err := NewProducer(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithSASL(plain.Auth{User: "user", Pass: "secret"}.AsMechanism()),
	)
	if !errors.Is(err, ErrPlaintextSASLNotAllowed) {
		t.Fatalf("NewProducer() error = %v; want ErrPlaintextSASLNotAllowed", err)
	}
}

// TestProducer_NilReceiver_Emit_ReturnsErr: calling Emit on a nil *Producer
// returns ErrNilProducer instead of panicking. Mirrors the
// nil-receiver-safe contract in circuitbreaker.
func TestProducer_NilReceiver_Emit_ReturnsErr(t *testing.T) {
	t.Parallel()

	var p *Producer

	err := p.Emit(context.Background(), sampleRequest())
	if !errors.Is(err, ErrNilProducer) {
		t.Errorf("nil.Emit err = %v; want ErrNilProducer", err)
	}
}

// TestProducer_NilReceiver_CloseAndHealthy: Close and Healthy also must not
// panic on nil receiver.
func TestProducer_NilReceiver_CloseAndHealthy(t *testing.T) {
	t.Parallel()

	var p *Producer

	if err := p.Close(); err != nil {
		t.Errorf("nil.Close err = %v; want nil", err)
	}

	err := p.Healthy(context.Background())
	if err == nil {
		t.Error("nil.Healthy err = nil; want *HealthError")
	}
}

// TestProducer_CredentialSanitization asserts that when the underlying
// client surfaces an error containing credentialed URLs, the EmitError's
// Error() string does NOT contain the raw credential. DX-B06.
//
// Rather than triggering a real SASL failure (kfake doesn't expose that
// directly without extra setup), we directly test the sanitization path via
// an EmitError synthesized with a credentialed cause — this is the layer
// that callers actually see.
func TestProducer_CredentialSanitization(t *testing.T) {
	t.Parallel()

	cause := errors.New("dial sasl://user:hunter2@broker:9092: connection refused")
	ee := &EmitError{
		ResourceType: "transaction",
		EventType:    "created",
		TenantID:     "t-abc",
		Topic:        "lerian.streaming.transaction.created",
		Class:        ClassBrokerUnavailable,
		Cause:        cause,
	}

	msg := ee.Error()

	if strings.Contains(msg, "hunter2") {
		t.Errorf("Error() contained credential; got %q", msg)
	}

	if strings.Contains(msg, "user:hunter2@") {
		t.Errorf("Error() contained raw userinfo; got %q", msg)
	}

	// Sanity: the error class + topic must still surface so operators can
	// correlate.
	if !strings.Contains(msg, string(ClassBrokerUnavailable)) {
		t.Errorf("Error() missing class; got %q", msg)
	}

	if !strings.Contains(msg, "lerian.streaming.transaction.created") {
		t.Errorf("Error() missing topic; got %q", msg)
	}
}

// TestProducer_New_RealProducer_AsProducer: happy path — New with valid
// kfake cfg returns a concrete *Producer.
func TestProducer_New_RealProducer_AsProducer(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	catalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(catalog))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	if p.producerID == "" {
		t.Error("producerID is empty; want non-empty UUID")
	}

	if len(p.targets) == 0 {
		t.Error("targets map is empty; want at least one target")
	}
	if p.catalog.Len() != 1 {
		t.Errorf("catalog len = %d; want 1", p.catalog.Len())
	}
}

// TestNewProducer_SystemEventInCatalogWithoutOptionFails asserts the
// catalog-level capability gate: a catalog containing any SystemEvent=true
// definition cannot construct a Producer unless WithAllowSystemEvents is
// explicitly opted in. Failing fast at construction prevents silent
// per-Emit rejections in production.
func TestNewProducer_SystemEventInCatalogWithoutOptionFails(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	catalog, err := NewCatalog(EventDefinition{
		Key:           "ledger.reaper_pass",
		ResourceType:  "ledger",
		EventType:     "reaper_pass",
		SchemaVersion: "1.0.0",
		SystemEvent:   true,
	})
	if err != nil {
		t.Fatalf("NewCatalog err = %v", err)
	}

	_, err = NewProducer(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(catalog))
	if !errors.Is(err, ErrSystemEventsNotAllowed) {
		t.Fatalf("NewProducer err = %v; want ErrSystemEventsNotAllowed", err)
	}

	if !strings.Contains(err.Error(), "ledger.reaper_pass") {
		t.Errorf("err = %q; want substring %q", err.Error(), "ledger.reaper_pass")
	}
}

// TestNewProducer_SystemEventInCatalogWithOptionSucceeds is the positive
// counterpart: the same catalog constructs cleanly when
// WithAllowSystemEvents is supplied.
func TestNewProducer_SystemEventInCatalogWithOptionSucceeds(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	catalog, err := NewCatalog(EventDefinition{
		Key:           "ledger.reaper_pass",
		ResourceType:  "ledger",
		EventType:     "reaper_pass",
		SchemaVersion: "1.0.0",
		SystemEvent:   true,
	})
	if err != nil {
		t.Fatalf("NewCatalog err = %v", err)
	}

	p, err := NewProducer(context.Background(), cfg,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("NewProducer err = %v; want nil", err)
	}

	t.Cleanup(func() { _ = p.Close() })
}

func TestProducer_NewProducer_RejectsUnknownPolicyOverrideKey(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	cfg.PolicyOverrides = map[string]DeliveryPolicyOverride{
		"transaction.cretaed": {Outbox: OutboxModeAlways},
	}

	_, err := NewProducer(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if !errors.Is(err, ErrUnknownEventDefinition) {
		t.Fatalf("NewProducer err = %v; want ErrUnknownEventDefinition", err)
	}
}

// TestProducer_New_WithPartitionKeyOverride: operator-supplied partition-key
// function is actually used on the produced record.
func TestProducer_New_WithPartitionKeyOverride(t *testing.T) {
	cfg, cluster := kfakeConfig(t)

	fixed := "FIXED-PART-KEY"
	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)),
		WithPartitionKey(func(_ Event) string { return fixed }),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	event := sampleRequest()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, event); err != nil {
		t.Fatalf("Emit err = %v", err)
	}

	consumer := newConsumer(t, cluster, "lerian.streaming.transaction.created")

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)

	var got *kgo.Record

	fetches.EachRecord(func(r *kgo.Record) {
		if got == nil {
			got = r
		}
	})

	if got == nil {
		t.Fatal("no record fetched")
	}

	if string(got.Key) != fixed {
		t.Errorf("partition key = %q; want %q", got.Key, fixed)
	}
}

type failingCBManager struct {
	err error
}

func (f *failingCBManager) GetOrCreate(_ string, _ circuitbreaker.Config) (circuitbreaker.CircuitBreaker, error) {
	return nil, f.err
}

func (f *failingCBManager) Execute(_ string, _ func() (any, error)) (any, error) {
	return nil, f.err
}

func (*failingCBManager) GetState(_ string) circuitbreaker.State {
	return circuitbreaker.StateUnknown
}

func (*failingCBManager) GetCounts(_ string) circuitbreaker.Counts {
	return circuitbreaker.Counts{}
}

func (*failingCBManager) IsHealthy(_ string) bool {
	return false
}

func (*failingCBManager) Reset(_ string) {}

func (*failingCBManager) RegisterStateChangeListener(_ circuitbreaker.StateChangeListener) {}
