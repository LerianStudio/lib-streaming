//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

const signingTestKeyID = "svc-sign-k1"

func signingTestKeyring(t *testing.T, id, source string) *envelopesig.Keyring {
	t.Helper()

	secret := make(envelopesig.Secret, envelopesig.MinSecretBytes)
	for i := range secret {
		secret[i] = byte(0x40 + i)
	}

	ring, err := envelopesig.NewKeyring(envelopesig.Key{ID: id, Source: source, Secret: secret})
	if err != nil {
		t.Fatalf("NewKeyring() error = %v", err)
	}

	return ring
}

// verifyMessage checks message the way a consumer checks a Kafka record: over
// the raw header bytes and the record value.
func verifyMessage(t *testing.T, ring *envelopesig.Keyring, message transport.TransportMessage) error {
	t.Helper()

	verifier, err := envelopesig.NewVerifier(ring, 0)
	if err != nil {
		t.Fatalf("NewVerifier() error = %v", err)
	}

	return verifier.Verify(recordHeaders(message.Headers), message.Payload)
}

func recordHeaders(headers []transport.Header) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}

	return out
}

func signatureHeaders(message transport.TransportMessage) []string {
	var found []string

	for _, h := range message.Headers {
		switch h.Key {
		case envelopesig.HeaderKeyID, envelopesig.HeaderSignedAt, envelopesig.HeaderSignature:
			found = append(found, h.Key)
		}
	}

	return found
}

// sequenceClock returns t0, t0+1s, t0+2s, ... on successive calls, so a test
// can tell which signing produced a header.
func sequenceClock(t0 time.Time) func() time.Time {
	var (
		mu sync.Mutex
		n  int
	)

	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()

		ts := t0.Add(time.Duration(n) * time.Second)
		n++

		return ts
	}
}

// TestEmitMulti_SignsEveryRouteWhenSignerConfigured proves one Emit fanned out
// to two targets of different transports carries a verifiable signature on
// every copy.
func TestEmitMulti_SignsEveryRouteWhenSignerConfigured(t *testing.T) {
	t.Parallel()

	const source = "svc-sign-emit"

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	custom := fake.NewAdapter(contract.TransportCustom)
	ring := signingTestKeyring(t, signingTestKeyID, source)
	catalog := sampleCatalog(t)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming."+source, contract.RouteRequired),
		contract.RouteDefinition{
			Key:           "transaction.created.custom.sink",
			DefinitionKey: "transaction.created",
			Target:        "custom",
			Destination:   contract.Destination{Kind: contract.TransportCustom, Name: "custom-sink"},
			Requirement:   contract.RouteRequired,
		},
	)

	p, err := NewProducerMulti(ctx, MultiProducerConfig{Source: source}, nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "custom", Kind: contract.TransportCustom, Adapter: custom},
		},
		routes, catalog,
		WithLogger(log.NewNop()),
		WithEnvelopeSigning(ring, signingTestKeyID),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	for name, adapter := range map[string]*fake.Adapter{"primary": primary, "custom": custom} {
		messages := adapter.Messages()
		if len(messages) != 1 {
			t.Fatalf("%s published %d messages; want 1", name, len(messages))
		}

		if err := verifyMessage(t, ring, messages[0]); err != nil {
			t.Errorf("%s copy does not verify: %v", name, err)
		}
	}
}

// TestEmitMulti_HeadersByteIdenticalWithoutSigner pins the opt-in: a producer
// with no signing configured writes exactly the codec's headers, nothing more.
func TestEmitMulti_HeadersByteIdenticalWithoutSigner(t *testing.T) {
	t.Parallel()

	const source = "svc-sign-off"

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming."+source, contract.RouteRequired),
	)

	p, err := NewProducerMulti(ctx, MultiProducerConfig{Source: source}, nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes, catalog, WithLogger(log.NewNop()),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	messages := primary.Messages()
	if len(messages) != 1 {
		t.Fatalf("published %d messages; want 1", len(messages))
	}

	parsed, err := cloudevents.ParseCloudEventsHeaders(recordHeaders(messages[0].Headers))
	if err != nil {
		t.Fatalf("ParseCloudEventsHeaders() error = %v", err)
	}

	want := cloudevents.BuildTransportHeaders(parsed)
	got := messages[0].Headers

	if len(got) != len(want) {
		t.Fatalf("header count = %d; want %d (codec only). got=%v", len(got), len(want), got)
	}

	for i := range want {
		if got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("header[%d] = %s=%q; want %s=%q", i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}

// TestOutboxRelay_SignsAtRelayInstantNotEnqueue drives the real fallback: an
// Emit with the breaker open writes the outbox row at t0, and the relay later
// publishes it at t1. The persisted row must carry no signature material and
// the relayed record must be signed at t1: an enqueue-time signature would age
// in the outbox and turn legitimate backlog into rejections.
func TestOutboxRelay_SignsAtRelayInstantNotEnqueue(t *testing.T) {
	t.Parallel()

	const source = "svc-sign-relay"

	ctx := context.Background()
	adapter := fake.NewAdapter(TransportKafkaLike)
	ring := signingTestKeyring(t, signingTestKeyID, source)
	cbManager := newFakeCBManager()
	repo := &fakeOutboxRepo{}
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming."+source, contract.RouteRequired),
	)

	p, err := NewProducerMulti(ctx, MultiProducerConfig{Source: source}, nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes, sampleCatalog(t),
		WithLogger(log.NewNop()),
		WithCircuitBreakerManager(cbManager),
		WithOutboxRepository(repo),
		WithEnvelopeSigning(ring, signingTestKeyID),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	enqueueAt := time.Date(2026, 9, 26, 12, 30, 0, 0, time.UTC)
	relayAt := enqueueAt.Add(6 * time.Hour)

	var clock atomic.Pointer[time.Time]

	clock.Store(&enqueueAt)

	p.signer, err = envelopesig.NewSigner(ring, signingTestKeyID, source, envelopesig.WithClock(func() time.Time { return *clock.Load() }))
	if err != nil {
		t.Fatalf("NewSigner() error = %v", err)
	}

	primaryService := p.targets["primary"].cbServiceName
	cbManager.ForceTransition(primaryService, circuitbreaker.StateOpen)

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() with the breaker open error = %v; want the outbox fallback", err)
	}

	if got := len(adapter.Messages()); got != 0 {
		t.Fatalf("breaker open: adapter received %d messages; want 0 (outboxed)", got)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("Emit with the breaker open wrote no outbox row")
	}

	for _, needle := range []string{envelopesig.HeaderKeyID, envelopesig.HeaderSignedAt, envelopesig.HeaderSignature, signingTestKeyID, enqueueAt.Format(time.RFC3339Nano)} {
		if strings.Contains(string(row.Payload), needle) {
			t.Errorf("persisted outbox row carries signature material %q: %s", needle, row.Payload)
		}
	}

	clock.Store(&relayAt)
	cbManager.ForceTransition(primaryService, circuitbreaker.StateClosed)

	if err := p.handleOutboxRow(ctx, row); err != nil {
		t.Fatalf("handleOutboxRow() error = %v", err)
	}

	messages := adapter.Messages()
	if len(messages) != 1 {
		t.Fatalf("relay published %d messages; want 1", len(messages))
	}

	signedAt, _ := dlqHeader(messages[0], envelopesig.HeaderSignedAt)
	if want := relayAt.Format(time.RFC3339Nano); signedAt != want {
		t.Errorf("%s = %q; want the relay instant %q", envelopesig.HeaderSignedAt, signedAt, want)
	}

	if err := verifyMessage(t, ring, messages[0]); err != nil {
		t.Errorf("relayed record does not verify: %v", err)
	}
}

// TestOutboxRelay_RefusesRowOfAnotherSource pins the signing binding at relay
// time: a row persisted under a source other than the producer's own (the
// service renamed its source with rows still in the outbox, or a table shared
// across sources) is not published with a signature every verifying consumer
// is certain to quarantine as a forgery. The refusal is a configuration fault,
// not a property of the durable row, so it stays RETRYABLE (never a caller
// error, which the documented IsCallerError retry classifier would send to
// INVALID on the first attempt) and is counted and logged as a relay
// rejection with reason signing_source_mismatch.
func TestOutboxRelay_RefusesRowOfAnotherSource(t *testing.T) {
	t.Parallel()

	const (
		source    = "svc-sign-relay"
		rowSource = "svc-sign-renamed"
	)

	ctx := context.Background()
	adapter := fake.NewAdapter(TransportKafkaLike)
	ring := signingTestKeyring(t, signingTestKeyID, source)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming."+source, contract.RouteRequired),
	)

	factory, snapshot := newManualMeterSetup(t)

	p, err := NewProducerMulti(ctx, MultiProducerConfig{Source: source}, nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes, sampleCatalog(t),
		WithLogger(log.NewNop()),
		WithMetricsRecorder(factory),
		WithEnvelopeSigning(ring, signingTestKeyID),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	event := sampleEvent()
	event.Source = rowSource
	event.ApplyDefaults()

	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), newTestUUIDv7(t))

	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal envelope error = %v", err)
	}

	row := &outbox.OutboxEvent{
		ID:          newTestUUIDv7(t),
		EventType:   StreamingOutboxEventType,
		AggregateID: envelope.AggregateID,
		Payload:     payload,
	}

	err = p.handleOutboxRow(ctx, row)
	if !errors.Is(err, contract.ErrSigningSourceMismatch) {
		t.Fatalf("handleOutboxRow() error = %v; want ErrSigningSourceMismatch", err)
	}

	if errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Errorf("handleOutboxRow() error = %v; ErrInvalidSigningKey is construction-only", err)
	}

	if contract.IsCallerError(err) {
		t.Errorf("IsCallerError(%v) = true; the retry classifier would move a durable row to INVALID on its first attempt", err)
	}

	for _, want := range []string{`"` + rowSource + `"`, `"` + source + `"`, `"` + signingTestKeyID + `"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}

	if got := len(adapter.Messages()); got != 0 {
		t.Errorf("relay published %d messages for a foreign-source row; want 0", got)
	}

	metric, ok := findMetric(snapshot(), metricNameOutboxRelayRejected)
	if !ok {
		t.Fatalf("metric %s was never recorded", metricNameOutboxRelayRejected)
	}

	_, attrSets := sumInt64DataPoints(t, metric)
	if len(attrSets) != 1 {
		t.Fatalf("attribute sets = %d, want exactly 1", len(attrSets))
	}

	if got := attrSets[0]["reason"]; got != relayRejectSigningSourceMismatch {
		t.Errorf("reason label = %q, want %q", got, relayRejectSigningSourceMismatch)
	}

	if got := attrSets[0][labelTarget]; got != "primary" {
		t.Errorf("target label = %q, want %q", got, "primary")
	}
}

// TestRouteDLQ_CopyIsSignedAtDLQInstant proves the producer's DLQ copy is a
// fresh publication with its own signature, taken when the DLQ write happens.
func TestRouteDLQ_CopyIsSignedAtDLQInstant(t *testing.T) {
	t.Parallel()

	const source = "svc-dlq-size"

	adapter := &sizeCappedRouteAdapter{
		sourceTopic: "lerian.streaming." + source,
		sourceErr:   errors.New("simulated source publish failure"),
	}
	ring := signingTestKeyring(t, signingTestKeyID, source)

	p := newSizeCappedProducer(t, adapter, WithEnvelopeSigning(ring, signingTestKeyID))

	t0 := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	var err error

	p.signer, err = envelopesig.NewSigner(ring, signingTestKeyID, source, envelopesig.WithClock(sequenceClock(t0)))
	if err != nil {
		t.Fatalf("NewSigner() error = %v", err)
	}

	if err := p.Emit(context.Background(), eventToRequest(sampleEvent())); err == nil {
		t.Fatal("Emit() = nil; want the source-route failure to surface")
	}

	msgs := adapter.dlq()
	if len(msgs) != 1 {
		t.Fatalf("DLQ accepted %d messages; want 1", len(msgs))
	}

	signedAt, _ := dlqHeader(msgs[0], envelopesig.HeaderSignedAt)
	if want := t0.Add(time.Second).Format(time.RFC3339Nano); signedAt != want {
		t.Errorf("DLQ %s = %q; want the second signing instant %q (the DLQ write, not the emit)",
			envelopesig.HeaderSignedAt, signedAt, want)
	}

	if err := verifyMessage(t, ring, msgs[0]); err != nil {
		t.Errorf("DLQ copy does not verify: %v", err)
	}
}

// TestRouteDLQ_PayloadOmittedCopyCarriesNoSignature proves the slim DLQ copy is
// never signed. A signed empty-payload record carrying the real ce-id would
// verify if moved onto a fact topic, and its ce-id would then dedupe away the
// real event on replay.
func TestRouteDLQ_PayloadOmittedCopyCarriesNoSignature(t *testing.T) {
	t.Parallel()

	const source = "svc-dlq-size"

	adapter := &sizeCappedRouteAdapter{
		sourceTopic: "lerian.streaming." + source,
		sourceErr:   errors.New("simulated source publish failure"),
		dlqMaxBytes: 2000,
	}
	ring := signingTestKeyring(t, signingTestKeyID, source)

	p := newSizeCappedProducer(t, adapter, WithEnvelopeSigning(ring, signingTestKeyID))

	request := eventToRequest(sampleEvent())
	request.Payload = []byte(`{"blob":"` + strings.Repeat("p", 8000) + `"}`)

	if err := p.Emit(context.Background(), request); err == nil {
		t.Fatal("Emit() = nil; want the source-route failure to surface")
	}

	msgs := adapter.dlq()
	if len(msgs) != 1 {
		t.Fatalf("DLQ accepted %d messages; want 1 (the payload-omitted retry)", len(msgs))
	}

	if found := signatureHeaders(msgs[0]); len(found) != 0 {
		t.Errorf("payload-omitted DLQ copy carries signature headers %v; want none", found)
	}
}

func TestNewProducerMulti_RejectsKeyBoundToOtherSource(t *testing.T) {
	t.Parallel()

	err := newSigningProducerErr(t, "svc-sign-me", WithEnvelopeSigning(signingTestKeyring(t, signingTestKeyID, "someone-else"), signingTestKeyID))
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("NewProducerMulti() error = %v; want ErrInvalidSigningKey", err)
	}
}

func TestNewProducerMulti_RejectsUnknownActiveKeyID(t *testing.T) {
	t.Parallel()

	err := newSigningProducerErr(t, "svc-sign-me", WithEnvelopeSigning(signingTestKeyring(t, signingTestKeyID, "svc-sign-me"), "not-in-ring"))
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("NewProducerMulti() error = %v; want ErrInvalidSigningKey", err)
	}
}

func TestNewProducerMulti_RejectsNilSigningKeyring(t *testing.T) {
	t.Parallel()

	err := newSigningProducerErr(t, "svc-sign-me", WithEnvelopeSigning(nil, signingTestKeyID))
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("NewProducerMulti() error = %v; want ErrInvalidSigningKey", err)
	}
}

func newSigningProducerErr(t *testing.T, source string, opt EmitterOption) error {
	t.Helper()

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming."+source, contract.RouteRequired),
	)

	p, err := NewProducerMulti(context.Background(), MultiProducerConfig{Source: source}, nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)}},
		routes, catalog, WithLogger(log.NewNop()), opt,
	)
	if err == nil {
		_ = p.Close()
	}

	return err
}
