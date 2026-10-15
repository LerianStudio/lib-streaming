//go:build integration

package streaming_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/v4/log"
	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/streamingtest"
)

// This file drives envelope signing end to end against the Kafka protocol: a
// real signing producer into a real verifying consumer, and a captured record
// tampered on the wire. Topic, apps and the consumer's own DLQ are the ones
// consumer_dispatch_kfake_integration_test.go seeds.

const signatureDefinitionKey = "loan_contract.disbursed"

func signatureKey(id string, seed byte) streaming.SigningKey {
	return streaming.SigningKey{ID: id, Source: dispatchApp, Secret: bytes.Repeat([]byte{seed}, streaming.MinSigningSecretBytes)}
}

func signatureRing(t *testing.T, keys ...streaming.SigningKey) *streaming.Keyring {
	t.Helper()

	ring, err := streaming.NewKeyring(keys...)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	return ring
}

// signingProducer builds lender's producer, signing every publish with
// activeKeyID from ring.
func signingProducer(t *testing.T, cluster *kfake.Cluster, ring *streaming.Keyring, activeKeyID string) streaming.Emitter {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:           signatureDefinitionKey,
		ResourceType:  "loan_contract",
		EventType:     "disbursed",
		SchemaVersion: "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}

	emitter, err := streaming.NewBuilder().
		Source(dispatchApp).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:         "primary.all",
			Target:      "primary",
			Destination: streaming.KafkaTopic(dispatchTopic),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  cluster.ListenAddrs(),
			ClientID: "signature-kfake-" + activeKeyID,
		}).
		SignEnvelopes(ring, activeKeyID).
		Logger(log.NewNop()).
		Build(context.Background())
	if err != nil {
		t.Fatalf("producer Build: %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	return emitter
}

// keepEmitting emits payload every 150ms until stop closes, so the consumer's
// group join latency never decides the outcome.
func keepEmitting(t *testing.T, emitter streaming.Emitter, payload []byte, stop <-chan struct{}, wg *sync.WaitGroup) {
	t.Helper()

	wg.Go(func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			err := emitter.Emit(context.Background(), streaming.EmitRequest{
				DefinitionKey: signatureDefinitionKey,
				TenantID:      "tenant-abc",
				Payload:       payload,
			})
			if err != nil {
				t.Errorf("Emit: %v", err)

				return
			}

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})
}

// payloadRecorder is a handler that records every payload it receives.
type payloadRecorder struct {
	mu       sync.Mutex
	payloads [][]byte
	notify   chan struct{}
}

func newPayloadRecorder() *payloadRecorder {
	return &payloadRecorder{notify: make(chan struct{}, 1)}
}

func (r *payloadRecorder) handle(_ context.Context, _ streaming.Event, body []byte) error {
	r.mu.Lock()
	r.payloads = append(r.payloads, append([]byte(nil), body...))
	r.mu.Unlock()

	select {
	case r.notify <- struct{}{}:
	default:
	}

	return nil
}

func (r *payloadRecorder) saw(payload []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range r.payloads {
		if bytes.Equal(p, payload) {
			return true
		}
	}

	return false
}

// awaitPayloads waits until the recorder has seen every payload.
func (r *payloadRecorder) awaitPayloads(t *testing.T, payloads ...[]byte) {
	t.Helper()

	deadline := time.After(dispatchWaitBudget)

	for {
		all := true

		for _, p := range payloads {
			if !r.saw(p) {
				all = false
			}
		}

		if all {
			return
		}

		select {
		case <-r.notify:
		case <-deadline:
			t.Fatalf("handler did not receive every signed payload within %s", dispatchWaitBudget)
		}
	}
}

// verifyingConsumer builds loan-projector's consumer of lender, requiring
// signatures from ring.
func verifyingConsumer(t *testing.T, cluster *kfake.Cluster, group string, ring *streaming.Keyring, h streaming.HandlerFunc) streaming.Consumer {
	t.Helper()

	c, err := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group(group).
		Source(dispatchConsumerApp).
		Apps(dispatchApp).
		On(dispatchEventKey, h).
		RequireSignatures(ring).
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("consumer Build: %v", err)
	}

	return c
}

// runConsumer starts c and returns a stop function that closes it and waits
// for Run to return.
func runConsumer(t *testing.T, c streaming.Consumer) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	return func() {
		cancel()

		_ = c.Close()

		select {
		case <-done:
		case <-time.After(dispatchWaitBudget):
			t.Error("Run did not return after Close")
		}
	}
}

func TestIntegration_SignedProducerToVerifyingConsumer(t *testing.T) {
	cluster := dispatchCluster(t)
	key := signatureKey("lender-k1", 1)

	recorder := newPayloadRecorder()
	stopConsumer := runConsumer(t, verifyingConsumer(t, cluster, dispatchGroup+"-signed", signatureRing(t, key), recorder.handle))

	defer stopConsumer()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	payload := []byte(`{"amount":"1200.00"}`)
	keepEmitting(t, signingProducer(t, cluster, signatureRing(t, key), key.ID), payload, stop, &producers)

	recorder.awaitPayloads(t, payload)

	close(stop)
	producers.Wait()
}

// TestIntegration_KeyRotationOverlap runs two producers of the same source on
// the old and the new key at once, as a rolling key switch does, into one
// consumer holding both.
func TestIntegration_KeyRotationOverlap(t *testing.T) {
	cluster := dispatchCluster(t)
	oldKey, newKey := signatureKey("lender-2026-08", 1), signatureKey("lender-2026-09", 2)

	recorder := newPayloadRecorder()
	stopConsumer := runConsumer(t, verifyingConsumer(t, cluster, dispatchGroup+"-rotation", signatureRing(t, oldKey, newKey), recorder.handle))

	defer stopConsumer()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	oldPayload, newPayload := []byte(`{"key":"old"}`), []byte(`{"key":"new"}`)
	keepEmitting(t, signingProducer(t, cluster, signatureRing(t, oldKey), oldKey.ID), oldPayload, stop, &producers)
	keepEmitting(t, signingProducer(t, cluster, signatureRing(t, newKey), newKey.ID), newPayload, stop, &producers)

	recorder.awaitPayloads(t, oldPayload, newPayload)

	close(stop)
	producers.Wait()
}

// TestIntegration_ForgedRecordQuarantinesWithSignatureCause captures a record
// a real signing producer published, changes its body, and writes it back.
// The consumer must quarantine it to its own DLQ as signature_invalid and
// never hand the forged body to the handler.
func TestIntegration_ForgedRecordQuarantinesWithSignatureCause(t *testing.T) {
	cluster := dispatchCluster(t)
	key := signatureKey("lender-k1", 1)

	original := []byte(`{"amount":"1200.00"}`)
	forged := []byte(`{"amount":"9999999.00"}`)

	emitter := signingProducer(t, cluster, signatureRing(t, key), key.ID)
	if err := emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: signatureDefinitionKey,
		TenantID:      "tenant-abc",
		Payload:       original,
	}); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	captured := awaitDLQRecord(t, cluster, dispatchTopic, dispatchWaitBudget)

	recorder := newPayloadRecorder()
	stopConsumer := runConsumer(t, verifyingConsumer(t, cluster, dispatchGroup+"-forged", signatureRing(t, key), recorder.handle))

	defer stopConsumer()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	keepProducingRecord(t, cluster, &kgo.Record{
		Topic:   dispatchTopic,
		Key:     captured.Key,
		Headers: captured.Headers,
		Value:   forged,
	}, stop, &producers)

	quarantined := awaitDLQRecord(t, cluster, dispatchConsumerDLQTopic, dispatchWaitBudget)

	close(stop)
	producers.Wait()

	headers := map[string]string{}
	for _, h := range quarantined.Headers {
		headers[h.Key] = string(h.Value)
	}

	if got := headers[streaming.DLQHeaderCauseKind]; got != streaming.DLQCauseSignatureInvalid {
		t.Errorf("%s = %q; want %q", streaming.DLQHeaderCauseKind, got, streaming.DLQCauseSignatureInvalid)
	}

	if headers[streaming.CloudEventsHeaderSignature] == "" {
		t.Error("the quarantine copy lost the original ce-sig; forensics need the signature it failed")
	}

	if recorder.saw(forged) {
		t.Error("the handler received the forged body; verification must quarantine before dispatch")
	}
}

// keepProducingRecord writes record every 150ms until stop closes.
func keepProducingRecord(t *testing.T, cluster *kfake.Cluster, record *kgo.Record, stop <-chan struct{}, wg *sync.WaitGroup) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("raw producer init: %v", err)
	}

	wg.Go(func() {
		defer cl.Close()

		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			copied := *record
			if err := cl.ProduceSync(context.Background(), &copied).FirstErr(); err != nil {
				t.Errorf("raw produce: %v", err)

				return
			}

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})
}

// TestIntegration_StreamingtestRecordsThroughVerifyingConsumer proves the
// streamingtest helpers build what the real consumer meets on the wire: the
// signed record reaches the handler, and the unsigned and forged records land
// on the consumer's own DLQ as signature_missing and signature_invalid.
func TestIntegration_StreamingtestRecordsThroughVerifyingConsumer(t *testing.T) {
	cluster := dispatchCluster(t)
	key := streamingtest.SigningKey(t, "lender-k1", dispatchApp)

	recorder := newPayloadRecorder()
	stopConsumer := runConsumer(t, verifyingConsumer(t, cluster, dispatchGroup+"-streamingtest", streamingtest.Keyring(t, key), recorder.handle))

	defer stopConsumer()

	event := func(payload string) streaming.Event {
		return streaming.Event{
			TenantID:     "tenant-abc",
			ResourceType: "loan_contract",
			EventType:    "disbursed",
			Source:       dispatchApp,
			Payload:      []byte(payload),
		}
	}

	signed := streamingtest.SignedRecord(t, dispatchTopic, key, event(`{"record":"signed"}`))
	unsigned := streamingtest.UnsignedRecord(t, dispatchTopic, event(`{"record":"unsigned"}`))
	forged := streamingtest.ForgedRecord(t, dispatchTopic, key, event(`{"record":"forged"}`))

	stop := make(chan struct{})

	var producers sync.WaitGroup

	for _, rec := range []*kgo.Record{signed, unsigned, forged} {
		keepProducingRecord(t, cluster, rec, stop, &producers)
	}

	recorder.awaitPayloads(t, signed.Value)
	causes := awaitDLQCauses(t, cluster, dispatchConsumerDLQTopic, streaming.DLQCauseSignatureMissing, streaming.DLQCauseSignatureInvalid)

	close(stop)
	producers.Wait()

	if recorder.saw(unsigned.Value) || recorder.saw(forged.Value) {
		t.Error("the handler received an unsigned or forged record; verification must quarantine before dispatch")
	}

	if got := causes[streaming.DLQCauseSignatureMissing]; !bytes.Equal(got, unsigned.Value) {
		t.Errorf("signature_missing entry carries %q; want the unsigned record %q", got, unsigned.Value)
	}

	if got := causes[streaming.DLQCauseSignatureInvalid]; !bytes.Equal(got, forged.Value) {
		t.Errorf("signature_invalid entry carries %q; want the forged record %q", got, forged.Value)
	}
}

// awaitDLQCauses reads topic from the start until an entry of every wanted
// cause kind has landed, returning the payload of the first entry per kind.
func awaitDLQCauses(t *testing.T, cluster *kfake.Cluster, topic string, wanted ...string) map[string][]byte {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("DLQ reader init: %v", err)
	}

	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dispatchWaitBudget)
	defer cancel()

	found := map[string][]byte{}

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("DLQ %s holds cause kinds %v within %s; want %v", topic, keysOf(found), dispatchWaitBudget, wanted)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			kind := streaming.ParseDiscardRecord(rec.Headers, rec.Value).CauseKind
			if _, seen := found[kind]; !seen {
				found[kind] = rec.Value
			}
		})

		complete := true

		for _, kind := range wanted {
			if _, ok := found[kind]; !ok {
				complete = false
			}
		}

		if complete {
			return found
		}
	}
}

func keysOf(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
