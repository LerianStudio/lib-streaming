package streamingtest

import (
	"crypto/sha256"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// Signed-record helpers: build the Kafka records a consumer that requires
// signatures meets on the wire, so a consuming service can test its own
// wiring (ring, expected sources, DLQ handling) against records signed,
// unsigned or forged exactly as the library's producer would produce them.
// Feed them to a kfake cluster with ProduceSync, or pass Headers and Value to
// code that reads records directly.

// SigningKey returns a key for id bound to source with a deterministic secret
// derived from both, so two processes (a producer and a consumer under test)
// agree on it without sharing state. The secret is public by construction:
// TEST USE ONLY, never in a deployed service.
func SigningKey(id, source string) streaming.SigningKey {
	sum := sha256.Sum256([]byte("lib-streaming/streamingtest\x00" + id + "\x00" + source))

	return streaming.SigningKey{ID: id, Source: source, Secret: streaming.SigningSecret(sum[:])}
}

// Keyring returns a keyring holding keys, failing the test when the library
// refuses them.
func Keyring(t testing.TB, keys ...streaming.SigningKey) *streaming.Keyring {
	t.Helper()

	ring, err := streaming.NewKeyring(keys...)
	if err != nil {
		t.Fatalf("streamingtest.Keyring: %v", err)
	}

	return ring
}

// SignedRecord returns event as a record on topic, signed by key at the
// current instant the way a producer holding key signs a publish. The event
// defaults a producer applies (ce-id, ce-time, schema version, content type)
// are filled in; an empty event.Source becomes key.Source. A producer can
// only sign for its own source, so an event.Source other than key.Source
// fails the test. The record value is a copy of event.Payload and the record
// key is the event's default partition key.
func SignedRecord(t testing.TB, topic string, key streaming.SigningKey, event streaming.Event) *kgo.Record {
	t.Helper()

	if event.Source == "" {
		event.Source = key.Source
	}

	signer, err := envelopesig.NewSigner(Keyring(t, key), key.ID, event.Source)
	if err != nil {
		t.Fatalf("streamingtest.SignedRecord: %v", err)
	}

	event.ApplyDefaults()

	return record(topic, event, signer.Sign(cloudevents.BuildTransportHeaders(event), event.Payload))
}

// UnsignedRecord returns event as a record on topic with the CloudEvents
// headers a producer without a signing key writes. A consumer that requires
// signatures quarantines it as signature_missing.
func UnsignedRecord(t testing.TB, topic string, event streaming.Event) *kgo.Record {
	t.Helper()

	event.ApplyDefaults()

	return record(topic, event, cloudevents.BuildTransportHeaders(event))
}

// ForgedRecord returns a record carrying SignedRecord's headers, signature
// included, over a body that differs from the one signed: the payload with
// one trailing space, which leaves a JSON payload valid, so the codec accepts
// it and only the signature gives the forgery away. A consumer that requires
// signatures quarantines it as signature_invalid.
func ForgedRecord(t testing.TB, topic string, key streaming.SigningKey, event streaming.Event) *kgo.Record {
	t.Helper()

	rec := SignedRecord(t, topic, key, event)
	rec.Value = append(rec.Value, ' ')

	return rec
}

func record(topic string, event streaming.Event, headers []transport.Header) *kgo.Record {
	out := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}

	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(event.PartitionKey()),
		Headers: out,
		Value:   append([]byte(nil), event.Payload...),
	}
}
